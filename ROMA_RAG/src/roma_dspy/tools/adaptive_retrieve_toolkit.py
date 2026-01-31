"""
Adaptive Retrieve Toolkit

Module 2: 强制模式切换（rag / web / hybrid）
负责编排 RAGFlow + Web Search 的检索，并产出统一的 RetrieveResult。

Module 5: MLflow 深度集成 - 记录决策过程、检索结果、推理链路
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import contextmanager, nullcontext
from typing import Any, Dict, List, Optional, Union

import dspy

from roma_dspy.tools.base.base import BaseToolkit
from roma_dspy.types.retrieve_result import (
    DecisionType,
    RetrieveContext,
    RetrieveDebugInfo,
    RetrieveResult,
    SourceType,
)

# MLflow integration (optional)
try:
    import mlflow
    from mlflow.tracing.fluent import start_span as mlflow_start_span
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    mlflow = None
    mlflow_start_span = None


class AdaptiveRetrieveToolkit(BaseToolkit):
    """
    自适应检索编排 Toolkit（Module 2）

    支持模式：
    - mode="rag": 仅 RAGFlow 内部知识库
    - mode="web": 仅 Exa Web 搜索
    - mode="hybrid": RAGFlow + Exa 混合
    - mode="auto": 自适应路由（根据置信度自动选择）

    配置参数（toolkit_config）:
        # RAGFlow 配置
        ragflow_toolkit_instance: RAGFlowToolkit 实例（可选）
        ragflow_config: dict - RAGFlow 配置（api_url, api_key, kb_id, timeout）
        
        # Web 搜索配置（使用 MCP Exa Toolkit）
        web_toolkit_instance: MCPToolkit 实例（可选）
        web_toolkit_config: dict - MCP Exa 配置（server_name, server_type, url, headers）
        web_tool_method: str - MCP 工具方法名（默认 "web_search_exa"）
        web_tool_kwargs: dict - 额外的搜索参数
        
        # 通用配置
        top_n_default: int - 默认返回结果数（默认 10）
    """

    def _setup_dependencies(self) -> None:
        """Setup RAGFlow and Exa MCP toolkits."""
        # 1) RAGFlow toolkit
        self.ragflow_toolkit = self.config.get("ragflow_toolkit_instance")
        if self.ragflow_toolkit is None:
            ragflow_config = self.config.get("ragflow_config", {})
            if not ragflow_config:
                ragflow_config = {
                    key: self.config.get(key)
                    for key in ("api_url", "api_key", "kb_id", "timeout")
                    if self.config.get(key) is not None
                }
            from roma_dspy.tools.ragflow_toolkit import RAGFlowToolkit
            self.ragflow_toolkit = RAGFlowToolkit(enabled=True, **ragflow_config)

        # 2) Web search toolkit (MCP Exa)
        self.web_toolkit = self.config.get("web_toolkit_instance")
        if self.web_toolkit is None:
            web_toolkit_config = self.config.get("web_toolkit_config")
            if web_toolkit_config:
                from roma_dspy.tools.mcp.toolkit import MCPToolkit
                # 确保显式传递 include_tools 和 exclude_tools（即使为 None）
                # 这样可以确保 BaseToolkit.__init__ 正确设置这些属性
                toolkit_kwargs = dict(web_toolkit_config)  # 复制配置，避免修改原始字典
                toolkit_kwargs.setdefault("enabled", True)
                toolkit_kwargs.setdefault("include_tools", None)
                toolkit_kwargs.setdefault("exclude_tools", None)
                self.web_toolkit = MCPToolkit(**toolkit_kwargs)
        
        self.web_tool_method = self.config.get("web_tool_method", "web_search_exa")
        self.web_tool_kwargs = self.config.get("web_tool_kwargs", {})

    def _initialize_tools(self) -> None:
        """Initialize default parameters."""
        self.top_n_default = int(self.config.get("top_n_default", 10))
        
        # MLflow integration flag
        self.mlflow_enabled = MLFLOW_AVAILABLE and self.config.get("mlflow_logging", True)
        
        # 用于防止并发重新初始化 MCP 连接的锁
        self._mcp_init_lock = asyncio.Lock()
    
    def _register_all_tools(self) -> None:
        """
        Override to register search_adaptive as a dspy.Tool with explicit schema.
        
        This ensures DSPy has strict type information, preventing parameter mismatches.
        """
        # Create dspy.Tool with explicit schema (like MCP tools)
        search_tool = dspy.Tool(
            func=self.search_adaptive_impl,
            name="search_adaptive",
            desc=self.__class__.search_adaptive.__doc__ or "Adaptive search tool",
            args={
                "query": {
                    "type": "string",
                    "description": "Search query (single string)"
                },
                "queries": {
                    "type": ["string", "array"],
                    "items": {"type": "string"},
                    "description": "Legacy alias for 'query'. If provided, will be treated as the search query."
                },
                "mode": {
                    "type": "string", 
                    "enum": ["auto", "rag", "web", "hybrid"],
                    "description": "Retrieval mode",
                    "default": "auto"
                },
                "top_n": {
                    "type": "integer",
                    "description": "Number of results to return",
                    "default": 8
                }
            },
            arg_types={
                "query": str,
                "mode": str,
                "top_n": int
            },
            arg_desc={
                "query": "The search query (in Chinese or English)",
                "queries": "Alternative for query (for compatibility)",
                "mode": "Retrieval mode: auto, rag, web, or hybrid",
                "top_n": "Maximum number of results to return"
            }
        )
        
        # Wrap with metrics tracking
        from roma_dspy.tools.metrics.decorators import track_tool_invocation
        wrapped_func = track_tool_invocation(
            tool_name="search_adaptive",
            toolkit_class=self.__class__.__name__
        )(search_tool.func)
        
        # Replace func in Tool object
        object.__setattr__(search_tool, "func", wrapped_func)
        
        # Register the Tool object (not just the function)
        self._tools["search_adaptive"] = search_tool
        self.log_debug("Registered search_adaptive as dspy.Tool with explicit schema")

    def get_available_tool_names(self) -> set[str]:
        """Return available tool names for ROMA DSPy integration."""
        return {"search_adaptive"}

    def _calculate_confidence(self, chunks: List[Dict[str, Any]]) -> float:
        """
        Module 3: 改进的置信度计算（V0.5）
        直接使用 top1_score，并考虑结果数量。
        """
        if not chunks:
            return 0.0

        top1_score = chunks[0].get("score", 0.0)
        result_count = len(chunks)

        if result_count == 0:
            return 0.0
        elif result_count == 1:
            # 只有一条结果时，打8折，最高给0.65，避免单条高分误判
            return min(top1_score * 0.8, 0.65)
        else:
            # 多条结果时，直接使用 top1_score，但上限为1.0
            return min(top1_score, 1.0)

    def _format_ragflow_contexts(self, chunks: List[Dict[str, Any]]) -> List[RetrieveContext]:
        """Convert RAGFlow chunks to RetrieveContext list."""
        raw_contexts = self.ragflow_toolkit.format_ragflow_contexts(chunks)
        contexts: List[RetrieveContext] = []
        for ctx in raw_contexts:
            contexts.append(
                RetrieveContext(
                    text=ctx["text"],
                    source=SourceType.RAGFLOW,
                    url=ctx["url"],
                    title=ctx.get("title", ""),
                    score=ctx.get("score", 0.0),
                )
            )
        return contexts

    def _format_web_contexts(self, web_results: Dict[str, Any]) -> List[RetrieveContext]:
        """转换 MCP Exa 搜索结果为 RetrieveContext 列表"""
        results = web_results.get("results", [])
        if not isinstance(results, list):
            return []

        contexts = []
        for item in results:
            text = (item.get("summary") or 
                   item.get("autopromptString") or 
                   (item.get("highlights", [])[0] if item.get("highlights") else "") or
                   item.get("text", ""))
            
            if text:
                contexts.append(RetrieveContext(
                    text=text,
                    source=SourceType.EXA,
                    url=item.get("url", ""),
                    title=item.get("title", ""),
                    score=item.get("score", item.get("autopromptScore", 1.0)),
                ))
        
        return contexts


    async def _call_web_search_async(self, query: str, top_n: int) -> Dict[str, Any]:
        """调用 MCP Exa web 搜索
        
        Exa API 参数说明（MCP Server）：
        - type: 搜索类型，"deep" 返回详细内容和 Summary（关键！）
        - numResults: 返回结果数量
        - contents: 内容配置对象（包含 text, summary, highlights 等）
        - useAutoprompt: 启用 LLM 优化查询
        
        注意：Exa MCP Server 的参数格式与直接 API 调用略有不同。
        """
        if not self.web_toolkit:
            raise RuntimeError("MCP web toolkit not configured")
        
        # 初始化 MCP 连接
        if hasattr(self.web_toolkit, 'initialize'):
            await self.web_toolkit.initialize()
        
        enabled_tools = self.web_toolkit.get_enabled_tools()
        
        # 如果工具列表为空，说明 MCP 连接有问题
        if not enabled_tools:
            raise RuntimeError(
                f"MCP toolkit '{self.web_toolkit._server_name if hasattr(self.web_toolkit, '_server_name') else 'unknown'}' "
                f"has no tools. Check Exa MCP server connection and API key (EXA_API_KEY environment variable)."
            )
        
        # 获取工具对象
        tool = enabled_tools.get(self.web_tool_method)
        
        if not tool:
            # 尝试常见的 Exa 工具名称
            for name in ['web_search_exa', 'search_exa', 'exa_search']:
                if name in enabled_tools:
                    tool = enabled_tools[name]
                    self.web_tool_method = name
                    self.log_debug(f"[Web Search] Found tool with name: {name}")
                    break
        
        if not tool:
            raise AttributeError(
                f"MCP tool '{self.web_tool_method}' not found. "
                f"Available: {list(enabled_tools.keys())}. "
                f"Check Exa MCP server configuration."
            )
        
        # 构建调用参数
        call_kwargs = {"query": query, "numResults": top_n, **self.web_tool_kwargs}
        
        # 关键：设置 type="deep" 以获取详细内容和 Summary
        # Exa 的 "deep" 搜索类型会返回完整的 summary 字段
        if "type" not in call_kwargs:
            call_kwargs["type"] = "deep"
        
        # 确保启用内容提取参数
        # Exa MCP Server 接受顶级参数格式
        call_kwargs.setdefault("text", True)
        
        # 记录调用参数（调试用）
        self.log_debug(f"[Web Search] Calling {self.web_tool_method} with params: {list(call_kwargs.keys())}")
        
        try:
            if hasattr(tool, 'func') and asyncio.iscoroutinefunction(tool.func):
                result = await tool.func(**call_kwargs)
            elif asyncio.iscoroutinefunction(tool):
                result = await tool(**call_kwargs)
            else:
                result = tool(**call_kwargs)
            
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    # Fallback: treat raw text as a single result
                    return {
                        "results": [{
                            "text": result,
                            "summary": result,  # Use full text as summary
                            "title": "Raw Search Result",
                            "score": 1.0
                        }]
                    }
            
            return result if isinstance(result, dict) else {"results": []}
        
        except Exception as e:
            self.log_error(f"[Web Search] Error: {e}")
            return {"results": []}


    @contextmanager
    def _trace_span(self, name: str, inputs: Optional[Dict[str, Any]] = None):
        """创建 MLflow span 的上下文管理器（增强版）"""
        if self.mlflow_enabled and mlflow_start_span:
            # Correctly use the context manager
            with mlflow_start_span(name=name) as span:
                try:
                    if inputs:
                        span.set_inputs(inputs)
                    yield span
                except Exception as e:
                    # 记录错误但不要重新抛出，以免打断主流程
                    self.log_debug(f"[MLflow] Error in span '{name}': {e}")
                    # 注意：MLflow span 的 set_status 不兼容 OpenTelemetry Status
                    # 使用 set_attribute 记录错误状态
                    if span:
                        try:
                            span.set_attributes({"error": True, "error.message": str(e)[:500]})
                        except Exception:
                            pass  # 忽略属性设置错误
                    raise
        else:
            yield None

    def _log_span_results(self, span: Any, results: List[Any], source_type: str):
        """统一记录检索结果到 Span，确保 MLflow UI 可视化效果
        
        格式化输出，防止大量文本导致 UI 响应缓慢，同时保留关键元数据。
        """
        if not span or not results:
            return
        
        display_results = []
        for r in results[:5]:  # 仅展示前5条作为摘要
            if source_type == "ragflow":
                display_results.append({
                    "text": r.get("content", "")[:200] + "...",
                    "score": r.get("score", 0.0),
                    "doc": r.get("document_name", "")
                })
            else:  # EXA / Web
                display_results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "score": r.get("score", 0.0)
                })
        
        try:
            span.set_outputs({
                "source": source_type,
                "count": len(results),
                "top_results": display_results
            })
            span.set_attributes({
                "roma.source": source_type,
                "roma.result_count": len(results),
                "roma.top1_score": results[0].get("score", 0.0) if results else 0.0
            })
        except Exception as e:
            self.log_debug(f"[MLflow] Failed to set outputs for span: {e}")

    def _retrieve_rag_step(self, query: str, top_n: int, span_name: str = "ragflow_retrieve") -> List[Dict[str, Any]]:
        """执行 RAGFlow 检索并自动记录追踪信息"""
        with self._trace_span(span_name, {"query": query, "top_n": top_n}) as span:
            chunks = self.ragflow_toolkit.retrieve(query, top_n=top_n)
            self._log_span_results(span, chunks, "ragflow")
            return chunks

    async def _retrieve_web_step(self, query: str, top_n: int, span_name: str = "web_search_exa") -> Dict[str, Any]:
        """执行 Web 检索并自动记录追踪信息"""
        with self._trace_span(span_name, {"query": query, "top_n": top_n}) as span:
            web_results = await self._call_web_search_async(query, top_n=top_n)
            exa_results = web_results.get("results", [])
            self._log_span_results(span, exa_results, "exa")
            return web_results

    async def adaptive_retrieve_async(
        self,
        query: str,
        mode: str = "rag",
        top_n: Optional[int] = None,
    ) -> RetrieveResult:
        """自适应检索（异步版本）：支持 rag/web/hybrid/auto 四种模式
        
        优化后的版本：将决策逻辑与追踪逻辑分离，使用私有方法处理各个检索阶段。
        """
        start = time.time()
        top_n = top_n or self.top_n_default
        
        # 获取当前工具调用的活跃 Span（由 DSPy 自动开启或用户开启）
        active_span = mlflow.get_current_active_span() if self.mlflow_enabled else None

        rag_chunks: List[Dict[str, Any]] = []
        web_results: Dict[str, Any] = {"results": []}
        contexts: List[RetrieveContext] = []
        decision = DecisionType.RAG
        confidence = 0.0
        web_triggered = False
        decision_reason = f"mode={mode}"

        if mode == "rag":
            rag_chunks = self._retrieve_rag_step(query, top_n)
            contexts = self._format_ragflow_contexts(rag_chunks)
            decision = DecisionType.RAG
            confidence = self._calculate_confidence(rag_chunks)
            decision_reason = "mode=rag (forced)"

        elif mode == "web":
            web_results = await self._retrieve_web_step(query, top_n)
            contexts = self._format_web_contexts(web_results)
            decision = DecisionType.WEB
            confidence = 1.0
            web_triggered = True
            decision_reason = "mode=web (forced)"

        elif mode == "hybrid":
            rag_top_n = max(1, top_n // 2)
            web_top_n = max(1, top_n - rag_top_n)
            
            rag_chunks = self._retrieve_rag_step(query, rag_top_n)
            web_results = await self._retrieve_web_step(query, web_top_n)
            
            contexts = self._format_ragflow_contexts(rag_chunks) + self._format_web_contexts(web_results)
            decision = DecisionType.HYBRID
            confidence = self._calculate_confidence(rag_chunks)
            web_triggered = True
            decision_reason = "mode=hybrid (forced)"

        elif mode == "auto":
            # Step 1: RAGFlow 初步检索
            rag_chunks = self._retrieve_rag_step(query, top_n, span_name="ragflow_initial_retrieve")
            confidence = self._calculate_confidence(rag_chunks)

            if confidence >= 0.7:
                # 高置信度 -> 仅用 RAG
                contexts = self._format_ragflow_contexts(rag_chunks)
                decision = DecisionType.RAG
                decision_reason = f"High confidence ({confidence:.2f} >= 0.7), RAGFlow sufficient"
            elif confidence >= 0.4:
                # 中等置信度 -> Hybrid
                rag_top_n = max(1, top_n // 2)
                web_top_n = max(1, top_n - rag_top_n)
                
                # 优化：直接复用第一次 RAG 检索的结果，进行切片，避免二次调用
                rag_chunks_hybrid = rag_chunks[:rag_top_n]
                web_results = await self._retrieve_web_step(query, web_top_n, span_name="web_search_exa_hybrid")
                
                contexts = self._format_ragflow_contexts(rag_chunks_hybrid) + self._format_web_contexts(web_results)
                decision = DecisionType.HYBRID
                web_triggered = True
                decision_reason = f"Medium confidence ({confidence:.2f} in [0.4, 0.7)), using Hybrid"
            else:
                # 低置信度 -> 仅用 Web
                web_results = await self._retrieve_web_step(query, top_n, span_name="web_search_exa_only")
                contexts = self._format_web_contexts(web_results)
                decision = DecisionType.WEB
                web_triggered = True
                decision_reason = f"Low confidence ({confidence:.2f} < 0.4), using Web"

        else:
            raise ValueError(f"Invalid mode '{mode}'. Use 'rag', 'web', 'hybrid', or 'auto'.")

        # 将关键决策元数据注入到当前活跃 Span 的 Attributes 中，无需额外开决策 Span
        if active_span:
            active_span.set_attributes({
                "roma.decision": decision.value,
                "roma.confidence": confidence,
                "roma.web_triggered": web_triggered,
                "roma.reason": decision_reason
            })

        # 注册 Sources 到 ExecutionContext (带外传递给 Executor)
        from roma_dspy.core.context import ExecutionContext
        sources_list = [ctx.url for ctx in contexts if ctx.url]
        ExecutionContext.add_sources(sources_list)

        duration_ms = int((time.time() - start) * 1000)
        debug = RetrieveDebugInfo(
            trigger_reason=decision_reason,
            rag_top1_score=rag_chunks[0].get("score", 0.0) if rag_chunks else 0.0,
            rag_result_count=len(rag_chunks),
            web_triggered=web_triggered,
            duration_ms=duration_ms,
        )
        
        return RetrieveResult(
            query=query,
            decision=decision,
            confidence=confidence,
            contexts=contexts,
            sources=[ctx.url for ctx in contexts],
            debug=debug,
        )

    def adaptive_retrieve(
        self,
        query: str,
        mode: str = "rag",
        top_n: Optional[int] = None,
    ) -> RetrieveResult:
        """
        自适应检索（同步包装器）
        
        注意: 这是 adaptive_retrieve_async 的同步包装器。
        如果已在异步上下文中，请直接使用 await adaptive_retrieve_async()。
        """
        return asyncio.run(self.adaptive_retrieve_async(query, mode, top_n))

    async def adaptive_retrieve_json_async(
        self,
        query: str,
        mode: str = "rag",
        top_n: Optional[int] = None,
    ) -> str:
        """返回 JSON 格式的检索结果（异步）"""
        result = await self.adaptive_retrieve_async(query, mode=mode, top_n=top_n)
        return json.dumps(result.to_dict(), ensure_ascii=False)

    def adaptive_retrieve_json(
        self,
        query: str,
        mode: str = "rag",
        top_n: Optional[int] = None,
    ) -> str:
        """返回 JSON 格式的检索结果（同步包装器）"""
        return asyncio.run(self.adaptive_retrieve_json_async(query, mode, top_n))

    async def search_adaptive_impl(
        self,
        query: Optional[str] = None,
        queries: Optional[Union[str, List[str]]] = None,
        mode: str = "auto",
        top_n: int = 8
    ) -> str:
        """
        Adaptive search implementation (异步版本)
        
        内部实现函数，通过 dspy.Tool 包装后对外暴露。
        
        Args:
            query: 搜索查询（单个字符串，语言应与 goal 一致）
            queries: 兼容性参数（当模型错误地输出 queries 时使用）
            mode: 检索模式 ("auto", "rag", "web", "hybrid")
            top_n: 返回结果数量
        """
        # 1. 参数归一化：处理模型可能输出 'queries' 而不是 'query' 的情况
        final_query = query
        if not final_query and queries:
            if isinstance(queries, list):
                # 如果是列表，合并为字符串进行搜索
                final_query = " ".join([str(q) for q in queries if q])
            else:
                final_query = str(queries)
        
        if not final_query:
            return "[Error] Please provide a valid search query in the 'query' argument."

        try:
            result = await self.adaptive_retrieve_async(query=final_query, mode=mode, top_n=top_n)
            
            # 格式化为 Markdown 字符串
            formatted_text = self._format_contexts_for_llm(result)
            
            return formatted_text
        except Exception as e:
            error_msg = f"### [检索失败]\n> {str(e)}"
            self.log_error(f"Adaptive search error: {e}")
            return error_msg
    
    def _format_contexts_for_llm(self, result: RetrieveResult) -> str:
        """
        优化后的格式化函数：支持混合模式的分组展示，保留 Web 搜索的 Markdown 格式。
        """
        if not result.contexts:
            return f"[检索结果为空]\n查询: {result.query}\n决策: {result.decision.value}"
        
        # 分离不同来源的结果
        rag_contexts = [c for c in result.contexts if c.source == SourceType.RAGFLOW]
        web_contexts = [c for c in result.contexts if c.source == SourceType.EXA]
        
        parts = []
        
        # 1. 头部决策信息
        parts.append(f"### 检索报告")
        parts.append(f"- **决策模式**: {result.decision.value.upper()}")
        parts.append(f"- **置信度**: {result.confidence:.2f}")
        parts.append(f"- **触发原因**: {result.debug.trigger_reason}")
        parts.append("")

        # 2. 内部知识库结果 (RAG) - 保持紧凑格式
        if rag_contexts:
            parts.append(f"### 📚 内部知识库 ({len(rag_contexts)} 条)")
            for i, ctx in enumerate(rag_contexts, 1):
                # RAG 结果通常较短，适合用引用块或列表
                parts.append(f"#### [{i}] {ctx.title or '未命名文档'}")
                parts.append(f"> **相关度**: {ctx.score:.2f}")
                parts.append(f"> {ctx.text.strip()}")
                if ctx.url:
                    parts.append(f"> *来源: {ctx.url}*")
                parts.append("")
        
        # 3. 外部搜索结果 (Web) - 恢复 Markdown 格式
        if web_contexts:
            parts.append(f"### 🌐 互联网搜索 ({len(web_contexts)} 条)")
            for i, ctx in enumerate(web_contexts, 1):
                # Web 结果通常较长且有结构，直接渲染 Markdown
                title = ctx.title or "无标题网页"
                url = ctx.url
                score = ctx.score
                content = ctx.text.strip()
                
                parts.append(f"#### [{i}] [{title}]({url})")
                parts.append(f"**Relevance**: {score:.2f}")
                parts.append("")
                # 直接展示内容，不加缩进，保留 Exa 可能返回的 Markdown 格式
                parts.append(content)
                parts.append(f"\n[Source Link]({url})")
                parts.append("---") # 分隔线
        
        # 4. 汇总引用列表 (给 LLM 用于生成引用标记)
        # 优化：为 RAGFlow 来源生成类似参考文献的格式，提供更丰富的上下文
        if result.contexts:
            parts.append("### 🔗 引用链接汇总 (References)")
            
            # 使用字典去重，保持顺序 (Python 3.7+ 字典有序)
            # 这里的目的是给 LLM 提供一个清晰的 Reference List，使其能将 URL 与文档标题对应起来
            seen_urls = set()
            ref_idx = 1
            
            for ctx in result.contexts:
                if ctx.url in seen_urls:
                    continue
                seen_urls.add(ctx.url)
                
                if ctx.source == SourceType.RAGFLOW:
                    # RAGFlow: 显示文档标题和 URI，模拟学术引用格式
                    # 格式: [i] 《Title》 (URI: <URL>)
                    title = ctx.title or "未命名文档"
                    # 尝试从 metadata 获取更多信息（如页码/位置）
                    # position = ctx.to_dict().get("metadata", {}).get("position")
                    
                    parts.append(f"[{ref_idx}] 《{title}》 (Source: {ctx.url})")
                else:
                    # Web: 显示 URL
                    parts.append(f"[{ref_idx}] {ctx.url}")
                
                ref_idx += 1

        return "\n".join(parts)

    def search_adaptive(
        self,
        query: Optional[str] = None,
        queries: Optional[Union[str, List[str]]] = None,
        mode: str = "auto",
        top_n: int = 8
    ) -> str:
        """Adaptive search combining internal knowledge base (RAGFlow) and external web search.

        This tool performs information retrieval by intelligently routing between internal and external sources.
        It supports automatic routing ("auto"), forced internal ("rag"), forced external ("web"), or hybrid ("hybrid") modes.

        Args:
            query: The search query.
            queries: Legacy alias for query.
            mode: Retrieval mode ("auto", "rag", "web", "hybrid"). Defaults to "auto".
            top_n: Number of results to return. Defaults to 8.

        Returns:
            Formatted string with retrieval decision, results, and citations.
        """
        return asyncio.run(self.search_adaptive_impl(query=query, queries=queries, mode=mode, top_n=top_n))

