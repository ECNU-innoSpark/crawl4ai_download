# ROMA × RAGFlow × CRAG 自适应检索集成：分步实施手册

> **目标**：在 Deep Research 的 `RETRIEVE` 阶段，先查 RAGFlow（内部知识库），再根据质量评估自适应选择：只用内部、只用 Web、或二者融合。
>
> **本文定位**：可直接开工的工程手册，按模块分步骤，每步都有验收标准和测试方法。
>
> **最新更新（2026-01）**：路线图已调整，优先完成核心自适应路由功能和 ROMA 集成，优化细节（排序/压缩）移至后期迭代。

---

## 📋 快速开始：你需要知道的 3 件事

### 1. 当前 ROMA v2 已有什么？

- ✅ Exa Web Search 已通过 MCP 接入（`config/profiles/deep_research.yaml`）
- ✅ RETRIEVE 节点用 ReAct 策略调用工具
- ✅ DR prompt 强制要求内联引用：`[Source: URL]`

### 2. 你要加什么？

- 新增 **RAGFlow Toolkit** 调用内部知识库
- 新增 **自适应路由逻辑**：先 RAG 后评估，决定要不要触发 Web
- 新增 **ROMA 集成**：将 toolkit 注册到 Deep Research 流程
- （可选）**Hybrid 融合优化**：排序、压缩等细节优化

### 3. 核心设计决策（先定死这 3 点，后面才能动手）

| 决策点 | 建议方案 | 说明 |
|--------|----------|------|
| **CRAG 放哪？** | ROMA 的 RETRIEVE Toolkit | 路由决策需要任务上下文（预算、是否允许出网） |
| **RAGFlow 接入方式** | 自定义 Toolkit 直连 API | 比 MCP 包装少一层，更易调试 |
| **内部引用方案** | `ragflow://kb/<kb_id>/doc/<doc_id>#chunk=<chunk_id>` | 可追溯，日志可还原到具体 chunk |

---

## 🎯 实施路线图（调整后，聚焦核心功能）

> **调整说明**：排序和压缩等优化细节移至 Module 6（持续优化），优先完成核心自适应路由和 ROMA 集成。

### 模块概览（6 个模块，前 5 个是核心）

| 模块 | 名称 | 状态 | 耗时 | 核心产出 |
|------|------|------|------|---------|
| **Module 0** | 准备工作 | ✅ 完成 | 1 天 | `RetrieveResult` 数据契约 |
| **Module 1** | RAGFlow API | ✅ 完成 | 2-3 天 | `RAGFlowToolkit` |
| **Module 2** | 强制模式切换 | ✅ 完成 | 1-2 天 | `rag/web/hybrid` 三种模式 |
| **Module 3** | 自适应路由 | ✅ 完成 | 2-3 天 | `mode=auto` 智能决策 |
| **Module 4** | ROMA 集成 | ✅ 完成 | 1-2 天 | 注册到 Deep Research |
| **Module 5** | MLflow集成 | ✅ 完成 | 2 天 | 决策记录/检索结果深度集成到MLflow |
| **Module 6** | 优化迭代 | ⏳ 可选 | 持续 | Score归一化/测试增强/评估器升级 |

---

### Module 0：准备工作（1 天，定契约）

**目标**：把"检索结果的输出格式"定死，后续所有模块都按这个契约对接。

#### Step 0.1：定义统一输出格式 `RetrieveResult`

创建一个数据类/dict 规范（可以先写在注释里，后续再正式实现）：

```python
# 概念契约（可先用 dict，后续改 dataclass）
RetrieveResult = {
    "query": str,                  # 原始查询
    "decision": str,               # "rag" | "web" | "hybrid"
    "confidence": float,           # 0-1，检索质量分数
    "contexts": [                  # 证据数组
        {
            "text": str,           # 证据正文
            "source": str,         # "ragflow" | "exa"
            "url": str,            # 可追溯引用（URL 或 ragflow://...）
            "title": str,          # 可选：标题
            "score": float,        # 可选：检索分数
        }
    ],
    "sources": [str],              # 扁平 URL 列表（给 Aggregator 用）
    "debug": {                     # 调试信息
        "trigger_reason": str,     # 为什么选这个 decision
        "rag_top1_score": float,   # RAG 最高分
        "rag_result_count": int,   # RAG 返回数
        "web_triggered": bool,     # 是否触发了 Web
        "duration_ms": int,        # 总耗时
    }
}
```

**验收标准**：
- [ ] 团队达成共识：所有检索相关代码都输出这个格式
- [ ] 在代码注释或文档里写下这个契约

**测试方法**：手写一个 mock `RetrieveResult`，确认 DR 的 THINK/WRITE 节点能正常消费 `contexts` 和 `sources`。

---

#### Step 0.2：确定 RAGFlow 内部引用 URI 规范

**要决定的**：
1. RAGFlow 导入的文档是否保留了 `original_url`？
   - 是 → 优先用真实 URL
   - 否 → 用 `ragflow://kb/<kb_id>/doc/<doc_id>#chunk=<chunk_id>`
2. 是否需要后续在 UI 里把 `ragflow://...` 渲染成可点击链接？
   - 暂时不需要 → 只用于日志追溯
   - 需要 → 预留映射机制（Phase 后期再做）

**验收标准**：
- [ ] 在文档里明确 URI 格式
- [ ] 确认 Aggregator 和最终报告能正常写入 `[Source: ragflow://...]`

---

#### Step 0.3：设定成本与时延预算

写死这些约束（后续 Phase 2 会用到）：

- 每个 RETRIEVE 子任务最多触发 **1 次** Web Search
- Web Search 超时阈值：**8 秒**
- RAGFlow 超时阈值：**3 秒**
- 单次检索最终返回 contexts 数量：**8–12 条**（内部+外部合计）

**验收标准**：
- [ ] 在配置文件或代码常量里写死这些值

---

### Module 1：RAGFlow API 调用（2-3 天，跑通内部检索）

**目标**：能在 ROMA 的 RETRIEVE 节点里调 RAGFlow，拿到结构化 chunk 列表。

#### Step 1.1：实现 `RAGFlowToolkit`（或测试脚本）

**要做的**：
1. 创建 `src/roma_dspy/tools/ragflow_toolkit.py`（或先写个独立测试脚本）
2. 实现 `retrieve(query: str, top_n: int = 10) -> List[Dict]`
3. 调用 RAGFlow `/retrieval` API
4. 处理返回的 renamed keys（见附录 A.1）

**最小可用代码框架**：

```python
# src/roma_dspy/tools/ragflow_toolkit.py
import requests
from typing import List, Dict

class RAGFlowToolkit:
    def __init__(self, api_url: str, api_key: str, kb_id: str):
        self.api_url = api_url
        self.api_key = api_key
        self.kb_id = kb_id
    
    def retrieve(self, query: str, top_n: int = 10) -> List[Dict]:
        """
        调用 RAGFlow /retrieval API
        返回格式化后的 chunks
        """
        response = requests.post(
            f"{self.api_url}/retrieval",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "question": query,
                "datasets": [self.kb_id],
                "top_n": top_n,
            },
            timeout=3,
        )
        response.raise_for_status()
        data = response.json()
        
        # 按附录 A.1 的映射提取字段
        chunks = []
        for chunk in data.get("chunks", []):
            chunks.append({
                "chunk_id": chunk["id"],
                "doc_id": chunk["document_id"],
                "kb_id": chunk["dataset_id"],
                "text": chunk["content"],
                "score": chunk["similarity"],
                "title": chunk.get("document_keyword", ""),
                "keywords": chunk.get("important_keywords", []),
            })
        return chunks
```

**验收标准**：
- [ ] 能成功调用 RAGFlow API，返回 chunks 列表
- [ ] 返回的 chunk 包含：`chunk_id`, `doc_id`, `text`, `score`
- [ ] 超时/失败能正常抛异常

**测试方法**（写一个独立测试脚本）：

```python
# tests/test_ragflow_integration.py
from roma_dspy.tools.ragflow_toolkit import RAGFlowToolkit

toolkit = RAGFlowToolkit(
    api_url="http://your-ragflow-url",
    api_key="your-api-key",
    kb_id="your-kb-id",
)

# 测试 1：正常查询
chunks = toolkit.retrieve("AI 教育现状", top_n=5)
assert len(chunks) > 0
assert "text" in chunks[0]
assert "score" in chunks[0]
print("✅ Test 1 passed: 正常返回 chunks")

# 测试 2：空结果
chunks_empty = toolkit.retrieve("skdjfhaksjdfhksdjfh", top_n=5)
assert len(chunks_empty) == 0
print("✅ Test 2 passed: 空结果处理正常")

# 测试 3：超时处理
# （手动断网或设置一个不存在的 URL 测试）
```

---

#### Step 1.2：把 chunks 转成 `RetrieveResult.contexts` 格式

**要做的**：
1. 实现 `_format_ragflow_contexts(chunks) -> List[Dict]`
2. 按 Module 0 定义的 `contexts` 格式填充字段
3. 处理内部引用 URI：`ragflow://kb/<kb_id>/doc/<doc_id>#chunk=<chunk_id>`

**代码**：

```python
def _format_ragflow_contexts(self, chunks: List[Dict]) -> List[Dict]:
    """
    把 RAGFlow chunks 转成统一 contexts 格式
    """
    contexts = []
    for chunk in chunks:
        # 尝试从文档元数据获取 original_url（需要额外查询，暂时跳过）
        # 先用内部 URI
        url = f"ragflow://kb/{chunk['kb_id']}/doc/{chunk['doc_id']}#chunk={chunk['chunk_id']}"
        
        contexts.append({
            "text": chunk["text"],
            "source": "ragflow",
            "url": url,
            "title": chunk.get("title", ""),
            "score": chunk.get("score", 0.0),
        })
    return contexts
```

**验收标准**：
- [ ] 返回的 `contexts` 符合 Module 0 定义的格式
- [ ] `url` 字段是可解析的 `ragflow://...` URI

**测试方法**：

```python
# 接上面的测试
formatted = toolkit._format_ragflow_contexts(chunks)
assert formatted[0]["source"] == "ragflow"
assert formatted[0]["url"].startswith("ragflow://")
print("✅ Test 3 passed: contexts 格式化正确")
```

---

#### Step 1.3：注册到 ROMA Toolkit 系统（可选，先跳过也行）

如果你想让 DR 的 RETRIEVE 节点直接用，需要：
1. 在 `src/roma_dspy/tools/base/manager.py` 注册 `RAGFlowToolkit`
2. 在 `config/profiles/deep_research.yaml` 的 `executors.RETRIEVE.toolkits` 里加一项

**暂时可以跳过**：先用测试脚本验证 Module 1 能跑，Module 2 再正式接入。

---

### Module 2：强制模式切换（1-2 天，跑通三种模式）

**目标**：能手动指定 `decision` 参数，强制走 `rag` / `web` / `hybrid`，验证每种模式都能产出正确的 `RetrieveResult`。

#### Step 2.1：实现 `adaptive_retrieve` 函数框架

**要做的**：
1. 创建一个函数（或 Toolkit 方法）：`adaptive_retrieve(query, mode="auto")`
2. 当 `mode="rag"` 时：只调 RAGFlow
3. 当 `mode="web"` 时：只调 Exa
4. 当 `mode="hybrid"` 时：调两个，简单拼接（不做去重，下个 Step 再做）

**代码框架**：

```python
def adaptive_retrieve(
    query: str,
    mode: str = "auto",  # "rag" | "web" | "hybrid" | "auto"
    top_n: int = 10,
) -> Dict:
    """
    自适应检索主入口
    """
    import time
    start = time.time()
    
    if mode == "rag":
        # 只用 RAGFlow
        rag_chunks = ragflow_toolkit.retrieve(query, top_n=top_n)
        contexts = _format_ragflow_contexts(rag_chunks)
        decision = "rag"
        confidence = _calculate_confidence(rag_chunks)  # Step 2.2 实现
    
    elif mode == "web":
        # 只用 Exa
        web_results = exa_toolkit.search(query, num_results=top_n)
        contexts = _format_exa_contexts(web_results)  # Step 2.3 实现
        decision = "web"
        confidence = 1.0  # Web 默认高置信
    
    elif mode == "hybrid":
        # 两个都调
        rag_chunks = ragflow_toolkit.retrieve(query, top_n=top_n//2)
        web_results = exa_toolkit.search(query, num_results=top_n//2)
        contexts = (
            _format_ragflow_contexts(rag_chunks) +
            _format_exa_contexts(web_results)
        )
        decision = "hybrid"
        confidence = 0.5  # 暂时写死
    
    else:  # mode == "auto"
        # Module 3 再实现
        raise NotImplementedError("Auto mode 在 Module 3 实现")
    
    # 组装 RetrieveResult
    result = {
        "query": query,
        "decision": decision,
        "confidence": confidence,
        "contexts": contexts,
        "sources": [ctx["url"] for ctx in contexts],
        "debug": {
            "duration_ms": int((time.time() - start) * 1000),
        }
    }
    return result
```

**验收标准**：
- [ ] `mode="rag"` 能返回只有 RAGFlow 证据的 `RetrieveResult`
- [ ] `mode="web"` 能返回只有 Exa 证据的 `RetrieveResult`
- [ ] `mode="hybrid"` 能返回两种证据混合的 `RetrieveResult`

**测试方法**：

```python
# tests/test_adaptive_retrieve.py

# 测试 1：强制 RAG
result_rag = adaptive_retrieve("AI 教育", mode="rag", top_n=5)
assert result_rag["decision"] == "rag"
assert all(ctx["source"] == "ragflow" for ctx in result_rag["contexts"])
print("✅ Test 1: RAG 模式正常")

# 测试 2：强制 Web
result_web = adaptive_retrieve("AI 教育", mode="web", top_n=5)
assert result_web["decision"] == "web"
assert all(ctx["source"] == "exa" for ctx in result_web["contexts"])
print("✅ Test 2: Web 模式正常")

# 测试 3：强制 Hybrid
result_hybrid = adaptive_retrieve("AI 教育", mode="hybrid", top_n=10)
assert result_hybrid["decision"] == "hybrid"
sources = {ctx["source"] for ctx in result_hybrid["contexts"]}
assert "ragflow" in sources and "exa" in sources
print("✅ Test 3: Hybrid 模式正常")
```

---

#### Step 2.2：实现 `_calculate_confidence` 启发式（V0 版本）

**要做的**：
根据 RAGFlow 返回的信号，给出一个 0-1 的置信度分数。

**最简单的规则**（V0）：

```python
def _calculate_confidence(chunks: List[Dict]) -> float:
    """
    V0 启发式：只看 top1 分数和结果数量
    """
    if not chunks:
        return 0.0
    
    top1_score = chunks[0]["score"]
    result_count = len(chunks)
    
    # 简单规则
    if result_count == 0:
        return 0.0
    elif top1_score >= 0.8 and result_count >= 3:
        return 0.9  # 高质量
    elif top1_score >= 0.6 and result_count >= 2:
        return 0.6  # 中等
    else:
        return 0.3  # 低质量
```

**验收标准**：
- [ ] 能根据 chunks 返回一个 0-1 分数

---

#### Step 2.3：实现 `_format_exa_contexts`（Exa 结果格式化）

**要做的**：
把 Exa MCP 返回的搜索结果转成 `contexts` 格式。

**代码**（假设 Exa 返回格式是 `{"results": [{"url": ..., "title": ..., "text": ...}]}`）：

```python
def _format_exa_contexts(web_results: Dict) -> List[Dict]:
    """
    把 Exa 搜索结果转成统一 contexts 格式
    """
    contexts = []
    for item in web_results.get("results", []):
        contexts.append({
            "text": item.get("text", ""),
            "source": "exa",
            "url": item.get("url", ""),
            "title": item.get("title", ""),
            "score": item.get("score", 1.0),  # Exa 可能没有 score
        })
    return contexts
```

**验收标准**：
- [ ] 能把 Exa 结果转成 `contexts` 格式
- [ ] `source` 是 `"exa"`，`url` 是真实 URL

---

### Module 3：自适应路由（V0 启发式）（2-3 天）

**目标**：实现 `mode="auto"`，根据 RAGFlow 质量自动决定要不要触发 Web。

#### Step 3.1：实现路由决策逻辑

**要做的**：
在 `adaptive_retrieve` 的 `mode="auto"` 分支里，实现这个流程：

1. 先调 RAGFlow
2. 计算 `confidence`
3. 根据阈值决定 `decision`：
   - `confidence >= 0.7` → `rag`（只用内部）
   - `confidence >= 0.4` → `hybrid`（两个都要）
   - `confidence < 0.4` → `web`（只用外部）

**代码**：

```python
elif mode == "auto":
    # 1. 先调 RAGFlow
    rag_chunks = ragflow_toolkit.retrieve(query, top_n=top_n)
    confidence = _calculate_confidence(rag_chunks)
    
    # 2. 决策
    if confidence >= 0.7:
        # 高质量，只用 RAG
        contexts = _format_ragflow_contexts(rag_chunks)
        decision = "rag"
    elif confidence >= 0.4:
        # 中等，Hybrid
        web_results = exa_toolkit.search(query, num_results=top_n//2)
        contexts = (
            _format_ragflow_contexts(rag_chunks[:top_n//2]) +
            _format_exa_contexts(web_results)
        )
        decision = "hybrid"
    else:
        # 低质量，只用 Web
        web_results = exa_toolkit.search(query, num_results=top_n)
        contexts = _format_exa_contexts(web_results)
        decision = "web"
    
    # 填充 debug 信息
    debug_info = {
        "trigger_reason": f"confidence={confidence:.2f}",
        "rag_top1_score": rag_chunks[0]["score"] if rag_chunks else 0.0,
        "rag_result_count": len(rag_chunks),
        "web_triggered": decision in ["web", "hybrid"],
    }
```

**验收标准**：
- [ ] `mode="auto"` 能根据 RAGFlow 质量自动选择模式
- [ ] `debug.trigger_reason` 能解释为什么选了这个 decision

**测试方法**：

```python
# 测试 1：高质量查询（应该只用 RAG）
result_good = adaptive_retrieve("深度学习基础", mode="auto", top_n=10)
# 如果你的知识库有"深度学习"相关文档，应该 decision="rag"
print(f"Query: 深度学习基础 -> decision={result_good['decision']}, confidence={result_good['confidence']}")

# 测试 2：低质量查询（应该触发 Web）
result_bad = adaptive_retrieve("skdjfhaksjdfh", mode="auto", top_n=10)
assert result_bad["decision"] in ["web", "hybrid"]
print("✅ Test 2: 低质量查询触发 Web")

# 测试 3：中等查询（应该是 hybrid）
result_medium = adaptive_retrieve("某个边缘话题", mode="auto", top_n=10)
# 根据实际情况验证
```

---

#### Step 3.2：调整阈值与规则（迭代优化）

**要做的**：
1. 准备 10-20 个测试 query（覆盖高/中/低质量）
2. 跑一遍，看 `decision` 分布
3. 调整 `_calculate_confidence` 的阈值（例如改成 0.75 / 0.5）

**验收标准**：
- [ ] Web 触发率在合理范围内（建议 20%–40%）
- [ ] 对明显的"内部知识不足"能触发 Web

---

### ~~Module 4：Hybrid 融合优化~~（暂缓，移至 Module 6 优化阶段）

> **调整说明**：排序、压缩等优化功能非核心自适应逻辑，暂缓实现。优先完成 ROMA 集成和端到端测试，优化细节放到 Module 6 持续迭代。

~~**目标**：当 `decision="hybrid"` 时，做去重、排序、压缩，避免上下文爆炸。~~

**当前状态**：**已跳过，直接进入 Module 4（原 Module 5）**

> **内容已移动到 Module 6 - Step 6.1**（优化迭代阶段）
> 
> 详见下方 Module 6 部分。

---

### Module 4：集成到 ROMA RETRIEVE 节点（1-2 天）

**目标**：把 `adaptive_retrieve` 注册成 ROMA 工具，让 DR 能直接调用。

#### Step 4.1：包装成 DSPy Tool

**要做的**：
1. 创建 `RAGFlowToolkit` 继承 `BaseToolkit`
2. 实现 `adaptive_retrieve` 作为一个 tool 方法
3. 确保返回格式是 JSON 字符串（DSPy 要求）

**代码**（简化版）：

```python
# src/roma_dspy/tools/ragflow_toolkit.py
from roma_dspy.tools.base.base import BaseToolkit
import json

class RAGFlowToolkit(BaseToolkit):
    def __init__(self, api_url: str, api_key: str, kb_id: str, **config):
        super().__init__(**config)
        self.api_url = api_url
        self.api_key = api_key
        self.kb_id = kb_id
    
    async def adaptive_retrieve(
        self,
        query: str,
        mode: str = "auto",
        top_n: int = 10,
    ) -> str:
        """
        自适应检索：根据内部知识库质量决定是否触发 Web Search。
        
        Args:
            query: 查询问题
            mode: "auto" | "rag" | "web" | "hybrid"
            top_n: 返回证据数量上限
        
        Returns:
            JSON 字符串，包含 contexts 和 sources
        """
        # 调用 Module 2-4 实现的 adaptive_retrieve
        result = adaptive_retrieve(query, mode=mode, top_n=top_n)
        
        # 返回 JSON 字符串
        return json.dumps(result, ensure_ascii=False)
```

**验收标准**：
- [ ] 方法签名正确，有 docstring
- [ ] 返回 JSON 字符串

---

#### Step 4.2：注册到 ROMA

**要做的**：
1. 在 `src/roma_dspy/tools/__init__.py` 里导入 `RAGFlowToolkit`
2. 在 `src/roma_dspy/tools/base/manager.py` 的 `BUILTIN_TOOLKITS` 里注册
3. 在 `config/profiles/deep_research.yaml` 的 `executors.RETRIEVE.toolkits` 里配置

**配置示例**：

```yaml
executors:
  RETRIEVE:
    toolkits:
      # 保留原有的 Exa MCP
      - class_name: MCPToolkit
        enabled: true
        toolkit_config:
          server_name: exa
          server_type: http
          url: https://mcp.exa.ai/mcp
          headers:
            Authorization: "Bearer ${oc.env:EXA_API_KEY}"
      
      # 新增 RAGFlow Toolkit
      - class_name: RAGFlowToolkit
        enabled: true
        toolkit_config:
          api_url: "${oc.env:RAGFLOW_API_URL}"
          api_key: "${oc.env:RAGFLOW_API_KEY}"
          kb_id: "${oc.env:RAGFLOW_KB_ID}"
```

**验收标准**：
- [ ] ROMA 启动时能识别 `RAGFlowToolkit`
- [ ] 工具列表里能看到 `adaptive_retrieve`

---

#### Step 4.3：端到端测试

**要做的**：
用真实的 DR 任务测试整个流程。

**测试方法**：

```bash
# 在 ROMA 根目录运行
just solve "请检索：AI 驱动的个性化教育的现状" -c config/profiles/deep_research.yaml
```

**期望结果**：
- [ ] RETRIEVE 节点调用了 `adaptive_retrieve`
- [ ] 返回的证据包含内联引用 `[Source: ...]`
- [ ] Aggregator 能正常合并 `sources`
- [ ] 最终报告里能看到引用

**如何验证**：
1. 查看日志：`logs/roma_dspy_*.log`
2. 查看 MLflow：检查 tool calls 记录
3. 查看最终报告：`storage/executions/<task_id>/results/reports/final_result.md`

---

### Module 5：MLflow 深度集成（2 天，可观测性升级）

**目标**：将自适应检索的决策过程、检索结果、推理链路深度集成到 MLflow trace 中，便于在 MLflow UI 中统一观测，为后续 LLM judge 和 CRAG 模型升级预留扩展性。

**设计理念**：
- ✅ 统一观测界面：所有信息在 MLflow UI 中查看，无需切换多个工具
- ✅ 结构化记录：使用 MLflow 的 attributes、metrics、artifacts 分层记录
- ✅ 控制信息密度：关键指标用 tags/params，详细内容用 artifacts
- ✅ 为未来扩展预留字段：支持 LLM judge 推理过程、CRAG 模型置信度分布

#### Step 5.1：集成 MLflow logging 到 AdaptiveRetrieveToolkit

**要做的**：
在 `adaptive_retrieve_async` 方法中，使用 MLflow API 记录关键信息。

**核心代码**（已实现在 `src/roma_dspy/tools/adaptive_retrieve_toolkit.py`）：

```python
def _log_to_mlflow(self, stage: str, data: Dict[str, Any], log_detailed_contexts: bool = False):
    """
    记录信息到 MLflow（如果可用且已启用）
    
    Args:
        stage: 当前阶段（如 "input", "ragflow_retrieve", "decision", "final_result"）
        data: 要记录的数据
        log_detailed_contexts: 是否记录详细的检索结果文本（默认False以控制信息密度）
    """
    if not self.mlflow_enabled:
        return
    
    try:
        span = mlflow.get_current_active_span()
        if not span:
            return
        
        # 根据阶段记录不同信息
        if stage == "input":
            # 记录输入参数
            span.set_attributes({
                "roma.adaptive_retrieve.query": data.get("query", ""),
                "roma.adaptive_retrieve.mode": data.get("mode", ""),
                "roma.adaptive_retrieve.top_n": str(data.get("top_n", 0)),
            })
        
        elif stage == "decision":
            # ⭐ 核心决策信息（最重要）
            span.set_attributes({
                "roma.decision.type": data.get("decision", ""),
                "roma.decision.confidence": f"{data.get('confidence', 0.0):.3f}",
                "roma.decision.reason": data.get("reason", ""),
                "roma.decision.web_triggered": str(data.get("web_triggered", False)),
                # 为未来的LLM judge/CRAG模型预留字段
                "roma.decision.method": data.get("method", "heuristic"),  # heuristic/llm_judge/crag_model
            })
            
            # 详细决策记录作为 artifact（包含推理过程）
            decision_record = {
                "decision": data.get("decision"),
                "confidence": data.get("confidence"),
                "reason": data.get("reason"),
                "method": data.get("method", "heuristic"),
                "ragflow_stats": data.get("ragflow_stats", {}),
                "exa_stats": data.get("exa_stats", {}),
                # 为LLM judge预留：可以记录LLM的推理过程
                "llm_reasoning": data.get("llm_reasoning", None),
                # 为CRAG模型预留：可以记录模型的置信度分布
                "model_scores": data.get("model_scores", None),
            }
            mlflow.log_dict(decision_record, "decision_record.json")
        
        elif stage == "final_result":
            # 最终结果统计
            span.set_attributes({
                "roma.result.total_contexts": str(data.get("total_contexts", 0)),
                "roma.result.ragflow_count": str(data.get("ragflow_count", 0)),
                "roma.result.exa_count": str(data.get("exa_count", 0)),
                "roma.result.duration_ms": str(data.get("duration_ms", 0)),
            })
            
            # ⭐ 完整的检索结果（包含文本内容和来源）
            if data.get("contexts"):
                final_contexts = []
                for ctx in data["contexts"]:
                    final_contexts.append({
                        "source": ctx.get("source", "unknown"),
                        "title": ctx.get("title", ""),
                        "text": ctx.get("text", "")[:500],  # 截断过长文本
                        "url": ctx.get("url", ""),
                        "score": ctx.get("score", 0.0),
                    })
                
                mlflow.log_dict(
                    {
                        "query": data.get("query"),
                        "decision": data.get("decision"),
                        "contexts": final_contexts,
                    },
                    "final_retrieve_result.json"
                )
    
    except Exception as e:
        self.log_debug(f"[MLflow] Failed to log {stage}: {e}")
```

**MLflow trace 层次结构**（调用链）：

```
Run: RETRIEVE_executor_call
├── Span: search_adaptive (DSPy tool call, auto-traced)
│   ├── Attributes:
│   │   ├── roma.adaptive_retrieve.query: "个性化教育"
│   │   ├── roma.adaptive_retrieve.mode: "auto"
│   │   ├── roma.adaptive_retrieve.top_n: "8"
│   │   ├── roma.ragflow.result_count: "8"
│   │   ├── roma.ragflow.top1_score: "0.752"
│   │   ├── roma.decision.type: "rag"
│   │   ├── roma.decision.confidence: "0.752"
│   │   ├── roma.decision.reason: "High confidence (0.75 ≥ 0.7), RAGFlow sufficient"
│   │   ├── roma.decision.method: "heuristic"
│   │   ├── roma.decision.web_triggered: "false"
│   │   ├── roma.result.total_contexts: "8"
│   │   ├── roma.result.ragflow_count: "8"
│   │   ├── roma.result.exa_count: "0"
│   │   └── roma.result.duration_ms: "523"
│   └── Artifacts:
│       ├── decision_record.json - 详细决策记录（包含推理过程）
│       ├── ragflow_detailed_results.json - RAGFlow 检索详细结果
│       └── final_retrieve_result.json - 最终结果（含文本和来源）
```

**验收标准**：
- [x] 在 MLflow UI 中能看到每次 `adaptive_retrieve` 的输入参数
- [x] 能看到决策类型（rag/web/hybrid）和置信度
- [x] 能看到决策原因（trigger_reason）
- [x] 能下载 artifacts 查看完整的检索结果文本和来源
- [x] 预留了 `roma.decision.method` 字段，支持未来切换到 LLM judge 或 CRAG 模型

---

#### Step 5.2：在 MLflow UI 中查看决策过程

**如何使用**：

1. **查看所有 adaptive_retrieve 调用**：
   - 打开 MLflow UI (`http://localhost:5000`)
   - 进入对应的 Experiment
   - 筛选器：按 `roma.decision.type` 过滤（如只看 `hybrid` 的调用）
   - 筛选器：按 `roma.decision.confidence` 范围筛选（如 0.4-0.7）

2. **查看单次调用详情**：
   - 点击某个 Run，进入详情页
   - **Params 标签页**：查看输入参数（query, mode, top_n）
   - **Tags 标签页**：查看决策信息（decision, confidence, reason, method）
   - **Metrics 标签页**：查看统计指标（result_count, duration_ms等）
   - **Artifacts 标签页**：
     - 下载 `decision_record.json` - 查看完整决策过程
     - 下载 `final_retrieve_result.json` - 查看所有检索结果的文本和来源

3. **对比不同调用**：
   - 选中多个 Run，点击 "Compare"
   - 对比不同查询的 confidence、decision、web_triggered 等指标

**验收标准**：
- [ ] 能在 MLflow UI 中筛选出所有 hybrid 模式的调用
- [ ] 能下载 artifacts 查看检索结果的文本内容和来源
- [ ] 能对比不同查询的决策过程

---

#### Step 5.3：为 LLM judge 和 CRAG 模型预留扩展性

**当前实现（Module 5）**：
- ✅ 使用启发式规则（`_calculate_confidence`）进行决策
- ✅ 在 MLflow 中记录 `roma.decision.method: "heuristic"`
- ✅ 在 `decision_record.json` 中预留了 `llm_reasoning` 和 `model_scores` 字段

**未来升级（Module 6）**：

当升级到 LLM judge 时，只需修改 `_calculate_confidence` 并更新 MLflow logging：

```python
# 伪代码：LLM judge 版本
def _calculate_confidence_llm(query: str, chunks: List[Dict]) -> Tuple[float, str]:
    """
    使用 LLM 作为评估器
    """
    # 调用 LLM 评估检索质量
    llm_response = llm.evaluate(query, chunks)
    confidence = llm_response.score
    reasoning = llm_response.explanation  # LLM 的推理过程
    
    return confidence, reasoning

# 在 adaptive_retrieve_async 中使用
confidence, llm_reasoning = self._calculate_confidence_llm(query, rag_chunks)

# MLflow logging 时添加推理过程
self._log_to_mlflow("decision", {
    "decision": decision.value,
    "confidence": confidence,
    "method": "llm_judge",  # ✅ 标记为 LLM judge
    "llm_reasoning": llm_reasoning,  # ✅ 记录 LLM 的推理过程
    # ...
})
```

**验收标准**：
- [x] 当前代码已预留 `llm_reasoning` 和 `model_scores` 字段
- [ ] 切换到 LLM judge 时，无需修改 MLflow logging 逻辑
- [ ] 可以在 MLflow UI 中对比 heuristic vs llm_judge 的决策差异

---

#### Step 5.4：批量分析历史决策（可选）

**要做的**：
编写脚本从 MLflow 批量读取历史决策，进行统计分析。

**脚本示例**：

```python
# scripts/analyze_retrieval_decisions.py
import mlflow
from collections import Counter

def analyze_decisions(experiment_name: str):
    """
    分析某个 experiment 下所有 adaptive_retrieve 的决策分布
    """
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    runs = client.search_runs(experiment.experiment_id)
    
    decisions = []
    web_triggered_count = 0
    
    for run in runs:
        decision = run.data.tags.get("roma.decision.type")
        web_triggered = run.data.tags.get("roma.decision.web_triggered")
        
        if decision:
            decisions.append(decision)
        if web_triggered == "true":
            web_triggered_count += 1
    
    # 统计
    decision_counts = Counter(decisions)
    print(f"Decision distribution: {dict(decision_counts)}")
    print(f"Web trigger rate: {web_triggered_count / len(runs) * 100:.1f}%")

# 使用示例
analyze_decisions("Deep_Research_Experiment")
```

**验收标准**：
- [ ] 能批量统计 decision 分布（rag/web/hybrid）
- [ ] 能计算 Web 触发率
- [ ] 能导出为报表供团队review

---

### Module 6：优化与迭代（持续进行）

> **包含内容**：置信度评估升级 + Hybrid 融合优化（排序/压缩/归一化）+ 测试增强

#### Step 6.1：Hybrid 融合优化（排序 + 压缩）

**目标**：优化 Hybrid 模式的证据质量，避免上下文爆炸。

**Step 6.1.1：排序（按权威性）**

按来源可信度排序：
- 内部知识库优先（私有数据，最权威）
- 学术/政府网站（.edu, .gov, .org, arxiv.org 等）
- 商业网站（.com, .cn）
- 其他

**Step 6.1.2：压缩（长文本截断）**

如果某条 `context.text` 超过 300 字，截断前 300 字 + "..."
- 保留 URL、title、score（引用可追溯）
- 节省 LLM 上下文窗口

**验收标准**：
- [ ] 内部证据排在前面
- [ ] 长文本被压缩（> 300 字符）
- [ ] Hybrid 模式下 contexts 数量不超过 `top_n`

---

#### Step 6.2：从 V0 升级到 V1 评估器（LLM-as-judge）

**要做的**：
把 `_calculate_confidence` 从启发式规则升级成 LLM 判断。

**代码**：

```python
def _calculate_confidence_v1(query: str, chunks: List[Dict]) -> float:
    """
    V1：用 LLM 判断检索质量
    """
    if not chunks:
        return 0.0
    
    # 构造 prompt
    evidence_summary = "\n".join([
        f"{i+1}. {chunk['text'][:100]}... (score={chunk['score']:.2f})"
        for i, chunk in enumerate(chunks[:5])
    ])
    
    prompt = f"""
请评估以下检索结果对问题的回答质量：

问题：{query}

检索结果：
{evidence_summary}

请给出评分（0-1）：
- 1.0: 完全能回答问题
- 0.5: 部分相关但不够完整
- 0.0: 完全无关

只返回数字，例如：0.8
"""
    
    # 调用 LLM（用 ROMA 的 LLM 配置）
    response = llm.generate(prompt)
    try:
        score = float(response.strip())
        return max(0.0, min(1.0, score))
    except:
        return 0.5  # 解析失败，默认中等
```

**验收标准**：
- [ ] V1 评估器能返回更准确的置信度
- [ ] 对比 V0 和 V1，看 web 触发率是否更合理

---

#### Step 6.3：引入 CRAG evaluator（V2）

**要做的**：
把 CRAG 项目的训练好的 evaluator 模型集成进来。

**这是可选项**，如果 V1 已经够用，可以暂时不做。

---

#### Step 6.4：Score 归一化（Hybrid 模式优化）

**目标**：将 RAGFlow 和 Exa 的 score 归一化到同一尺度，使 Hybrid 模式排序更合理。

**问题背景**：
- RAGFlow score 范围：通常 0.2-0.8（相似度分数）
- Exa Neural Search score 范围：通常 0.15-0.45（神经搜索相关性）
- 直接比较会导致排序不公平

**解决方案**：

**方案 A：Min-Max 归一化**
```python
def _normalize_scores(contexts: List[Dict]) -> List[Dict]:
    """
    将不同来源的 score 归一化到 [0, 1]
    """
    # 分别归一化 RAGFlow 和 Exa 的分数
    rag_contexts = [ctx for ctx in contexts if ctx["source"] == "ragflow"]
    exa_contexts = [ctx for ctx in contexts if ctx["source"] == "exa"]
    
    # RAGFlow 归一化
    if rag_contexts:
        rag_scores = [ctx["score"] for ctx in rag_contexts]
        rag_min, rag_max = min(rag_scores), max(rag_scores)
        if rag_max > rag_min:
            for ctx in rag_contexts:
                ctx["score_normalized"] = (ctx["score"] - rag_min) / (rag_max - rag_min)
        else:
            for ctx in rag_contexts:
                ctx["score_normalized"] = 0.5
    
    # Exa 归一化
    if exa_contexts:
        exa_scores = [ctx["score"] for ctx in exa_contexts]
        exa_min, exa_max = min(exa_scores), max(exa_scores)
        if exa_max > exa_min:
            for ctx in exa_contexts:
                ctx["score_normalized"] = (ctx["score"] - exa_min) / (exa_max - exa_min)
        else:
            for ctx in exa_contexts:
                ctx["score_normalized"] = 0.5
    
    # 按归一化后的 score 排序
    all_contexts = rag_contexts + exa_contexts
    all_contexts.sort(key=lambda x: x["score_normalized"], reverse=True)
    
    return all_contexts
```

**方案 B：Z-Score 标准化**
```python
def _normalize_scores_zscore(contexts: List[Dict]) -> List[Dict]:
    """
    使用 Z-Score 标准化（适合分数分布差异大的情况）
    """
    import numpy as np
    
    rag_contexts = [ctx for ctx in contexts if ctx["source"] == "ragflow"]
    exa_contexts = [ctx for ctx in contexts if ctx["source"] == "exa"]
    
    # RAGFlow Z-Score
    if len(rag_contexts) > 1:
        rag_scores = np.array([ctx["score"] for ctx in rag_contexts])
        rag_mean, rag_std = rag_scores.mean(), rag_scores.std()
        if rag_std > 0:
            for ctx in rag_contexts:
                z_score = (ctx["score"] - rag_mean) / rag_std
                ctx["score_normalized"] = 0.5 + (z_score / 6)  # 映射到 [0, 1]
    
    # Exa Z-Score
    if len(exa_contexts) > 1:
        exa_scores = np.array([ctx["score"] for ctx in exa_contexts])
        exa_mean, exa_std = exa_scores.mean(), exa_scores.std()
        if exa_std > 0:
            for ctx in exa_contexts:
                z_score = (ctx["score"] - exa_mean) / exa_std
                ctx["score_normalized"] = 0.5 + (z_score / 6)
    
    # 排序
    all_contexts = rag_contexts + exa_contexts
    all_contexts.sort(key=lambda x: x.get("score_normalized", 0.5), reverse=True)
    
    return all_contexts
```

**方案 C：加权融合（考虑来源可信度）**
```python
def _normalize_and_weight_scores(contexts: List[Dict]) -> List[Dict]:
    """
    归一化 + 加权（内部知识库权重更高）
    """
    # 先归一化
    contexts = _normalize_scores(contexts)
    
    # 加权调整
    SOURCE_WEIGHTS = {
        "ragflow": 1.2,  # 内部知识库加权 20%
        "exa": 1.0,      # 外部搜索基准权重
    }
    
    for ctx in contexts:
        weight = SOURCE_WEIGHTS.get(ctx["source"], 1.0)
        ctx["score_final"] = ctx["score_normalized"] * weight
    
    # 按最终分数排序
    contexts.sort(key=lambda x: x["score_final"], reverse=True)
    
    return contexts
```

**验收标准**：
- [ ] Hybrid 模式下，高质量的 RAGFlow 结果不会被低质量的 Exa 结果排挤
- [ ] 归一化后的 score 在 [0, 1] 范围内
- [ ] 排序结果更符合人工判断的相关性

**测试方法**：
```python
# 准备测试数据
contexts = [
    {"source": "ragflow", "score": 0.75, "text": "高质量内部文档"},
    {"source": "exa", "score": 0.38, "text": "高质量外部网页"},
    {"source": "ragflow", "score": 0.30, "text": "低质量内部文档"},
    {"source": "exa", "score": 0.20, "text": "低质量外部网页"},
]

# 归一化并排序
normalized = _normalize_scores(contexts)

# 预期结果：
# 1. RAGFlow 0.75 (归一化后 1.0)
# 2. Exa 0.38 (归一化后 1.0)
# 3. RAGFlow 0.30 (归一化后 0.0)
# 4. Exa 0.20 (归一化后 0.0)
```

---

#### Step 6.5：增强测试覆盖（边界条件 + 错误处理 + 超时）

**目标**：确保系统在各种异常情况下都能稳定工作。

**Step 6.5.1：边界条件测试**

```python
# tests/test_edge_cases.py
import pytest
from roma_dspy.tools.adaptive_retrieve_toolkit import AdaptiveRetrieveToolkit

def test_empty_query():
    """测试空查询"""
    toolkit = create_toolkit()
    result = toolkit.adaptive_retrieve(query="", mode="auto")
    assert result.decision == "web"  # 空查询应该触发 Web
    assert result.confidence == 0.0

def test_very_long_query():
    """测试超长查询（> 1000 字符）"""
    long_query = "人工智能" * 500
    result = toolkit.adaptive_retrieve(query=long_query, mode="auto")
    assert len(result.query) <= 1000  # 应该截断

def test_zero_top_n():
    """测试 top_n=0"""
    result = toolkit.adaptive_retrieve(query="AI教育", mode="rag", top_n=0)
    assert len(result.contexts) == 0

def test_negative_top_n():
    """测试 top_n < 0"""
    with pytest.raises(ValueError):
        toolkit.adaptive_retrieve(query="AI教育", top_n=-5)

def test_special_characters():
    """测试特殊字符查询"""
    queries = [
        "AI & ML",
        "C++ 教程",
        "Python 3.x",
        "什么是 <AI>？",
        'Query with "quotes"',
    ]
    for query in queries:
        result = toolkit.adaptive_retrieve(query=query, mode="auto")
        assert result.query == query  # 应该保留特殊字符

def test_unicode_query():
    """测试 Unicode 查询"""
    queries = [
        "人工智能🤖",
        "AI教育（中文）",
        "Educação em IA",  # 葡萄牙语
    ]
    for query in queries:
        result = toolkit.adaptive_retrieve(query=query, mode="auto")
        assert result is not None
```

**Step 6.5.2：错误处理测试**

```python
def test_ragflow_api_down():
    """测试 RAGFlow API 不可用"""
    # Mock RAGFlow 返回 500 错误
    with patch('requests.post') as mock_post:
        mock_post.return_value.status_code = 500
        mock_post.return_value.raise_for_status.side_effect = HTTPError()
        
        result = toolkit.adaptive_retrieve(query="AI教育", mode="auto")
        
        # 应该降级到 Web 搜索
        assert result.decision == "web"
        assert result.debug.get("ragflow_error") is not None

def test_exa_api_down():
    """测试 Exa API 不可用"""
    # Mock Exa 返回错误
    result = toolkit.adaptive_retrieve(query="AI教育", mode="web")
    
    # 应该返回错误信息
    assert "error" in result or len(result.contexts) == 0

def test_network_timeout():
    """测试网络超时"""
    # Mock 超时
    with patch('requests.post') as mock_post:
        mock_post.side_effect = Timeout()
        
        result = toolkit.adaptive_retrieve(query="AI教育", mode="rag")
        
        # 应该降级或返回错误
        assert result.decision in ["web", "rag"]
        assert result.debug.get("timeout") is True

def test_invalid_json_response():
    """测试 API 返回无效 JSON"""
    with patch('requests.post') as mock_post:
        mock_post.return_value.json.side_effect = json.JSONDecodeError("", "", 0)
        
        result = toolkit.adaptive_retrieve(query="AI教育", mode="rag")
        
        # 应该优雅降级
        assert result is not None

def test_partial_failure_hybrid_mode():
    """测试 Hybrid 模式下部分失败"""
    # RAGFlow 成功，Exa 失败
    result = toolkit.adaptive_retrieve(query="AI教育", mode="hybrid")
    
    # 应该至少返回 RAGFlow 结果
    assert len(result.contexts) > 0
    assert any(ctx["source"] == "ragflow" for ctx in result.contexts)
```

**Step 6.5.3：超时测试**

```python
def test_ragflow_timeout_handling():
    """测试 RAGFlow 超时处理"""
    import time
    
    # 设置极短的超时时间
    toolkit = AdaptiveRetrieveToolkit(
        api_url="http://localhost:8080",
        api_key="test",
        kb_id="test",
        timeout=0.001,  # 1ms，必然超时
    )
    
    start = time.time()
    result = toolkit.adaptive_retrieve(query="AI教育", mode="rag")
    duration = time.time() - start
    
    # 应该在超时时间内返回
    assert duration < 1.0  # 不应该等太久
    assert result.decision in ["web", "rag"]

def test_web_search_timeout():
    """测试 Web 搜索超时"""
    # 设置超时后应该返回空结果或降级
    result = toolkit.adaptive_retrieve(query="AI教育", mode="web")
    
    # 不应该无限等待
    assert result.debug.get("duration_ms") < 30000  # < 30 秒

def test_concurrent_timeout():
    """测试 Hybrid 模式下并发超时"""
    # RAGFlow 和 Exa 都超时
    result = toolkit.adaptive_retrieve(query="AI教育", mode="hybrid")
    
    # 应该返回错误或部分结果
    assert result is not None
    assert result.debug.get("duration_ms") < 60000  # < 1 分钟
```

**Step 6.5.4：性能基准测试**

```python
def test_performance_benchmark():
    """测试性能基准"""
    import time
    
    queries = [
        "人工智能教育",
        "深度学习基础",
        "机器学习应用",
        "AI伦理问题",
        "教育技术发展",
    ]
    
    durations = []
    for query in queries:
        start = time.time()
        result = toolkit.adaptive_retrieve(query=query, mode="auto")
        duration = time.time() - start
        durations.append(duration)
    
    avg_duration = sum(durations) / len(durations)
    
    # 性能基准
    assert avg_duration < 5.0  # 平均 < 5 秒
    assert max(durations) < 10.0  # 最慢 < 10 秒
    print(f"平均响应时间: {avg_duration:.2f}s")

def test_memory_usage():
    """测试内存使用"""
    import tracemalloc
    
    tracemalloc.start()
    
    # 执行多次查询
    for _ in range(100):
        result = toolkit.adaptive_retrieve(query="AI教育", mode="auto")
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # 内存不应该持续增长（检测内存泄漏）
    assert peak < 100 * 1024 * 1024  # < 100MB
    print(f"峰值内存: {peak / 1024 / 1024:.2f}MB")
```

**验收标准**：
- [ ] 所有边界条件测试通过
- [ ] 错误处理不会导致系统崩溃
- [ ] 超时机制正常工作
- [ ] 性能满足基准要求（< 5 秒平均响应）
- [ ] 无明显内存泄漏

**测试覆盖率目标**：
- 核心功能：**100%**（adaptive_retrieve, format_contexts）
- 工具方法：**> 80%**（RAGFlowToolkit, exa_native_search）
- 边界条件：**> 90%**

---

## 📊 总体进度检查清单（调整后）

### 核心功能（优先完成）
- [x] **Module 0 完成**：数据契约已定义 ✅
- [x] **Module 1 完成**：能调 RAGFlow API ✅
- [x] **Module 2 完成**：三种模式都能跑通（rag/web/hybrid）✅
- [x] **Module 3 完成**：自适应路由能工作（mode=auto）✅
- [x] **Module 4 完成**：已集成到 ROMA DR 流程，使用原生 Exa API ✅
- [ ] **Module 5 进行中**：有日志和回放能力

### 优化迭代（持续进行）
- [ ] **Module 6 待开始**：持续优化
  - [ ] Step 6.1: Hybrid 融合优化（排序 + 压缩）
  - [ ] Step 6.2: V1 评估器（LLM-as-judge）
  - [ ] Step 6.3: V2 评估器（CRAG 模型）
  - [ ] Step 6.4: Score 归一化（RAGFlow + Exa 统一尺度）
  - [ ] Step 6.5: 增强测试覆盖（边界条件 + 错误处理 + 超时）

---

## 附录 A：技术细节（查阅用）

### A.1 RAGFlow `/retrieval` 返回字段映射

根据你提供的代码，RAGFlow SDK 层会做这些 rename：

| 底层字段 | SDK 重命名后 | 说明 |
|---------|-------------|------|
| `chunk_id` | `id` | chunk 唯一标识 |
| `doc_id` | `document_id` | 文档 ID |
| `kb_id` | `dataset_id` | 知识库 ID |
| `content_with_weight` | `content` | chunk 正文 |
| `important_kwd` | `important_keywords` | 关键词 |
| `question_kwd` | `questions` | 问题关键词 |
| `docnm_kwd` | `document_keyword` | 文档名/标题 |
| `similarity` | `similarity` | 相似度分数 |

### A.2 RAGFlow 配置参数建议

| 参数 | 默认值 | 最大值 | 建议（Deep Research） |
|------|--------|--------|---------------------|
| `top_n` | 6–8 | 30 | **8–12**（更多证据） |
| `top_k` | 1024 | 无限制 | 1024（用于统计信号） |
| `chunk_token_num` | 512 | 2048 | **512–1024**（便于去重） |

### A.3 CRAG 核心思想

CRAG 论文提出的三个关键组件：
1. **Retrieval Evaluator**：评估检索质量，返回置信度
2. **Knowledge Preparation**：
   - Correct：内部检索高质量 → 分解重组精炼
   - Incorrect：内部检索低质量 → Web Search 补充
   - Ambiguous：中等 → 两者结合
3. **Dynamic Action Selection**：根据置信度选择动作

本方案把这个思想"工程化"成可控的路由策略。

---

## 🎯 下一步行动建议（调整后）

### 当前状态（2026-01-21）
- ✅ **Module 0-4 已完成**：核心自适应路由 + ROMA 集成已完成
  - 已切换到 Exa 原生 API（Neural Search，有真实 score）
  - 所有测试通过（注册、初始化、工具方法、API 调用）
- 🔄 **Module 5 进行中**：下一步是可观测性和日志增强

### 推荐路线
1. **✅ 已完成**：Module 4（ROMA 集成）
   - ✅ 注册 `AdaptiveRetrieveToolkit` 到 ROMA
   - ✅ 配置 `deep_research.yaml`（已切换到原生 Exa API）
   - ✅ 端到端测试通过

2. **本周**：完成 Module 5（可观测性）
   - 增强 debug 日志
   - 导出决策记录
   - 实现回放脚本

3. **后续迭代**：Module 6（优化）
   - 先跑通流程，再回来优化细节
   - Step 6.1: Hybrid 排序/压缩（按需）
   - Step 6.2: LLM 评估器（提升准确度）
   - Step 6.3: CRAG 模型（可选）

每完成一个 Module，在清单里打勾，保持节奏！
