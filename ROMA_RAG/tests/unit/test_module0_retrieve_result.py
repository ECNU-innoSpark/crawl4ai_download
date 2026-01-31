"""
Module 0 测试套件：测试检索结果数据模型

测试 RetrieveResult、RetrieveContext、RetrieveDebugInfo 的创建、转换和序列化。
"""

import pytest
from src.roma_dspy.types.retrieve_result import (
    RetrieveResult,
    RetrieveContext,
    RetrieveDebugInfo,
    DecisionType,
    SourceType,
)


class TestRetrieveContext:
    """测试 RetrieveContext 数据类"""

    def test_create_context_with_all_fields(self):
        """测试：创建完整的 RetrieveContext"""
        ctx = RetrieveContext(
            text="这是一段测试文本",
            source=SourceType.RAGFLOW,
            url="ragflow://kb/test123/doc/doc456#chunk=chunk789",
            title="测试文档",
            score=0.85,
        )
        
        assert ctx.text == "这是一段测试文本"
        assert ctx.source == SourceType.RAGFLOW
        assert ctx.url == "ragflow://kb/test123/doc/doc456#chunk=chunk789"
        assert ctx.title == "测试文档"
        assert ctx.score == 0.85

    def test_create_context_minimal(self):
        """测试：创建最小字段的 RetrieveContext"""
        ctx = RetrieveContext(
            text="最小测试",
            source=SourceType.EXA,
            url="https://example.com",
        )
        
        assert ctx.text == "最小测试"
        assert ctx.source == SourceType.EXA
        assert ctx.url == "https://example.com"
        assert ctx.title == ""
        assert ctx.score == 0.0

    def test_context_to_dict(self):
        """测试：RetrieveContext 转换为字典"""
        ctx = RetrieveContext(
            text="测试内容",
            source=SourceType.RAGFLOW,
            url="ragflow://kb/test/doc/doc1",
            title="标题",
            score=0.9,
        )
        
        result = ctx.to_dict()
        
        assert result["text"] == "测试内容"
        assert result["source"] == "ragflow"
        assert result["url"] == "ragflow://kb/test/doc/doc1"
        assert result["title"] == "标题"
        assert result["score"] == 0.9

    def test_context_with_string_source(self):
        """测试：source 可以接受字符串"""
        ctx = RetrieveContext(
            text="测试",
            source="ragflow",  # 字符串而非枚举
            url="test://url",
        )
        
        result = ctx.to_dict()
        assert result["source"] == "ragflow"


class TestRetrieveDebugInfo:
    """测试 RetrieveDebugInfo 数据类"""

    def test_create_debug_info_full(self):
        """测试：创建完整的 DebugInfo"""
        debug = RetrieveDebugInfo(
            trigger_reason="confidence=0.65, hybrid mode",
            rag_top1_score=0.82,
            rag_result_count=5,
            web_triggered=True,
            duration_ms=450,
            timestamp="2026-01-17T10:30:00Z",
        )
        
        assert debug.trigger_reason == "confidence=0.65, hybrid mode"
        assert debug.rag_top1_score == 0.82
        assert debug.rag_result_count == 5
        assert debug.web_triggered is True
        assert debug.duration_ms == 450
        assert debug.timestamp == "2026-01-17T10:30:00Z"

    def test_debug_info_to_dict(self):
        """测试：DebugInfo 转换为字典"""
        debug = RetrieveDebugInfo(
            trigger_reason="test",
            rag_top1_score=0.7,
            rag_result_count=3,
            web_triggered=False,
            duration_ms=200,
        )
        
        result = debug.to_dict()
        
        assert result["trigger_reason"] == "test"
        assert result["rag_top1_score"] == 0.7
        assert result["rag_result_count"] == 3
        assert result["web_triggered"] is False
        assert result["duration_ms"] == 200
        assert "timestamp" not in result  # 没有设置时不应该出现

    def test_debug_info_with_timestamp(self):
        """测试：DebugInfo 包含 timestamp"""
        debug = RetrieveDebugInfo(
            trigger_reason="test",
            timestamp="2026-01-17T00:00:00Z",
        )
        
        result = debug.to_dict()
        assert result["timestamp"] == "2026-01-17T00:00:00Z"


class TestRetrieveResult:
    """测试 RetrieveResult 核心契约"""

    def test_create_retrieve_result_rag_mode(self):
        """测试：创建 RAG 模式的 RetrieveResult"""
        contexts = [
            RetrieveContext(
                text="内部文档内容1",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/kb1/doc/doc1#chunk=c1",
                score=0.9,
            ),
            RetrieveContext(
                text="内部文档内容2",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/kb1/doc/doc2#chunk=c2",
                score=0.85,
            ),
        ]
        
        debug = RetrieveDebugInfo(
            trigger_reason="confidence=0.9, high quality RAG",
            rag_top1_score=0.9,
            rag_result_count=2,
            web_triggered=False,
            duration_ms=150,
        )
        
        result = RetrieveResult(
            query="AI教育现状",
            decision=DecisionType.RAG,
            confidence=0.9,
            contexts=contexts,
            sources=[ctx.url for ctx in contexts],
            debug=debug,
        )
        
        assert result.query == "AI教育现状"
        assert result.decision == DecisionType.RAG
        assert result.confidence == 0.9
        assert len(result.contexts) == 2
        assert len(result.sources) == 2
        assert result.debug.web_triggered is False

    def test_create_retrieve_result_web_mode(self):
        """测试：创建 Web 模式的 RetrieveResult"""
        contexts = [
            RetrieveContext(
                text="Web搜索结果1",
                source=SourceType.EXA,
                url="https://example.com/page1",
                title="Example Page 1",
                score=1.0,
            ),
        ]
        
        debug = RetrieveDebugInfo(
            trigger_reason="confidence=0.2, low quality, web fallback",
            rag_result_count=0,
            web_triggered=True,
            duration_ms=8000,
        )
        
        result = RetrieveResult(
            query="最新科技新闻",
            decision=DecisionType.WEB,
            confidence=0.3,
            contexts=contexts,
            sources=["https://example.com/page1"],
            debug=debug,
        )
        
        assert result.decision == DecisionType.WEB
        assert result.debug.web_triggered is True
        assert all(ctx.source == SourceType.EXA for ctx in result.contexts)

    def test_create_retrieve_result_hybrid_mode(self):
        """测试：创建 Hybrid 模式的 RetrieveResult"""
        contexts = [
            RetrieveContext(
                text="内部文档",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/kb1/doc/doc1",
                score=0.75,
            ),
            RetrieveContext(
                text="外部网页",
                source=SourceType.EXA,
                url="https://example.com/page1",
                score=0.9,
            ),
        ]
        
        debug = RetrieveDebugInfo(
            trigger_reason="confidence=0.55, medium quality, hybrid",
            rag_top1_score=0.75,
            rag_result_count=1,
            web_triggered=True,
            duration_ms=4000,
        )
        
        result = RetrieveResult(
            query="混合检索测试",
            decision=DecisionType.HYBRID,
            confidence=0.55,
            contexts=contexts,
            sources=["ragflow://kb/kb1/doc/doc1", "https://example.com/page1"],
            debug=debug,
        )
        
        assert result.decision == DecisionType.HYBRID
        assert len(result.contexts) == 2
        # 验证包含两种来源
        sources = {ctx.source for ctx in result.contexts}
        assert SourceType.RAGFLOW in sources
        assert SourceType.EXA in sources

    def test_retrieve_result_to_dict(self):
        """测试：RetrieveResult 转换为字典（完整序列化）"""
        contexts = [
            RetrieveContext(
                text="测试文本",
                source=SourceType.RAGFLOW,
                url="ragflow://test",
                score=0.8,
            ),
        ]
        
        debug = RetrieveDebugInfo(
            trigger_reason="test reason",
            duration_ms=100,
        )
        
        result = RetrieveResult(
            query="测试查询",
            decision=DecisionType.RAG,
            confidence=0.8,
            contexts=contexts,
            sources=["ragflow://test"],
            debug=debug,
        )
        
        data = result.to_dict()
        
        # 验证所有必需字段
        assert data["query"] == "测试查询"
        assert data["decision"] == "rag"
        assert data["confidence"] == 0.8
        assert len(data["contexts"]) == 1
        assert data["contexts"][0]["text"] == "测试文本"
        assert data["contexts"][0]["source"] == "ragflow"
        assert data["sources"] == ["ragflow://test"]
        assert data["debug"]["trigger_reason"] == "test reason"
        assert data["debug"]["duration_ms"] == 100

    def test_retrieve_result_from_dict(self):
        """测试：从字典创建 RetrieveResult（反序列化）"""
        data = {
            "query": "测试查询",
            "decision": "hybrid",
            "confidence": 0.65,
            "contexts": [
                {
                    "text": "内容1",
                    "source": "ragflow",
                    "url": "ragflow://kb/test/doc/d1",
                    "title": "标题1",
                    "score": 0.85,
                },
                {
                    "text": "内容2",
                    "source": "exa",
                    "url": "https://example.com",
                    "title": "标题2",
                    "score": 0.9,
                },
            ],
            "sources": ["ragflow://kb/test/doc/d1", "https://example.com"],
            "debug": {
                "trigger_reason": "hybrid triggered",
                "rag_top1_score": 0.85,
                "rag_result_count": 1,
                "web_triggered": True,
                "duration_ms": 500,
            },
        }
        
        result = RetrieveResult.from_dict(data)
        
        assert result.query == "测试查询"
        assert result.decision == DecisionType.HYBRID
        assert result.confidence == 0.65
        assert len(result.contexts) == 2
        assert result.contexts[0].source == SourceType.RAGFLOW
        assert result.contexts[1].source == SourceType.EXA
        assert result.sources == ["ragflow://kb/test/doc/d1", "https://example.com"]
        assert result.debug.trigger_reason == "hybrid triggered"
        assert result.debug.web_triggered is True

    def test_retrieve_result_round_trip(self):
        """测试：序列化+反序列化的往返一致性"""
        original = RetrieveResult(
            query="往返测试",
            decision=DecisionType.RAG,
            confidence=0.75,
            contexts=[
                RetrieveContext(
                    text="测试内容",
                    source=SourceType.RAGFLOW,
                    url="ragflow://test",
                    title="测试",
                    score=0.8,
                ),
            ],
            sources=["ragflow://test"],
            debug=RetrieveDebugInfo(
                trigger_reason="test",
                duration_ms=200,
            ),
        )
        
        # 往返：对象 -> 字典 -> 对象
        data = original.to_dict()
        restored = RetrieveResult.from_dict(data)
        
        # 验证关键字段一致
        assert restored.query == original.query
        assert restored.decision == original.decision
        assert restored.confidence == original.confidence
        assert len(restored.contexts) == len(original.contexts)
        assert restored.contexts[0].text == original.contexts[0].text
        assert restored.contexts[0].source == original.contexts[0].source
        assert restored.sources == original.sources
        assert restored.debug.trigger_reason == original.debug.trigger_reason

    def test_retrieve_result_repr(self):
        """测试：RetrieveResult 的字符串表示"""
        result = RetrieveResult(
            query="这是一个很长的查询问题用于测试字符串表示功能",
            decision=DecisionType.HYBRID,
            confidence=0.68,
            contexts=[
                RetrieveContext(text="c1", source=SourceType.RAGFLOW, url="url1"),
                RetrieveContext(text="c2", source=SourceType.EXA, url="url2"),
            ],
            sources=["url1", "url2"],
            debug=RetrieveDebugInfo(trigger_reason="test", duration_ms=100),
        )
        
        repr_str = repr(result)
        
        # 验证包含关键信息
        assert "query=" in repr_str
        assert "decision=hybrid" in repr_str
        assert "confidence=0.68" in repr_str
        assert "contexts=2" in repr_str
        assert "sources=2" in repr_str
        # 验证 query 被截断（只显示前30个字符）
        assert len(result.query) > 30  # 原始 query 很长
        assert "..." in repr_str  # 应该有省略号


class TestDecisionTypeEnum:
    """测试 DecisionType 枚举"""

    def test_decision_type_values(self):
        """测试：DecisionType 的所有值"""
        assert DecisionType.RAG.value == "rag"
        assert DecisionType.WEB.value == "web"
        assert DecisionType.HYBRID.value == "hybrid"

    def test_decision_type_from_string(self):
        """测试：从字符串创建 DecisionType"""
        assert DecisionType("rag") == DecisionType.RAG
        assert DecisionType("web") == DecisionType.WEB
        assert DecisionType("hybrid") == DecisionType.HYBRID


class TestSourceTypeEnum:
    """测试 SourceType 枚举"""

    def test_source_type_values(self):
        """测试：SourceType 的所有值"""
        assert SourceType.RAGFLOW.value == "ragflow"
        assert SourceType.EXA.value == "exa"

    def test_source_type_from_string(self):
        """测试：从字符串创建 SourceType"""
        assert SourceType("ragflow") == SourceType.RAGFLOW
        assert SourceType("exa") == SourceType.EXA


# ========== 集成测试 ==========

class TestRetrieveResultIntegration:
    """集成测试：模拟真实使用场景"""

    def test_mock_rag_only_scenario(self):
        """
        模拟场景：内部知识库质量高，只用 RAG
        验证：DR 的 THINK/WRITE 节点能正常消费 contexts 和 sources
        """
        # 模拟 RAGFlow 返回的高质量结果
        result = RetrieveResult(
            query="深度学习基础知识",
            decision=DecisionType.RAG,
            confidence=0.92,
            contexts=[
                RetrieveContext(
                    text="深度学习是机器学习的一个分支，它基于人工神经网络...",
                    source=SourceType.RAGFLOW,
                    url="ragflow://kb/ai_edu/doc/dl_basics#chunk=intro",
                    title="深度学习基础教程",
                    score=0.95,
                ),
                RetrieveContext(
                    text="神经网络由多层节点组成，每层节点接收输入并产生输出...",
                    source=SourceType.RAGFLOW,
                    url="ragflow://kb/ai_edu/doc/dl_basics#chunk=nn_intro",
                    title="神经网络介绍",
                    score=0.88,
                ),
            ],
            sources=[
                "ragflow://kb/ai_edu/doc/dl_basics#chunk=intro",
                "ragflow://kb/ai_edu/doc/dl_basics#chunk=nn_intro",
            ],
            debug=RetrieveDebugInfo(
                trigger_reason="confidence=0.92, high quality, RAG only",
                rag_top1_score=0.95,
                rag_result_count=2,
                web_triggered=False,
                duration_ms=280,
            ),
        )
        
        # 验收：检查 DR 能消费的格式
        assert result.query is not None
        assert result.decision == DecisionType.RAG
        assert len(result.contexts) >= 1
        assert len(result.sources) >= 1
        
        # 验证 contexts 格式
        for ctx in result.contexts:
            assert ctx.text  # 有正文
            assert ctx.url  # 有可追溯引用
            assert ctx.source == SourceType.RAGFLOW
        
        # 验证 sources 可以直接用于内联引用 [Source: URL]
        for source_url in result.sources:
            assert source_url.startswith("ragflow://")
        
        print(f"✅ 测试通过: {result}")

    def test_mock_web_fallback_scenario(self):
        """
        模拟场景：内部知识库为空，回退到 Web
        """
        result = RetrieveResult(
            query="2026年最新AI政策",
            decision=DecisionType.WEB,
            confidence=0.25,
            contexts=[
                RetrieveContext(
                    text="2026年1月，国家发布新AI政策...",
                    source=SourceType.EXA,
                    url="https://news.example.com/ai-policy-2026",
                    title="2026 AI政策解读",
                    score=1.0,
                ),
            ],
            sources=["https://news.example.com/ai-policy-2026"],
            debug=RetrieveDebugInfo(
                trigger_reason="confidence=0.25, low RAG quality, web fallback",
                rag_result_count=0,
                web_triggered=True,
                duration_ms=7800,
            ),
        )
        
        assert result.decision == DecisionType.WEB
        assert result.debug.web_triggered is True
        assert all(ctx.source == SourceType.EXA for ctx in result.contexts)
        assert all(url.startswith("http") for url in result.sources)
        
        print(f"✅ 测试通过: {result}")

    def test_mock_hybrid_scenario(self):
        """
        模拟场景：内外部融合
        """
        result = RetrieveResult(
            query="个性化教育技术应用",
            decision=DecisionType.HYBRID,
            confidence=0.58,
            contexts=[
                # 内部文档
                RetrieveContext(
                    text="个性化教育系统能根据学生特点定制学习路径...",
                    source=SourceType.RAGFLOW,
                    url="ragflow://kb/edu_tech/doc/personalized",
                    score=0.75,
                ),
                # 外部网页
                RetrieveContext(
                    text="最新研究表明，AI驱动的个性化教育能提升30%学习效率...",
                    source=SourceType.EXA,
                    url="https://research.example.com/ai-edu-2026",
                    score=0.92,
                ),
            ],
            sources=[
                "ragflow://kb/edu_tech/doc/personalized",
                "https://research.example.com/ai-edu-2026",
            ],
            debug=RetrieveDebugInfo(
                trigger_reason="confidence=0.58, medium quality, hybrid mode",
                rag_top1_score=0.75,
                rag_result_count=1,
                web_triggered=True,
                duration_ms=4200,
            ),
        )
        
        assert result.decision == DecisionType.HYBRID
        sources_types = {ctx.source for ctx in result.contexts}
        assert SourceType.RAGFLOW in sources_types
        assert SourceType.EXA in sources_types
        
        # 验证混合来源
        ragflow_count = sum(1 for ctx in result.contexts if ctx.source == SourceType.RAGFLOW)
        exa_count = sum(1 for ctx in result.contexts if ctx.source == SourceType.EXA)
        assert ragflow_count > 0 and exa_count > 0
        
        print(f"✅ 测试通过: {result}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

