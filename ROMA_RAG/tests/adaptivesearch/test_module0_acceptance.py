"""
Module 0 验收测试：端到端集成验证

这个测试验证 Module 0 的所有组件能够正确集成，
确保不影响现有 ROMA 系统的其他模块。
"""

import pytest
import json
from pathlib import Path

from src.roma_dspy.types import (
    RetrieveResult,
    RetrieveContext,
    RetrieveDebugInfo,
    DecisionType,
    SourceType,
    RAGFlowReference,
    create_ragflow_uri,
    is_ragflow_uri,
)
from src.roma_dspy.config.retrieve_config import (
    RetrieveConfig,
    get_retrieve_config,
)


class TestModule0Acceptance:
    """Module 0 验收测试套件"""

    def test_module0_data_contract_complete(self):
        """
        验收标准 0.1: 数据契约完整性
        验证所有必需的数据结构都已定义并可用
        """
        # 验证类型可以导入
        assert RetrieveResult is not None
        assert RetrieveContext is not None
        assert RetrieveDebugInfo is not None
        assert DecisionType is not None
        assert SourceType is not None
        
        print("✅ 验收 0.1: 数据契约定义完整")

    def test_module0_ragflow_uri_specification(self):
        """
        验收标准 0.2: RAGFlow URI 规范完整
        验证 URI 格式和解析工具可用
        """
        # 测试创建 URI
        uri = create_ragflow_uri("kb123", "doc456", "chunk789")
        assert is_ragflow_uri(uri)
        
        # 测试解析 URI
        ref = RAGFlowReference.from_uri(uri)
        assert ref is not None
        assert ref.kb_id == "kb123"
        
        print("✅ 验收 0.2: RAGFlow URI 规范完整")

    def test_module0_config_budget_defined(self):
        """
        验收标准 0.3: 成本与时延预算已设定
        验证配置常量已定义并在合理范围内
        """
        config = get_retrieve_config()
        
        # 验证预算限制
        assert config.MAX_WEB_SEARCH_PER_TASK == 1
        assert config.WEB_SEARCH_TIMEOUT_SECONDS == 8.0
        assert config.RAGFLOW_TIMEOUT_SECONDS == 3.0
        
        # 验证结果数量控制
        assert 8 <= config.MIN_CONTEXTS_PER_RETRIEVE <= config.MAX_CONTEXTS_PER_RETRIEVE <= 12
        
        print("✅ 验收 0.3: 成本与时延预算已设定")

    def test_module0_mock_dr_consumption(self):
        """
        验收标准: DR 的 THINK/WRITE 节点能正常消费 contexts 和 sources
        
        模拟 DR 流程中的数据消费：
        1. RETRIEVE 节点返回 RetrieveResult
        2. THINK 节点读取 contexts
        3. WRITE 节点生成内联引用 [Source: URL]
        """
        # 1. 模拟 RETRIEVE 返回
        retrieve_result = RetrieveResult(
            query="AI教育的未来发展趋势",
            decision=DecisionType.HYBRID,
            confidence=0.68,
            contexts=[
                RetrieveContext(
                    text="AI教育正在向个性化学习方向发展，通过智能算法分析学生特点...",
                    source=SourceType.RAGFLOW,
                    url="ragflow://kb/edu_kb/doc/ai_edu_future#chunk=trend_1",
                    title="AI教育趋势报告",
                    score=0.82,
                ),
                RetrieveContext(
                    text="根据2026年最新研究，AI驱动的教育技术投资增长了45%...",
                    source=SourceType.EXA,
                    url="https://example.com/research/ai-edu-2026",
                    title="AI教育投资报告",
                    score=0.91,
                ),
            ],
            sources=[
                "ragflow://kb/edu_kb/doc/ai_edu_future#chunk=trend_1",
                "https://example.com/research/ai-edu-2026",
            ],
            debug=RetrieveDebugInfo(
                trigger_reason="confidence=0.68, medium quality, hybrid mode",
                rag_top1_score=0.82,
                rag_result_count=1,
                web_triggered=True,
                duration_ms=4200,
            ),
        )
        
        # 2. 模拟 THINK 节点消费 contexts
        contexts = retrieve_result.contexts
        assert len(contexts) > 0, "THINK 节点需要至少 1 条 context"
        
        for ctx in contexts:
            # THINK 节点读取证据正文
            assert ctx.text, "context 必须有正文"
            assert ctx.url, "context 必须有可追溯 URL"
            
            # THINK 节点可以区分来源类型
            assert ctx.source in [SourceType.RAGFLOW, SourceType.EXA]
        
        # 3. 模拟 WRITE 节点生成内联引用
        sources = retrieve_result.sources
        assert len(sources) > 0, "WRITE 节点需要至少 1 个 source"
        
        # 生成报告片段（模拟）
        report_snippet = f"""
根据检索到的证据，{contexts[0].text[:50]}... [Source: {sources[0]}]

进一步分析显示，{contexts[1].text[:50]}... [Source: {sources[1]}]
"""
        
        # 验证引用格式正确
        assert "[Source: ragflow://" in report_snippet
        assert "[Source: https://" in report_snippet
        
        print("✅ 验收测试: DR 节点能正常消费数据")
        print(f"  - RETRIEVE 返回: {len(contexts)} 条 contexts")
        print(f"  - THINK 可读取: {len([c for c in contexts if c.text])} 条正文")
        print(f"  - WRITE 可引用: {len(sources)} 个 sources")

    def test_module0_serialization_compatibility(self):
        """
        验收标准: RetrieveResult 可以序列化为 JSON（供后续模块使用）
        """
        result = RetrieveResult(
            query="测试查询",
            decision=DecisionType.RAG,
            confidence=0.85,
            contexts=[
                RetrieveContext(
                    text="测试内容",
                    source=SourceType.RAGFLOW,
                    url="ragflow://test",
                    score=0.9,
                ),
            ],
            sources=["ragflow://test"],
            debug=RetrieveDebugInfo(
                trigger_reason="test",
                duration_ms=100,
            ),
        )
        
        # 序列化
        data_dict = result.to_dict()
        json_str = json.dumps(data_dict, ensure_ascii=False)
        
        # 反序列化
        restored_dict = json.loads(json_str)
        restored_result = RetrieveResult.from_dict(restored_dict)
        
        # 验证一致性
        assert restored_result.query == result.query
        assert restored_result.decision == result.decision
        assert restored_result.confidence == result.confidence
        assert len(restored_result.contexts) == len(result.contexts)
        
        print("✅ 验收测试: JSON 序列化兼容性正常")

    def test_module0_no_impact_on_existing_modules(self):
        """
        验收标准: Module 0 不影响现有模块
        
        验证：
        1. 导入不会破坏现有类型
        2. 新类型与现有类型隔离
        3. 配置不冲突
        """
        # 1. 验证可以同时导入新旧类型
        from src.roma_dspy.types import (
            NodeType,  # 现有类型
            ExecutionStatus,  # 现有类型
            RetrieveResult,  # 新增类型
        )
        
        assert NodeType is not None
        assert ExecutionStatus is not None
        assert RetrieveResult is not None
        
        # 2. 验证新类型不会与现有枚举冲突
        from src.roma_dspy.types import TaskType
        assert TaskType is not None
        assert DecisionType is not None
        # 不同的枚举类
        assert TaskType != DecisionType
        
        # 3. 验证配置独立
        config = get_retrieve_config()
        assert isinstance(config, RetrieveConfig)
        
        print("✅ 验收测试: 不影响现有模块")

    def test_module0_all_files_created(self):
        """
        验收清单: 确认所有 Module 0 文件都已创建
        """
        base_path = Path("src/roma_dspy")
        
        required_files = [
            base_path / "types" / "retrieve_result.py",
            base_path / "types" / "ragflow_types.py",
            base_path / "config" / "retrieve_config.py",
        ]
        
        for file_path in required_files:
            assert file_path.exists(), f"文件缺失: {file_path}"
        
        print("✅ 验收清单: 所有文件已创建")

    def test_module0_ready_for_module1(self):
        """
        验收标准: Module 0 为 Module 1 做好准备
        
        Module 1 需要的前置条件：
        1. RetrieveResult 格式已定义
        2. RAGFlow URI 工具可用
        3. 超时配置已设定
        """
        config = get_retrieve_config()
        
        # 1. RetrieveResult 可用
        result = RetrieveResult(
            query="test",
            decision=DecisionType.RAG,
            confidence=0.8,
            contexts=[],
            sources=[],
            debug=RetrieveDebugInfo(trigger_reason="test", duration_ms=0),
        )
        assert result is not None
        
        # 2. URI 工具可用
        uri = create_ragflow_uri("kb", "doc", "chunk")
        assert is_ragflow_uri(uri)
        
        # 3. 超时配置可用
        assert config.RAGFLOW_TIMEOUT_SECONDS > 0
        
        print("✅ 验收标准: 已为 Module 1 做好准备")


class TestModule0Documentation:
    """验证 Module 0 文档完整性"""

    def test_all_classes_have_docstrings(self):
        """验证所有类都有文档字符串"""
        classes_to_check = [
            RetrieveResult,
            RetrieveContext,
            RetrieveDebugInfo,
            RAGFlowReference,
            RetrieveConfig,
        ]
        
        for cls in classes_to_check:
            assert cls.__doc__ is not None, f"{cls.__name__} 缺少文档字符串"
            assert len(cls.__doc__.strip()) > 0, f"{cls.__name__} 文档字符串为空"
        
        print("✅ 所有类都有文档字符串")

    def test_example_usage_in_docstrings(self):
        """验证关键类有使用示例"""
        # RetrieveResult 应该有使用示例
        assert "Example:" in RetrieveResult.__doc__
        
        # RAGFlowReference 的方法应该有示例
        assert "Example:" in RAGFlowReference.to_uri.__doc__
        
        print("✅ 关键类有使用示例")


# ========== 性能验收测试 ==========

class TestModule0Performance:
    """Module 0 性能验收测试"""

    def test_retrieve_result_creation_performance(self):
        """验证创建 RetrieveResult 的性能"""
        import time
        
        start = time.time()
        
        # 创建 1000 个 RetrieveResult
        for i in range(1000):
            result = RetrieveResult(
                query=f"query_{i}",
                decision=DecisionType.RAG,
                confidence=0.8,
                contexts=[
                    RetrieveContext(
                        text=f"content_{i}",
                        source=SourceType.RAGFLOW,
                        url=f"ragflow://kb/kb{i}/doc/doc{i}",
                        score=0.9,
                    ),
                ],
                sources=[f"ragflow://kb/kb{i}/doc/doc{i}"],
                debug=RetrieveDebugInfo(
                    trigger_reason="test",
                    duration_ms=100,
                ),
            )
        
        duration = time.time() - start
        
        # 应该在 1 秒内完成
        assert duration < 1.0, f"创建速度太慢: {duration:.3f}s"
        
        print(f"✅ 性能测试: 1000 次创建耗时 {duration*1000:.1f}ms")

    def test_json_serialization_performance(self):
        """验证 JSON 序列化性能"""
        import time
        
        result = RetrieveResult(
            query="测试查询",
            decision=DecisionType.HYBRID,
            confidence=0.7,
            contexts=[
                RetrieveContext(
                    text=f"内容{i}",
                    source=SourceType.RAGFLOW if i % 2 == 0 else SourceType.EXA,
                    url=f"url_{i}",
                    score=0.8,
                )
                for i in range(10)
            ],
            sources=[f"url_{i}" for i in range(10)],
            debug=RetrieveDebugInfo(trigger_reason="test", duration_ms=100),
        )
        
        start = time.time()
        
        # 序列化 + 反序列化 1000 次
        for _ in range(1000):
            data_dict = result.to_dict()
            json_str = json.dumps(data_dict)
            restored_dict = json.loads(json_str)
            restored = RetrieveResult.from_dict(restored_dict)
        
        duration = time.time() - start
        
        # 应该在 2 秒内完成
        assert duration < 2.0, f"序列化速度太慢: {duration:.3f}s"
        
        print(f"✅ 性能测试: 1000 次序列化往返耗时 {duration*1000:.1f}ms")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

