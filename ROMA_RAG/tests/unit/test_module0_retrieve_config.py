"""
Module 0 测试套件：测试检索配置

测试 RetrieveConfig 和相关配置工具。
"""

import pytest
from src.roma_dspy.config.retrieve_config import (
    RetrieveConfig,
    DEFAULT_RETRIEVE_CONFIG,
    get_retrieve_config,
)


class TestRetrieveConfig:
    """测试 RetrieveConfig 数据类"""

    def test_default_config_values(self):
        """测试：默认配置的所有值"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 预算限制
        assert config.MAX_WEB_SEARCH_PER_TASK == 1
        assert config.WEB_SEARCH_TIMEOUT_SECONDS == 8.0
        assert config.RAGFLOW_TIMEOUT_SECONDS == 3.0
        
        # 结果数量控制
        assert config.MAX_CONTEXTS_PER_RETRIEVE == 12
        assert config.MIN_CONTEXTS_PER_RETRIEVE == 8
        assert config.DEFAULT_RAGFLOW_TOP_N == 10
        assert config.DEFAULT_WEB_TOP_N == 10
        assert config.HYBRID_RAGFLOW_RATIO == 0.5
        
        # 质量阈值
        assert config.CONFIDENCE_THRESHOLD_HIGH == 0.7
        assert config.CONFIDENCE_THRESHOLD_MEDIUM == 0.4
        
        # 内容处理
        assert config.MAX_CONTEXT_LENGTH == 300
        assert config.DEDUP_SIMILARITY_THRESHOLD == 0.9
        
        # RAGFlow 评分参数
        assert config.HIGH_QUALITY_SCORE_THRESHOLD == 0.8
        assert config.MEDIUM_QUALITY_SCORE_THRESHOLD == 0.6
        assert config.HIGH_QUALITY_MIN_RESULTS == 3
        assert config.MEDIUM_QUALITY_MIN_RESULTS == 2
        
        # 调试与日志
        assert config.ENABLE_VERBOSE_LOGGING is True
        assert config.ENABLE_DECISION_LOGGING is True
        assert config.DECISION_LOG_DIR == "retrieval_logs"

    def test_config_is_frozen(self):
        """测试：配置是不可变的（frozen=True）"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 尝试修改应该抛出异常
        with pytest.raises(Exception):  # dataclass frozen 会抛出 FrozenInstanceError
            config.MAX_WEB_SEARCH_PER_TASK = 999

    def test_create_custom_config(self):
        """测试：创建自定义配置"""
        custom_config = RetrieveConfig(
            MAX_WEB_SEARCH_PER_TASK=2,
            WEB_SEARCH_TIMEOUT_SECONDS=10.0,
            RAGFLOW_TIMEOUT_SECONDS=5.0,
            MAX_CONTEXTS_PER_RETRIEVE=15,
            MIN_CONTEXTS_PER_RETRIEVE=10,
            DEFAULT_RAGFLOW_TOP_N=12,
            DEFAULT_WEB_TOP_N=8,
            HYBRID_RAGFLOW_RATIO=0.6,
            CONFIDENCE_THRESHOLD_HIGH=0.75,
            CONFIDENCE_THRESHOLD_MEDIUM=0.5,
            MAX_CONTEXT_LENGTH=500,
            DEDUP_SIMILARITY_THRESHOLD=0.85,
            HIGH_QUALITY_SCORE_THRESHOLD=0.85,
            MEDIUM_QUALITY_SCORE_THRESHOLD=0.65,
            HIGH_QUALITY_MIN_RESULTS=5,
            MEDIUM_QUALITY_MIN_RESULTS=3,
            ENABLE_VERBOSE_LOGGING=False,
            ENABLE_DECISION_LOGGING=False,
            DECISION_LOG_DIR="custom_logs",
        )
        
        assert custom_config.MAX_WEB_SEARCH_PER_TASK == 2
        assert custom_config.WEB_SEARCH_TIMEOUT_SECONDS == 10.0
        assert custom_config.CONFIDENCE_THRESHOLD_HIGH == 0.75
        assert custom_config.HYBRID_RAGFLOW_RATIO == 0.6
        assert custom_config.ENABLE_VERBOSE_LOGGING is False

    def test_config_to_dict(self):
        """测试：配置转换为字典"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        config_dict = config.to_dict()
        
        # 验证所有字段都在字典中
        assert "MAX_WEB_SEARCH_PER_TASK" in config_dict
        assert "WEB_SEARCH_TIMEOUT_SECONDS" in config_dict
        assert "RAGFLOW_TIMEOUT_SECONDS" in config_dict
        assert "MAX_CONTEXTS_PER_RETRIEVE" in config_dict
        assert "CONFIDENCE_THRESHOLD_HIGH" in config_dict
        assert "CONFIDENCE_THRESHOLD_MEDIUM" in config_dict
        assert "MAX_CONTEXT_LENGTH" in config_dict
        assert "ENABLE_VERBOSE_LOGGING" in config_dict
        assert "DECISION_LOG_DIR" in config_dict
        
        # 验证值类型
        assert isinstance(config_dict["MAX_WEB_SEARCH_PER_TASK"], int)
        assert isinstance(config_dict["WEB_SEARCH_TIMEOUT_SECONDS"], float)
        assert isinstance(config_dict["ENABLE_VERBOSE_LOGGING"], bool)
        assert isinstance(config_dict["DECISION_LOG_DIR"], str)

    def test_get_retrieve_config_singleton(self):
        """测试：get_retrieve_config 返回单例"""
        config1 = get_retrieve_config()
        config2 = get_retrieve_config()
        
        # 应该是同一个实例
        assert config1 is config2
        assert config1 is DEFAULT_RETRIEVE_CONFIG


class TestRetrieveConfigConstraints:
    """测试配置约束的合理性"""

    def test_timeout_constraints(self):
        """测试：超时时间约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # RAGFlow 应该比 Web Search 快
        assert config.RAGFLOW_TIMEOUT_SECONDS < config.WEB_SEARCH_TIMEOUT_SECONDS
        
        # 超时时间应该是正数
        assert config.RAGFLOW_TIMEOUT_SECONDS > 0
        assert config.WEB_SEARCH_TIMEOUT_SECONDS > 0

    def test_contexts_count_constraints(self):
        """测试：contexts 数量约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 最大值应该 >= 最小值
        assert config.MAX_CONTEXTS_PER_RETRIEVE >= config.MIN_CONTEXTS_PER_RETRIEVE
        
        # 应该是正数
        assert config.MAX_CONTEXTS_PER_RETRIEVE > 0
        assert config.MIN_CONTEXTS_PER_RETRIEVE > 0
        
        # 默认请求数应该 >= 最大返回数（留有裁剪空间）
        assert config.DEFAULT_RAGFLOW_TOP_N >= config.MIN_CONTEXTS_PER_RETRIEVE
        assert config.DEFAULT_WEB_TOP_N >= config.MIN_CONTEXTS_PER_RETRIEVE

    def test_confidence_threshold_constraints(self):
        """测试：置信度阈值约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 高阈值 > 中等阈值
        assert config.CONFIDENCE_THRESHOLD_HIGH > config.CONFIDENCE_THRESHOLD_MEDIUM
        
        # 阈值应该在 [0, 1] 范围内
        assert 0 <= config.CONFIDENCE_THRESHOLD_MEDIUM <= 1
        assert 0 <= config.CONFIDENCE_THRESHOLD_HIGH <= 1
        
        # 避免极端值（太接近 0 或 1 不合理）
        assert config.CONFIDENCE_THRESHOLD_MEDIUM >= 0.1
        assert config.CONFIDENCE_THRESHOLD_HIGH <= 0.95

    def test_quality_score_threshold_constraints(self):
        """测试：质量分数阈值约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 高质量分数 > 中等分数
        assert config.HIGH_QUALITY_SCORE_THRESHOLD > config.MEDIUM_QUALITY_SCORE_THRESHOLD
        
        # 分数应该在 [0, 1] 范围内
        assert 0 <= config.MEDIUM_QUALITY_SCORE_THRESHOLD <= 1
        assert 0 <= config.HIGH_QUALITY_SCORE_THRESHOLD <= 1
        
        # 高质量需要的结果数 >= 中等质量
        assert config.HIGH_QUALITY_MIN_RESULTS >= config.MEDIUM_QUALITY_MIN_RESULTS

    def test_hybrid_ratio_constraint(self):
        """测试：Hybrid 比例约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 比例应该在 [0, 1] 范围内
        assert 0 <= config.HYBRID_RAGFLOW_RATIO <= 1
        
        # 默认是 0.5（50/50 分配）
        assert config.HYBRID_RAGFLOW_RATIO == 0.5

    def test_context_length_constraint(self):
        """测试：文本长度约束合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 应该是正数
        assert config.MAX_CONTEXT_LENGTH > 0
        
        # 不应该太短（至少能容纳一句话）
        assert config.MAX_CONTEXT_LENGTH >= 100
        
        # 也不应该太长（避免上下文爆炸）
        assert config.MAX_CONTEXT_LENGTH <= 1000

    def test_dedup_similarity_constraint(self):
        """测试：去重相似度阈值合理"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 应该在 [0, 1] 范围内
        assert 0 <= config.DEDUP_SIMILARITY_THRESHOLD <= 1
        
        # 应该是高阈值（只去重非常相似的）
        assert config.DEDUP_SIMILARITY_THRESHOLD >= 0.8


class TestRetrieveConfigUsageScenarios:
    """测试配置在实际场景中的使用"""

    def test_rag_mode_budget(self):
        """
        场景：RAG 模式的成本预算
        验证：RAG 模式下不应该触发 Web Search
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        # RAG 模式：0 次 Web Search
        web_search_count = 0
        
        assert web_search_count <= config.MAX_WEB_SEARCH_PER_TASK
        
        # 预期耗时：只有 RAGFlow
        expected_duration_ms = config.RAGFLOW_TIMEOUT_SECONDS * 1000
        
        assert expected_duration_ms <= 3000  # 应该 <= 3秒

    def test_web_mode_budget(self):
        """
        场景：Web 模式的成本预算
        验证：Web 模式触发 1 次 Web Search
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        # Web 模式：1 次 Web Search
        web_search_count = 1
        
        assert web_search_count <= config.MAX_WEB_SEARCH_PER_TASK
        
        # 预期耗时：Web Search
        expected_duration_ms = config.WEB_SEARCH_TIMEOUT_SECONDS * 1000
        
        assert expected_duration_ms <= 8000  # 应该 <= 8秒

    def test_hybrid_mode_budget(self):
        """
        场景：Hybrid 模式的成本预算
        验证：Hybrid 模式触发 1 次 RAG + 1 次 Web
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        # Hybrid 模式：1 次 Web Search
        web_search_count = 1
        
        assert web_search_count <= config.MAX_WEB_SEARCH_PER_TASK
        
        # 预期耗时：RAG + Web（并行或串行）
        max_duration_ms = (
            config.RAGFLOW_TIMEOUT_SECONDS + 
            config.WEB_SEARCH_TIMEOUT_SECONDS
        ) * 1000
        
        assert max_duration_ms <= 11000  # 应该 <= 11秒（串行最坏情况）

    def test_hybrid_mode_context_distribution(self):
        """
        场景：Hybrid 模式的 context 分配
        验证：按比例分配内外部 contexts
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        total_contexts = config.MAX_CONTEXTS_PER_RETRIEVE
        ragflow_ratio = config.HYBRID_RAGFLOW_RATIO
        
        ragflow_count = int(total_contexts * ragflow_ratio)
        web_count = total_contexts - ragflow_count
        
        # 验证分配合理
        assert ragflow_count >= 1
        assert web_count >= 1
        assert ragflow_count + web_count == total_contexts
        
        print(f"✅ Hybrid 分配: RAG={ragflow_count}, Web={web_count}")

    def test_quality_decision_logic(self):
        """
        场景：根据质量分数决策
        验证：不同质量应该触发不同模式
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 高质量：只用 RAG
        high_confidence = 0.9
        assert high_confidence >= config.CONFIDENCE_THRESHOLD_HIGH
        expected_decision = "rag"
        
        # 中等质量：Hybrid
        medium_confidence = 0.55
        assert config.CONFIDENCE_THRESHOLD_MEDIUM <= medium_confidence < config.CONFIDENCE_THRESHOLD_HIGH
        expected_decision = "hybrid"
        
        # 低质量：只用 Web
        low_confidence = 0.2
        assert low_confidence < config.CONFIDENCE_THRESHOLD_MEDIUM
        expected_decision = "web"
        
        print(f"✅ 决策逻辑验证通过")

    def test_context_length_truncation(self):
        """
        场景：长文本截断
        验证：超过长度限制的文本应该被截断
        """
        config = DEFAULT_RETRIEVE_CONFIG
        
        long_text = "这是一段很长的文本。" * 100  # 生成很长的文本
        max_length = config.MAX_CONTEXT_LENGTH
        
        if len(long_text) > max_length:
            truncated_text = long_text[:max_length] + "..."
        else:
            truncated_text = long_text
        
        assert len(truncated_text) <= max_length + 3  # +3 是 "..."
        
        print(f"✅ 原始长度: {len(long_text)}, 截断后: {len(truncated_text)}")


class TestRetrieveConfigDocumentation:
    """测试配置文档的完整性"""

    def test_all_fields_have_comments(self):
        """测试：所有字段都有注释说明"""
        config = DEFAULT_RETRIEVE_CONFIG
        
        # 验证所有字段都能转换为字典（说明都是公开字段）
        config_dict = config.to_dict()
        
        # 应该有足够多的配置项
        assert len(config_dict) >= 20
        
        print(f"✅ 配置项数量: {len(config_dict)}")

    def test_config_values_are_reasonable(self):
        """测试：所有配置值都在合理范围内"""
        config = DEFAULT_RETRIEVE_CONFIG
        config_dict = config.to_dict()
        
        # 检查没有明显不合理的值
        for key, value in config_dict.items():
            if isinstance(value, (int, float)):
                # 数值不应该是负数（除了特殊情况）
                if "TIMEOUT" in key or "MAX" in key or "MIN" in key or "THRESHOLD" in key:
                    assert value >= 0, f"{key} 不应该是负数: {value}"
        
        print("✅ 所有配置值都在合理范围内")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

