"""
自适应检索配置常量

Module 0 - Step 0.3: 设定成本与时延预算
定义检索过程的各项约束参数和配置常量
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class RetrieveConfig:
    """
    检索配置参数（不可变）
    
    这些参数控制自适应检索的行为边界，确保成本和延迟可控。
    """
    
    # ========== 预算限制 ==========
    
    # 每个 RETRIEVE 子任务最多触发 Web Search 次数
    MAX_WEB_SEARCH_PER_TASK: int = 1
    
    # Web Search 超时阈值（秒）
    WEB_SEARCH_TIMEOUT_SECONDS: float = 8.0
    
    # RAGFlow 检索超时阈值（秒）
    RAGFLOW_TIMEOUT_SECONDS: float = 3.0
    
    # ========== 结果数量控制 ==========
    
    # 单次检索最终返回 contexts 数量上限
    MAX_CONTEXTS_PER_RETRIEVE: int = 12
    
    # 单次检索最少返回 contexts 数量
    MIN_CONTEXTS_PER_RETRIEVE: int = 8
    
    # RAGFlow 默认请求数量
    DEFAULT_RAGFLOW_TOP_N: int = 10
    
    # Web Search 默认请求数量
    DEFAULT_WEB_TOP_N: int = 10
    
    # Hybrid 模式下内外部比例（RAGFlow:Web）
    HYBRID_RAGFLOW_RATIO: float = 0.5  # 50% 内部，50% 外部
    
    # ========== 质量阈值（启发式 V0）==========
    
    # 高质量阈值：>= 此值只用 RAG
    CONFIDENCE_THRESHOLD_HIGH: float = 0.7
    
    # 中等阈值：>= 此值触发 Hybrid
    CONFIDENCE_THRESHOLD_MEDIUM: float = 0.4
    
    # 低于中等阈值：只用 Web
    # (implicit: < CONFIDENCE_THRESHOLD_MEDIUM)
    
    # ========== 内容处理 ==========
    
    # 单条 context 文本最大长度（字符数）
    MAX_CONTEXT_LENGTH: int = 300
    
    # 去重时的语义相似度阈值（暂未实现，预留）
    DEDUP_SIMILARITY_THRESHOLD: float = 0.9
    
    # ========== RAGFlow 评分参数 ==========
    
    # 判定为"高质量"的 top1 分数下限
    HIGH_QUALITY_SCORE_THRESHOLD: float = 0.8
    
    # 判定为"中等质量"的 top1 分数下限
    MEDIUM_QUALITY_SCORE_THRESHOLD: float = 0.6
    
    # 判定为"高质量"需要的最少结果数
    HIGH_QUALITY_MIN_RESULTS: int = 3
    
    # 判定为"中等质量"需要的最少结果数
    MEDIUM_QUALITY_MIN_RESULTS: int = 2
    
    # ========== 调试与日志 ==========
    
    # 是否启用详细日志
    ENABLE_VERBOSE_LOGGING: bool = True
    
    # 是否保存决策记录到文件
    ENABLE_DECISION_LOGGING: bool = True
    
    # 决策日志保存目录（相对于 execution 目录）
    DECISION_LOG_DIR: str = "retrieval_logs"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "MAX_WEB_SEARCH_PER_TASK": self.MAX_WEB_SEARCH_PER_TASK,
            "WEB_SEARCH_TIMEOUT_SECONDS": self.WEB_SEARCH_TIMEOUT_SECONDS,
            "RAGFLOW_TIMEOUT_SECONDS": self.RAGFLOW_TIMEOUT_SECONDS,
            "MAX_CONTEXTS_PER_RETRIEVE": self.MAX_CONTEXTS_PER_RETRIEVE,
            "MIN_CONTEXTS_PER_RETRIEVE": self.MIN_CONTEXTS_PER_RETRIEVE,
            "DEFAULT_RAGFLOW_TOP_N": self.DEFAULT_RAGFLOW_TOP_N,
            "DEFAULT_WEB_TOP_N": self.DEFAULT_WEB_TOP_N,
            "HYBRID_RAGFLOW_RATIO": self.HYBRID_RAGFLOW_RATIO,
            "CONFIDENCE_THRESHOLD_HIGH": self.CONFIDENCE_THRESHOLD_HIGH,
            "CONFIDENCE_THRESHOLD_MEDIUM": self.CONFIDENCE_THRESHOLD_MEDIUM,
            "MAX_CONTEXT_LENGTH": self.MAX_CONTEXT_LENGTH,
            "DEDUP_SIMILARITY_THRESHOLD": self.DEDUP_SIMILARITY_THRESHOLD,
            "HIGH_QUALITY_SCORE_THRESHOLD": self.HIGH_QUALITY_SCORE_THRESHOLD,
            "MEDIUM_QUALITY_SCORE_THRESHOLD": self.MEDIUM_QUALITY_SCORE_THRESHOLD,
            "HIGH_QUALITY_MIN_RESULTS": self.HIGH_QUALITY_MIN_RESULTS,
            "MEDIUM_QUALITY_MIN_RESULTS": self.MEDIUM_QUALITY_MIN_RESULTS,
            "ENABLE_VERBOSE_LOGGING": self.ENABLE_VERBOSE_LOGGING,
            "ENABLE_DECISION_LOGGING": self.ENABLE_DECISION_LOGGING,
            "DECISION_LOG_DIR": self.DECISION_LOG_DIR,
        }


# 默认配置实例（单例）
DEFAULT_RETRIEVE_CONFIG = RetrieveConfig()


def get_retrieve_config() -> RetrieveConfig:
    """
    获取检索配置（单例模式）
    
    后续可以扩展为从环境变量或配置文件加载
    
    Returns:
        RetrieveConfig 实例
    """
    return DEFAULT_RETRIEVE_CONFIG


# 导出方便的访问方式
__all__ = [
    "RetrieveConfig",
    "DEFAULT_RETRIEVE_CONFIG",
    "get_retrieve_config",
]

