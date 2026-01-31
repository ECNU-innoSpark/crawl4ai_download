"""Atomizer instruction seed prompt for Deep Research (DR).

This module provides a specialized instruction prompt for the hierarchical task
atomizer, optimized for Deep Research workflows with Chinese support.
"""

import dspy

ATOMIZER_DR_PROMPT = r"""
# Atomizer — Deep Research Instruction Prompt

Role
Classify the goal as ATOMIC or NOT and set `node_type`. Your primary objective is to ensure complex research questions are adequately decomposed into manageable sub-investigations.
你的职责是判断给定目标是否为原子任务（ATOMIC）。在深度研究（DR）场景下，你必须倾向于任务分解。

Language Requirement (strict)
- Output ONLY the required JSON object. Do not add any prose.
- `reasoning` (if requested in signature) or internal logic should consider Chinese semantics.

Decision Rules for Deep Research (深度研究判定规则)

1. **STRONGLY FAVOR DECOMPOSITION (强烈倾向分解)**:
   In a Deep Research context, almost any question worth asking is worth breaking down.
   在深度研究背景下，绝大多数有价值的问题都需要分解。
   - If the user asks for "analysis (分析)", "report (报告)", "strategy (策略)", "comprehensive review (综述)", or "comparison (对比)", it is NON-ATOMIC.
   - If the user asks a broad open-ended question (e.g., "future of X", "impact of Y"), it is NON-ATOMIC.

2. **Atomic Task Criteria (原子任务标准 - 极严格)**:
   Classify as ATOMIC (→ EXECUTE) only if:
   - It is a simple, factual lookup (e.g., "法国的首都是哪里？").
   - It is a simple calculation.
   - It is a request to write a short text *where all context is already provided*.
   - It requires NO multi-step reasoning or multi-source synthesis.

3. **Non-Atomic Indicators (非原子任务指标 - 触发 PLAN)**:
   - **Multi-dimensionality (多维性)**: Does the topic have technical, economic, and social angles? -> PLAN.
   - **Ambiguity (模糊性)**: Does "best" or "impact" need defining? -> PLAN.
   - **Volume (信息量)**: Does the answer require reading more than 1-2 web pages? -> PLAN.
   - **Synthesis (合成性)**: Does it require combining conflicting views? -> PLAN.

Strict Output Contract
- Return ONLY this JSON object:
{
  "is_atomic": true|false,
  "node_type": "EXECUTE"|"PLAN"
}
"""

ATOMIZER_DR_DEMOS = [
    # 1. 宽泛的研究话题 -> PLAN
    dspy.Example(
        goal="分析固态电池技术的现状及其对电动汽车行业的潜在影响。",
        is_atomic=False,
        node_type="PLAN",
    ).with_inputs("goal"),

    # 2. 简单事实查询 -> EXECUTE
    dspy.Example(
        goal="现任丰田汽车的 CEO 是谁？",
        is_atomic=True,
        node_type="EXECUTE",
    ).with_inputs("goal"),

    # 3. 比较分析 -> PLAN
    dspy.Example(
        goal="对比远程办公对美国一线城市与二线城市经济的影响。",
        is_atomic=False,
        node_type="PLAN",
    ).with_inputs("goal"),

    # 4. 具体但复杂 -> PLAN
    dspy.Example(
        goal="找出 H100 GPU 的最新技术规格，并将其与 A100 进行详细对比。",
        is_atomic=False,
        node_type="PLAN",
    ).with_inputs("goal"),
    
    # 5. 已有上下文的写作 -> EXECUTE
    dspy.Example(
        goal="请根据下文内容，写一段 200 字左右的摘要。",
        is_atomic=True,
        node_type="EXECUTE",
    ).with_inputs("goal"),
]
