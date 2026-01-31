"""WRITE planner instruction seed prompt for Deep Research (DR).

This planner is selected when the *current task node* is TaskType.WRITE and
Atomizer decided it is a PLAN node (i.e., the writing goal itself is too broad
and needs further decomposition into sections/steps).
"""

from __future__ import annotations

import dspy

from roma_dspy.core.signatures.base_models.subtask import SubTask
from roma_dspy.types.task_type import TaskType


PLANNER_DR_WRITE_PROMPT = r"""
# Planner (WRITE) — Deep Research Decomposition Instructions

Role
You are the planner for WRITE-type tasks. When the writing goal is too broad, decompose it into **clear, well-scoped, sequential writing subtasks** (sections/chapters/components). You must only plan, not write the final content.

Language Requirement (STRICT)
- **CRITICAL**: All natural-language text you produce MUST match the language of the input `goal` (the user query).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write all subtask descriptions in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write all subtask descriptions in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- This applies to: `subtasks[*].goal` and any `subtasks[*].context_input`.
- Do NOT translate enum-like fields: `task_type` must be one of "WRITE"/"RETRIEVE"/"THINK"; dependency IDs must be strings like "0","1".
- Allowed exceptions (regardless of query language): code snippets, URLs, proper nouns/brand names, citation/source titles.

Task Count Constraint (STRICT)
- Generate 3–5 subtasks (5 is the hard limit). Do NOT return an empty list.

WRITE Decomposition Principles
1) **Narrative sequencing**: Writing is typically sequential (e.g., background → analysis → conclusion). Use `dependencies` to explicitly encode order.
2) **One concrete deliverable per subtask**: e.g., "write introduction & scope", "write methodology & data sources", "write main analysis + tables", "write conclusion + recommendations + limitations".
3) **No vague goals**: Each `goal` must specify audience, scope, and required structure/content points.
4) **Assume data availability**: All necessary data/citations should already be available from parent task context or dependencies. DO NOT create RETRIEVE subtasks. Focus only on structuring and writing content.
5) **Use THINK for synthesis**: If complex analysis or cross-section synthesis is needed before writing, create a THINK subtask. Otherwise, use WRITE subtasks only.

Output Contract (STRICT)
- Return only a single JSON object with exactly two keys: `subtasks` and `dependencies_graph`. No extra keys, no prose, no Markdown fences.
- `subtasks` is list[SubTask]. Each SubTask MUST include:
  - `goal` (string)
  - `task_type` (ONLY "WRITE" or "THINK"; DO NOT use "RETRIEVE")
    **CRITICAL**: 
    - DO NOT use "RETRIEVE" - all data should come from parent context or dependencies
    - DO NOT use "CODE_INTERPRET" or "IMAGE_GENERATION" - these are NOT supported in this profile
    - Use "THINK" only when complex synthesis/analysis is needed before writing
    - Use "WRITE" for all actual content creation subtasks
  - `dependencies` (list[str])
  - `context_input` (optional; when dependencies exist, briefly state what to consume from them)
- `dependencies_graph`:
  - Keys/values are 0-based index strings
  - Must be consistent with `subtasks[*].dependencies`
"""


PLANNER_DR_WRITE_DEMOS = [
    dspy.Example(
        goal="撰写一份面向产品与研发管理者的报告：总结\"AI 代码生成工具\"在企业落地的价值、风险与实施路线图",
        subtasks=[
            SubTask(
                goal="撰写报告的引言与背景章节：界定 AI 代码生成工具（如 GitHub Copilot、Cursor、CodeWhisperer 等）的能力边界与适用场景，明确报告读者、范围、以及评价维度（效率/质量/安全/合规/成本），并简要概述当前企业采用现状。",
                task_type=TaskType.WRITE,
                dependencies=[],
            ),
            SubTask(
                goal="撰写\"价值与收益\"章节：用结构化方式说明可量化收益（交付速度、缺陷率、开发者体验）与不可量化收益（知识迁移、最佳实践扩散），并用表格呈现关键价值点与典型案例。",
                task_type=TaskType.WRITE,
                dependencies=["0"],
                context_input="沿用背景章节确立的范围与评价维度框架。",
            ),
            SubTask(
                goal="撰写\"风险与治理\"章节：覆盖安全漏洞、版权/合规、模型幻觉、数据泄露、供应商锁定等风险，并给出可执行的治理控制项（代码审查、策略/权限、评估基准、红队/安全扫描）。",
                task_type=TaskType.WRITE,
                dependencies=["0"],
                context_input="保持与背景章节一致的术语与评价维度。",
            ),
            SubTask(
                goal="撰写\"实施路线图\"章节：给出分阶段落地方案（试点→扩面→规模化），包含组织角色分工、工具选型原则、KPI 设计与验收标准、以及回滚/停用触发条件。",
                task_type=TaskType.WRITE,
                dependencies=["1", "2"],
                context_input="综合收益与风险章节的关键结论，形成可执行路线图。",
            ),
            SubTask(
                goal="撰写报告的总结与建议章节：汇总核心洞察，提供决策建议矩阵（适用场景 vs 不适用场景），并附录进一步阅读资源与参考文献。",
                task_type=TaskType.WRITE,
                dependencies=["0", "1", "2", "3"],
                context_input="综合前面所有章节的关键发现与结论。",
            ),
        ],
        dependencies_graph={
            "0": [],
            "1": ["0"],
            "2": ["0"],
            "3": ["1", "2"],
            "4": ["0", "1", "2", "3"],
        },
    ).with_inputs("goal"),

    dspy.Example(
        goal="撰写一份关于中国生成式AI监管框架的政策解读报告（面向企业合规与产品负责人）",
        subtasks=[
            SubTask(
                goal="撰写报告的背景与范围章节：界定\"生成式AI服务\"的适用范围、监管目标与关键术语（如算法备案、内容安全、数据合规），说明企业常见的合规触发点与误区，并概述主要监管法规框架与主管部门。",
                task_type=TaskType.WRITE,
                dependencies=[],
            ),
            SubTask(
                goal="撰写\"监管要求拆解\"章节：按主题拆解要求（训练数据与版权/隐私、内容安全与审核机制、模型/算法备案与评估、标识与透明度、用户/日志管理），并用清单化结构输出企业应做事项与法规依据。",
                task_type=TaskType.WRITE,
                dependencies=["0"],
                context_input="沿用背景章节确立的术语定义与监管框架。",
            ),
            SubTask(
                goal="撰写\"典型场景与合规要点\"章节：针对不同产品形态（ToC/ToB/API/私有化）与典型应用场景（对话助手、内容生成、代码辅助等），说明差异化的合规重点与常见陷阱。",
                task_type=TaskType.WRITE,
                dependencies=["1"],
                context_input="基于监管要求拆解章节，针对不同场景进行具体化解读。",
            ),
            SubTask(
                goal="撰写\"落地合规路线图\"章节：给出从 0 到 1 的合规实施步骤（组织角色、流程、技术控制、文档与审计、应急响应），并提供实施优先级建议与检查清单。",
                task_type=TaskType.WRITE,
                dependencies=["1", "2"],
                context_input="综合监管要求拆解与场景要点，形成可执行的实施路径。",
            ),
        ],
        dependencies_graph={"0": [], "1": ["0"], "2": ["1"], "3": ["1", "2"]},
    ).with_inputs("goal")
]


