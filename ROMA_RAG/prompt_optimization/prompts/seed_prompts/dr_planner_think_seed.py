"""THINK planner instruction seed prompt for Deep Research (DR).

This planner is selected when the *current task node* is TaskType.THINK and
Atomizer decided it is a PLAN node (i.e., the reasoning goal itself is too broad
and needs further decomposition).
"""

from __future__ import annotations

import dspy

from roma_dspy.core.signatures.base_models.subtask import SubTask
from roma_dspy.types.task_type import TaskType


PLANNER_DR_THINK_PROMPT = r"""
# Planner (THINK) — Deep Research Decomposition Instructions

Role
You are the planner for THINK-type tasks. When the reasoning/analysis goal is too broad, decompose it into multiple **independently executable** THINK subtasks. Focus ONLY on reasoning and analysis dimensions. Do NOT generate RETRIEVE subtasks - assume all necessary information is already available.

Language Requirement (STRICT)
- **CRITICAL**: All natural-language text you produce MUST match the language of the input `goal` (the user query).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write all subtask descriptions in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write all subtask descriptions in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- This applies to: `subtasks[*].goal` and any `subtasks[*].context_input`.
- Do NOT translate enum-like fields: `task_type` must be "THINK" (ONLY); dependency IDs must be strings like "0","1".
- Allowed exceptions (regardless of query language): code snippets, URLs, proper nouns/brand names, citation/source titles.

Task Count Constraint (STRICT)
- Generate 3–5 subtasks (5 is the hard limit). Do NOT return an empty list.

THINK Decomposition Principles
1) **Multi-lens split**: Break the analysis into complementary lenses (logical validity, causal mechanisms, counterexamples/boundaries, risks/trade-offs, actionable evaluation frameworks, etc.).
2) **Parallel-first**: Default to `dependencies: []`. Only add dependencies when a subtask truly requires outputs from another.
3) **Self-contained goals**: Each `goal` must specify the analysis target, method/framework, and scope boundaries. Avoid phrasing like "based on previous step" or "using earlier results".
4) **MECE**: Avoid overlap; do not repeat the same reasoning dimension across multiple subtasks.
5) **THINK-only**: ALL subtasks must be task_type="THINK". Do NOT generate RETRIEVE or WRITE subtasks.

Output Contract (STRICT)
- Return only a single JSON object with exactly two keys: `subtasks` and `dependencies_graph`. No extra keys, no prose, no Markdown fences.
- `subtasks` is list[SubTask]. Each SubTask MUST include:
  - `goal` (string)
  - `task_type` (MUST be "THINK" - no exceptions)
    **CRITICAL**: DO NOT use "RETRIEVE", "WRITE", "CODE_INTERPRET" or "IMAGE_GENERATION" - ONLY "THINK" is allowed.
  - `dependencies` (list[str], default [])
  - `context_input` (optional; only include when dependencies exist)
- `dependencies_graph`:
  - Keys/values are 0-based index strings
  - Must be consistent with `subtasks[*].dependencies`
  - Use {} when there are no dependencies (recommended)
"""


PLANNER_DR_THINK_DEMOS = [
    dspy.Example(
        goal="分析远程办公是否会提升团队生产力，并给出可落地的评估框架",
        subtasks=[
            SubTask(
                goal="分析\"远程办公提升生产力\"这一主张的关键因果机制与必要前提（例如任务类型、协作密度、管理制度、工具栈、员工自主性），明确哪些条件下可能成立。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="分析远程办公可能降低生产力的主要反机制与风险因素（例如沟通成本、创新受损、文化稀释、信息孤岛、评估扭曲），并给出可观测的预警信号清单。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="对比分析不同行业与团队规模下远程办公的适用性边界（例如研发类 vs 销售类、初创团队 vs 成熟组织），识别关键调节变量与交互效应。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="设计一个可落地的生产力评估框架：定义指标体系（交付周期/缺陷率/客户满意度/员工倦怠等）、对照组设置、时间窗口、以及避免\"表面指标\"误导的校正方法。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
        ],
        dependencies_graph={"0": [], "1": [], "2": [], "3": []},
    ).with_inputs("goal"),

    dspy.Example(
        goal="评估政府是否应在公共场所部署人脸识别监控：给出多维度分析框架",
        subtasks=[
            SubTask(
                goal="分析在人脸识别用于公共空间监控场景下的主要权利与法理风险（隐私权、行动自由、平等保护/歧视风险、正当程序），并提出需要满足的最低合法性与比例原则要件。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="分析人脸识别在公共安全/治安场景的有效性与可操作性（误报/漏报、数据漂移、部署维护成本、对犯罪预防的可验证影响），并提出评估指标与实验/审计设计。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="对比分析不同监管模式的利弊权衡（完全禁止 vs 有限许可 vs 透明公开），结合主要司法辖区（欧盟、美国、中国等）的政策实践，提炼可借鉴的限制条件与例外条款。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
            SubTask(
                goal="分析在明确的目标边界与约束条件下，人脸识别监控的伦理权衡与治理方案（例如用途限制、最小化采集、独立监督、透明度报告、申诉机制），并形成可落地的决策树/准入清单。",
                task_type=TaskType.THINK,
                dependencies=[],
            ),
        ],
        dependencies_graph={"0": [], "1": [], "2": [], "3": []},
    ).with_inputs("goal")
]


