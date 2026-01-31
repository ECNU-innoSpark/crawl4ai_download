"""RETRIEVE planner instruction seed prompt for Deep Research (DR).

This planner is selected when the *current task node* is TaskType.RETRIEVE and
Atomizer decided it is a PLAN node (i.e., the retrieval goal itself is too broad
and needs further decomposition).
"""

from __future__ import annotations

import dspy

from roma_dspy.core.signatures.base_models.subtask import SubTask
from roma_dspy.types.task_type import TaskType


PLANNER_DR_RETRIEVE_PROMPT = r"""
# Planner (RETRIEVE) — Deep Research Decomposition Instructions

Role
You are the planner for RETRIEVE-type tasks. When the RETRIEVE goal is too broad, decompose it into multiple **parallelizable, non-overlapping, self-contained** RETRIEVE subtasks. You must only plan, not execute.

Language Requirement (STRICT)
- **CRITICAL**: All natural-language text you produce MUST match the language of the input `goal` (the user query).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write all subtask descriptions in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write all subtask descriptions in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- This applies to: `subtasks[*].goal` and any `subtasks[*].context_input`.
- Do NOT translate enum-like fields: `task_type` must be one of "RETRIEVE"/"THINK"/"WRITE"; dependency IDs must be strings like "0","1".
- Allowed exceptions (regardless of query language): code snippets, URLs, proper nouns/brand names, citation/source titles.

Task Count Constraint (STRICT)
- Generate 5–8 subtasks to ensure comprehensive source coverage for deep research.
- **Target: 6-8 subtasks** for typical broad research goals (e.g., market analysis, policy review).
- Minimum: 5 subtasks. Maximum: 8 subtasks (hard limit).
- Rationale: More subtasks = more diverse sources = higher quality research (target 50+ URL citations).
- Do NOT return an empty list.

RETRIEVE Decomposition Principles
1) **Parallel-first**: Default every subtask to `dependencies: []`. Only add dependencies when truly necessary.
2) **Source / perspective split**: Split retrieval by source types or orthogonal sub-areas, e.g.:
   - Official/regulatory/standards vs academic papers vs industry reports/market data vs news/announcements/case studies
   - Different regions, different time windows, different measurement definitions
3) **Executable & self-contained goals**: Each `goal` must specify:
   - Exactly what data points / fields to retrieve (avoid vague verbs like “research/understand/explore”)
   - Preferred source types (e.g., official releases, annual reports, whitepapers, statistical agencies, paper databases)
   - Time range (e.g., 2023–2025, “as of today”)
4) **MECE**: Avoid duplicate retrieval across subtasks; keep them mutually exclusive where possible.

Output Contract (STRICT)
- Return only a single JSON object with exactly two keys: `subtasks` and `dependencies_graph`. No extra keys, no prose, no Markdown fences.
- `subtasks` is list[SubTask]. Each SubTask MUST include:
  - `goal` (string)
  - `task_type` (preferably all "RETRIEVE"; optionally include limited "THINK"/"WRITE" when necessary)
    **CRITICAL**: DO NOT use "CODE_INTERPRET" or "IMAGE_GENERATION" - these are NOT supported in this profile.
  - `dependencies` (list[str], default [])
  - `context_input` (optional; only include when dependencies exist)
- `dependencies_graph`:
  - Keys/values are 0-based index strings
  - Must be consistent with `subtasks[*].dependencies`
  - Use {} when there are no dependencies (recommended)

Strict Output Shape Example
{
  "subtasks": [
    {"goal": "...", "task_type": "RETRIEVE", "dependencies": []}
  ],
  "dependencies_graph": {}
}
"""


PLANNER_DR_RETRIEVE_DEMOS = [
    dspy.Example(
        goal="检索 2024 年全球人工智能（AI）市场规模、增长率与主要细分领域数据",
        subtasks=[
            SubTask(
                goal="检索 2024 年全球 AI 市场总规模与 CAGR（含定义口径），优先使用权威统计/研究机构报告（如 Statista、IDC、Gartner、麦肯锡等）并记录发布日期与统计范围。要求：至少 5-10 个不同来源；给出 2-3 个机构口径差异对比与关键数字。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年生成式 AI（Generative AI）细分市场规模、增速与 2030 预测，优先使用研究机构/咨询公司报告并注明口径差异。要求：至少 5-10 个不同来源；包含 2-3 个具体厂商/产品的市场案例或增长数据。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年 AI 基础设施市场（AI 芯片、云计算、数据中心）规模与增速，优先使用半导体行业报告与云服务商财报数据。要求：至少 5-10 个不同来源；包含 2-3 个具体公司或项目的量化指标。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年企业 AI 采用率、投资规模与主要应用场景（按行业/功能分类），优先使用咨询公司调研报告（如德勤、普华永道、麦肯锡等）。要求：至少 5-10 个不同来源；按行业给出 2-3 个具体应用案例。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年全球 AI 市场的区域分布数据（北美、欧洲、亚太、中国的份额与增速），优先使用区域性统计机构或多区域比较研究报告。要求：至少 5-10 个不同来源；包含 2-3 个跨区域对比的数据点。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年 AI 投融资数据（总额、轮次分布、头部交易），优先使用 VC 数据库（如 Crunchbase、PitchBook）与行业分析报告。要求：至少 5-10 个不同来源；列出 2-3 笔头部交易的金额与时间。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2024 年主要 AI 厂商市场份额与竞争格局（OpenAI、Google、Microsoft、Anthropic 等），优先使用行业分析报告与公司财报数据。要求：至少 5-10 个不同来源；给出 2-3 个厂商的份额/营收/用户规模数据。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
        ],
        dependencies_graph={"0": [], "1": [], "2": [], "3": [], "4": [], "5": [], "6": []},
    ).with_inputs("goal"),

    dspy.Example(
        goal="检索欧盟《AI法案》（EU AI Act）在 2023-至今对大模型/高风险AI系统的合规要求与官方解释材料",
        subtasks=[
            SubTask(
                goal="检索欧盟官方渠道发布的《EU AI Act》正式文本/条例内容（最终通过版本），并提取与「高风险AI系统」「通用/基础模型」相关的条款定位与关键要求。要求：至少 5-10 个不同来源；列出 2-3 条关键条款编号与原文要点。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索《EU AI Act》中关于「透明度义务」「信息提供要求」「合格评定」「CE标识」的具体条款与技术标准，优先使用官方文本与标准文档。要求：至少 5-10 个不同来源；标注 2-3 个关键条款或标准编号。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索欧盟委员会/EU AI Office 在 2023-至今发布的官方FAQ、实施指南、解释性文件或合规路线图，重点提取企业落地的操作性要求与时间表。要求：至少 5-10 个不同来源；给出 2-3 个明确时间节点或义务触发点。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2023-至今主要云厂商/AI平台（如 AWS、Azure、GCP、OpenAI 等）针对 EU AI Act 的公开合规声明、白皮书或产品合规页面，提取其合规策略与责任边界。要求：至少 5-10 个不同来源；覆盖至少 2-3 家厂商的具体合规举措。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2023-至今欧盟成员国层面的 AI Act 实施细则、国家级补充法规或监管机构公告（如德国、法国、荷兰等），提取各国差异化要求。要求：至少 5-10 个不同来源；覆盖至少 2-3 个成员国的差异点。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2023-至今学术/智库/行业分析对 EU AI Act 的关键争议点与对大模型开发流程影响（如数据治理、评测与报告、模型透明度），并提取不同解读与出处。要求：至少 5-10 个不同来源；明确支持与批判观点各 2-3 个来源。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
        ],
        dependencies_graph={"0": [], "1": [], "2": [], "3": [], "4": [], "5": []},
    ).with_inputs("goal"),

    dspy.Example(
        goal="检索 2020-至今东南亚城市的城市热缓解措施，并关注是否包含健康系统适应与社会公平条款",
        subtasks=[
            SubTask(
                goal="检索东南亚主要城市（新加坡、曼谷、雅加达、吉隆坡、马尼拉等）自 2020 年以来的城市热缓解物理工程措施（城市绿化、反照率材料、遮阴基础设施），优先使用政府官网与官方规划文件。要求：至少 5-10 个不同来源；覆盖 2-3 个城市的具体工程案例与数据。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索东南亚城市自 2020 年以来的热岛监测与数据收集系统（温度监测网络、卫星遥感、热地图发布），优先使用气象部门/环境部门官方数据源。要求：至少 5-10 个不同来源；给出 2-3 个城市的监测网络或数据发布细节。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索东南亚地区自 2020 年以来的热浪预警系统与公众健康宣传措施（预警标准、发布渠道、健康建议），优先使用卫生部门/市政部门/WHO 等权威来源。要求：至少 5-10 个不同来源；给出 2-3 个城市的预警阈值或触发条件。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索东南亚城市的高温健康风险应对措施（医院应急容量、冷却中心/避暑场所、户外工作时间限制），优先使用卫生与劳工部门政策文件。要求：至少 5-10 个不同来源；覆盖 2-3 个城市的具体政策或设施数据。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索东南亚城市热适应政策中对弱势群体（低收入群体、户外劳动者、老年人、儿童）的公平性设计与资源配置条款（补贴、定向服务、社区覆盖），优先使用政府/NGO/国际组织评估报告。要求：至少 5-10 个不同来源；给出 2-3 个具体公平性条款或项目案例。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
            SubTask(
                goal="检索 2021-至今关于东南亚城市热适应的多城市比较研究或综述（学术论文/智库报告），提取各城市政策组合差异、「健康+公平」整合程度与最佳实践案例。要求：至少 5-10 个不同来源；明确 2-3 个跨城市对比结论与案例。",
                task_type=TaskType.RETRIEVE,
                dependencies=[],
            ),
        ],
        dependencies_graph={"0": [], "1": [], "2": [], "3": [], "4": [], "5": []},
    ).with_inputs("goal")
]


