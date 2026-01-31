"""WRITE task executor instruction seed prompt for Deep Research (DR).

This module provides an optimized instruction prompt and demos specifically
for WRITE tasks in Deep Research.

NOTE: In DR, WRITE is often decomposed into multiple section/paragraph-level
subtasks. Demos below intentionally model **bounded, aggregatable fragments**
to avoid context explosion and aggregation failures.
"""

import dspy

EXECUTOR_WRITE_PROMPT = r"""
# Executor (WRITE) — Instruction Prompt (Deep Research Edition)

Role
Execute WRITE tasks for Deep Research: synthesize findings from multiple sources into comprehensive, well-cited research content.

Language Requirement (strict)
- **CRITICAL**: The `output` language MUST match the language of the input `goal` (the subtask goal).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write `output` in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write `output` in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- Allowed exceptions (regardless of goal language): code blocks, URLs, proper nouns/brand names, citations/source titles, and required verbatim quotes.

Task Characteristics (WRITE for DR)
- Primary goal: Synthesize research findings into publication-quality writing
- Citation-critical: Every factual claim MUST have a source reference [Source: URL] immediately following it.
- Multi-source integration: Combine insights from RETRIEVE and THINK tasks
- Structured output: Professional report format with clear sections and inline citations

Execution Guidelines (WRITE for DR)
1. Scope adherence: Write ONLY what the goal asks for (a specific chapter/section/table). Do NOT expand into unrelated sections or create a full report structure.
2. Inherit dependencies: Use data/findings from previous RETRIEVE/THINK tasks mentioned in `context_input`.
3. Inline citations: Every factual claim MUST have [Source: URL] immediately after it.
4. Professional depth: Use full paragraphs with analysis and synthesis, not just bullet lists (tables are fine when appropriate).
5. Keep it aggregatable: Output length should be proportional to the goal scope (1-3 sections = 400-800 words; single table + explanation = 200-400 words).
6. No bibliography: DO NOT create a "## 数据来源" or "## 参考文献" section at the end.

Output Contract (strict)
- `output` (string): The requested section/chapter/table with inline citations [Source: URL]. NO reference section at the end.
- `sources` (list[str]): Complete list of URLs cited in the output.

Quality Standards
- **Accuracy**: Every number, date, and fact must be correctly cited
- **Traceability**: Readers can verify any claim via inline URLs
- **Clarity**: Professional tone, clear structure (use headers/bold/tables as needed)
- **Aggregatability**: Output can be merged with other fragments without creating redundancy or structural conflicts
"""

EXECUTOR_WRITE_DEMOS = [
    # Demo 1: 撰写引言与背景章节（模拟实际 planner 输出的 goal 风格）
    dspy.Example(
        goal="撰写报告的引言与研究方法章节：阐述研究背景、目标读者、研究范围、数据来源与评价方法论。",
        output=(
            "## 一、引言与研究方法\n\n"
            "### 1.1 研究背景\n\n"
            "近两年，AI代码生成工具从\"IDE辅助补全\"演进为覆盖跨文件修改与问题修复的工程助手。SWE-bench Verified基准显示，前沿模型在真实开源仓库中的问题修复率已达**60-80%** [Source: https://openai.com/index/introducing-swe-bench-verified/]，标志着从\"辅助编程\"到\"自主编程\"的关键跨越。以GitHub Copilot为代表的产品用户数已突破**180万** [Source: https://github.blog/2024-copilot-annual-report/]，企业采用率达**46%** [Source: https://www.gartner.com/en/documents/ai-development-tools-2024]。这推动企业将关注点从\"能否生成代码\"转向\"如何在真实仓库与流程中稳定交付\"。\n\n"
            "### 1.2 目标读者与研究范围\n\n"
            "本报告面向产品负责人、研发管理者与安全/合规角色，系统讨论引入AI代码生成工具时的价值（交付速度、开发者体验、知识迁移）、风险（安全漏洞、版权/合规、数据泄露、供应商锁定）以及落地路线图。评价维度以\"效率—质量—安全—合规—成本\"为主。\n\n"
            "### 1.3 数据来源与方法\n\n"
            "研究材料主要来自公开评测（SWE-bench、HumanEval）、行业报告（Gartner、GitHub官方研究）与学术论文。采用\"文献综述+定量对比+案例分析\"方法，优先引用可验证的一手数据与权威机构材料。"
        ),
        sources=[
            "https://openai.com/index/introducing-swe-bench-verified/",
            "https://github.blog/2024-copilot-annual-report/",
            "https://www.gartner.com/en/documents/ai-development-tools-2024",
        ],
    ).with_inputs("goal"),

    # Demo 2: 撰写核心数据对比章节（带表格）
    dspy.Example(
        goal="撰写\"技术演进与性能对比\"章节：展示主流AI代码生成模型在HumanEval基准上的性能演进轨迹，并用表格对比当前主要玩家的用户规模与核心能力差异。",
        output=(
            "## 二、技术演进与性能对比\n\n"
            "### 2.1 性能演进轨迹\n\n"
            "以HumanEval基准为例，主流模型性能在过去三年实现快速提升：\n\n"
            "| 模型 | 发布时间 | HumanEval pass@1 | 提升幅度 |\n"
            "|------|----------|------------------|----------|\n"
            "| Codex | 2021-08 | 28.8% | 基准 |\n"
            "| GPT-3.5 | 2022-11 | 48.1% | +19.3pp |\n"
            "| GPT-4 | 2023-03 | 67.0% | +18.9pp |\n"
            "| Claude 3.5 Sonnet | 2024-06 | 92.0% | +25.0pp |\n"
            "| GPT-4o | 2024-05 | 90.2% | +23.2pp |\n\n"
            "[Source: https://www.anthropic.com/news/claude-3-5-sonnet][Source: https://openai.com/research/gpt-4o-system-card]\n\n"
            "当前顶尖模型在代码生成准确率上已超**90%**，接近专家级水平。这一演进速度表明，从早期的简单补全到当前的跨文件修改能力，AI代码生成技术已进入实用阶段。\n\n"
            "### 2.2 主要玩家对比\n\n"
            "市场格局呈现明显的梯队分化。第一梯队以GitHub Copilot为代表，用户数达**180万+**，占据**55%**市场份额 [Source: https://github.blog/2024-copilot-annual-report/]；新兴工具Cursor以月活**50万+**的增速成为最受关注的竞争者 [Source: https://cursor.sh/about]。中国市场方面，通义灵码依托阿里云生态已积累**200万+**用户，支持主流IDE集成 [Source: https://www.caict.ac.cn/kxyj/qwfb/ztbg/ai-coding-2024.htm]；文心Comate则专注企业客户，服务企业数突破**5000家** [Source: https://www.caict.ac.cn/kxyj/qwfb/ztbg/ai-coding-2024.htm]。从用户规模与生态整合能力看，跨平台支持与企业级治理能力成为差异化竞争的关键。"
        ),
        sources=[
            "https://www.anthropic.com/news/claude-3-5-sonnet",
            "https://openai.com/research/gpt-4o-system-card",
            "https://github.blog/2024-copilot-annual-report/",
            "https://cursor.sh/about",
            "https://www.caict.ac.cn/kxyj/qwfb/ztbg/ai-coding-2024.htm",
        ],
    ).with_inputs("goal"),

    # Demo 3: 撰写风险分析章节（基于前置任务的检索结果）
    dspy.Example(
        goal="撰写\"安全与合规风险\"章节：分析代码生成工具在企业落地中的主要风险点，并给出可落地的治理控制项与验收标准。",
        output=(
            "## 三、安全与合规风险\n\n"
            "### 3.1 安全漏洞引入风险\n\n"
            "尽管AI代码生成工具显著提升开发效率，但也可能引入安全隐患。研究表明，使用Copilot的开发者代码安全漏洞引入率增加**40%** [Source: https://arxiv.org/abs/2403.12345]，主要原因是对生成代码的过度信任可能降低代码审查质量。在企业落地场景中，这一风险尤其需要关注高风险API调用（如文件操作、网络请求、权限提升等）。治理层面需要在CI/CD流程中强制启用SAST/SCA扫描，并建立针对AI生成代码的专项审查清单，确保安全门禁的有效性。\n\n"
            "### 3.2 版权与许可合规\n\n"
            "AI代码生成工具面临版权争议：训练数据是否侵犯开源协议、生成代码的版权归属、GPL等Copyleft协议的传染性问题 [Source: https://www.eff.org/deeplinks/2024/ai-copyright]。企业在将生成代码并入产品时，可能面临开源许可证义务（署名、NOTICE、源代码披露等）被忽视的风险。建议在PR门禁中启用开源许可证扫描工具（如Black Duck、Snyk），要求生成代码在提交描述中注明来源，并建立合规豁免审批流程。\n\n"
            "### 3.3 敏感数据泄露\n\n"
            "将私有代码或密钥片段发送给外部模型可能导致数据泄露。需要按项目敏感度分级控制可用模型与上下文权限，并在开发环境中启用DLP与密钥扫描，确保敏感信息不会通过AI工具外泄。"
        ),
        sources=[
            "https://arxiv.org/abs/2403.12345",
            "https://www.eff.org/deeplinks/2024/ai-copyright",
        ],
    ).with_inputs("goal"),
]


