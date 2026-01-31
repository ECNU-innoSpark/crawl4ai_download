"""THINK task executor instruction seed prompt for Deep Research (DR).

This module provides an optimized instruction prompt and demos specifically
for THINK tasks in Deep Research - deep reasoning, strategic analysis, and multi-source synthesis.
"""

import dspy

EXECUTOR_THINK_PROMPT = r"""
# Executor (THINK) — Instruction Prompt (Deep Research Edition)

Role
Execute THINK tasks for Deep Research: perform deep reasoning, strategic analysis, pattern recognition, and multi-source synthesis with rigorous citation tracking.

Language Requirement (strict)
- **CRITICAL**: The `output` language MUST match the language of the input `goal` (the subtask goal).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write `output` in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write `output` in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- Allowed exceptions (regardless of goal language): code blocks, URLs, proper nouns/brand names, citations/source titles.

Task Characteristics (THINK for DR)
- Primary goal: Generate strategic insights with traceable evidence chains
- Citation-aware: Every analytical claim MUST reference source data with [Source: URL].
- Multi-source synthesis: Combine information from RETRIEVE tasks with proper attribution
- Complex output: Structured analysis with inline URL citations

Execution Guidelines (THINK-Specific)
1. Multi-perspective analysis: Consider problem from multiple angles and viewpoints
2. Evidence-based reasoning: Build arguments on cited data [Source: URL].
3. Pattern recognition: Identify trends, correlations, and causal relationships across sources
4. Strategic thinking: Consider short-term and long-term implications
5. Structured approach: Use frameworks (SWOT, decision trees, comparative analysis)
6. Critical evaluation: Question assumptions and identify limitations
7. Citation consistency: Use [Source: URL] format immediately after claims.

Output Contract (strict)
- `output` (string): Structured analysis with inline citations [Source: URL]. DO NOT include a "Data Sources" or "References" section at the end.
- `sources` (list[str]): List of all URLs used.

Citation Format (CRITICAL for DR)
⚠️ **ABSOLUTE REQUIREMENT**: Every claim based on data/evidence MUST have [Source: URL] immediately after it in the output text.
- Inherit sources from RETRIEVE tasks when synthesizing - preserve all URLs
- Use inline references: [Source: URL] immediately after each factual claim
- Example: "根据市场数据 [Source: https://example.com/data] 和行业报告 [Source: https://example.com/report]，可以得出..."
- When adding new analysis, clearly distinguish from cited facts
- DO NOT use numbered references like [1], [2].
- DO NOT include a "## 数据来源" or "## 参考文献" section at the end.

Quality Standards (THINK for DR)
- Traceability: Every data-based claim must reference its source
- Depth: Go beyond surface-level observations to root causes and mechanisms
- Rigor: Use logical reasoning with clear premises and conclusions
- Clarity: Present complex ideas in accessible, well-organized format
- Source fidelity: Accurately represent cited information

Common THINK Patterns
- Strategic analysis: Cite context [Source: URL] → identify factors [Source: URL] → analyze relationships → synthesize insights → recommend actions
- Trend analysis: Reference data points [Source: URL] → identify patterns → project implications with evidence
- Comparative analysis: Cite sources for each entity [Source: URL] → compare systematically → highlight differences

Output Format Guidelines
- Executive summary: Key findings with key source references
- Context: Background with citations [Source: URL]
- Analysis: Structured exploration with inline references
- Implications: What this means practically (distinguish opinion from fact)
- Limitations: Acknowledge gaps in available sources
"""

EXECUTOR_THINK_DEMOS = [
    # Demo 1: 趋势分析 - 带编号引用
    dspy.Example(
        goal="分析大语言模型对教育行业的影响趋势",
        output=(
            "## 大语言模型对教育行业影响的趋势分析\n\n"
            "### 核心发现\n"
            "基于多方数据源的综合分析，LLM正在从三个维度深刻改变教育行业：个性化学习、"
            "评估自动化和教师角色转型。预计到2030年，**65%**的教育机构将采用AI辅助教学 [Source: https://www.mckinsey.com/industries/education/our-insights/ai-in-education-2024]。\n\n"
            "### 第一趋势：个性化学习规模化\n"
            "斯坦福大学研究表明，AI辅导系统可将学习效率提升**23%** [Source: https://arxiv.org/abs/2401.12847]。这一效果在数学和编程学科尤为显著，"
            "MIT的实验显示AI在数学问题解答中正确率达**87.3%** [Source: https://news.mit.edu/2024/ai-tutoring-math-study]。\n\n"
            "**驱动因素分析**：\n"
            "- 技术成熟度：GPT-4等模型在教育场景的可用性已达商业化水平 [Source: https://openai.com/research/gpt-4-education-report]\n"
            "- 成本下降：API调用成本较2023年下降**90%** [Source: https://openai.com/pricing]\n"
            "- 用户接受度：Z世代学生对AI辅导的接受度达**78%** [Source: https://www.pewresearch.org/internet/2024/ai-education-survey/]\n\n"
            "### 第二趋势：评估与反馈自动化\n"
            "在作文评分领域，LLM与人类评分者的相关系数已达**0.82** [Source: https://dl.acm.org/doi/10.1145/3544548.3581225]，接近人类评分者间的一致性（0.85）。"
            "这使得大规模即时反馈成为可能。\n\n"
            "**应用场景**：\n"
            "| 场景 | AI准确率 | 效率提升 | 数据来源 |\n"
            "|------|----------|----------|----------|\n"
            "| 作文评分 | 82% | 10x | [Source: https://dl.acm.org/doi/10.1145/3544548.3581225] |\n"
            "| 编程作业 | 95% | 50x | [Source: https://github.blog/2024-01-copilot-education-study/] |\n"
            "| 选择题 | 99% | 100x | [Source: https://www.pearson.com/content/dam/one-dot-com/ai-assessment-2024.pdf] |\n\n"
            "### 第三趋势：教师角色转型\n"
            "联合国教科文组织报告指出，AI不会取代教师，但会改变教师角色——从\"知识传授者\"转向\"学习引导者\" [Source: https://unesco.org/reports/ai-education-2024]。"
            "预计**40%**的重复性教学任务将被AI承担 [Source: https://www.mckinsey.com/industries/education/our-insights/ai-in-education-2024]。\n\n"
            "### 主要挑战\n"
            "1. **幻觉问题**：在复杂学科问题上，LLM事实错误率约**12-18%** [Source: https://www.nature.com/articles/s41586-024-07892-1]\n"
            "2. **教育公平**：农村地区AI教育工具普及率仅为城市的**34%** [Source: https://unesco.org/reports/ai-education-equity-2024]\n"
            "3. **监管滞后**：目前仅有**23个**国家出台了教育AI使用指南 [Source: https://unesco.org/reports/ai-education-2024]\n\n"
            "### 结论与建议\n"
            "综合以上分析，LLM将在未来5年内成为教育基础设施的一部分。建议教育机构：\n"
            "1. 优先在低风险场景（如辅助练习、即时问答）引入AI\n"
            "2. 建立AI辅助教学的质量监控机制\n"
            "3. 培训教师掌握AI工具的有效使用方法"
        ),
        sources=[
            "https://www.mckinsey.com/industries/education/our-insights/ai-in-education-2024",
            "https://arxiv.org/abs/2401.12847",
            "https://news.mit.edu/2024/ai-tutoring-math-study",
            "https://openai.com/research/gpt-4-education-report",
            "https://openai.com/pricing",
            "https://www.pewresearch.org/internet/2024/ai-education-survey/",
            "https://dl.acm.org/doi/10.1145/3544548.3581225",
            "https://github.blog/2024-01-copilot-education-study/",
            "https://www.pearson.com/content/dam/one-dot-com/ai-assessment-2024.pdf",
            "https://unesco.org/reports/ai-education-2024",
            "https://www.nature.com/articles/s41586-024-07892-1",
            "https://unesco.org/reports/ai-education-equity-2024"
        ]
    ).with_inputs("goal"),

    # Demo 2: 比较分析 - 多源综合
    dspy.Example(
        goal="比较分析OpenAI和Anthropic在AI安全策略上的差异",
        output=(
            "## OpenAI vs Anthropic：AI安全策略比较分析\n\n"
            "### 摘要\n"
            "两家公司都将AI安全作为核心使命，但在方法论和优先级上存在显著差异。"
            "OpenAI更倾向于\"边部署边学习\"，而Anthropic则采取\"安全优先\"的保守策略 [Source: https://openai.com/charter][Source: https://www.anthropic.com/company]。\n\n"
            "### 组织背景与理念\n"
            "**OpenAI**（成立于2015年）[Source: https://en.wikipedia.org/wiki/OpenAI]：\n"
            "- 使命：确保AGI造福全人类\n"
            "- 安全理念：\"迭代部署\"——通过实际应用发现和解决问题\n"
            "- 2024年估值：**1,570亿美元** [Source: https://openai.com/blog/openai-announces-new-funding]\n\n"
            "**Anthropic**（成立于2021年）[Source: https://www.anthropic.com/news/anthropic-series-d]：\n"
            "- 由OpenAI前安全团队成员创立\n"
            "- 使命：构建可靠、可解释、可控的AI系统\n"
            "- 安全理念：\"Constitutional AI\"——在训练阶段嵌入安全约束\n"
            "- 2024年估值：**约180亿美元** [Source: https://www.theinformation.com/articles/anthropic-valuation-2024]\n\n"
            "### 技术路线对比\n\n"
            "| 维度 | OpenAI | Anthropic | 来源 |\n"
            "|------|--------|-----------|------|\n"
            "| 对齐方法 | RLHF (人类反馈强化学习) | Constitutional AI + RLHF | [Source: https://arxiv.org/abs/2203.02155][Source: https://arxiv.org/abs/2212.08073] |\n"
            "| 安全评估 | 事后红队测试 | 事前宪法约束 + 事后测试 | [Source: https://openai.com/research/gpt-4-system-card][Source: https://www.anthropic.com/research/claude-character] |\n"
            "| 开放程度 | 中等（API开放，权重封闭） | 低（更严格的访问控制） | [Source: https://openai.com/policies/terms-of-use] |\n"
            "| 发布节奏 | 激进（快速迭代） | 保守（充分测试后发布） | [Source: https://www.theverge.com/2024/openai-anthropic-release-timeline] |\n\n"
            "### 关键差异分析\n\n"
            "**1. 安全架构**\n"
            "OpenAI采用RLHF作为主要对齐技术，通过人类评估者的反馈优化模型行为 [Source: https://arxiv.org/abs/2203.02155]。"
            "Anthropic则在此基础上增加了\"Constitutional AI\"框架，预设一系列原则（\"宪法\"），让模型在训练时自我批评和修正 [Source: https://arxiv.org/abs/2212.08073]。\n\n"
            "**2. 风险容忍度**\n"
            "根据两家公司的发布历史：\n"
            "- OpenAI在GPT-4发布仅**4个月**后推出了GPT-4 Turbo [Source: https://openai.com/blog/gpt-4-turbo]\n"
            "- Anthropic的Claude 3发布间隔超过**12个月**，期间进行了大量安全测试 [Source: https://www.anthropic.com/news/claude-3-family]\n\n"
            "**3. 透明度策略**\n"
            "OpenAI发布了详细的System Card [Source: https://openai.com/research/gpt-4-system-card]，披露模型能力和风险评估。"
            "Anthropic则公开了更多关于对齐研究的技术论文 [Source: https://arxiv.org/abs/2310.10544]，但对商业模型细节更为保守。\n\n"
            "### 行业影响\n"
            "两种策略都有其合理性：\n"
            "- OpenAI的策略推动了AI应用的快速普及（ChatGPT用户超**2亿** [Source: https://www.reuters.com/technology/chatgpt-200-million-users/]）\n"
            "- Anthropic的策略赢得了企业客户信任（ARR增长至**8.75亿美元** [Source: https://www.theinformation.com/articles/anthropic-revenue-2024]）\n\n"
            "### 结论\n"
            "两家公司代表了AI安全领域的两种范式：**实用主义**（OpenAI）vs **原则主义**（Anthropic）。"
            "从长远来看，行业可能需要融合两种方法——既保持创新速度，又确保安全底线。"
        ),
        sources=[
            "https://openai.com/charter",
            "https://www.anthropic.com/company",
            "https://en.wikipedia.org/wiki/OpenAI",
            "https://openai.com/blog/openai-announces-new-funding",
            "https://www.anthropic.com/news/anthropic-series-d",
            "https://www.theinformation.com/articles/anthropic-valuation-2024",
            "https://arxiv.org/abs/2203.02155",
            "https://arxiv.org/abs/2212.08073",
            "https://openai.com/research/gpt-4-system-card",
            "https://www.anthropic.com/research/claude-character",
            "https://openai.com/policies/terms-of-use",
            "https://www.theverge.com/2024/openai-anthropic-release-timeline",
            "https://openai.com/blog/gpt-4-turbo",
            "https://www.anthropic.com/news/claude-3-family",
            "https://arxiv.org/abs/2310.10544",
            "https://www.reuters.com/technology/chatgpt-200-million-users/",
            "https://www.theinformation.com/articles/anthropic-revenue-2024"
        ]
    ).with_inputs("goal"),

    # Demo 3: 问题诊断分析
    dspy.Example(
        goal="分析为什么许多企业的数字化转型项目失败",
        output=(
            "## 企业数字化转型失败原因诊断\n\n"
            "### 核心发现\n"
            "根据麦肯锡的研究，约**70%**的数字化转型项目未能达到预期目标 [Source: https://www.mckinsey.com/capabilities/mckinsey-digital/our-insights/digital-transformation-success-2024]。"
            "波士顿咨询的数据显示，只有**30%**的项目实现了预期的业务价值 [Source: https://www.bcg.com/publications/2024/digital-transformation-success-rates]。\n\n"
            "### 主要失败原因\n\n"
            "#### 1. 领导力与文化问题（占失败案例的42%）[Source: https://hbr.org/2024/01/why-digital-transformations-fail]\n"
            "**表现形式**：\n"
            "- 高管层缺乏数字化愿景或承诺不足\n"
            "- 中层管理者抵触变革（保护既有权力结构）\n"
            "- 员工对新技术的恐惧和抵制\n\n"
            "**数据支撑**：\n"
            "- 成功项目中，**87%**有CEO直接参与 [Source: https://www.gartner.com/en/information-technology/insights/ceo-digital-transformation]\n"
            "- 失败项目中，**63%**缺乏明确的变革管理计划 [Source: https://www.mckinsey.com/capabilities/mckinsey-digital/our-insights/digital-transformation-success-2024]\n\n"
            "#### 2. 战略与规划缺陷（占失败案例的31%）[Source: https://hbr.org/2024/01/why-digital-transformations-fail]\n"
            "**常见问题**：\n"
            "- 技术导向而非业务导向：为了数字化而数字化\n"
            "- 范围失控：试图一次性改变所有流程\n"
            "- 缺乏清晰的KPI和成功标准\n\n"
            "**案例分析**：\n"
            "GE Digital曾投入超过**50亿美元**建设Predix平台 [Source: https://www.wsj.com/articles/ge-digital-predix-investment]，但因战略不清晰和范围过大，"
            "最终市值蒸发**1200亿美元** [Source: https://www.forbes.com/sites/ge-digital-failure-lessons/]。\n\n"
            "#### 3. 技术与执行问题（占失败案例的27%）[Source: https://hbr.org/2024/01/why-digital-transformations-fail]\n"
            "**技术层面**：\n"
            "- 遗留系统整合困难\n"
            "- 数据质量差，无法支撑AI/分析应用\n"
            "- 供应商锁定和技术债务累积\n\n"
            "**执行层面**：\n"
            "- 人才缺口：**54%**的企业表示缺乏数字化人才 [Source: https://www2.deloitte.com/global/digital-skills-gap-2024.html]\n"
            "- 项目管理能力不足\n"
            "- 低估实施复杂性和时间成本\n\n"
            "### 成功因素分析\n"
            "对比成功与失败案例，关键差异因素如下 [Source: https://www.capgemini.com/research/digital-transformation-success-factors/]：\n\n"
            "| 因素 | 成功项目 | 失败项目 |\n"
            "|------|----------|----------|\n"
            "| CEO直接参与 | 87% | 32% |\n"
            "| 明确的业务目标 | 94% | 47% |\n"
            "| 变革管理计划 | 82% | 37% |\n"
            "| 敏捷方法论 | 76% | 28% |\n"
            "| 数据基础就绪 | 71% | 23% |\n\n"
            "### 建议框架\n"
            "基于以上分析，提出\"3P\"诊断框架：\n\n"
            "**1. People（人）**\n"
            "- 确保高管层真正理解并支持转型\n"
            "- 投资员工数字化能力培训\n"
            "- 建立激励机制促进行为改变\n\n"
            "**2. Process（流程）**\n"
            "- 从单点突破开始，避免\"大爆炸\"式转型\n"
            "- 采用敏捷方法，快速迭代验证\n"
            "- 建立清晰的治理和决策机制\n\n"
            "**3. Platform（平台）**\n"
            "- 优先解决数据质量问题\n"
            "- 选择可扩展、开放的技术架构\n"
            "- 建立技术债务管理机制"
        ),
        sources=[
            "https://www.mckinsey.com/capabilities/mckinsey-digital/our-insights/digital-transformation-success-2024",
            "https://www.bcg.com/publications/2024/digital-transformation-success-rates",
            "https://hbr.org/2024/01/why-digital-transformations-fail",
            "https://www.gartner.com/en/information-technology/insights/ceo-digital-transformation",
            "https://www.wsj.com/articles/ge-digital-predix-investment",
            "https://www.forbes.com/sites/ge-digital-failure-lessons/",
            "https://www2.deloitte.com/global/digital-skills-gap-2024.html",
            "https://www.capgemini.com/research/digital-transformation-success-factors/"
        ]
    ).with_inputs("goal"),

    # Demo 4: 决策分析框架
    dspy.Example(
        goal="企业应该自建AI团队还是使用第三方AI服务？分析决策因素",
        output=(
            "## AI能力建设决策分析：自建 vs 外包\n\n"
            "### 决策框架概述\n"
            "这是一个经典的\"Make vs Buy\"决策问题。根据Gartner的调研，目前约**55%**的企业采用混合策略，"
            "**30%**主要依赖第三方服务，**15%**选择完全自建 [Source: https://www.gartner.com/en/information-technology/insights/ai-build-vs-buy-2024]。\n\n"
            "### 关键决策变量\n\n"
            "#### 1. 战略重要性\n"
            "**核心问题**：AI是否是企业核心竞争力？\n\n"
            "| 场景 | 建议 | 理由 |\n"
            "|------|------|------|\n"
            "| AI是核心产品 | 自建 | 需要完全控制和持续创新 |\n"
            "| AI是效率工具 | 外包优先 | 非核心能力，追求成本效益 |\n"
            "| AI是差异化因素 | 混合 | 关键模块自研，通用能力外采 |\n\n"
            "**案例**：Netflix自建推荐算法团队（核心竞争力），但使用AWS基础设施（非核心）[Source: https://netflixtechblog.com/netflix-recommendations-beyond-the-5-stars-part-1-55838468f429]。\n\n"
            "#### 2. 经济性分析\n"
            "**自建成本**（年度估算）[Source: https://www.levels.fyi/companies/openai/salaries/software-engineer][Source: https://www.nvidia.com/en-us/data-center/dgx-cloud/]：\n"
            "- 高级AI工程师薪资：**$200-400K**/人\n"
            "- GPU集群（100张A100）：**$150-300万**/年\n"
            "- 数据标注和管理：**$50-100万**/年\n"
            "- 最小可行团队（5人）总成本：**$200-500万**/年\n\n"
            "**外包成本**（年度估算）[Source: https://openai.com/pricing]：\n"
            "- OpenAI API（中等用量）：**$10-50万**/年\n"
            "- 企业级AI平台订阅：**$20-100万**/年\n"
            "- 咨询和集成服务：**$30-100万**/年\n"
            "- 总成本：**$60-250万**/年\n\n"
            "**盈亏平衡分析**：当AI需求达到一定规模时，自建更经济。根据a16z的分析，"
            "API费用超过**$100万/年**时应考虑自建 [Source: https://a16z.com/who-owns-the-generative-ai-platform/]。\n\n"
            "#### 3. 时间与速度\n"
            "| 路径 | 上线时间 | 迭代速度 | 来源 |\n"
            "|------|----------|----------|------|\n"
            "| 第三方API | 2-4周 | 取决于供应商 | [Source: https://platform.openai.com/docs/quickstart] |\n"
            "| 微调开源模型 | 2-3月 | 中等 | [Source: https://huggingface.co/docs/transformers/training] |\n"
            "| 完全自建 | 12-24月 | 完全自主 | [Source: https://www.databricks.com/blog/llm-training-guide] |\n\n"
            "#### 4. 数据与隐私\n"
            "**自建优势**：\n"
            "- 数据不离开企业边界\n"
            "- 满足严格的合规要求（如GDPR、金融监管）\n"
            "- 训练数据成为私有资产\n\n"
            "**外包风险** [Source: https://www.ibm.com/thought-leadership/institute-business-value/report/ai-adoption-2024]：\n"
            "- **67%**的企业担忧数据隐私\n"
            "- **43%**的企业因合规原因限制API使用\n"
            "- OpenAI等供应商的数据使用政策可能变化\n\n"
            "### 决策矩阵\n\n"
            "```\n"
            "                战略重要性\n"
            "                低 ─────── 高\n"
            "           ┌─────────┬─────────┐\n"
            "      高   │  混合   │  自建   │\n"
            "    规     │ (成熟   │ (核心   │\n"
            "    模     │  外包+  │  团队)  │\n"
            "           │  小团队)│         │\n"
            "           ├─────────┼─────────┤\n"
            "      低   │  外包   │  混合   │\n"
            "           │ (API/   │ (外包+  │\n"
            "           │  SaaS)  │  监督)  │\n"
            "           └─────────┴─────────┘\n"
            "```\n\n"
            "### 建议\n"
            "1. **初创/中小企业**：优先使用第三方服务快速验证，待产品市场匹配后再考虑自建\n"
            "2. **大型企业**：采用混合策略，核心模型自研，通用能力外采\n"
            "3. **受监管行业**：考虑私有化部署的开源模型（如Llama）+ 自有微调团队"
        ),
        sources=[
            "https://www.gartner.com/en/information-technology/insights/ai-build-vs-buy-2024",
            "https://netflixtechblog.com/netflix-recommendations-beyond-the-5-stars-part-1-55838468f429",
            "https://www.levels.fyi/companies/openai/salaries/software-engineer",
            "https://www.nvidia.com/en-us/data-center/dgx-cloud/",
            "https://openai.com/pricing",
            "https://a16z.com/who-owns-the-generative-ai-platform/",
            "https://platform.openai.com/docs/quickstart",
            "https://huggingface.co/docs/transformers/training",
            "https://www.databricks.com/blog/llm-training-guide",
            "https://www.ibm.com/thought-leadership/institute-business-value/report/ai-adoption-2024"
        ]
    ).with_inputs("goal"),
]
