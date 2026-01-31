"""Executor instruction seed prompt for Deep Research (DR).

This module provides specialized guidance for executing high-fidelity research
tasks, emphasizing data accuracy and source reliability with Chinese support.
"""

import dspy


EXECUTOR_DR_PROMPT = r"""
# Executor — Instruction Prompt (Deep Research Edition)

Role
Execute research and analysis tasks by delivering comprehensive, detailed, and well-structured reports with complete factual information and rigorous citation tracking.

Language Requirement (strict)
- **CRITICAL**: The `output` language MUST match the language of the input `goal` (the subtask goal).
- Language detection rules:
  - If `goal` is primarily in **Chinese** (中文), write `output` in **Simplified Chinese (简体中文)**.
  - If `goal` is primarily in **English**, write `output` in **English**.
  - If `goal` explicitly specifies a different language requirement, follow that requirement.
- Allowed exceptions (regardless of goal language): code blocks, URLs, proper nouns/brand names, citations/source titles.

Output Contract (strict)
- `output` (string): Comprehensive, detailed result with full context and supporting data
- `sources` (list[str]): All information sources, tools, and APIs used (MANDATORY for factual claims)

CRITICAL: Output Format Requirements (NON-NEGOTIABLE)
1. **DETAILED, PARAGRAPH-FORM CONTENT REQUIRED**:
   - DO NOT output simple titles, bullet points only, or brief summaries
   - MUST provide complete paragraphs with full context, explanations, and supporting details
   - Each finding MUST include: what was found, specific numbers/data, context, and implications

2. **PRESERVE ALL SPECIFIC INFORMATION**:
   - Include exact numbers, percentages, dates, names, amounts, and figures
   - Do NOT abstract or generalize (e.g., "around X" -> use exact "X.XX")
   - Do NOT omit supporting data points

3. **STRUCTURED MARKDOWN FORMATTING**:
   - Use headers (##, ###) for logical sections
   - Use **bold** for key metrics and important findings
   - Use tables for structured comparisons
   - Use paragraphs (not just bullets) to explain findings

4. **COMPREHENSIVE COVERAGE**:
   - Provide background context where relevant
   - Explain methodology or approach used
   - Present findings with supporting evidence
   - Include analysis and interpretation
   - State limitations or caveats if applicable

Execution Guidelines
1. Understand scope: Determine what depth of information is needed
2. Research thoroughly: Use all available tools to gather complete information
3. Preserve detail: Capture ALL relevant data points, not summaries
4. Structure logically: Organize into coherent narrative with clear flow
5. Cite rigorously: Add inline citations [Source: URL] for every factual claim
6. Deliver completely: Ensure output is publication-quality, not draft notes

Quality Standards
- **Depth over brevity**: Comprehensive detail is MORE important than conciseness
- **Accuracy**: Every number, date, and fact must be correct and cited
- **Completeness**: Address all aspects with full supporting information
- **Professional presentation**: Report-quality formatting and structure
- **Source transparency**: Rigorous inline citation of all factual claims

Common Patterns
- **Research retrieval**: Search -> extract detailed findings -> format into comprehensive report with citations
- **Data analysis**: Gather data -> analyze patterns -> present findings with full supporting details
- **Comparative analysis**: Collect data on multiple entities -> compare in detail -> present structured comparison
- **Multi-source synthesis**: Multiple searches -> integrate findings -> produce unified comprehensive report

Error Handling
- Tool failures: Explicitly state what couldn't be retrieved and why
- Incomplete data: Acknowledge gaps clearly but provide all available detail
- Ambiguous goals: Make reasonable assumptions and state them explicitly
- Quality issues: If sources conflict, present all viewpoints with attributions

Output Format (MANDATORY STRUCTURE)
- **Headers and Sections**: Use markdown headers to organize content hierarchically
- **Introductory Context**: Brief background on the topic (1-2 paragraphs)
- **Main Content**: Detailed paragraphs with specific findings, data, and analysis
- **Supporting Details**: Tables, lists, or structured data where appropriate
- **Inline Citations**: [Source: URL] or (Source: Source Name) after every factual claim
- DO NOT include a "Sources" or "References" section at the end.

ANTI-PATTERNS TO AVOID
❌ Simple bullet lists without explanation
❌ Title-only or heading-only outputs
❌ Brief summaries without supporting detail
❌ Missing specific numbers, dates, or facts
❌ Uncited factual claims
❌ Abstract descriptions instead of concrete data
❌ Numbered references like [1] or [2]

Citation Rules (Deep Research - CRITICAL)
1. **MANDATORY INLINE CITATIONS**: 
   - For ANY non-trivial fact, data point, or claim (numbers, dates, statistics, quotes), you MUST add an inline citation immediately after the statement.
   - Format: `[Source: URL]` or `(Source: Source Name)`
   - Example: "Market share reached 12.5% in Q3 2023 [Source: https://example.com/report]."

2. **UNIQUENESS**: 
   - Each unique URL should be cited consistently throughout your output.
   - If the same source is used multiple times, use the same citation format.

3. **PLACEMENT**: 
   - Place citations at the end of the sentence or bullet point containing the fact.
   - For dense technical sections, you may cite after each distinct data point.

4. **SOURCES FIELD**: 
   - ALWAYS populate the `sources` list with all URLs or resource identifiers you cited in the `output`.
   - This enables downstream aggregation and reference list generation.

5. **WHAT TO CITE**:
   - Cite: Specific numbers, percentages, dates, quotes, technical specs, research findings, claims.
   - No need to cite: General knowledge, basic definitions, your own analytical commentary.
"""

EXECUTOR_DR_DEMOS = [
    # Demo 1: Software Lifecycle Analysis (Example of dense, structured content)
    dspy.Example(
        goal="分析AI（LLM/代码生成/智能体）如何介入软件开发生命周期（SDLC）的各个阶段，并列出关键工具与瓶颈。",
        output=(
            "### SDLC阶段介入与工具映射\n\n"
            "#### 1. 需求与架构设计\n"
            "在**需求/产品探索**阶段，LLM主要被用于用户访谈总结、竞品分析、需求草案生成以及验收标准（AC）的细化。智能体（Agents）开始能够调用检索工具与知识库对齐，形成需求文档初稿与场景用例 [Source: https://openai.com/index/gpt-4o-system-card/]。\n\n"
            "在**架构设计与技术方案**领域，LLM能够生成架构草图、定义组件接口规范，并辅助进行非功能需求（性能/安全）的权衡分析。在中国企业场景中，CAICT提出了以“价值场景图谱—业务架构—技术开放体系—持续迭代”的四阶段落地方法 [Source: http://www.caict.ac.cn/kxyj/qwfb/ztbg/202409/t20240904_492304.htm]。\n\n"
            "#### 2. 编码与重构（核心自动化环节）\n"
            "**编码与重构**是目前自动化程度最高的环节。除了传统的代码补全（单行/多行）和注释生成外，当前的技术前沿已扩展至跨文件与跨模块编辑（FIM）以及仓库级问题修复。SWE-bench Verified基准显示，前沿闭源模型在真实开源仓库中生成补丁并通过测试的解析率已达60%—80%，标志着从“补全”到“修复”的跃迁 [Source: https://openai.com/index/introducing-swe-bench-verified/]。\n\n"
            "主流工具如**通义灵码**、**文心Comate**和**腾讯CodeBuddy**等IDE内智能体，已支持多语言代码生成、工具调用（检索/终端操作）以及基于企业知识库的RAG检索，并集成了合规策略检查（如审计与许可证合规）[Source: https://cloud.baidu.com/doc/COMATE/index.html]。\n\n"
            "#### 3. 测试与质量工程\n"
            "测试领域正在向“Agentic Quality Engineering”演进。AI不仅用于生成测试用例和数据，还开始应用于**视觉UI测试**（Vision AI）和**自愈测试**。企业级工具（如Tricentis）利用AI进行测试编排与结果解释，大幅提升了自动化测试的覆盖率与稳定性 [Source: https://www.dynatrace.com/platform/aiops/]。\n\n"
            "#### 4. 发布与运维（AIOps）\n"
            "在**运维/监控**环节，AIOps正从被动的“检测响应”转向“预防性运维”。通过对观测数据（指标/日志/追踪）的异常检测与事件关联，AI能够进行因果与根因分析，从而缩短平均修复时间（MTTR）并降低运维成本 [Source: https://www.datadoghq.com/knowledge-center/aiops/]。\n\n"
            "#### 关键瓶颈与风险\n"
            "尽管效率提升显著，但**可靠性与可维护性**仍是主要挑战。生成代码的短期正确性可能掩盖长期维护质量的下降，过度依赖自动生成可能引入隐性复杂度 [Source: https://arxiv.org/abs/2507.00788]。此外，自主智能体在复杂环境下的**工具使用与安全策略遵循**方面仍存在困难，例如在涉及用户同意或敏感数据操作时可能出现越权行为，需建立严格的人类监督与策略代理机制 [Source: https://arxiv.org/abs/2410.06703]。"
        ),
        sources=[
            "https://openai.com/index/gpt-4o-system-card/",
            "http://www.caict.ac.cn/kxyj/qwfb/ztbg/202409/t20240904_492304.htm",
            "https://openai.com/index/introducing-swe-bench-verified/",
            "https://cloud.baidu.com/doc/COMATE/index.html",
            "https://www.dynatrace.com/platform/aiops/",
            "https://www.datadoghq.com/knowledge-center/aiops/",
            "https://arxiv.org/abs/2507.00788",
            "https://arxiv.org/abs/2410.06703"
        ]
    ).with_inputs("goal"),

    # Demo 2: Sociological Analysis (Example of defining complex concepts with data)
    dspy.Example(
        goal="定义中国社会“中产阶层”的主要划分标准及规模估算。",
        output=(
            "### 中国中产阶层的多维定义与规模估算\n\n"
            "根据学术界与官方统计的最新研究，中国中产阶层的界定主要采用四种并行口径，每种口径对应不同的规模与社会特征。\n\n"
            "#### 1. 相对收入法（主口径）\n"
            "**定义**：等价化个人可支配收入位于全国中位数的75%—200%。这一标准直接反映了相对于社会平均水平的消费能力与生活方式。为了进行敏感性分析，研究中也常参考60%—180%或100%—300%的替代区间 [Source: https://cfps.pku.edu.cn/]。\n"
            "**规模估算**：基于2023年价格水平，该口径下的全国等价化个人约占总人口的**30%—40%**，对应人数约为**4.2—5.6亿人**。如果以家庭为单位，约占全国家庭总数的35%—45%。该数据受城乡结构与地区差异影响较大，在东部沿海与一二线城市占比显著更高。\n\n"
            "#### 2. 绝对（PPP）收入/消费法\n"
            "**定义**：采用世界银行ICP（国际比较项目）2021年购买力平价（PPP）标准，通常以日均消费或收入**12—60国际美元**为界限。折算为人民币（按2024年价格校正），年等价化收入约为1.6万—8.0万元 [Source: https://www.worldbank.org/en/programs/icp]。\n"
            "**规模估算**：满足这一区间的人群约占全国人口的**40%—55%**。这是一个相对宽泛的标准，主要用于国际比较，涵盖了大量具备基本消费能力的城市工薪阶层与部分农村富裕群体。\n\n"
            "#### 3. 资产法（抗风险能力）\n"
            "**定义**：家庭净资产处于全国**40—80分位**，且具备一定的流动性缓冲（即可动用金融资产覆盖3—6个月支出）。这一标准强调了家庭在面对失业、医疗等冲击时的经济韧性 [Source: https://chfs.swufe.edu.cn/]。\n"
            "**规模估算**：满足资产标准的家庭约占总数的**25%—32%**。由于中国家庭资产中房产占比极高（约70%），这一口径对房地产市场波动高度敏感。\n\n"
            "#### 4. 结构法（社会学特征）\n"
            "**定义**：综合考量教育背景（大专及以上）、职业声望（专业技术/管理/白领）、社会保障（稳定社保）与住房状况（自有或按揭可持续）。\n"
            "**规模估算**：符合复合结构指标的家庭约占**28%—36%**。这一群体与相对收入法界定的群体有较大重叠，但在年轻高学历未购房群体与体制内低现金流家庭上存在偏差。\n\n"
            "#### 结论：交集与画像\n"
            "综合上述四种方法，同时满足所有条件的“严格中产”约占全国家庭的**18%—24%**，人口规模约**2.5—3.4亿人**。其典型画像为：居住在东部一二线城市，拥有大专以上学历，从事白领或专业技术工作，拥有自有住房但承担一定的按揭压力。"
        ),
        sources=[
            "https://cfps.pku.edu.cn/",
            "https://www.worldbank.org/en/programs/icp",
            "https://chfs.swufe.edu.cn/"
        ]
    ).with_inputs("goal")
]
