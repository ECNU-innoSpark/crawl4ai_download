"""Verifier instruction seed prompt for Deep Research (DR).

This module provides a strict instruction prompt for verifying deep research reports,
focusing on citation accuracy, comprehensive coverage, and language compliance.
"""

import dspy

VERIFIER_DR_PROMPT = r"""
# Verifier — Deep Research Validator

Role
Validate that the research report fully satisfies the original goal, strictly follows citation rules, and meets professional standards.

Language Requirement (strict)
- `feedback` MUST be written in Simplified Chinese (简体中文).

Inputs
- `goal` (string): The original task objective.
- `candidate_output` (string): The synthesized research report.
- `context` (string, optional): Additional context.

Output Contract (strict)
- `verdict` (bool): true if satisfied, false otherwise.
- `feedback` (string, optional): Detailed, actionable explanation when false.

Verification Criteria (Deep Research Specific)

1. **CITATION COMPLIANCE (CRITICAL)**:
   - Does the report contain inline citations (e.g., `[1]`, `[Source: URL]`)?
   - Is there a "References" or "参考文献" section at the end?
   - Are URLs present and valid-looking?
   - **Verdict must be FALSE if citations are missing.**

2. **GOAL SATISFACTION**:
   - Does the report address ALL parts of the goal?
   - Is the depth sufficient for "Deep Research"?

3. **LANGUAGE**:
   - Is the report in Simplified Chinese (unless requested otherwise)?

4. **FORMATTING**:
   - Are Markdown headers used correctly?
   - Is the structure logical?

Strict Output Shape
{
  "verdict": true|false,
  "feedback": "<explanation>"
}
"""

VERIFIER_DR_DEMOS = [
    dspy.Example(
        goal="分析 X 公司的财务状况。",
        candidate_output="X 公司 2023 年营收很好。",
        verdict=False,
        feedback="报告过于简略，缺乏具体数据和引用。请补充具体的财务数据（如营收、利润的具体数字），并添加来源引用（[n]）和参考文献列表。"
    ).with_inputs("goal", "candidate_output"),
    
    dspy.Example(
        goal="分析 X 公司的财务状况。",
        candidate_output="""# X 公司财务分析
2023 年营收为 100 亿 [1]。

### 参考文献
[1] X 公司财报: https://x.com/report""",
        verdict=True,
        feedback="报告包含具体数据和规范的引用，结构清晰。"
    ).with_inputs("goal", "candidate_output"),
]

