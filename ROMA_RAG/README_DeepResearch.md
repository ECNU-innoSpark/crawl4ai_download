# Deep Research Profile Guide

## 简介
Deep Research (DR) Profile 是 ROMA 项目中专为深度研究任务设计的配置方案。它通过集成自适应检索（Adaptive Retrieval）、专门优化的 Prompt 策略以及高性能的 Gemini 模型，实现了从任务规划、信息检索、深度思考到报告撰写的全流程自动化研究能力。

## 核心特性

### 1. 自适应检索 (Adaptive Retrieval)
集成 `AdaptiveRetrieveToolkit`，能够根据查询的置信度自动在以下模式间切换：
- **RAG模式**：优先查询内部知识库 (RAGFlow)，确保利用私有高质量数据。
- **Web模式**：当内部知识不足时，自动调用外部搜索 (Exa AI)，获取最新互联网信息。
- **混合模式**：结合两者优势，提供全面的信息覆盖。

### 2. 任务感知型 Agent (Task-Aware Agents)
配置文件通过 `agent_mapping` 将不同类型的任务分发给专门优化的 Agent：
- **RETRIEVE Agent**：专注于数据搜集，强调来源的可追溯性和精确性。
- **THINK Agent**：专注于逻辑推理和多源信息综合，不直接进行搜索。
- **WRITE Agent**：专注于生成结构化、引用严谨的研究报告。

### 3. 深度优化的 Prompt (Deep Research Prompts)
位于 `prompt_optimization/prompts/seed_prompts/` 下的一系列 `dr_*` 种子 Prompt，专为研究场景定制：
- 严格的引用格式 `[Source: URL]`。
- 拒绝幻觉，强调“未找到”时的诚实回答。
- 结构化的输出要求。

## 快速开始

### 1. 环境配置
在运行之前，请确保 `.env` 文件或环境变量中包含以下配置：

```bash
# LLM Provider (Google Gemini)
GOOGLE_API_KEY=your_google_api_key

# RAGFlow (内部知识库)
RAGFLOW_API_URL=your_ragflow_url
RAGFLOW_API_KEY=your_ragflow_key
RAGFLOW_KB_ID=your_knowledge_base_id

# Exa AI (外部搜索)
EXA_API_KEY=your_exa_api_key

# Observability (可选)
MLFLOW_ENABLED=true
MLFLOW_TRACKING_URI=http://localhost:5000
```

### 2. 运行命令
使用 `roma-dspy` CLI 并指定 `deep_research` profile 即可启动：

```bash
# 基础运行
roma-dspy solve "分析2024年全球生成式AI的市场规模及主要竞争格局" --profile deep_research

# 启用详细日志
roma-dspy solve "查询最新的Transformer架构优化论文" --profile deep_research --verbose

# 指定最大分解深度
roma-dspy solve "撰写一份关于量子计算最新进展的深度报告" --profile deep_research --max-depth 2
```

## 配置文件详解 (`config/profiles/deep_research.yaml`)

### Agent 模型配置
Deep Research Profile 全面采用 Google Gemini 系列模型以平衡性能与成本：
- **Planner/Executor/Aggregator (主要)**: `gemini/gemini-3-pro-preview` - 擅长复杂逻辑和长文生成。
- **Retrieve Executor**: `gemini/gemini-3-flash-preview` - 响应速度快，适合工具调用。

### 关键组件配置

#### AdaptiveRetrieveToolkit
在 `executors.RETRIEVE` 中配置，关键参数：
- `web_tool_kwargs`: 设置 `type: deep` 和 `useAutoprompt: true` 以获取高质量搜索结果。
- `top_n_default`: 默认为 8，确保有足够的信息源。

#### Agent Mapping
定义了针对不同任务类型的 specialized prompts：
- **RETRIEVE**: 使用 `dr_executor_retrieve_seed.py`，强调 `[Source: URL]` 引用。
- **THINK**: 使用 `dr_executor_think_seed.py`，强调逻辑链条。
- **WRITE**: 使用 `dr_executor_write_seed.py`，强调报告结构。

## Prompt 体系 (`prompt_optimization/prompts/seed_prompts/`)

该目录下新增了多个 `dr_` 开头的文件，构成了 Deep Research 的核心指令集：

| 文件名 | 用途 | 核心特点 |
|--------|------|----------|
| `dr_planner_seed.py` | 任务规划 | 强调 MECE 原则，避免重复子任务，最大子任务数限制为 5。 |
| `dr_executor_retrieve_seed.py` | 信息检索 | 强制要求行内引用，禁止伪造来源，要求多源交叉验证。 |
| `dr_executor_think_seed.py` | 深度思考 | 引导模型进行多角度分析，识别信息冲突。 |
| `dr_aggregator_seed.py` | 结果汇总 | 将碎片化信息整合成连贯报告，保留所有原始引用。 |
| `dr_verifier_seed.py` | 结果验证 | 检查引用有效性、数据一致性和回答完整性。 |

## 开发与调试

### 1. 修改 Prompt
如果需要调整 Agent 的行为，请直接修改 `prompt_optimization/prompts/seed_prompts/` 下对应的 `dr_*.py` 文件。修改后直接运行即可生效（无需重新编译，除非涉及到 DSPy 的签名变更）。

### 2. 查看追踪 (Trace)
建议开启 MLflow (`MLFLOW_ENABLED=true`)。在 MLflow UI 中，你可以看到：
- **Adaptive Retrieve 决策**：查看是走了 RAG、Web 还是 Hybrid 模式。
- **置信度分数**：查看模型对检索结果的打分。
- **原始搜索结果**：查看 Exa 或 RAGFlow 返回的原始数据。

## 常见问题

**Q: 为什么检索结果很少？**
A: 检查 `AdaptiveRetrieveToolkit` 的日志。可能是 RAGFlow 中无相关知识，且 Exa 搜索未返回足够结果。尝试调大 `top_n_default` 或优化查询词。

**Q: 报告中没有引用链接？**
A: 确保使用的是 `dr_executor_retrieve_seed` prompt，它强制要求输出 `[Source: URL]`。普通 executor prompt 可能没有此约束。

**Q: 任务执行时间过长？**
A: Deep Research 涉及多次外部搜索和长上下文处理。可以在配置文件中调整 `runtime.timeout` (默认 600s)。

