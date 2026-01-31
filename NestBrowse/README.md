# NestBrowse - Nested Browser-Use Learning for Agentic Information Seeking

基于论文 *"Nested Browser-Use Learning for Agentic Information Seeking"* 的实现。

NestBrowse 是一个用于信息搜索（Information Seeking）任务的浏览器使用智能体框架，由阿里巴巴通义实验室开发。

## 项目概述

### 核心特点

1. **最小化但功能完整的浏览器工具包** - 提供四个核心工具：
   - `search`: 执行 Google 批量查询，返回每个查询的 Top-10 结果
   - `visit`: 访问网页 URL 并提取与目标相关的信息
   - `click`: 点击可交互元素，可能触发页面跳转
   - `fill`: 在表单或可编辑元素中输入文本

2. **嵌套浏览器使用框架 (Nested Browser-Use Framework)**：
   - **外循环 (Outer Loop)**: 工具集成推理，进行 ReAct 风格的函数调用
   - **内循环 (Inner Loop)**: 页面内探索，提取与目标相关的信息

3. **多任务模仿学习** - 联合训练外循环推理和内循环证据提取能力

---

## ViNest 升级（本项目扩展）

在 NestBrowse 基础上，本项目实现了 **ViNest** 扩展，主要包含两点：

### 1. Visual Inner Loop（多模态视觉内循环）

- **目的**：内循环不仅能处理文本 DOM，还能利用网页截图理解图表、复杂 UI 等。
- **实现**：
  - **utils.py**：`call_llm` 支持 OpenAI 多模态消息（`content` 可为 `[{"type": "text", ...}, {"type": "image_url", ...}]`），兼容原有纯文本调用。
  - **mcp_browser_server.py**：`browser_navigate`、`browser_click` 支持参数 `include_screenshot=True`，在返回的文本末尾附加 Base64 截图块 `[SCREENSHOT_BASE64]...[/SCREENSHOT_BASE64]`；截图失败时仅打日志，不中断流程。
  - **toolkit/browser.py**：Visit/Click 调用 MCP 时传入 `include_screenshot=True`，从响应中解析出正文与 Base64 截图，将 `screenshot` 传给 `process_response`。
  - **toolkit/tool_explore.py**：`process_response` 新增可选参数 `screenshot=None`；若有截图，将首段 shard 的 user 消息构建为多模态（文本 + `image_url`），使 Summary Model 同时看到网页文本与截图。

### 2. Adaptive Goal Refinement（自适应目标修正）

- **目的**：内循环在发现当前 Goal 不合理或存在更好线索时，可向外循环反馈修正建议。
- **实现**：
  - **prompts.py**：`SYSTEM_PROMPT_SUMMARY_OURS` 的 JSON schema 增加字段 `"refinement": "string | null"`，并说明：当用户目标不清晰、本页无法满足、或发现关键线索/更好方向时，在 `refinement` 中输出简短建议，否则为 `null`。SUMMARY_PROMPT / SUMMARY_PROMPT_INCREMENTAL 的 Final Output Format 中补充 `refinement` 说明。
  - **toolkit/tool_explore.py**：解析 JSON 后读取 `refinement`；若为非空字符串，在返回给外循环的 `processed_response` 末尾追加 `\n\n[URGENT ADVICE]: <内容>`，供外循环下一轮推理使用。

### 涉及文件一览

| 文件 | 改动概要 |
|------|----------|
| `utils.py` | `call_llm` 多模态消息支持；`_normalize_messages_for_api` |
| `mcp_browser_server.py` | `browser_navigate`/`browser_click` 支持 `include_screenshot`，返回截图块 |
| `toolkit/browser.py` | 请求截图、解析响应、向 `process_response` 传 `screenshot` |
| `toolkit/tool_explore.py` | 多模态 user 消息构建；`refinement` 解析与 `[URGENT ADVICE]` 附加 |
| `prompts.py` | Summary 的 JSON 增加 `refinement` 字段及说明 |

---

## 项目结构

```
NestBrowse/
├── data/                           # 输入数据目录（存放 benchmark 数据）
│   └── .gitkeep
├── results/                        # 输出结果目录
│   └── .gitkeep
├── toolkit/                        # 工具包目录
│   ├── browser.py                  # Visit、Click、Fill 浏览器操作类
│   ├── mcp_client.py               # MCP 客户端（用于浏览器交互）
│   ├── tool_search.py              # 搜索工具（支持多种后端）
│   └── tool_explore.py             # 页面探索/内容提取
├── infer_async_nestbrowse.py       # 主推理脚本
├── mcp_browser_server.py           # MCP 浏览器服务器（基于 Playwright）
├── prompts.py                      # 提示词模板
├── utils.py                        # 工具函数（LLM 调用、token 计数等）
├── vllm_deploy.sh                  # vLLM 模型部署脚本
├── requirements.txt                # Python 依赖
└── README.md                       # 本文件
```

## 安装

### 1. 安装 Python 依赖

```bash
pip install -r requirements.txt

# 安装 Playwright 浏览器
playwright install chromium
```

### 2. 启动 MCP 浏览器服务器

```bash
# 默认在 8080 端口启动（headless 模式）
python mcp_browser_server.py --port 8080

# 或使用 uvicorn（推荐生产环境）
uvicorn mcp_browser_server:app --host 0.0.0.0 --port 8080

# 如需调试，可关闭 headless 模式查看浏览器界面
python mcp_browser_server.py --port 8080 --no-headless
```

### 3. 配置搜索工具

搜索工具支持多种后端，按优先级自动选择：

```bash
# 方式一：使用 SerpAPI（推荐）
export SERPAPI_API_KEY="your-serpapi-key"

# 方式二：使用 Google Custom Search API
export GOOGLE_API_KEY="your-google-api-key"
export GOOGLE_CSE_ID="your-custom-search-engine-id"

# 方式三：使用 Bing Search API
export BING_API_KEY="your-bing-api-key"

# 方式四：使用 DuckDuckGo（免费，无需 API key，默认选项）
# 无需配置，自动使用
```

### 4. 部署 NestBrowse 模型

使用 vLLM 部署模型：

```bash
# 修改 vllm_deploy.sh 中的 MODEL_PATH 为实际模型路径
MODEL_PATH="/path/to/your/nestbrowse-model"

python -m vllm.entrypoints.openai.api_server \
    --model $MODEL_PATH \
    --port 8000 \
    --max-model-len 131072 \
    --tensor-parallel-size 4 \
    --gpu-memory-utilization 0.8
```

**模型选择**：
- `NestBrowse-4B`: 基于 Qwen3-4B-Thinking-2507
- `NestBrowse-30B-A3B`: 基于 Qwen3-30B-A3B-Thinking-2507

## 配置

### 必需的配置项

在 `infer_async_nestbrowse.py` 中配置以下参数：

```python
# 1. MCP 浏览器服务器 URL（启动 mcp_browser_server.py 后的地址）
BROWSER_SERVER_URL = "http://localhost:8080/sse"

# 2. Agent LLM API 配置（本地部署的 NestBrowse 模型）
AGENT_LLM_BASE_URL = "http://localhost:8000/v1"
AGENT_LLM_API_KEY = "EMPTY"

# 3. Tokenizer 路径（使用与模型相同的 tokenizer）
tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen3-4B")  # 或你的模型路径

# 4. 基准测试名称和模型名称
benchmark_name = "browsecomp"  # 例如: browsecomp, gaia, xbench
MODEL_NAME = "nestbrowse-4b"
```

### 可选配置项

```python
rollout_count = 1           # 每个问题的 rollout 次数
MAX_AGENT_TURN = 100        # 最大 agent 轮次
MAX_AGENT_LEN = 128 * 1024  # 最大上下文长度 (128K tokens)
MAX_SINGLE_GEN_TOKENS = 32 * 1024  # 单次生成最大 tokens
MAX_SUMMARY_SHARD_LEN = 64 * 1024  # 页面分片最大长度
MAX_WORKERS = 16            # 并发 worker 数量
```

## 快速启动

完整启动流程（需要 3 个终端）：

```bash
# 终端 1: 启动 MCP 浏览器服务器
python mcp_browser_server.py --port 8080

# 终端 2: 启动 vLLM 模型服务（如果使用本地模型）
MODEL_PATH="/path/to/your/model"
python -m vllm.entrypoints.openai.api_server \
    --model $MODEL_PATH \
    --port 8000 \
    --max-model-len 131072

# 终端 3: 运行推理
python infer_async_nestbrowse.py
```

## 运行

### 1. 准备输入数据

将 benchmark 数据放入 `data/` 目录，格式为 JSONL：

```jsonl
{"question": "问题内容", "answer": "标准答案"}
{"question": "问题内容", "answer": "标准答案"}
```

### 2. 运行推理

```bash
python infer_async_nestbrowse.py
```

### 3. 查看结果

结果将保存在 `results/` 目录下，文件名格式为：`{MODEL_NAME}_results_{benchmark_name}.jsonl`

## 依赖的外部服务

### 1. MCP 浏览器服务器（已提供）

本项目已包含 `mcp_browser_server.py`，基于 Playwright 实现。

提供的工具：
- `browser_navigate`: 导航到指定 URL，返回页面内容和可交互元素
- `browser_click`: 点击页面元素（通过 ref 标识）
- `browser_type`: 在输入框中输入文本
- `browser_scroll`: 滚动页面
- `browser_screenshot`: 截取页面截图

### 2. 搜索 API（已提供多种选择）

`toolkit/tool_search.py` 支持以下搜索后端：

| 后端 | 配置 | 说明 |
|------|------|------|
| DuckDuckGo | 无需配置 | 免费，默认选项 |
| SerpAPI | `SERPAPI_API_KEY` | 推荐，支持多种搜索引擎 |
| Google | `GOOGLE_API_KEY` + `GOOGLE_CSE_ID` | 需要创建自定义搜索引擎 |
| Bing | `BING_API_KEY` | 需要 Azure 订阅 |

## 组件说明

### ✅ 已实现的组件

1. **Search 工具** (`toolkit/tool_search.py`)
   - 支持多种搜索后端：SerpAPI、Google Custom Search、Bing、DuckDuckGo
   - DuckDuckGo 后端免费无需 API key
   - 自动检测可用后端

2. **MCP 浏览器服务器** (`mcp_browser_server.py`)
   - 基于 Playwright 的无头浏览器
   - 提供 `browser_navigate`、`browser_click`、`browser_type`、`browser_scroll` 等工具
   - 自动提取页面 DOM 和可交互元素

### ⚠️ 需要用户自行准备

1. **模型权重**
   - NestBrowse-4B 或 NestBrowse-30B-A3B 模型权重
   - 需要从官方渠道获取或自行训练
   - 或使用其他兼容的 LLM（如 Qwen3、GPT-4 等）

2. **Benchmark 数据集**（用于评测）
   - BrowseComp / BrowseComp-zh
   - GAIA
   - XBench-DeepSearch

## 运行所需 API 清单

要跑通**完整推理流程**，当前只需要以下 API/服务：

| 类型 | 用途 | 是否必需 | 如何满足 |
|------|------|----------|----------|
| **Agent LLM API** | 外循环：推理 + 选工具 + 生成答案 | ✅ 必需 | 任选其一：<br>• 本地 vLLM 部署 NestBrowse/Qwen（`AGENT_LLM_BASE_URL` + `AGENT_LLM_API_KEY`）<br>• 或任意 OpenAI 兼容 API（如 OpenAI / 国内中转） |
| **Summary LLM API** | 内循环：从页面文本/截图提取证据与 summary | ✅ 必需 | 默认与 Agent 共用（`SUMMARY_LLM_*` 未设则用 `AGENT_LLM_*`）。若启用 ViNest 截图，Summary 模型需**支持视觉输入**（多模态）。 |
| **MCP 浏览器服务** | visit/click/fill 的底层执行 | ✅ 必需 | 本仓库自带：`python mcp_browser_server.py --port 8080`，配置 `BROWSER_SERVER_URL=http://...:8080/sse` |
| **搜索 API** | search 工具 | ❌ 可选 | 不配置则用 DuckDuckGo（免费）；也可配置 SerpAPI / Google CSE / Bing 等 |

### 还缺什么就能运行？

- **必配**：  
  1. **Agent LLM**：在 `infer_async_nestbrowse.py` 里设置 `AGENT_LLM_BASE_URL`、`AGENT_LLM_API_KEY`（本地 vLLM 可填 `EMPTY`），以及 `MODEL_NAME`、tokenizer 路径、`benchmark_name`。  
  2. **MCP 浏览器**：启动 `mcp_browser_server.py` 并填对 `BROWSER_SERVER_URL`。  

- **若用 ViNest 视觉内循环**：  
  - Summary 与 Agent 共用同一模型时，该模型需**支持多模态**（能接收 `image_url`）；若 Summary 单独配置（`SUMMARY_LLM_BASE_URL` / `SUMMARY_LLM_API_KEY`），则只需 Summary 模型支持多模态即可。  

- **可选**：  
  - 搜索：不配任何 key 即用 DuckDuckGo；要更好效果再配 SerpAPI 等。  
  - Benchmark 数据：在 `data/<benchmark_name>.jsonl` 放置题目，否则推理会没有输入。

## 硬件要求

- **NestBrowse-4B**: 建议 1-2 张 GPU (约 8-16GB 显存)
- **NestBrowse-30B-A3B**: 建议 4 张 GPU，使用 tensor parallelism

训练时间参考（NVIDIA H20 集群）：
- 4B 模型: 约 1,344 GPU 小时
- 30B-A3B 模型: 约 4,096 GPU 小时

## 性能参考

基于论文报告的性能（pass@1 指标）：

| 模型 | BrowseComp | BrowseComp-zh | GAIA | XBench |
|------|------------|---------------|------|--------|
| NestBrowse-4B | 22.4 | 28.4 | 68.9 | 74.0 |
| NestBrowse-30B-A3B | 31.6 | 42.6 | 75.7 | 75.0 |

## 相关链接

- **论文**: [Nested Browser-Use Learning for Agentic Information Seeking](https://arxiv.org/abs/2512.23647)
- **官方仓库**: https://github.com/Alibaba-NLP/DeepResearch
- **项目主页**: https://tongyi-agent.github.io/blog

## 引用

```bibtex
@article{li2025nestbrowse,
  title={Nested Browser-Use Learning for Agentic Information Seeking},
  author={Li, Baixuan and Wu, Jialong and Yin, Wenbiao and Li, Kuan and Zhang, Zhongwang and Yin, Huifeng and Tao, Zhengwei and Zhang, Liwen and Xie, Pengjun and Zhou, Jingren and Jiang, Yong},
  journal={arXiv preprint arXiv:2512.23647},
  year={2025}
}
```

## License

请参考阿里巴巴通义实验室的官方许可协议。
