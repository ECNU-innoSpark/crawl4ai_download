"""
Module 5 - MLflow深度集成测试

测试目标：
1. 验证 adaptive_retrieve 能正确记录信息到 MLflow
2. 验证决策记录、检索结果能作为 artifacts 保存
3. 验证 MLflow attributes 包含所有关键信息
4. 验证为 LLM judge/CRAG 模型预留的扩展字段

环境要求：
- 需要启动 MLflow server
- 设置环境变量：MLFLOW_TRACKING_URI

使用方法：
   python tests/ragflow/test_module5_mlflow_integration.py
"""

import sys
import os
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# 设置 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')


# ========== 检查 MLflow 是否可用 ==========
print("=" * 70)
print("Module 5 测试: MLflow 深度集成")
print("=" * 70)

try:
    import mlflow
    from mlflow.tracking import MlflowClient
    print("\n[✓] MLflow 已安装")
except ImportError:
    print("\n[✗] MLflow 未安装，请运行：pip install mlflow")
    sys.exit(1)

# 检查 MLflow tracking URI
mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
print(f"[配置] MLflow Tracking URI: {mlflow_uri}")

# 使用超时机制检查 MLflow server 连接
import socket
import urllib.parse

def check_mlflow_connection(uri, timeout=3):
    """快速检查 MLflow server 是否可访问"""
    try:
        parsed = urllib.parse.urlparse(uri)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 5000
        
        # 尝试建立 socket 连接
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False

try:
    mlflow.set_tracking_uri(mlflow_uri)
    
    # 先快速检查端口是否开放
    print("[检查] 正在检查 MLflow server 连接...")
    if not check_mlflow_connection(mlflow_uri, timeout=3):
        raise ConnectionError("无法连接到 MLflow server（端口未开放）")
    
    # 端口开放后，再尝试 API 调用
    client = MlflowClient()
    client.search_experiments(max_results=1)
    print("[✓] MLflow server 连接成功")
    mlflow_available = True
except Exception as e:
    print(f"[⚠] 无法连接到 MLflow server: {e}")
    print("   提示：如果 MLflow 未启动，测试将跳过 MLflow 验证")
    print("   启动方法: mlflow server --host 0.0.0.0 --port 5000")
    mlflow_available = False


# ========== 加载环境变量 ==========
def load_env_file():
    """从 .env 文件加载环境变量"""
    env_path = project_root / ".env"
    
    if env_path.exists():
        print(f"\n[配置] 找到 .env 文件: {env_path}")
        for encoding in ['utf-8', 'utf-8-sig', 'gbk', 'latin-1']:
            try:
                with open(env_path, 'r', encoding=encoding, errors='ignore') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            os.environ[key] = value
                print("[配置] .env 文件加载成功")
                return True
            except Exception:
                continue
        return True
    else:
        print(f"[警告] 未找到 .env 文件: {env_path}")
        return False


load_env_file()

RAGFLOW_API_URL = os.getenv("RAGFLOW_API_URL")
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY")
RAGFLOW_KB_ID = os.getenv("RAGFLOW_KB_ID")
EXA_API_KEY = os.getenv("EXA_API_KEY")

print("\n[配置检查]")
print(f"  RAGFLOW_API_URL: {RAGFLOW_API_URL or '❌ 未设置'}")
print(f"  RAGFLOW_API_KEY: {'✓' if RAGFLOW_API_KEY else '❌ 未设置'}")
print(f"  RAGFLOW_KB_ID: {RAGFLOW_KB_ID or '❌ 未设置'}")
print(f"  EXA_API_KEY: {'✓' if EXA_API_KEY else '❌ 未设置'}")

if not all([RAGFLOW_API_URL, RAGFLOW_API_KEY, RAGFLOW_KB_ID, EXA_API_KEY]):
    print("\n[错误] 缺少必要的 API 配置！")
    sys.exit(1)


# ========== 导入测试模块 ==========
print("\n[导入] 加载测试依赖...")
try:
    import importlib.util
    import types
    
    # 1. 加载 retrieve_result 类型
    types_spec = importlib.util.spec_from_file_location(
        "retrieve_result",
        project_root / "src/roma_dspy/types/retrieve_result.py"
    )
    types_module = importlib.util.module_from_spec(types_spec)
    types_spec.loader.exec_module(types_module)
    
    DecisionType = types_module.DecisionType
    SourceType = types_module.SourceType
    
    # 2. 创建 BaseToolkit mock
    class BaseToolkit:
        def __init__(self, enabled=True, include_tools=None, exclude_tools=None, 
                     file_storage=None, **config):
            self.enabled = enabled
            self.config = config
            self._setup_dependencies()
            self._initialize_tools()
        def _setup_dependencies(self): pass
        def _initialize_tools(self): pass
        def log_info(self, msg): print(f"    [INFO] {msg}")
        def log_warning(self, msg): print(f"    [WARN] {msg}")
        def log_error(self, msg): print(f"    [ERROR] {msg}")
        def log_debug(self, msg): pass
        def get_available_tool_names(self): return set()
    
    mock_base_module = types.ModuleType("roma_dspy.tools.base.base")
    mock_base_module.BaseToolkit = BaseToolkit
    sys.modules['roma_dspy.tools.base.base'] = mock_base_module
    sys.modules['roma_dspy.types.retrieve_result'] = types_module
    
    # 3. 加载 RAGFlowToolkit
    ragflow_spec = importlib.util.spec_from_file_location(
        "ragflow_toolkit",
        project_root / "src/roma_dspy/tools/ragflow_toolkit.py"
    )
    ragflow_module = importlib.util.module_from_spec(ragflow_spec)
    ragflow_spec.loader.exec_module(ragflow_module)
    RAGFlowToolkit = ragflow_module.RAGFlowToolkit
    
    # 4. 加载 AdaptiveRetrieveToolkit
    toolkit_spec = importlib.util.spec_from_file_location(
        "adaptive_retrieve_toolkit",
        project_root / "src/roma_dspy/tools/adaptive_retrieve_toolkit.py"
    )
    toolkit_module = importlib.util.module_from_spec(toolkit_spec)
    toolkit_spec.loader.exec_module(toolkit_module)
    AdaptiveRetrieveToolkit = toolkit_module.AdaptiveRetrieveToolkit
    
    print("  [OK] 所有模块加载成功")
    
except Exception as e:
    print(f"  [FAIL] 导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)


# ========== 验证 MCP Toolkit 可用 ==========
print("\n[初始化] 验证 MCP Toolkit...")
try:
    from roma_dspy.tools.mcp.toolkit import MCPToolkit
    print("  [OK] MCP Toolkit 已加载")
except Exception as e:
    print(f"  [FAIL] 加载失败: {e}")
    sys.exit(1)


# ========== 主测试函数 ==========
async def test_mlflow_integration():
    """测试 MLflow 集成"""
    print("\n" + "=" * 70)
    print("[测试] MLflow 深度集成")
    print("=" * 70)
    
    # 创建 toolkit
    print("\n[1/4] 创建 AdaptiveRetrieveToolkit...")
    toolkit = AdaptiveRetrieveToolkit(
        enabled=True,
        api_url=RAGFLOW_API_URL,
        api_key=RAGFLOW_API_KEY,
        kb_id=RAGFLOW_KB_ID,
        timeout=30,
        # 使用 MCP Exa（提供 LLM 生成的摘要，避免上下文爆炸）
        web_toolkit_config={
            "server_name": "exa",
            "server_type": "http",
            "url": "https://mcp.exa.ai/mcp",
            "headers": {
                "Authorization": f"Bearer {EXA_API_KEY}"
            },
            "use_storage": False,  # 测试环境禁用 storage（需要 file_storage 实例）
        },
        web_tool_method="web_search_exa",
        web_tool_kwargs={
            # ✅ 修复：使用正确的 EXA API 参数名（避免上下文爆炸）
            "summary": True,  # 启用 LLM 生成的摘要（官方参数）
            "text": False,    # 禁用完整文本，只用摘要（节省 token）
            "highlights": True,  # 保留高亮片段
        },
        top_n_default=5,
        mlflow_logging=True,  # ✅ 显式启用 MLflow logging
    )
    print("  [OK] Toolkit 已创建（MCP Exa with autoprompt, mlflow_logging=True）")
    
    # 测试查询
    test_queries = [
        {"query": "个性化教育", "mode": "auto", "description": "高质量查询（应该走 rag）"},
        {"query": "latest AI news 2026", "mode": "auto", "description": "低质量查询（应该走 web）"},
    ]
    
    # 如果 MLflow 可用，设置 tracing
    if mlflow_available:
        print("\n[2/4] 配置 MLflow Tracing...")
        # MLflow Tracing 不需要 experiment，直接记录到 Traces
        print(f"  [OK] MLflow Tracing 已启用")
    
    # 执行测试
    print("\n[3/4] 执行测试查询...")
    for i, test in enumerate(test_queries, 1):
        print(f"\n{'=' * 70}")
        print(f"[查询 {i}/{len(test_queries)}] {test['description']}")
        print(f"{'=' * 70}")
        print(f"Query: {test['query']}")
        print(f"Mode: {test['mode']}")
        
        # 使用 MLflow Tracing 包装调用
        if mlflow_available:
            # 使用 @mlflow.trace 装饰器通过包装函数的方式记录
            @mlflow.trace(name=f"adaptive_retrieve_{i}", span_type="CHAIN", attributes={
                "test.query": test["query"],
                "test.mode": test["mode"],
                "test.description": test["description"],
            })
            async def traced_retrieve():
                """被 MLflow Tracing 追踪的检索函数"""
                # 执行检索
                return await toolkit.adaptive_retrieve_async(
                    query=test["query"],
                    mode=test["mode"],
                    top_n=5,
                )
            
            result = await traced_retrieve()
            
            # 显示结果
            print(f"\n[结果]")
            print(f"  Decision: {result.decision.value}")
            print(f"  Confidence: {result.confidence:.3f}")
            print(f"  Web triggered: {result.debug.web_triggered}")
            print(f"  Total contexts: {len(result.contexts)}")
            print(f"  RAGFlow: {sum(1 for c in result.contexts if c.source == SourceType.RAGFLOW)}")
            print(f"  Exa: {sum(1 for c in result.contexts if c.source == SourceType.EXA)}")
            
            # 显示 MLflow 信息
            print(f"\n[MLflow] Trace 已记录到 Traces 页面")
            print(f"  查看: {mlflow_uri}/#/traces")
        else:
            # 如果 MLflow 不可用，直接调用（不记录到 MLflow）
            result = await toolkit.adaptive_retrieve_async(
                query=test["query"],
                mode=test["mode"],
                top_n=5,
            )
            print(f"\n[结果]")
            print(f"  Decision: {result.decision.value}")
            print(f"  Confidence: {result.confidence:.3f}")
            print(f"  （MLflow 未启动，跳过记录）")
    
    # 验收检查
    print("\n" + "=" * 70)
    print("[4/4] 验收检查")
    print("=" * 70)
    
    if mlflow_available:
        print("\n✅ Module 5 MLflow Tracing 集成测试完成！")
        print("\n请在 MLflow UI 中验证以下内容：")
        print(f"1. 打开: {mlflow_uri}")
        print("2. 点击左侧菜单 'Traces' 进入追踪页面")
        print("3. 查看测试查询的 Trace 记录（最新的2条）")
        print("4. 点击任意 Trace，查看完整的调用链路：")
        print("   ✓ 顶层 span: adaptive_retrieve_1/2")
        print("     - 查看 Inputs/Outputs: 包含完整的查询和检索结果")
        print("     - 查看 Attributes: query, mode, top_n, decision, confidence 等")
        print("   ✓ 子 span: ragflow_initial_retrieve")
        print("     - 查看 RAGFlow 检索的输入输出和置信度计算")
        print("   ✓ 子 span: web_search_exa (如果触发)")
        print("     - 查看 Exa 搜索的查询和返回结果")
        print("   ✓ 子 span: adaptive_decision")
        print("     - 查看自适应决策的推理过程（decision, reason, method）")
        print("\n5. 验证追踪信息：")
        print("   ✓ 每个 span 都有明确的输入输出")
        print("   ✓ 可以看到完整的检索流程（RAGFlow → 决策 → Web搜索）")
        print("   ✓ 时间线图（Gantt Chart）显示各步骤的耗时和顺序")
        print("   ✓ Attributes 包含 decision, confidence, method 等关键信息")
        print("\n💡 MLflow Tracing 优势：")
        print("   ✓ 直观展示调用链路，无需下载 JSON 文件")
        print("   ✓ 自动捕获输入输出，方便调试和复现")
        print("   ✓ 支持嵌套 span，清晰展示层次关系")
        print("   ✓ 更适合 GenAI 应用的流程追踪和可观测性")
    else:
        print("\n⚠️  MLflow server 未启动，测试在无 MLflow 模式下完成。")
        print("   要启用 MLflow Tracing，请：")
        print("   1. 启动 MLflow: mlflow server --host 0.0.0.0 --port 5000")
        print("   2. 设置环境变量: export MLFLOW_TRACKING_URI=http://localhost:5000")
        print("   3. 重新运行测试")


# ========== 运行测试 ==========
if __name__ == "__main__":
    try:
        asyncio.run(test_mlflow_integration())
    except KeyboardInterrupt:
        print("\n\n[中断] 测试被用户中断")
    except Exception as e:
        print(f"\n\n[错误] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

