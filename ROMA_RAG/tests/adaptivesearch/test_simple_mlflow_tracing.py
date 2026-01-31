"""
Simple MLflow Tracing Test for AdaptiveRetrieveToolkit

This script verifies that the refactored MLflow tracing logic in AdaptiveRetrieveToolkit
works as expected and produces traces in the MLflow UI.

Requirements:
1. MLflow server running: mlflow server --host 0.0.0.0 --port 5000
2. Environment variables set in .env:
   RAGFLOW_API_URL, RAGFLOW_API_KEY, RAGFLOW_KB_ID, EXA_API_KEY
"""

import os
import asyncio
import mlflow
from roma_dspy.tools.adaptive_retrieve_toolkit import AdaptiveRetrieveToolkit

# 移除全局副作用，防止 import 阶段阻塞
# MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
# mlflow.set_tracking_uri(MLFLOW_URI)
# mlflow.set_experiment("Simple_Trace_Test")

async def test_tracing():
    # 1. Setup MLflow Tracking (移动到函数内)
    MLFLOW_URI = "http://localhost:5000"
    print(f"[*] MLflow Tracking URI: {MLFLOW_URI}")
    
    try:
        print("[*] 正在连接 MLflow...")
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("Simple_Trace_Test")
        print("[✓] MLflow 连接成功")
    except Exception as e:
        print(f"[!] 警告: 连接 MLflow 失败 ({e})")
        print("    测试将继续运行，但 Trace 可能无法记录。")
        print("    请确保运行了: mlflow server --host 0.0.0.0 --port 5000")

    # 检查必要的环境变量
    rag_url = os.getenv("RAGFLOW_API_URL")
    if not rag_url:
        print("[!] 警告: RAGFLOW_API_URL 未设置，RAG 检索步骤可能会失败或阻塞")

    # 2. Initialize Toolkit with MLflow enabled
    print("[*] 初始化 AdaptiveRetrieveToolkit...")
    # Note: Using env vars for RAGFlow and Web search config
    toolkit = AdaptiveRetrieveToolkit(
        mlflow_logging=True,
        ragflow_config={
            "api_url": rag_url,
            "api_key": os.getenv("RAGFLOW_API_KEY"),
            "kb_id": os.getenv("RAGFLOW_KB_ID"),
            "timeout": 30  # 与其他测试保持一致
        },
        web_toolkit_config={
            "server_name": "exa",
            "server_type": "http",
            "url": "https://mcp.exa.ai/mcp",
            "headers": {"Authorization": f"Bearer {os.getenv('EXA_API_KEY')}"},
            # 不指定 transport_type，让 MCPToolkit 自动选择（Exa 用 StreamableHttp）
            "tool_timeout": 10.0,
            "use_storage": False  # 测试环境禁用 storage
        }
    )

    query = "AI教育在2025年的最新发展趋势"
    print(f"\n[*] 执行检索查询: {query}")
    print("[*] 检索模式: auto (智能路由)")

    # 3. Use mlflow.trace to wrap the high-level call
    # This creates the "Parent" span in the Trace UI
    @mlflow.trace(name="Simple_Adaptive_Retrieve_Test")
    async def run_query():
        return await toolkit.adaptive_retrieve_async(
            query=query,
            mode="auto",
            top_n=5
        )

    try:
        # 添加全局超时保护，防止死锁
        print("[*] 正在执行 (超时限制: 60s)...")
        result = await asyncio.wait_for(run_query(), timeout=60.0)
        
        print("\n[✓] 检索完成!")
        print(f"    - 决策结果: {result.decision.value}")
        print(f"    - 置信度: {result.confidence:.2f}")
        print(f"    - 是否触发 Web: {result.debug.web_triggered}")
        print(f"    - 结果数量: {len(result.contexts)}")
        
        # 打印完整检索内容（包含完整 text）
        print("\n[*] 检索结果详情:")
        for i, ctx in enumerate(result.contexts[:3], 1):  # 只显示前 3 条
            print(f"\n  结果 #{i}:")
            print(f"    来源: {ctx.source.value}")
            print(f"    标题: {ctx.title}")
            print(f"    URL: {ctx.url}")
            print(f"    评分: {ctx.score:.3f}")
            print(f"    内容: {ctx.text[:300]}...")  # 显示前 300 字符
        
        print(f"\n[!] 请访问 MLflow UI 查看 Trace: {MLFLOW_URI}/#/traces")
        print("    你应该能看到:")
        print("    1. 'Simple_Adaptive_Retrieve_Test' (总入口)")
        print("    2. 'ragflow_initial_retrieve' (内部 RAG 步骤)")
        print("    3. 如果触发了 Web，还会看到 'web_search_exa_only' 或混合检索步骤")
        
    except asyncio.TimeoutError:
        print("\n[✗] 测试超时! 可能是网络连接问题 (RAGFlow 或 MCP Exa 连接失败)")
    except Exception as e:
        print(f"\n[✗] 测试失败: {e}")

if __name__ == "__main__":
    # Ensure environment variables are loaded
    if not os.getenv("RAGFLOW_API_KEY"):
        print("[!] 警告: 未检测到 RAGFLOW_API_KEY 环境变量，请确保已配置。")
    
    asyncio.run(test_tracing())
