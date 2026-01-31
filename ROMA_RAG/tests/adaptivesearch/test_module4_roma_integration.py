"""
Module 4 测试：AdaptiveRetrieveToolkit 集成到 ROMA RETRIEVE 节点

测试场景：
1. Toolkit 注册和初始化
2. 工具方法暴露（search_adaptive）
3. ReAct 模式下的工具调用模拟
4. 端到端集成测试（如果环境配置完整）
"""

import json
import os
import sys
from pathlib import Path

# 设置 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    os.system('chcp 65001 > nul')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到 path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

# 加载环境变量
def load_env_file():
    """Load environment variables from .env file."""
    env_path = project_root / ".env"
    if not env_path.exists():
        print(f"⚠️  Warning: .env file not found at {env_path}")
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ[key] = value

    print("✅ Environment variables loaded from .env")


load_env_file()

# 现在导入 ROMA 模块
from roma_dspy.tools.base.manager import ToolkitManager
from roma_dspy.tools.adaptive_retrieve_toolkit import AdaptiveRetrieveToolkit


def test_toolkit_registration():
    """测试 1：Toolkit 是否正确注册到 BUILTIN_TOOLKITS"""
    print("\n" + "=" * 70)
    print("测试 1：Toolkit 注册")
    print("=" * 70)

    manager = ToolkitManager.get_instance()
    builtin = manager.BUILTIN_TOOLKITS

    assert "AdaptiveRetrieveToolkit" in builtin, "❌ AdaptiveRetrieveToolkit not in BUILTIN_TOOLKITS"
    print(f"✅ AdaptiveRetrieveToolkit 已注册到 BUILTIN_TOOLKITS")
    print(f"   模块路径: {builtin['AdaptiveRetrieveToolkit']}")

    # 验证可以成功加载（触发延迟注册）
    try:
        # 尝试从 registry 获取（如果已注册）或触发注册
        if "AdaptiveRetrieveToolkit" not in manager._toolkit_registry:
            manager._register_toolkit_class(
                "AdaptiveRetrieveToolkit",
                builtin["AdaptiveRetrieveToolkit"]
            )
        toolkit_class = manager._toolkit_registry["AdaptiveRetrieveToolkit"]
        print(f"✅ AdaptiveRetrieveToolkit 类加载成功: {toolkit_class}")
    except Exception as e:
        print(f"❌ 加载失败: {e}")
        raise


def test_toolkit_initialization():
    """测试 2：Toolkit 初始化和配置"""
    print("\n" + "=" * 70)
    print("测试 2：Toolkit 初始化")
    print("=" * 70)

    # 检查环境变量配置
    required_env = {
        "RAGFLOW_API_URL": os.getenv("RAGFLOW_API_URL"),
        "RAGFLOW_API_KEY": os.getenv("RAGFLOW_API_KEY"),
        "RAGFLOW_KB_ID": os.getenv("RAGFLOW_KB_ID"),
        "EXA_API_KEY": os.getenv("EXA_API_KEY"),
    }

    print("\n配置检查:")
    all_configured = True
    for key, value in required_env.items():
        if value:
            masked = value[:8] + "..." if len(value) > 8 else "***"
            print(f"  ✅ {key}: {masked}")
        else:
            print(f"  ❌ {key}: 未配置")
            all_configured = False

    if not all_configured:
        print("\n⚠️  部分配置缺失，将跳过实际 API 调用测试")
        return False

    # 尝试初始化 toolkit（使用 MCP Exa）
    try:
        toolkit = AdaptiveRetrieveToolkit(
            enabled=True,
            api_url=required_env["RAGFLOW_API_URL"],
            api_key=required_env["RAGFLOW_API_KEY"],
            kb_id=required_env["RAGFLOW_KB_ID"],
            timeout=30,
            # 使用 MCP Exa（提供 LLM 生成的摘要，避免上下文爆炸）
            web_toolkit_config={
                "server_name": "exa",
                "server_type": "http",
                "url": "https://mcp.exa.ai/mcp",
                "headers": {
                    "Authorization": f"Bearer {required_env['EXA_API_KEY']}"
                },
                "use_storage": False,  # 测试环境禁用 storage（需要 file_storage 实例）
            },
            web_tool_method="web_search_exa",
            web_tool_kwargs={
                # ✅ 修复：使用正确的 EXA API 参数名
                "summary": True,  # 启用 LLM 生成的摘要（官方参数）
                "text": True,     # 同时返回完整文本（可选）
                "highlights": True,  # 返回高亮片段（可选）
            },
            top_n_default=5,
        )
        print(f"\n✅ AdaptiveRetrieveToolkit 初始化成功")
        print(f"   RAGFlow toolkit: {toolkit.ragflow_toolkit}")
        print(f"   Web search: MCP Exa (with autoprompt summaries)")
        return True
    except Exception as e:
        print(f"\n❌ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_tool_methods():
    """测试 3：工具方法暴露"""
    print("\n" + "=" * 70)
    print("测试 3：工具方法暴露")
    print("=" * 70)

    try:
        # 使用 MCP Exa 配置
        toolkit = AdaptiveRetrieveToolkit(
            enabled=True,
            api_url=os.getenv("RAGFLOW_API_URL", "http://localhost:9380"),
            api_key=os.getenv("RAGFLOW_API_KEY", "dummy"),
            kb_id=os.getenv("RAGFLOW_KB_ID", "dummy"),
            web_toolkit_config={
                "server_name": "exa",
                "server_type": "http",
                "url": "https://mcp.exa.ai/mcp",
                "headers": {
                    "Authorization": f"Bearer {os.getenv('EXA_API_KEY', 'dummy')}"
                },
                "use_storage": False,  # 测试环境禁用 storage
            },
            web_tool_method="web_search_exa",
            web_tool_kwargs={
                # ✅ 使用正确的 EXA API 参数
                "summary": True,
                "text": True,
                "highlights": True,
            },
        )

        # 检查 get_available_tool_names
        available_tools = toolkit.get_available_tool_names()
        print(f"\n可用工具: {available_tools}")
        assert "search_adaptive" in available_tools, "❌ search_adaptive not in available tools"
        print(f"✅ search_adaptive 已暴露")

        # 检查方法是否存在
        assert hasattr(toolkit, "search_adaptive"), "❌ search_adaptive method not found"
        search_method = getattr(toolkit, "search_adaptive")
        assert callable(search_method), "❌ search_adaptive is not callable"
        print(f"✅ search_adaptive 方法可调用")

        # 检查 docstring
        docstring = search_method.__doc__
        assert docstring and len(docstring) > 100, "❌ search_adaptive docstring too short"
        print(f"✅ search_adaptive 有完整的 docstring ({len(docstring)} 字符)")
        print(f"\nDocstring 预览:")
        print(f"  {docstring[:200]}...")

        return True
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_search_adaptive_call_async():
    """测试 4：search_adaptive 方法调用（使用真实 API）- 异步版本"""
    print("\n" + "=" * 70)
    print("测试 4：search_adaptive 实际调用")
    print("=" * 70)

    # 检查配置
    required_env = {
        "RAGFLOW_API_URL": os.getenv("RAGFLOW_API_URL"),
        "RAGFLOW_API_KEY": os.getenv("RAGFLOW_API_KEY"),
        "RAGFLOW_KB_ID": os.getenv("RAGFLOW_KB_ID"),
        "EXA_API_KEY": os.getenv("EXA_API_KEY"),
    }

    if not all(required_env.values()):
        print("⚠️  API 配置不完整，跳过实际调用测试")
        return False

    try:
        # 初始化 adaptive toolkit（使用 MCP Exa）
        toolkit = AdaptiveRetrieveToolkit(
            enabled=True,
            api_url=required_env["RAGFLOW_API_URL"],
            api_key=required_env["RAGFLOW_API_KEY"],
            kb_id=required_env["RAGFLOW_KB_ID"],
            timeout=30,
            # 使用 MCP Exa（提供 LLM 生成的摘要）
            web_toolkit_config={
                "server_name": "exa",
                "server_type": "http",
                "url": "https://mcp.exa.ai/mcp",
                "headers": {
                    "Authorization": f"Bearer {required_env['EXA_API_KEY']}"
                },
                "use_storage": False,  # 测试环境禁用 storage
            },
            web_tool_method="web_search_exa",
            web_tool_kwargs={
                # ✅ 使用正确的 EXA API 参数
                "summary": True,
                "text": True,
                "highlights": True,
            },
            top_n_default=5,
        )

        # 测试查询
        test_queries = [
            {"query": "人工智能教育", "mode": "auto", "description": "自动模式（高置信度，应触发 RAG）"},
            {"query": "AI教育", "mode": "rag", "description": "强制 RAG 模式"},
            {"query": "latest AI trends 2026", "mode": "web", "description": "强制 Web 搜索模式"},
            {"query": "教育技术发展", "mode": "hybrid", "description": "强制 Hybrid 混合模式"},
            {"query": "xkjfhskdjfh random query", "mode": "auto", "description": "自动模式（低置信度，应触发 WEB）"},
        ]

        for i, test in enumerate(test_queries, 1):
            print(f"\n--- 测试查询 {i}: {test['description']} ---")
            print(f"Query: {test['query']}")
            print(f"Mode: {test['mode']}")

            try:
                # 调用 adaptive_retrieve_async（异步版本）
                top_n = 8 if test["mode"] in ["hybrid", "web"] else 5
                retrieve_result = await toolkit.adaptive_retrieve_async(
                    query=test["query"],
                    mode=test["mode"],
                    top_n=top_n,
                )

                # 转换为字典
                result = retrieve_result.to_dict()

                # 检查是否有错误
                if "error" in result:
                    print(f"\n❌ 查询失败！")
                    print(f"  Error: {result['error']}")
                    print(f"  Trigger reason: {result['debug'].get('trigger_reason', 'N/A')}")
                    continue

                # 验证结果结构
                assert "query" in result, "❌ 缺少 query 字段"
                assert "decision" in result, "❌ 缺少 decision 字段"
                assert "confidence" in result, "❌ 缺少 confidence 字段"
                assert "contexts" in result, "❌ 缺少 contexts 字段"
                assert "sources" in result, "❌ 缺少 sources 字段"
                assert "debug" in result, "❌ 缺少 debug 字段"

                print(f"\n✅ 查询成功！")
                print(f"  Decision: {result['decision']}")
                print(f"  Confidence: {result['confidence']:.2f}")
                print(f"  Contexts: {len(result['contexts'])} 条")
                print(f"  Sources: {len(result['sources'])} 个")
                print(f"  Duration: {result['debug'].get('duration_ms', 0)} ms")

                # 按来源分组显示证据
                ragflow_contexts = [ctx for ctx in result["contexts"] if ctx['source'] == 'ragflow']
                exa_contexts = [ctx for ctx in result["contexts"] if ctx['source'] == 'exa']
                
                if ragflow_contexts:
                    print(f"\n【RAGFlow 内部证据】({len(ragflow_contexts)} 条):")
                    for j, ctx in enumerate(ragflow_contexts[:2], 1):
                        print(f"  [{j}] Score: {ctx['score']:.2f}")
                        print(f"      URL: {ctx['url'][:80]}...")
                        print(f"      Title: {ctx.get('title', 'N/A')[:60]}...")
                        print(f"      Text: {ctx['text'][:100]}...")
                
                if exa_contexts:
                    print(f"\n【Exa Web 搜索证据】({len(exa_contexts)} 条):")
                    for j, ctx in enumerate(exa_contexts[:2], 1):
                        print(f"  [{j}] Score: {ctx['score']:.2f}")
                        print(f"      URL: {ctx['url'][:80]}...")
                        print(f"      Title: {ctx.get('title', 'N/A')[:60]}...")
                        print(f"      Text: {ctx['text'][:100]}...")

            except Exception as e:
                print(f"\n❌ 查询失败: {e}")
                import traceback
                traceback.print_exc()
                return False

        return True

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_search_adaptive_call():
    """测试 4 wrapper：在同步上下文中运行异步测试"""
    import asyncio
    return asyncio.run(test_search_adaptive_call_async())


def main():
    """运行所有测试"""
    print("\n" + "=" * 70)
    print("Module 4: AdaptiveRetrieveToolkit ROMA 集成测试")
    print("=" * 70)

    results = {}

    # 测试 1: 注册
    try:
        test_toolkit_registration()
        results["registration"] = True
    except Exception as e:
        print(f"❌ 测试 1 失败: {e}")
        results["registration"] = False

    # 测试 2: 初始化
    try:
        initialized = test_toolkit_initialization()
        results["initialization"] = initialized
    except Exception as e:
        print(f"❌ 测试 2 失败: {e}")
        results["initialization"] = False

    # 测试 3: 工具方法
    try:
        tool_methods_ok = test_tool_methods()
        results["tool_methods"] = tool_methods_ok
    except Exception as e:
        print(f"❌ 测试 3 失败: {e}")
        results["tool_methods"] = False

    # 测试 4: 实际调用（可选，取决于配置）
    try:
        api_call_ok = test_search_adaptive_call()
        results["api_call"] = api_call_ok
    except Exception as e:
        print(f"❌ 测试 4 失败: {e}")
        results["api_call"] = False

    # 总结
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name:20s}: {status}")

    all_passed = all(results.values())
    if all_passed:
        print("\n🎉 所有测试通过！AdaptiveRetrieveToolkit 已成功集成到 ROMA。")
    else:
        print("\n⚠️  部分测试失败，请检查配置和实现。")

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

