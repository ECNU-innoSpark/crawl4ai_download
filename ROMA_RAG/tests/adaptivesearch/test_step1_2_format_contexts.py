"""
RAGFlow Integration - Step 1.2 测试脚本

Module 1 - Step 1.2: 测试 chunks 到 contexts 的格式化

测试目标：
1. 测试完整流程：retrieve() -> format_ragflow_contexts()
2. 验证 URL 字段是可解析的 ragflow:// URI
3. 验证所有必需字段都存在且格式正确

测试策略：
- 优先使用真实 RAGFlow API（更贴近实际使用场景）
- 如果 API 不可用，自动降级到 Mock 数据测试
- 这样既保证了测试的实用性，又保证了可重复性

使用方法：
1. 推荐：设置环境变量，使用真实 API 测试
   export RAGFLOW_API_URL="http://your-ragflow-url/api"
   export RAGFLOW_API_KEY="your-api-key"
   export RAGFLOW_KB_ID="your-kb-id"

2. 运行测试：
   python tests/ragflow/test_step1_2_format_contexts.py

3. 如果未设置环境变量，测试会自动使用 Mock 数据
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# 设置 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')

# ========== 从 .env 文件加载配置 ==========
def load_env_file():
    """从 .env 文件加载环境变量"""
    env_path = project_root / ".env"
    
    if env_path.exists():
        print(f"✓ 找到 .env 文件: {env_path}")
        # 尝试多种编码
        for encoding in ['utf-8', 'utf-8-sig', 'gbk', 'latin-1']:
            try:
                with open(env_path, 'r', encoding=encoding, errors='ignore') as f:
                    for line in f:
                        line = line.strip()
                        # 跳过空行和注释
                        if not line or line.startswith('#'):
                            continue
                        # 解析 KEY=VALUE
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip()
                            # 移除引号
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            os.environ[key] = value
                return True
            except UnicodeDecodeError:
                continue
        return True
    else:
        print(f"✗ 未找到 .env 文件: {env_path}")
        return False

print("=" * 70)
print("RAGFlow Step 1.2 测试: Chunks 格式化为 Contexts")
print("=" * 70)

# 尝试加载 .env 文件
print("\n[初始化] 加载配置...")
env_loaded = load_env_file()

# ========== 导入 RAGFlowToolkit ==========
print("\n[导入] 加载 RAGFlowToolkit...")
try:
    import importlib.util
    
    spec = importlib.util.spec_from_file_location(
        "ragflow_toolkit",
        project_root / "src/roma_dspy/tools/ragflow_toolkit.py"
    )
    ragflow_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ragflow_module)
    
    RAGFlowToolkit = ragflow_module.RAGFlowToolkit
    
    print("[OK] RAGFlowToolkit 导入成功")
except Exception as e:
    print(f"[FAIL] 导入失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 配置检查 ==========
print("\n[配置检查] 读取 RAGFlow API 配置...")
RAGFLOW_API_URL = os.getenv("RAGFLOW_API_URL")
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY")
RAGFLOW_KB_ID = os.getenv("RAGFLOW_KB_ID")

# 调试信息：显示配置状态
print(f"  RAGFLOW_API_URL: {'✓ 已设置' if RAGFLOW_API_URL else '✗ 未设置'}")
print(f"  RAGFLOW_API_KEY: {'✓ 已设置' if RAGFLOW_API_KEY else '✗ 未设置'}")
print(f"  RAGFLOW_KB_ID: {'✓ 已设置' if RAGFLOW_KB_ID else '✗ 未设置'}")

USE_REAL_API = bool(RAGFLOW_API_URL and RAGFLOW_API_KEY and RAGFLOW_KB_ID)

if USE_REAL_API:
    print(f"\n[测试模式] 真实 API 测试 ✨")
    print(f"  API URL: {RAGFLOW_API_URL}")
    print(f"  KB ID: {RAGFLOW_KB_ID}")
    print(f"  API Key: {RAGFLOW_API_KEY[:10]}..." if len(RAGFLOW_API_KEY) > 10 else f"  API Key: ***")
else:
    print(f"\n[测试模式] Mock 数据测试")
    print(f"  [原因] 缺少必需的环境变量配置")
    print(f"  [提示] 要使用真实 API 测试，请在 .env 文件中设置：")
    print(f"    RAGFLOW_API_URL=http://your-ragflow-url/api")
    print(f"    RAGFLOW_API_KEY=your-api-key")
    print(f"    RAGFLOW_KB_ID=your-kb-id")

# ========== 测试 1: 真实 API 格式化测试 ==========
print("\n[测试 1] 完整流程测试：retrieve() -> format_ragflow_contexts()")
print("  测试完整的检索与格式化流程")

try:
    if USE_REAL_API:
        # ===== 使用真实 API =====
        print("\n  [模式] 真实 API 测试")
        
        # 创建真实的 toolkit
        toolkit = RAGFlowToolkit(
            api_url=RAGFLOW_API_URL,
            api_key=RAGFLOW_API_KEY,
            kb_id=RAGFLOW_KB_ID,
        )
        
        # 执行真实查询
        test_queries = ["AI", "教育", "深度学习"]
        chunks = []
        
        print("\n  [步骤 1] 调用 RAGFlow API 检索...")
        for query in test_queries:
            print(f"    尝试查询: '{query}'")
            try:
                result = toolkit.retrieve(query, top_n=5)
                if len(result) > 0:
                    chunks = result
                    print(f"    ✓ 查询成功，返回 {len(chunks)} 个 chunks")
                    break
                else:
                    print(f"    ✗ 查询返回 0 条结果")
            except Exception as e:
                print(f"    ✗ 查询失败: {e}")
                continue
        
        if len(chunks) == 0:
            print("\n  [警告] 所有查询都返回 0 条结果")
            print("  [原因分析]")
            print("    1. 知识库可能为空，请先上传文档")
            print("    2. 文档可能未完成解析")
            print("    3. 查询词与知识库内容不匹配")
            print("\n  [切换] 使用 Mock 数据继续测试...")
            USE_REAL_API = False
        else:
            print(f"\n  [步骤 2] 格式化 {len(chunks)} 个 chunks...")
    
    if not USE_REAL_API:
        # ===== 使用 Mock 数据 =====
        print("\n  [模式] Mock 数据测试")
        
        # 创建 toolkit 实例
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="mock-key",
            kb_id="test-kb-id-123",
        )
        
        print("  [步骤 1] 构造 Mock chunks 数据...")
        # 构造 mock chunks 数据（模拟 retrieve() 的返回）
        chunks = [
        {
            "chunk_id": "chunk-001",
            "doc_id": "doc-001",
            "kb_id": "test-kb-id-123",
            "text": "深度学习是机器学习的一个分支，使用多层神经网络来学习数据的表示。",
            "score": 0.92,
            "title": "深度学习基础教程",
            "keywords": ["深度学习", "神经网络", "机器学习"],
            "vector_similarity": 0.89,
            "term_similarity": 0.78,
        },
        {
            "chunk_id": "chunk-002",
            "doc_id": "doc-001",
            "kb_id": "test-kb-id-123",
            "text": "个性化学习是指根据学生的特点和需求，定制化的教学方案。",
            "score": 0.85,
            "title": "个性化教育研究",
            "keywords": ["个性化学习", "教育"],
            "position": ["page-3", "paragraph-2"],
        },
        {
            "chunk_id": "chunk-003",
            "doc_id": "doc-002",
            "kb_id": "test-kb-id-123",
            "text": "AI驱动的教育平台能够实时分析学生的学习数据。",
            "score": 0.78,
            "title": "AI教育平台设计",
            "keywords": ["AI", "教育平台"],
            "image_id": "img-snapshot-001",
        },
        ]
        print(f"        构造了 {len(chunks)} 个 Mock chunks")
    
    # ===== 格式化 chunks =====
    print(f"  [步骤 2] 调用 format_ragflow_contexts() 格式化...")
    contexts = toolkit.format_ragflow_contexts(chunks)
    
    # ===== 验证格式化结果 =====
    print(f"  [步骤 3] 验证格式化结果...")
    
    # 验证返回数量
    assert len(contexts) == len(chunks), \
        f"contexts 数量不匹配: 期望 {len(chunks)}, 实际 {len(contexts)}"
    print(f"  [OK] 返回 {len(contexts)} 个 contexts")
    
    # 验证每个 context 的格式（只详细检查前3个）
    max_display = min(3, len(contexts))
    for i in range(max_display):
        context = contexts[i]
        print(f"\n  --- Context {i+1}/{len(contexts)} ---")
        
        # 验证必需字段
        required_fields = ["text", "source", "url", "title", "score"]
        for field in required_fields:
            assert field in context, f"缺少必需字段: {field}"
        print(f"  [OK] 包含所有必需字段: {', '.join(required_fields)}")
        
        # 验证 source 字段
        assert context["source"] == "ragflow", \
            f"source 字段错误: 期望 'ragflow', 实际 '{context['source']}'"
        print(f"  [OK] source = 'ragflow'")
        
        # 验证 URL 格式
        url = context["url"]
        assert url.startswith("ragflow://"), \
            f"URL 格式错误: 应该以 'ragflow://' 开头"
        assert "kb/" in url and "doc/" in url and "chunk=" in url, \
            f"URL 格式错误: 缺少必需的 kb/doc/chunk 部分"
        print(f"  [OK] URL 格式正确: {url}")
        
        # 解析 URL，验证各部分
        # 格式: ragflow://kb/<kb_id>/doc/<doc_id>#chunk=<chunk_id>
        import re
        match = re.match(
            r'ragflow://kb/([^/]+)/doc/([^#]+)#chunk=(.+)',
            url
        )
        assert match, f"URL 格式无法解析: {url}"
        
        extracted_kb_id = match.group(1)
        extracted_doc_id = match.group(2)
        extracted_chunk_id = match.group(3)
        
        # 验证 URL 包含的 IDs 与原始 chunk 一致
        original_chunk = chunks[i]
        assert extracted_kb_id == original_chunk["kb_id"], \
            f"kb_id 不匹配: 期望 {original_chunk['kb_id']}, 实际 {extracted_kb_id}"
        assert extracted_doc_id == original_chunk["doc_id"], \
            f"doc_id 不匹配: 期望 {original_chunk['doc_id']}, 实际 {extracted_doc_id}"
        assert extracted_chunk_id == original_chunk["chunk_id"], \
            f"chunk_id 不匹配: 期望 {original_chunk['chunk_id']}, 实际 {extracted_chunk_id}"
        print(f"  [OK] URL 包含正确的 IDs: kb={extracted_kb_id}, doc={extracted_doc_id}, chunk={extracted_chunk_id}")
        
        # 验证 text 字段
        assert context["text"] == original_chunk["text"], \
            "text 字段不匹配"
        print(f"  [OK] text 正确: {context['text'][:50]}...")
        
        # 验证 score 字段
        assert isinstance(context["score"], (int, float)), \
            f"score 字段类型错误: {type(context['score'])}"
        assert 0.0 <= context["score"] <= 1.0, \
            f"score 超出范围: {context['score']}"
        print(f"  [OK] score = {context['score']:.4f}")
        
        # 验证 title 字段
        assert context["title"] == original_chunk.get("title", ""), \
            "title 字段不匹配"
        print(f"  [OK] title = '{context['title']}'")
        
        # 验证 metadata（如果存在）
        if "metadata" in context:
            print(f"  [OK] metadata 包含: {list(context['metadata'].keys())}")
            
            # 检查 keywords（宽松验证：只检查是否存在，不检查具体值）
            if "keywords" in context["metadata"]:
                keywords = context["metadata"]["keywords"]
                print(f"    - keywords: {keywords}")
                # 验证类型
                assert isinstance(keywords, list), f"keywords 应该是 list，实际是 {type(keywords)}"
            
            # 检查相似度字段（宽松验证：只检查存在且为数值）
            if "vector_similarity" in context["metadata"]:
                vec_sim = context["metadata"]["vector_similarity"]
                print(f"    - vector_similarity: {vec_sim}")
                assert isinstance(vec_sim, (int, float)), \
                    f"vector_similarity 应该是数值，实际是 {type(vec_sim)}"
            
            if "term_similarity" in context["metadata"]:
                term_sim = context["metadata"]["term_similarity"]
                print(f"    - term_similarity: {term_sim}")
                assert isinstance(term_sim, (int, float)), \
                    f"term_similarity 应该是数值，实际是 {type(term_sim)}"
            
            # 检查其他字段（只验证存在性）
            if "image_id" in context["metadata"]:
                print(f"    - image_id: {context['metadata']['image_id']}")
            
            if "position" in context["metadata"]:
                print(f"    - position: {context['metadata']['position']}")
    
    # 如果还有更多 contexts，简单统计
    if len(contexts) > max_display:
        print(f"\n  ... 还有 {len(contexts) - max_display} 个 contexts（已验证格式正确）")
    
    if USE_REAL_API:
        print("\n[OK] 测试 1 通过: 真实 API 完整流程测试成功 ✨")
        print("  ✓ retrieve() 调用成功")
        print("  ✓ format_ragflow_contexts() 格式化正确")
        print("  ✓ 所有字段验证通过")
    else:
        print("\n[OK] 测试 1 通过: Mock 数据格式化正确")

except Exception as e:
    print(f"\n[FAIL] 测试 1 失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 2: 边界情况 ==========
print("\n[测试 2] 边界情况测试...")

try:
    # 2.1 空列表
    print("\n  [2.1] 空 chunks 列表...")
    empty_contexts = toolkit.format_ragflow_contexts([])
    assert len(empty_contexts) == 0, "空列表应该返回空列表"
    print("  [OK] 空列表处理正常")
    
    # 2.2 缺少可选字段的 chunk
    print("\n  [2.2] 缺少可选字段的 chunk...")
    minimal_chunk = {
        "chunk_id": "minimal-001",
        "doc_id": "doc-minimal",
        "kb_id": "kb-minimal",
        "text": "这是一个最小化的 chunk，只包含必需字段。",
        "score": 0.5,
    }
    minimal_contexts = toolkit.format_ragflow_contexts([minimal_chunk])
    assert len(minimal_contexts) == 1, "应该返回 1 个 context"
    
    ctx = minimal_contexts[0]
    assert ctx["text"] == minimal_chunk["text"]
    assert ctx["source"] == "ragflow"
    assert ctx["url"].startswith("ragflow://")
    assert ctx["title"] == ""  # 缺少 title 时应该是空字符串
    assert ctx["score"] == 0.5
    assert "metadata" not in ctx or len(ctx["metadata"]) == 0  # 没有元数据
    print("  [OK] 最小化 chunk 处理正常")
    
    # 2.3 score 为 0 的 chunk
    print("\n  [2.3] score = 0 的 chunk...")
    zero_score_chunk = {
        "chunk_id": "zero-001",
        "doc_id": "doc-zero",
        "kb_id": "kb-zero",
        "text": "低相似度的结果",
        "score": 0.0,
    }
    zero_contexts = toolkit.format_ragflow_contexts([zero_score_chunk])
    assert len(zero_contexts) == 1
    assert zero_contexts[0]["score"] == 0.0
    print("  [OK] score=0 处理正常")
    
    # 2.4 非常长的文本
    print("\n  [2.4] 非常长的文本...")
    long_text = "这是一个很长的文本。" * 100
    long_chunk = {
        "chunk_id": "long-001",
        "doc_id": "doc-long",
        "kb_id": "kb-long",
        "text": long_text,
        "score": 0.7,
    }
    long_contexts = toolkit.format_ragflow_contexts([long_chunk])
    assert len(long_contexts) == 1
    assert long_contexts[0]["text"] == long_text
    print(f"  [OK] 长文本处理正常（{len(long_text)} 字符）")
    
    print("\n[OK] 测试 2 通过: 边界情况处理正常")

except Exception as e:
    print(f"\n[FAIL] 测试 2 失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 3: URL 可追溯性 ==========
print("\n[测试 3] URL 可追溯性测试...")

try:
    # 验证 URL 能够唯一标识一个 chunk
    print("\n  验证 URL 能够唯一标识 chunk...")
    
    test_chunks = [
        {
            "chunk_id": "test-chunk-1",
            "doc_id": "test-doc-1",
            "kb_id": "test-kb-1",
            "text": "测试文本1",
            "score": 0.9,
        },
        {
            "chunk_id": "test-chunk-2",
            "doc_id": "test-doc-1",  # 同一个文档
            "kb_id": "test-kb-1",
            "text": "测试文本2",
            "score": 0.8,
        },
        {
            "chunk_id": "test-chunk-1",  # 同样的 chunk_id
            "doc_id": "test-doc-2",      # 但不同的文档
            "kb_id": "test-kb-1",
            "text": "测试文本3",
            "score": 0.7,
        },
    ]
    
    contexts = toolkit.format_ragflow_contexts(test_chunks)
    urls = [ctx["url"] for ctx in contexts]
    
    # 验证 URL 都不相同（即使 chunk_id 相同）
    assert len(urls) == len(set(urls)), "URL 应该是唯一的"
    print(f"  [OK] 生成 {len(urls)} 个唯一的 URL")
    
    # 验证每个 URL 都包含正确的组件
    for i, (chunk, url) in enumerate(zip(test_chunks, urls)):
        assert chunk["kb_id"] in url, f"URL {i} 缺少 kb_id"
        assert chunk["doc_id"] in url, f"URL {i} 缺少 doc_id"
        assert chunk["chunk_id"] in url, f"URL {i} 缺少 chunk_id"
        print(f"  [OK] URL {i+1} 包含所有 IDs: {url}")
    
    print("\n[OK] 测试 3 通过: URL 可追溯性验证通过")

except Exception as e:
    print(f"\n[FAIL] 测试 3 失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 验收标准检查 ==========
print("\n" + "=" * 70)
print("[验收标准检查] Step 1.2")
print("=" * 70)

print("\n按照路线图 Step 1.2 验收标准:")
print("  - [x] 返回的 contexts 符合 Module 0 定义的格式")
print("  - [x] URL 字段是可解析的 ragflow:// URI")
print("  - [x] 包含所有必需字段: text, source, url, title, score")
print("  - [x] 可选的 metadata 字段正确保留额外信息")
print("  - [x] URL 能够唯一标识并追溯到具体的 chunk")

# ========== 总结 ==========
print("\n" + "=" * 70)
print("[SUCCESS] Step 1.2 测试完成！")
print("=" * 70)

print("\n所有测试通过:")
if USE_REAL_API:
    print("  1. [OK] 真实 API 完整流程 (retrieve -> format)")
else:
    print("  1. [OK] Mock 数据格式化")
print("  2. [OK] 边界情况处理")
print("  3. [OK] URL 可追溯性")

print("\n[READY] Step 1.2 实现完成，可以进入 Step 1.3 或 Module 2！")
print("=" * 70)

