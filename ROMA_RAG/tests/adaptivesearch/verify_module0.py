"""
Module 0 快速验证脚本（独立版本）
验证所有核心功能是否正常工作，不依赖其他 ROMA 模块
"""

import sys
import os
from pathlib import Path

# 设置 UTF-8 输出（Windows 兼容）
if sys.platform == 'win32':
    os.system('chcp 65001 > nul')
    sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 60)
print("Module 0 验证脚本")
print("=" * 60)

# ========== 测试 1: 直接导入模块文件 ==========
print("\n[测试 1] 导入核心类型...")
try:
    # 直接导入，避免触发 roma_dspy.__init__
    import importlib.util
    
    # 导入 retrieve_result
    spec1 = importlib.util.spec_from_file_location(
        "retrieve_result",
        project_root / "src/roma_dspy/types/retrieve_result.py"
    )
    retrieve_result = importlib.util.module_from_spec(spec1)
    spec1.loader.exec_module(retrieve_result)
    
    RetrieveResult = retrieve_result.RetrieveResult
    RetrieveContext = retrieve_result.RetrieveContext
    RetrieveDebugInfo = retrieve_result.RetrieveDebugInfo
    DecisionType = retrieve_result.DecisionType
    SourceType = retrieve_result.SourceType
    
    print("[OK] RetrieveResult 模块导入成功")
except Exception as e:
    print(f"[FAIL] RetrieveResult 模块导入失败: {e}")
    sys.exit(1)

try:
    # 导入 ragflow_types
    spec2 = importlib.util.spec_from_file_location(
        "ragflow_types",
        project_root / "src/roma_dspy/types/ragflow_types.py"
    )
    ragflow_types = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(ragflow_types)
    
    RAGFlowReference = ragflow_types.RAGFlowReference
    create_ragflow_uri = ragflow_types.create_ragflow_uri
    parse_ragflow_uri = ragflow_types.parse_ragflow_uri
    is_ragflow_uri = ragflow_types.is_ragflow_uri
    
    print("[OK] RAGFlow 类型模块导入成功")
except Exception as e:
    print(f"[FAIL] RAGFlow 类型模块导入失败: {e}")
    sys.exit(1)

try:
    # 导入 retrieve_config
    spec3 = importlib.util.spec_from_file_location(
        "retrieve_config",
        project_root / "src/roma_dspy/config/retrieve_config.py"
    )
    retrieve_config = importlib.util.module_from_spec(spec3)
    spec3.loader.exec_module(retrieve_config)
    
    RetrieveConfig = retrieve_config.RetrieveConfig
    get_retrieve_config = retrieve_config.get_retrieve_config
    
    print("[OK] 检索配置模块导入成功")
except Exception as e:
    print(f"[FAIL] 检索配置模块导入失败: {e}")
    sys.exit(1)

# ========== 测试 2: RetrieveResult 创建 ==========
print("\n[测试 2] 创建 RetrieveResult...")
try:
    result = RetrieveResult(
        query="测试查询",
        decision=DecisionType.RAG,
        confidence=0.85,
        contexts=[
            RetrieveContext(
                text="这是一段测试文本",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/test123/doc/doc456#chunk=chunk789",
                title="测试文档",
                score=0.9,
            ),
        ],
        sources=["ragflow://kb/test123/doc/doc456#chunk=chunk789"],
        debug=RetrieveDebugInfo(
            trigger_reason="测试原因",
            rag_top1_score=0.9,
            rag_result_count=1,
            web_triggered=False,
            duration_ms=150,
        ),
    )
    print(f"[OK] RetrieveResult 创建成功")
    print(f"     {result}")
except Exception as e:
    print(f"[FAIL] RetrieveResult 创建失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 3: JSON 序列化 ==========
print("\n[测试 3] JSON 序列化测试...")
try:
    import json
    
    # 序列化
    data_dict = result.to_dict()
    json_str = json.dumps(data_dict, ensure_ascii=False, indent=2)
    print(f"[OK] 序列化成功，JSON 长度: {len(json_str)} 字符")
    
    # 反序列化
    restored_dict = json.loads(json_str)
    restored_result = RetrieveResult.from_dict(restored_dict)
    print(f"[OK] 反序列化成功")
    
    # 验证一致性
    assert restored_result.query == result.query
    assert restored_result.decision == result.decision
    assert restored_result.confidence == result.confidence
    print("[OK] 往返一致性验证通过")
except Exception as e:
    print(f"[FAIL] JSON 序列化测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 4: RAGFlow URI ==========
print("\n[测试 4] RAGFlow URI 工具测试...")
try:
    # 创建 URI
    uri = create_ragflow_uri("kb123", "doc456", "chunk789")
    print(f"[OK] 创建 URI: {uri}")
    
    # 验证 URI
    assert is_ragflow_uri(uri) is True
    print(f"[OK] URI 验证通过")
    
    # 解析 URI
    parsed = parse_ragflow_uri(uri)
    assert parsed["kb_id"] == "kb123"
    assert parsed["doc_id"] == "doc456"
    assert parsed["chunk_id"] == "chunk789"
    print(f"[OK] URI 解析成功: {parsed}")
    
    # 往返测试
    ref = RAGFlowReference(kb_id="test", doc_id="doc", chunk_id="chunk")
    uri2 = ref.to_uri()
    ref2 = RAGFlowReference.from_uri(uri2)
    assert ref2.kb_id == ref.kb_id
    assert ref2.doc_id == ref.doc_id
    assert ref2.chunk_id == ref.chunk_id
    print("[OK] URI 往返测试通过")
except Exception as e:
    print(f"[FAIL] RAGFlow URI 测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 5: 检索配置 ==========
print("\n[测试 5] 检索配置测试...")
try:
    config = get_retrieve_config()
    print(f"[OK] 获取配置成功")
    
    # 验证关键配置项
    assert config.MAX_WEB_SEARCH_PER_TASK == 1
    assert config.WEB_SEARCH_TIMEOUT_SECONDS == 8.0
    assert config.RAGFLOW_TIMEOUT_SECONDS == 3.0
    assert config.MAX_CONTEXTS_PER_RETRIEVE == 12
    assert config.MIN_CONTEXTS_PER_RETRIEVE == 8
    print("[OK] 配置项验证通过")
    
    # 验证不可变性
    try:
        config.MAX_WEB_SEARCH_PER_TASK = 999
        print("[FAIL] 配置应该是不可变的！")
        sys.exit(1)
    except Exception:
        print("[OK] 配置不可变性验证通过")
    
    # 验证 to_dict
    config_dict = config.to_dict()
    assert len(config_dict) > 15
    print(f"[OK] 配置转字典成功，包含 {len(config_dict)} 个配置项")
except Exception as e:
    print(f"[FAIL] 检索配置测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 6: 三种检索模式 ==========
print("\n[测试 6] 三种检索模式测试...")
try:
    # RAG 模式
    rag_result = RetrieveResult(
        query="RAG 测试",
        decision=DecisionType.RAG,
        confidence=0.9,
        contexts=[
            RetrieveContext(
                text="内部文档",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/kb1/doc/doc1",
                score=0.9,
            ),
        ],
        sources=["ragflow://kb/kb1/doc/doc1"],
        debug=RetrieveDebugInfo(trigger_reason="high quality", duration_ms=200),
    )
    assert rag_result.decision == DecisionType.RAG
    print("[OK] RAG 模式测试通过")
    
    # Web 模式
    web_result = RetrieveResult(
        query="Web 测试",
        decision=DecisionType.WEB,
        confidence=0.3,
        contexts=[
            RetrieveContext(
                text="外部网页",
                source=SourceType.EXA,
                url="https://example.com",
                score=1.0,
            ),
        ],
        sources=["https://example.com"],
        debug=RetrieveDebugInfo(trigger_reason="low quality", web_triggered=True, duration_ms=8000),
    )
    assert web_result.decision == DecisionType.WEB
    assert web_result.debug.web_triggered is True
    print("[OK] Web 模式测试通过")
    
    # Hybrid 模式
    hybrid_result = RetrieveResult(
        query="Hybrid 测试",
        decision=DecisionType.HYBRID,
        confidence=0.6,
        contexts=[
            RetrieveContext(
                text="内部",
                source=SourceType.RAGFLOW,
                url="ragflow://test",
                score=0.7,
            ),
            RetrieveContext(
                text="外部",
                source=SourceType.EXA,
                url="https://example.com",
                score=0.9,
            ),
        ],
        sources=["ragflow://test", "https://example.com"],
        debug=RetrieveDebugInfo(trigger_reason="medium quality", web_triggered=True, duration_ms=4000),
    )
    assert hybrid_result.decision == DecisionType.HYBRID
    sources_types = {ctx.source for ctx in hybrid_result.contexts}
    assert SourceType.RAGFLOW in sources_types
    assert SourceType.EXA in sources_types
    print("[OK] Hybrid 模式测试通过")
except Exception as e:
    print(f"[FAIL] 三种检索模式测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 7: DR 节点数据消费模拟 ==========
print("\n[测试 7] DR 节点数据消费模拟...")
try:
    # 创建一个模拟的检索结果
    dr_result = RetrieveResult(
        query="AI教育现状",
        decision=DecisionType.HYBRID,
        confidence=0.68,
        contexts=[
            RetrieveContext(
                text="AI教育正在向个性化学习方向发展...",
                source=SourceType.RAGFLOW,
                url="ragflow://kb/edu/doc/ai_edu#chunk=1",
                title="AI教育趋势",
                score=0.82,
            ),
            RetrieveContext(
                text="2026年AI教育投资增长45%...",
                source=SourceType.EXA,
                url="https://example.com/research",
                title="AI教育投资报告",
                score=0.91,
            ),
        ],
        sources=[
            "ragflow://kb/edu/doc/ai_edu#chunk=1",
            "https://example.com/research",
        ],
        debug=RetrieveDebugInfo(
            trigger_reason="confidence=0.68, medium quality, hybrid",
            rag_top1_score=0.82,
            rag_result_count=1,
            web_triggered=True,
            duration_ms=4200,
        ),
    )
    
    # 模拟 THINK 节点读取 contexts
    contexts = dr_result.contexts
    assert len(contexts) > 0
    for ctx in contexts:
        assert ctx.text
        assert ctx.url
    print(f"[OK] THINK 节点可读取 {len(contexts)} 条 contexts")
    
    # 模拟 WRITE 节点生成引用
    sources = dr_result.sources
    assert len(sources) > 0
    citation_1 = f"[Source: {sources[0]}]"
    citation_2 = f"[Source: {sources[1]}]"
    assert "[Source: ragflow://" in citation_1
    assert "[Source: https://" in citation_2
    print(f"[OK] WRITE 节点可生成 {len(sources)} 个引用")
    print(f"     - {citation_1}")
    print(f"     - {citation_2}")
except Exception as e:
    print(f"[FAIL] DR 节点数据消费模拟失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 测试 8: 性能测试 ==========
print("\n[测试 8] 性能测试...")
try:
    import time
    
    # 创建性能测试
    start = time.time()
    for i in range(100):
        test_result = RetrieveResult(
            query=f"query_{i}",
            decision=DecisionType.RAG,
            confidence=0.8,
            contexts=[
                RetrieveContext(
                    text=f"content_{i}",
                    source=SourceType.RAGFLOW,
                    url=f"ragflow://kb/kb{i}/doc/doc{i}",
                    score=0.9,
                ),
            ],
            sources=[f"ragflow://kb/kb{i}/doc/doc{i}"],
            debug=RetrieveDebugInfo(trigger_reason="test", duration_ms=100),
        )
    duration = time.time() - start
    
    print(f"[OK] 创建 100 个 RetrieveResult 耗时: {duration*1000:.1f}ms")
    assert duration < 0.5, "性能不达标"
except Exception as e:
    print(f"[FAIL] 性能测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ========== 总结 ==========
print("\n" + "=" * 60)
print("[SUCCESS] Module 0 所有验证通过！")
print("=" * 60)
print("\n验证项目:")
print("  1. [OK] 核心类型导入")
print("  2. [OK] RetrieveResult 创建")
print("  3. [OK] JSON 序列化往返")
print("  4. [OK] RAGFlow URI 工具")
print("  5. [OK] 检索配置验证")
print("  6. [OK] 三种检索模式")
print("  7. [OK] DR 节点数据消费")
print("  8. [OK] 性能测试")
print("\n[READY] Module 0 已就绪，可以开始 Module 1！")
