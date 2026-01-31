"""
Module 0 测试套件：测试 RAGFlow URI 工具

测试 RAGFlowReference 和相关 URI 解析工具。
"""

import pytest
from src.roma_dspy.types.ragflow_types import (
    RAGFlowReference,
    create_ragflow_uri,
    parse_ragflow_uri,
    is_ragflow_uri,
    RAGFLOW_URI_SCHEME,
)


class TestRAGFlowReference:
    """测试 RAGFlowReference 数据类"""

    def test_create_reference_full(self):
        """测试：创建完整的 RAGFlowReference"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
            chunk_id="chunk789",
            original_url="https://example.com/original",
        )
        
        assert ref.kb_id == "kb123"
        assert ref.doc_id == "doc456"
        assert ref.chunk_id == "chunk789"
        assert ref.original_url == "https://example.com/original"

    def test_create_reference_minimal(self):
        """测试：创建最小字段的 RAGFlowReference"""
        ref = RAGFlowReference(
            kb_id="kb001",
            doc_id="doc001",
        )
        
        assert ref.kb_id == "kb001"
        assert ref.doc_id == "doc001"
        assert ref.chunk_id is None
        assert ref.original_url is None

    def test_to_uri_with_chunk(self):
        """测试：生成包含 chunk 的 URI"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
            chunk_id="chunk789",
        )
        
        uri = ref.to_uri()
        
        assert uri == "ragflow://kb/kb123/doc/doc456#chunk=chunk789"
        assert uri.startswith(RAGFLOW_URI_SCHEME)

    def test_to_uri_without_chunk(self):
        """测试：生成不包含 chunk 的 URI"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
        )
        
        uri = ref.to_uri()
        
        assert uri == "ragflow://kb/kb123/doc/doc456"
        assert "#chunk=" not in uri

    def test_to_display_url_with_original(self):
        """测试：优先显示 original_url"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
            chunk_id="chunk789",
            original_url="https://example.com/article",
        )
        
        display_url = ref.to_display_url()
        
        assert display_url == "https://example.com/article"
        assert display_url != ref.to_uri()

    def test_to_display_url_without_original(self):
        """测试：无 original_url 时回退到 URI"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
            chunk_id="chunk789",
        )
        
        display_url = ref.to_display_url()
        
        assert display_url == ref.to_uri()
        assert display_url == "ragflow://kb/kb123/doc/doc456#chunk=chunk789"

    def test_from_uri_with_chunk(self):
        """测试：从完整 URI 解析"""
        uri = "ragflow://kb/kb123/doc/doc456#chunk=chunk789"
        
        ref = RAGFlowReference.from_uri(uri)
        
        assert ref is not None
        assert ref.kb_id == "kb123"
        assert ref.doc_id == "doc456"
        assert ref.chunk_id == "chunk789"

    def test_from_uri_without_chunk(self):
        """测试：从不含 chunk 的 URI 解析"""
        uri = "ragflow://kb/kb123/doc/doc456"
        
        ref = RAGFlowReference.from_uri(uri)
        
        assert ref is not None
        assert ref.kb_id == "kb123"
        assert ref.doc_id == "doc456"
        assert ref.chunk_id is None

    def test_from_uri_invalid(self):
        """测试：无效 URI 返回 None"""
        invalid_uris = [
            "https://example.com",
            "ragflow://invalid/format",
            "ragflow://kb/",
            "not-a-uri",
            "",
        ]
        
        for uri in invalid_uris:
            ref = RAGFlowReference.from_uri(uri)
            assert ref is None, f"应该无法解析: {uri}"

    def test_reference_round_trip(self):
        """测试：创建 -> URI -> 解析的往返一致性"""
        original = RAGFlowReference(
            kb_id="test_kb",
            doc_id="test_doc",
            chunk_id="test_chunk",
        )
        
        # 往返：对象 -> URI -> 对象
        uri = original.to_uri()
        restored = RAGFlowReference.from_uri(uri)
        
        assert restored is not None
        assert restored.kb_id == original.kb_id
        assert restored.doc_id == original.doc_id
        assert restored.chunk_id == original.chunk_id

    def test_reference_repr(self):
        """测试：RAGFlowReference 的字符串表示"""
        ref = RAGFlowReference(
            kb_id="kb123",
            doc_id="doc456",
            chunk_id="chunk789",
        )
        
        repr_str = repr(ref)
        
        assert "kb=kb123" in repr_str
        assert "doc=doc456" in repr_str
        assert "chunk=chunk789" in repr_str


class TestRAGFlowURIHelpers:
    """测试 URI 工具函数"""

    def test_create_ragflow_uri_with_chunk(self):
        """测试：create_ragflow_uri 完整参数"""
        uri = create_ragflow_uri(
            kb_id="kb001",
            doc_id="doc002",
            chunk_id="chunk003",
        )
        
        assert uri == "ragflow://kb/kb001/doc/doc002#chunk=chunk003"

    def test_create_ragflow_uri_without_chunk(self):
        """测试：create_ragflow_uri 不含 chunk"""
        uri = create_ragflow_uri(
            kb_id="kb001",
            doc_id="doc002",
        )
        
        assert uri == "ragflow://kb/kb001/doc/doc002"
        assert "#chunk=" not in uri

    def test_parse_ragflow_uri_valid(self):
        """测试：parse_ragflow_uri 解析有效 URI"""
        uri = "ragflow://kb/ai_edu/doc/dl_basics#chunk=intro"
        
        result = parse_ragflow_uri(uri)
        
        assert result is not None
        assert result["kb_id"] == "ai_edu"
        assert result["doc_id"] == "dl_basics"
        assert result["chunk_id"] == "intro"

    def test_parse_ragflow_uri_without_chunk(self):
        """测试：parse_ragflow_uri 解析不含 chunk 的 URI"""
        uri = "ragflow://kb/ai_edu/doc/dl_basics"
        
        result = parse_ragflow_uri(uri)
        
        assert result is not None
        assert result["kb_id"] == "ai_edu"
        assert result["doc_id"] == "dl_basics"
        assert result["chunk_id"] is None

    def test_parse_ragflow_uri_invalid(self):
        """测试：parse_ragflow_uri 处理无效 URI"""
        invalid_uris = [
            "https://example.com",
            "ragflow://wrong/format",
            "not-a-uri",
        ]
        
        for uri in invalid_uris:
            result = parse_ragflow_uri(uri)
            assert result is None, f"应该返回 None: {uri}"

    def test_is_ragflow_uri_valid(self):
        """测试：is_ragflow_uri 识别有效 URI"""
        valid_uris = [
            "ragflow://kb/kb1/doc/doc1",
            "ragflow://kb/kb123/doc/doc456#chunk=chunk789",
            "ragflow://kb/test/doc/test",
        ]
        
        for uri in valid_uris:
            assert is_ragflow_uri(uri) is True, f"应该是有效 URI: {uri}"

    def test_is_ragflow_uri_invalid(self):
        """测试：is_ragflow_uri 识别无效 URI"""
        invalid_uris = [
            "https://example.com",
            "http://ragflow.com",
            "ragflow://invalid",
            "not-a-uri",
            "",
        ]
        
        for uri in invalid_uris:
            assert is_ragflow_uri(uri) is False, f"应该是无效 URI: {uri}"


class TestRAGFlowURIEdgeCases:
    """测试边界情况和特殊字符"""

    def test_uri_with_special_characters_in_ids(self):
        """测试：ID 中包含特殊字符"""
        # 注意：实际使用中应该对特殊字符进行 URL 编码
        ref = RAGFlowReference(
            kb_id="kb-with-dash_and_underscore",
            doc_id="doc.with.dots",
            chunk_id="chunk_123",
        )
        
        uri = ref.to_uri()
        restored = RAGFlowReference.from_uri(uri)
        
        assert restored is not None
        assert restored.kb_id == ref.kb_id
        assert restored.doc_id == ref.doc_id

    def test_uri_with_long_ids(self):
        """测试：很长的 ID"""
        long_id = "a" * 100
        ref = RAGFlowReference(
            kb_id=long_id,
            doc_id=long_id,
            chunk_id=long_id,
        )
        
        uri = ref.to_uri()
        restored = RAGFlowReference.from_uri(uri)
        
        assert restored is not None
        assert restored.kb_id == long_id
        assert restored.doc_id == long_id
        assert restored.chunk_id == long_id

    def test_uri_scheme_case_sensitive(self):
        """测试：URI scheme 大小写敏感性"""
        # 标准是小写 ragflow://
        valid_uri = "ragflow://kb/kb1/doc/doc1"
        invalid_uri_upper = "RAGFLOW://kb/kb1/doc/doc1"
        
        assert is_ragflow_uri(valid_uri) is True
        assert is_ragflow_uri(invalid_uri_upper) is False


class TestRAGFlowURIIntegration:
    """集成测试：实际使用场景"""

    def test_create_and_verify_uri_for_chunk(self):
        """
        场景：从 RAGFlow API 返回的 chunk 数据创建 URI
        """
        # 模拟 RAGFlow API 返回
        chunk_data = {
            "id": "chunk_abc123",
            "document_id": "doc_xyz789",
            "dataset_id": "kb_edu_001",
            "content": "这是文档内容...",
        }
        
        # 创建 URI
        uri = create_ragflow_uri(
            kb_id=chunk_data["dataset_id"],
            doc_id=chunk_data["document_id"],
            chunk_id=chunk_data["id"],
        )
        
        # 验证格式
        assert is_ragflow_uri(uri) is True
        
        # 解析验证
        parsed = parse_ragflow_uri(uri)
        assert parsed["kb_id"] == "kb_edu_001"
        assert parsed["doc_id"] == "doc_xyz789"
        assert parsed["chunk_id"] == "chunk_abc123"
        
        print(f"✅ 生成的 URI: {uri}")

    def test_batch_create_uris_for_multiple_chunks(self):
        """
        场景：批量为多个 chunks 创建 URI
        """
        chunks = [
            {"kb": "kb1", "doc": "doc1", "chunk": "c1"},
            {"kb": "kb1", "doc": "doc1", "chunk": "c2"},
            {"kb": "kb1", "doc": "doc2", "chunk": "c3"},
        ]
        
        uris = [
            create_ragflow_uri(c["kb"], c["doc"], c["chunk"])
            for c in chunks
        ]
        
        assert len(uris) == 3
        assert all(is_ragflow_uri(uri) for uri in uris)
        
        # 验证不同 chunk 生成不同 URI
        assert len(set(uris)) == 3
        
        print("✅ 批量生成 URI:")
        for uri in uris:
            print(f"  - {uri}")

    def test_uri_in_citation_format(self):
        """
        场景：在 Deep Research 报告中使用内联引用格式
        """
        uri = create_ragflow_uri(
            kb_id="ai_education_kb",
            doc_id="personalized_learning_paper",
            chunk_id="section_2_para_3",
        )
        
        # 模拟 DR 中的引用格式
        citation = f"[Source: {uri}]"
        
        assert citation.startswith("[Source: ragflow://")
        assert citation.endswith("]")
        
        # 验证能从引用中提取 URI
        extracted_uri = citation.replace("[Source: ", "").replace("]", "")
        assert is_ragflow_uri(extracted_uri) is True
        
        print(f"✅ 引用格式: {citation}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

