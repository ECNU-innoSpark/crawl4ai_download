"""
Module 1 单元测试：RAGFlowToolkit

使用 Mock 对象测试 RAGFlowToolkit 的所有功能，无需真实 API。
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
from requests.exceptions import Timeout, RequestException


# 导入要测试的模块
from src.roma_dspy.tools.ragflow_toolkit import (
    RAGFlowToolkit,
    RAGFlowAPIError,
    RAGFlowTimeoutError,
    create_ragflow_toolkit,
)


class TestRAGFlowToolkitInit:
    """测试 RAGFlowToolkit 初始化"""

    def test_init_with_all_params(self):
        """测试：使用所有参数初始化"""
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
            timeout=5.0,
        )
        
        assert toolkit.api_url == "http://localhost:9380/api"
        assert toolkit.api_key == "test-key"
        assert toolkit.kb_id == "test-kb-id"
        assert toolkit.timeout == 5.0

    def test_init_removes_trailing_slash(self):
        """测试：自动移除 URL 尾部斜杠"""
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api/",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        assert toolkit.api_url == "http://localhost:9380/api"

    def test_init_uses_config_timeout(self):
        """测试：未指定 timeout 时使用配置中的值"""
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        # 应该使用配置中的默认值（3.0秒）
        assert toolkit.timeout == 3.0

    def test_repr(self):
        """测试：字符串表示"""
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
            timeout=5.0,
        )
        
        repr_str = repr(toolkit)
        assert "RAGFlowToolkit" in repr_str
        assert "http://localhost:9380/api" in repr_str
        assert "test-kb-id" in repr_str
        assert "5.0s" in repr_str


class TestRAGFlowToolkitRetrieve:
    """测试 RAGFlowToolkit.retrieve 方法"""

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_success(self, mock_post):
        """测试：成功检索"""
        # Mock 响应
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "chunks": [
                    {
                        "id": "chunk_1",
                        "document_id": "doc_1",
                        "dataset_id": "kb_1",
                        "content": "这是测试文本内容",
                        "similarity": 0.85,
                        "document_keyword": "测试文档",
                        "important_keywords": ["测试", "文本"],
                    },
                ]
            }
        }
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        # 执行测试
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("测试查询", top_n=5)
        
        # 验证
        assert len(chunks) == 1
        assert chunks[0]["chunk_id"] == "chunk_1"
        assert chunks[0]["doc_id"] == "doc_1"
        assert chunks[0]["text"] == "这是测试文本内容"
        assert chunks[0]["score"] == 0.85
        assert chunks[0]["title"] == "测试文档"
        assert chunks[0]["keywords"] == ["测试", "文本"]
        
        # 验证 API 调用
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "http://localhost:9380/api/retrieval" in call_args[0]
        assert call_args[1]["json"]["question"] == "测试查询"
        assert call_args[1]["json"]["top_n"] == 5

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_old_api_format(self, mock_post):
        """测试：兼容旧版 API 格式（chunks 在顶层）"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "chunks": [
                {
                    "id": "chunk_1",
                    "document_id": "doc_1",
                    "dataset_id": "kb_1",
                    "content": "旧版格式内容",
                    "similarity": 0.9,
                },
            ]
        }
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("测试", top_n=5)
        
        assert len(chunks) == 1
        assert chunks[0]["text"] == "旧版格式内容"

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_empty_result(self, mock_post):
        """测试：返回空结果"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "chunks": []
            }
        }
        mock_response.elapsed.total_seconds.return_value = 0.3
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("不存在的内容", top_n=5)
        
        assert len(chunks) == 0

    def test_retrieve_empty_query(self):
        """测试：空查询返回空列表"""
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        # 测试空字符串
        chunks = toolkit.retrieve("", top_n=5)
        assert len(chunks) == 0
        
        # 测试只有空格的字符串
        chunks = toolkit.retrieve("   ", top_n=5)
        assert len(chunks) == 0

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_filters_empty_content(self, mock_post):
        """测试：过滤掉空内容的 chunks"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "chunks": [
                    {
                        "id": "chunk_1",
                        "document_id": "doc_1",
                        "dataset_id": "kb_1",
                        "content": "有效内容",
                        "similarity": 0.8,
                    },
                    {
                        "id": "chunk_2",
                        "document_id": "doc_2",
                        "dataset_id": "kb_1",
                        "content": "",  # 空内容，应该被过滤
                        "similarity": 0.9,
                    },
                ]
            }
        }
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("测试", top_n=5)
        
        # 应该只有 1 条（空内容的被过滤）
        assert len(chunks) == 1
        assert chunks[0]["chunk_id"] == "chunk_1"

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_timeout(self, mock_post):
        """测试：请求超时抛出异常"""
        mock_post.side_effect = Timeout("Connection timeout")
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
            timeout=3.0,
        )
        
        with pytest.raises(RAGFlowTimeoutError):
            toolkit.retrieve("测试", top_n=5)

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_http_error(self, mock_post):
        """测试：HTTP 错误抛出异常"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = RequestException("Server error")
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        with pytest.raises(RAGFlowAPIError):
            toolkit.retrieve("测试", top_n=5)

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_retrieve_invalid_json(self, mock_post):
        """测试：无效 JSON 响应抛出异常"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        with pytest.raises(RAGFlowAPIError):
            toolkit.retrieve("测试", top_n=5)


class TestRAGFlowToolkitHealthCheck:
    """测试健康检查功能"""

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_health_check_success(self, mock_post):
        """测试：健康检查成功"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"chunks": []}}
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        is_healthy = toolkit.health_check()
        
        assert is_healthy is True

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_health_check_failure(self, mock_post):
        """测试：健康检查失败"""
        mock_post.side_effect = RequestException("Connection failed")
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        is_healthy = toolkit.health_check()
        
        assert is_healthy is False


class TestCreateRAGFlowToolkit:
    """测试 create_ragflow_toolkit 便捷函数"""

    @patch.dict('os.environ', {
        'RAGFLOW_API_URL': 'http://test-url',
        'RAGFLOW_API_KEY': 'test-key',
        'RAGFLOW_KB_ID': 'test-kb-id',
    })
    def test_create_from_env(self):
        """测试：从环境变量创建"""
        toolkit = create_ragflow_toolkit()
        
        assert toolkit.api_url == "http://test-url"
        assert toolkit.api_key == "test-key"
        assert toolkit.kb_id == "test-kb-id"

    def test_create_with_explicit_params(self):
        """测试：使用显式参数创建"""
        toolkit = create_ragflow_toolkit(
            api_url="http://explicit-url",
            api_key="explicit-key",
            kb_id="explicit-kb",
        )
        
        assert toolkit.api_url == "http://explicit-url"
        assert toolkit.api_key == "explicit-key"
        assert toolkit.kb_id == "explicit-kb"

    @patch.dict('os.environ', {}, clear=True)
    def test_create_missing_api_url(self):
        """测试：缺少 api_url 抛出异常"""
        with pytest.raises(ValueError, match="api_url"):
            create_ragflow_toolkit()

    @patch.dict('os.environ', {'RAGFLOW_API_URL': 'http://test'}, clear=True)
    def test_create_missing_api_key(self):
        """测试：缺少 api_key 抛出异常"""
        with pytest.raises(ValueError, match="api_key"):
            create_ragflow_toolkit()

    @patch.dict('os.environ', {
        'RAGFLOW_API_URL': 'http://test',
        'RAGFLOW_API_KEY': 'test-key',
    }, clear=True)
    def test_create_missing_kb_id(self):
        """测试：缺少 kb_id 抛出异常"""
        with pytest.raises(ValueError, match="kb_id"):
            create_ragflow_toolkit()


class TestRAGFlowToolkitEdgeCases:
    """测试边界情况"""

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_large_top_n(self, mock_post):
        """测试：大 top_n 值"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"chunks": []}}
        mock_response.elapsed.total_seconds.return_value = 1.0
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("测试", top_n=1000)
        
        # 验证 API 调用参数
        call_args = mock_post.call_args
        assert call_args[1]["json"]["top_n"] == 1000

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_special_characters_in_query(self, mock_post):
        """测试：查询中包含特殊字符"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"chunks": []}}
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        special_queries = [
            "查询包含\"引号\"",
            "查询包含\n换行符",
            "查询包含\t制表符",
            "查询包含<HTML>标签</HTML>",
        ]
        
        for query in special_queries:
            chunks = toolkit.retrieve(query, top_n=5)
            # 应该能正常处理，不抛异常
            assert isinstance(chunks, list)

    @patch('src.roma_dspy.tools.ragflow_toolkit.requests.post')
    def test_malformed_chunk_data(self, mock_post):
        """测试：格式错误的 chunk 数据"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "chunks": [
                    {
                        "id": "chunk_1",
                        "document_id": "doc_1",
                        "content": "正常内容",
                        "similarity": 0.8,
                    },
                    {
                        # 缺少必需字段的 chunk，应该被跳过
                        "id": "chunk_2",
                        # 缺少 document_id
                    },
                    {
                        "id": "chunk_3",
                        "document_id": "doc_3",
                        "content": "另一个正常内容",
                        "similarity": "invalid",  # 无效的 similarity 类型
                    },
                ]
            }
        }
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_post.return_value = mock_response
        
        toolkit = RAGFlowToolkit(
            api_url="http://localhost:9380/api",
            api_key="test-key",
            kb_id="test-kb-id",
        )
        
        chunks = toolkit.retrieve("测试", top_n=5)
        
        # 应该只保留格式正确的 chunks
        # chunk_2 应该被跳过（缺少字段）
        # chunk_3 应该被处理（即使 similarity 类型错误，也会尝试转换）
        assert len(chunks) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])













