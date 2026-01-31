"""
Search Tool for NestBrowse
支持多种搜索后端：Google Custom Search API、SerpAPI、Bing Search API
"""

import os
import json
import asyncio
import aiohttp
from typing import List, Dict, Optional, Union


class Search:
    """
    搜索工具类，执行批量网页搜索并返回 Top-10 结果
    
    支持的搜索后端:
    - google: Google Custom Search API
    - serpapi: SerpAPI (Google Search)
    - bing: Bing Web Search API
    - duckduckgo: DuckDuckGo (免费，无需 API key)
    """
    
    tool_schema = {
        "type": "function",
        "function": {
            "name": "search",
            "description": "Perform Google web searches then returns a string of the top search results. Accepts multiple queries.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "The search query."
                        },
                        "minItems": 1,
                        "description": "The list of search queries."
                    }
                },
                "required": ["query"]
            }
        }
    }

    def __init__(
        self,
        backend: str = "serpapi",
        num_results: int = 10,
        google_api_key: Optional[str] = None,
        google_cse_id: Optional[str] = None,
        serpapi_api_key: Optional[str] = None,
        bing_api_key: Optional[str] = None,
    ):
        """
        初始化搜索工具
        
        Args:
            backend: 搜索后端，可选 "google", "serpapi", "bing", "duckduckgo"
            num_results: 每个查询返回的结果数量
            google_api_key: Google Custom Search API Key
            google_cse_id: Google Custom Search Engine ID
            serpapi_api_key: SerpAPI Key
            bing_api_key: Bing Search API Key
        """
        self.backend = backend
        self.num_results = num_results
        
        # 从环境变量或参数获取 API keys
        self.google_api_key = google_api_key or os.getenv("GOOGLE_API_KEY")
        self.google_cse_id = google_cse_id or os.getenv("GOOGLE_CSE_ID")
        self.serpapi_api_key = serpapi_api_key or os.getenv("SERPAPI_API_KEY")
        self.bing_api_key = bing_api_key or os.getenv("BING_API_KEY")

    async def call(self, params: Union[str, Dict]) -> str:
        """
        执行搜索调用
        
        Args:
            params: 包含 "query" 字段的 JSON 字符串或字典
            
        Returns:
            格式化的搜索结果字符串
        """
        try:
            if isinstance(params, str):
                params = json.loads(params)
            elif not isinstance(params, dict):
                raise ValueError("Invalid params type")
            
            queries = params.get("query", [])
            if isinstance(queries, str):
                queries = [queries]
                
            if not queries:
                return "[search] Error: No search queries provided."
                
        except Exception as e:
            return f"[search] Invalid request format: {str(e)}"

        # 并发执行所有查询
        tasks = [self._search_single(query) for query in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 格式化结果
        output_parts = []
        for i, (query, result) in enumerate(zip(queries, results)):
            if isinstance(result, Exception):
                output_parts.append(f"Query {i+1}: {query}\nError: {str(result)}\n")
            else:
                output_parts.append(f"Query {i+1}: {query}\n{result}\n")
        
        return "[search] Search Results:\n\n" + "\n".join(output_parts)

    async def _search_single(self, query: str) -> str:
        """执行单个查询"""
        if self.backend == "google":
            return await self._google_search(query)
        elif self.backend == "serpapi":
            return await self._serpapi_search(query)
        elif self.backend == "bing":
            return await self._bing_search(query)
        elif self.backend == "duckduckgo":
            return await self._duckduckgo_search(query)
        else:
            raise ValueError(f"Unsupported search backend: {self.backend}")

    async def _google_search(self, query: str) -> str:
        """使用 Google Custom Search API"""
        if not self.google_api_key or not self.google_cse_id:
            return "Error: Google API Key or CSE ID not configured."
        
        url = "https://www.googleapis.com/customsearch/v1"
        params = {
            "key": self.google_api_key,
            "cx": self.google_cse_id,
            "q": query,
            "num": min(self.num_results, 10)  # Google CSE 最多返回 10 个结果
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return f"Error: Google API returned status {response.status}"
                
                data = await response.json()
                return self._format_google_results(data)

    def _format_google_results(self, data: Dict) -> str:
        """格式化 Google 搜索结果"""
        items = data.get("items", [])
        if not items:
            return "No results found."
        
        results = []
        for i, item in enumerate(items[:self.num_results], 1):
            title = item.get("title", "No title")
            link = item.get("link", "")
            snippet = item.get("snippet", "No description")
            results.append(f"{i}. [{title}]({link})\n   {snippet}")
        
        return "\n".join(results)

    async def _serpapi_search(self, query: str) -> str:
        """使用 SerpAPI"""
        if not self.serpapi_api_key:
            return "Error: SerpAPI Key not configured."
        
        url = "https://serpapi.com/search"
        params = {
            "api_key": self.serpapi_api_key,
            "q": query,
            "engine": "google",
            "num": self.num_results
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return f"Error: SerpAPI returned status {response.status}"
                
                data = await response.json()
                return self._format_serpapi_results(data)

    def _format_serpapi_results(self, data: Dict) -> str:
        """格式化 SerpAPI 结果"""
        organic_results = data.get("organic_results", [])
        if not organic_results:
            return "No results found."
        
        results = []
        for i, item in enumerate(organic_results[:self.num_results], 1):
            title = item.get("title", "No title")
            link = item.get("link", "")
            snippet = item.get("snippet", "No description")
            results.append(f"{i}. [{title}]({link})\n   {snippet}")
        
        return "\n".join(results)

    async def _bing_search(self, query: str) -> str:
        """使用 Bing Web Search API"""
        if not self.bing_api_key:
            return "Error: Bing API Key not configured."
        
        url = "https://api.bing.microsoft.com/v7.0/search"
        headers = {"Ocp-Apim-Subscription-Key": self.bing_api_key}
        params = {
            "q": query,
            "count": self.num_results,
            "textDecorations": False,
            "textFormat": "Raw"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status != 200:
                    return f"Error: Bing API returned status {response.status}"
                
                data = await response.json()
                return self._format_bing_results(data)

    def _format_bing_results(self, data: Dict) -> str:
        """格式化 Bing 搜索结果"""
        web_pages = data.get("webPages", {}).get("value", [])
        if not web_pages:
            return "No results found."
        
        results = []
        for i, item in enumerate(web_pages[:self.num_results], 1):
            title = item.get("name", "No title")
            link = item.get("url", "")
            snippet = item.get("snippet", "No description")
            results.append(f"{i}. [{title}]({link})\n   {snippet}")
        
        return "\n".join(results)

    async def _duckduckgo_search(self, query: str) -> str:
        """使用 DuckDuckGo (免费，无需 API key)"""
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            return "Error: duckduckgo_search package not installed. Run: pip install duckduckgo_search"
        
        try:
            # DuckDuckGo 搜索是同步的，需要在线程中运行
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                lambda: list(DDGS().text(query, max_results=self.num_results))
            )
            return self._format_duckduckgo_results(results)
        except Exception as e:
            return f"Error: DuckDuckGo search failed: {str(e)}"

    def _format_duckduckgo_results(self, results: List[Dict]) -> str:
        """格式化 DuckDuckGo 结果"""
        if not results:
            return "No results found."
        
        formatted = []
        for i, item in enumerate(results[:self.num_results], 1):
            title = item.get("title", "No title")
            link = item.get("href", "")
            snippet = item.get("body", "No description")
            formatted.append(f"{i}. [{title}]({link})\n   {snippet}")
        
        return "\n".join(formatted)


# 便捷函数：创建默认搜索实例
def create_search_tool(backend: str = None) -> Search:
    """
    创建搜索工具实例，自动检测可用的后端
    
    优先级: serpapi > google > bing > duckduckgo
    """
    if backend:
        return Search(backend=backend)
    
    # 自动检测可用后端
    if os.getenv("SERPAPI_API_KEY"):
        return Search(backend="serpapi")
    elif os.getenv("GOOGLE_API_KEY") and os.getenv("GOOGLE_CSE_ID"):
        return Search(backend="google")
    elif os.getenv("BING_API_KEY"):
        return Search(backend="bing")
    else:
        # 默认使用 DuckDuckGo (免费)
        return Search(backend="duckduckgo")
