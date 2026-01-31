"""MCP 客户端封装"""
import os
import asyncio
from contextlib import asynccontextmanager

from mcp import ClientSession
from mcp.client.sse import sse_client

MCP_TIMEOUT = float(os.environ.get("MCP_CONNECT_TIMEOUT", "60"))


@asynccontextmanager
async def mcp_client(server_url: str):
    """
    连接 MCP 浏览器服务，返回 (session, lock)。
    """
    lock = asyncio.Lock()
    
    async with sse_client(url=server_url) as streams:
        async with ClientSession(*streams) as session:
            await session.initialize()
            tools = await session.list_tools()
            print(f"[MCP] Connected. Tools: {[t.name for t in tools.tools]}")
            yield session, lock
