"""
MCP Browser Server for NestBrowse
基于 Playwright 的无头浏览器服务，通过 MCP 协议提供浏览器交互能力

启动方式:
    python mcp_browser_server.py --port 8080
    
或使用 uvicorn:
    uvicorn mcp_browser_server:app --host 0.0.0.0 --port 8080
"""

import os
import re
import json
import asyncio
import argparse
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

# Web server
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import StreamingResponse, JSONResponse, Response
from starlette.requests import Request
import uvicorn

# Playwright
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BrowserSession:
    """浏览器会话管理"""
    context: BrowserContext
    page: Page
    current_url: str = ""
    dom_snapshot: Dict = field(default_factory=dict)
    element_refs: Dict[str, Any] = field(default_factory=dict)


class BrowserManager:
    """浏览器管理器"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.sessions: Dict[str, BrowserSession] = {}
        self._lock = asyncio.Lock()

    async def start(self):
        """启动浏览器"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        logger.info(f"Browser started (headless={self.headless})")

    async def stop(self):
        """停止浏览器"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser stopped")

    async def get_or_create_session(self, session_id: str) -> BrowserSession:
        """获取或创建会话"""
        async with self._lock:
            if session_id not in self.sessions:
                context = await self.browser.new_context(
                    viewport={'width': 1280, 'height': 800},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = await context.new_page()
                self.sessions[session_id] = BrowserSession(context=context, page=page)
                logger.info(f"Created new session: {session_id}")
            return self.sessions[session_id]

    async def close_session(self, session_id: str):
        """关闭会话"""
        async with self._lock:
            if session_id in self.sessions:
                session = self.sessions[session_id]
                await session.context.close()
                del self.sessions[session_id]
                logger.info(f"Closed session: {session_id}")


# 全局浏览器管理器
browser_manager: Optional[BrowserManager] = None


async def extract_dom_snapshot(page: Page) -> tuple[str, Dict[str, Any]]:
    """
    提取页面的 DOM 快照，返回文本内容和元素引用映射
    """
    # 等待页面加载
    try:
        await page.wait_for_load_state('domcontentloaded', timeout=10000)
    except:
        pass
    
    # 执行 JavaScript 提取 DOM
    snapshot_script = """
    () => {
        const elements = [];
        const refs = {};
        let refCounter = 0;
        
        function processNode(node, depth = 0) {
            if (node.nodeType === Node.TEXT_NODE) {
                const text = node.textContent.trim();
                if (text) {
                    return text;
                }
                return null;
            }
            
            if (node.nodeType !== Node.ELEMENT_NODE) {
                return null;
            }
            
            const tag = node.tagName.toLowerCase();
            
            // 跳过不可见元素和脚本/样式
            if (['script', 'style', 'noscript', 'meta', 'link'].includes(tag)) {
                return null;
            }
            
            const style = window.getComputedStyle(node);
            if (style.display === 'none' || style.visibility === 'hidden') {
                return null;
            }
            
            const result = { tag, children: [] };
            
            // 检查是否可交互
            const isClickable = tag === 'a' || tag === 'button' || 
                              node.onclick !== null ||
                              style.cursor === 'pointer' ||
                              node.getAttribute('role') === 'button' ||
                              node.getAttribute('role') === 'link';
            
            const isInput = tag === 'input' || tag === 'textarea' || 
                           node.contentEditable === 'true' ||
                           tag === 'select';
            
            if (isClickable || isInput) {
                const ref = 'e' + (refCounter++);
                result.ref = ref;
                
                // 获取元素的定位信息
                const rect = node.getBoundingClientRect();
                refs[ref] = {
                    tag: tag,
                    type: isInput ? 'input' : 'clickable',
                    text: node.innerText?.slice(0, 100) || node.value || '',
                    href: node.href || '',
                    placeholder: node.placeholder || '',
                    inputType: node.type || '',
                    rect: {
                        x: rect.x,
                        y: rect.y,
                        width: rect.width,
                        height: rect.height
                    }
                };
            }
            
            // 提取重要属性
            if (tag === 'a' && node.href) {
                result.href = node.href;
            }
            if (tag === 'img' && node.alt) {
                result.alt = node.alt;
            }
            
            // 处理子节点
            for (const child of node.childNodes) {
                const childResult = processNode(child, depth + 1);
                if (childResult) {
                    if (typeof childResult === 'string') {
                        result.children.push(childResult);
                    } else {
                        result.children.push(childResult);
                    }
                }
            }
            
            // 简化：如果只有文本子节点，直接返回文本
            if (result.children.length === 1 && typeof result.children[0] === 'string') {
                result.text = result.children[0];
                delete result.children;
            } else if (result.children.length === 0) {
                delete result.children;
            }
            
            return result;
        }
        
        const bodyResult = processNode(document.body);
        return { dom: bodyResult, refs: refs };
    }
    """
    
    try:
        result = await page.evaluate(snapshot_script)
        
        # 将 DOM 转换为文本格式
        text_content = dom_to_text(result.get('dom', {}))
        element_refs = result.get('refs', {})
        
        return text_content, element_refs
    except Exception as e:
        logger.error(f"Error extracting DOM: {e}")
        # 降级：直接获取文本内容
        try:
            text = await page.inner_text('body')
            return text[:50000], {}
        except:
            return "Failed to extract page content", {}


def dom_to_text(node: Any, indent: int = 0) -> str:
    """将 DOM 节点转换为格式化文本"""
    if isinstance(node, str):
        return node
    
    if not isinstance(node, dict):
        return ""
    
    parts = []
    tag = node.get('tag', '')
    ref = node.get('ref', '')
    text = node.get('text', '')
    href = node.get('href', '')
    children = node.get('children', [])
    
    # 构建元素描述
    prefix = ""
    if ref:
        prefix = f"[ref={ref}] "
    
    if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        level = int(tag[1])
        parts.append(f"\n{'#' * level} {prefix}{text}\n")
    elif tag == 'a':
        link_text = text or "link"
        if href:
            parts.append(f"{prefix}[{link_text}]({href})")
        else:
            parts.append(f"{prefix}{link_text}")
    elif tag == 'button':
        parts.append(f"{prefix}[Button: {text}]")
    elif tag in ['input', 'textarea']:
        input_type = node.get('inputType', 'text')
        placeholder = node.get('placeholder', '')
        parts.append(f"{prefix}[Input({input_type}): {placeholder}]")
    elif tag == 'img':
        alt = node.get('alt', 'image')
        parts.append(f"[Image: {alt}]")
    elif tag == 'li':
        parts.append(f"• {prefix}{text}")
    elif tag == 'p':
        if text:
            parts.append(f"{prefix}{text}\n")
    elif tag in ['div', 'span', 'section', 'article', 'main']:
        if text:
            parts.append(f"{prefix}{text}")
    else:
        if text:
            parts.append(f"{prefix}{text}")
    
    # 处理子节点
    for child in children:
        child_text = dom_to_text(child, indent + 1)
        if child_text:
            parts.append(child_text)
    
    return " ".join(parts)


class MCPBrowserTools:
    """MCP 浏览器工具集"""
    
    def __init__(self, browser_manager: BrowserManager):
        self.browser_manager = browser_manager
    
    async def browser_navigate(self, session_id: str, params: Dict) -> Dict:
        """导航到指定 URL。支持 include_screenshot=True 时在 content 末尾附加 Base64 截图块（ViNest）。"""
        url = params.get('url', '')
        include_screenshot = params.get('include_screenshot', False)
        if not url:
            return {"error": "URL is required"}
        
        # 确保 URL 有协议
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            session = await self.browser_manager.get_or_create_session(session_id)
            
            # 导航到页面
            response = await session.page.goto(url, timeout=30000, wait_until='domcontentloaded')
            
            # 等待额外时间让 JS 渲染
            await asyncio.sleep(1)
            
            # 提取 DOM 快照
            text_content, element_refs = await extract_dom_snapshot(session.page)
            
            session.current_url = session.page.url
            session.element_refs = element_refs
            
            if include_screenshot:
                try:
                    import base64
                    screenshot = await session.page.screenshot(type='png')
                    screenshot_b64 = base64.b64encode(screenshot).decode('utf-8')
                    text_content = text_content + "\n\n[SCREENSHOT_BASE64]\n" + screenshot_b64 + "\n[/SCREENSHOT_BASE64]"
                except Exception as e:
                    logger.warning(f"browser_navigate: screenshot failed, continuing without: {e}")
            
            return {
                "success": True,
                "url": session.current_url,
                "title": await session.page.title(),
                "content": text_content,
                "elements": self._format_interactive_elements(element_refs)
            }
        except Exception as e:
            logger.error(f"Navigation error: {e}")
            return {"error": str(e)}
    
    async def browser_click(self, session_id: str, params: Dict) -> Dict:
        """点击页面元素。支持 include_screenshot=True 时在 content 末尾附加 Base64 截图块（ViNest）。"""
        ref = params.get('ref', '')
        include_screenshot = params.get('include_screenshot', False)
        if not ref:
            return {"error": "Element ref is required"}
        
        try:
            session = await self.browser_manager.get_or_create_session(session_id)
            
            if ref not in session.element_refs:
                return {"error": f"Element ref '{ref}' not found. Available refs: {list(session.element_refs.keys())[:20]}"}
            
            element_info = session.element_refs[ref]
            rect = element_info.get('rect', {})
            
            # 使用坐标点击
            x = rect.get('x', 0) + rect.get('width', 0) / 2
            y = rect.get('y', 0) + rect.get('height', 0) / 2
            
            await session.page.mouse.click(x, y)
            
            # 等待页面响应
            await asyncio.sleep(2)
            
            # 重新提取 DOM
            text_content, element_refs = await extract_dom_snapshot(session.page)
            session.current_url = session.page.url
            session.element_refs = element_refs
            
            if include_screenshot:
                try:
                    import base64
                    screenshot = await session.page.screenshot(type='png')
                    screenshot_b64 = base64.b64encode(screenshot).decode('utf-8')
                    text_content = text_content + "\n\n[SCREENSHOT_BASE64]\n" + screenshot_b64 + "\n[/SCREENSHOT_BASE64]"
                except Exception as e:
                    logger.warning(f"browser_click: screenshot failed, continuing without: {e}")
            
            return {
                "success": True,
                "url": session.current_url,
                "title": await session.page.title(),
                "content": text_content,
                "elements": self._format_interactive_elements(element_refs)
            }
        except Exception as e:
            logger.error(f"Click error: {e}")
            return {"error": str(e)}
    
    async def browser_type(self, session_id: str, params: Dict) -> Dict:
        """在输入框中输入文本"""
        ref = params.get('ref', '')
        text = params.get('text', '')
        submit = params.get('submit', False)
        
        if not ref:
            return {"error": "Element ref is required"}
        
        try:
            session = await self.browser_manager.get_or_create_session(session_id)
            
            if ref not in session.element_refs:
                return {"error": f"Element ref '{ref}' not found"}
            
            element_info = session.element_refs[ref]
            rect = element_info.get('rect', {})
            
            # 点击输入框
            x = rect.get('x', 0) + rect.get('width', 0) / 2
            y = rect.get('y', 0) + rect.get('height', 0) / 2
            await session.page.mouse.click(x, y)
            
            # 清除现有内容并输入新文本
            await session.page.keyboard.press('Control+a')
            await session.page.keyboard.type(text)
            
            if submit:
                await session.page.keyboard.press('Enter')
                await asyncio.sleep(2)
            
            return {
                "success": True,
                "message": f"Typed '{text}' into element [ref={ref}]"
            }
        except Exception as e:
            logger.error(f"Type error: {e}")
            return {"error": str(e)}
    
    async def browser_scroll(self, session_id: str, params: Dict) -> Dict:
        """滚动页面"""
        direction = params.get('direction', 'down')
        amount = params.get('amount', 500)
        
        try:
            session = await self.browser_manager.get_or_create_session(session_id)
            
            if direction == 'down':
                await session.page.mouse.wheel(0, amount)
            elif direction == 'up':
                await session.page.mouse.wheel(0, -amount)
            
            await asyncio.sleep(0.5)
            
            # 重新提取 DOM
            text_content, element_refs = await extract_dom_snapshot(session.page)
            session.element_refs = element_refs
            
            return {
                "success": True,
                "content": text_content,
                "elements": self._format_interactive_elements(element_refs)
            }
        except Exception as e:
            logger.error(f"Scroll error: {e}")
            return {"error": str(e)}
    
    async def browser_screenshot(self, session_id: str, params: Dict) -> Dict:
        """截取页面截图"""
        try:
            session = await self.browser_manager.get_or_create_session(session_id)
            
            screenshot = await session.page.screenshot(type='png')
            import base64
            screenshot_b64 = base64.b64encode(screenshot).decode('utf-8')
            
            return {
                "success": True,
                "screenshot": screenshot_b64,
                "format": "base64/png"
            }
        except Exception as e:
            logger.error(f"Screenshot error: {e}")
            return {"error": str(e)}
    
    def _format_interactive_elements(self, refs: Dict) -> str:
        """格式化可交互元素列表"""
        if not refs:
            return "No interactive elements found."
        
        clickable = []
        inputs = []
        
        for ref, info in refs.items():
            elem_type = info.get('type', '')
            text = info.get('text', '')[:50]
            href = info.get('href', '')
            
            if elem_type == 'clickable':
                if href:
                    clickable.append(f"[ref={ref}] {info.get('tag')}: {text} -> {href[:60]}")
                else:
                    clickable.append(f"[ref={ref}] {info.get('tag')}: {text}")
            elif elem_type == 'input':
                placeholder = info.get('placeholder', '')
                input_type = info.get('inputType', 'text')
                inputs.append(f"[ref={ref}] input({input_type}): {placeholder}")
        
        result = []
        if clickable:
            result.append("Clickable elements:\n" + "\n".join(clickable[:30]))
        if inputs:
            result.append("Input fields:\n" + "\n".join(inputs[:20]))
        
        return "\n\n".join(result) if result else "No interactive elements found."


# MCP 工具定义
MCP_TOOLS = [
    {
        "name": "browser_navigate",
        "description": "Navigate to a URL and return the page content. Set include_screenshot=True (ViNest) to also return a base64 screenshot.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to navigate to"},
                "include_screenshot": {"type": "boolean", "description": "If true, append base64 screenshot to content for visual inner loop (ViNest)"}
            },
            "required": ["url"]
        }
    },
    {
        "name": "browser_click",
        "description": "Click on an element identified by its ref. Set include_screenshot=True (ViNest) to also return a base64 screenshot.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ref": {"type": "string", "description": "The element reference (e.g., e0, e1)"},
                "element": {"type": "string", "description": "Element description (optional)"},
                "include_screenshot": {"type": "boolean", "description": "If true, append base64 screenshot to content for visual inner loop (ViNest)"}
            },
            "required": ["ref"]
        }
    },
    {
        "name": "browser_type",
        "description": "Type text into an input field",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ref": {"type": "string", "description": "The input element reference"},
                "text": {"type": "string", "description": "Text to type"},
                "submit": {"type": "boolean", "description": "Whether to submit after typing"},
                "element": {"type": "string", "description": "Element description (optional)"}
            },
            "required": ["ref", "text"]
        }
    },
    {
        "name": "browser_scroll",
        "description": "Scroll the page",
        "inputSchema": {
            "type": "object",
            "properties": {
                "direction": {"type": "string", "enum": ["up", "down"], "description": "Scroll direction"},
                "amount": {"type": "integer", "description": "Pixels to scroll"}
            }
        }
    },
    {
        "name": "browser_screenshot",
        "description": "Take a screenshot of the current page",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    }
]


# 会话消息队列：session_id -> asyncio.Queue
session_queues: Dict[str, asyncio.Queue] = {}


def format_sse_message(event: str, data: Any) -> str:
    """格式化 SSE 消息"""
    if isinstance(data, dict):
        data = json.dumps(data)
    return f"event: {event}\ndata: {data}\n\n"


async def handle_sse(request: Request):
    """SSE 连接：发送 endpoint（含 session ID），然后转发消息队列中的响应"""
    import uuid
    session_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    session_queues[session_id] = queue
    
    async def event_generator():
        try:
            # 在 endpoint URL 中嵌入 session_id，这样 POST 请求会自动带上
            yield format_sse_message("endpoint", f"/message?session={session_id}")
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield format_sse_message("message", msg)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            session_queues.pop(session_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


async def handle_message(request: Request):
    """处理 JSON-RPC 请求，响应通过 SSE 流发送"""
    global browser_manager

    if request.method == "GET":
        return JSONResponse({"ok": True, "hint": "POST JSON-RPC here"})

    # 从 URL query string 获取 session_id
    session_id = request.query_params.get("session", "default")
    queue = session_queues.get(session_id)
    
    try:
        body = await request.json()
    except Exception:
        return Response(status_code=400)
    
    method = body.get('method', '')
    params = body.get('params', {})
    request_id = body.get('id')
    
    # Notification（无 id）无需响应
    if request_id is None:
        return Response(status_code=202)
    
    tools = MCPBrowserTools(browser_manager)
    
    # 处理 MCP 方法
    if method == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "nestbrowse-browser", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {"tools": MCP_TOOLS}
    elif method == "tools/call":
        tool_name = params.get('name', '')
        tool_args = params.get('arguments', {})
        
        if tool_name == 'browser_navigate':
            result = await tools.browser_navigate(session_id, tool_args)
        elif tool_name == 'browser_click':
            result = await tools.browser_click(session_id, tool_args)
        elif tool_name == 'browser_type':
            result = await tools.browser_type(session_id, tool_args)
        elif tool_name == 'browser_scroll':
            result = await tools.browser_scroll(session_id, tool_args)
        elif tool_name == 'browser_screenshot':
            result = await tools.browser_screenshot(session_id, tool_args)
        else:
            result = {"error": f"Unknown tool: {tool_name}"}
        
        # 格式化工具调用结果
        if 'error' in result:
            result = {
                "content": [{"type": "text", "text": f"Error: {result['error']}"}],
                "isError": True
            }
        else:
            # 组合内容
            content_parts = []
            if 'content' in result:
                content_parts.append(result['content'])
            if 'elements' in result:
                content_parts.append(f"\n\nInteractive Elements:\n{result['elements']}")
            
            text_content = "\n".join(content_parts) if content_parts else json.dumps(result)
            result = {
                "content": [{"type": "text", "text": text_content}],
                "isError": False
            }
    else:
        response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
        if queue:
            await queue.put(response)
        return Response(status_code=202)

    # 把响应放入队列，通过 SSE 发送
    response = {"jsonrpc": "2.0", "id": request_id, "result": result}
    if queue:
        await queue.put(response)
    return Response(status_code=202)


# Starlette 应用
@asynccontextmanager
async def lifespan(app):
    """应用生命周期管理"""
    global browser_manager
    
    headless = os.getenv('BROWSER_HEADLESS', 'true').lower() == 'true'
    browser_manager = BrowserManager(headless=headless)
    await browser_manager.start()
    
    yield
    
    await browser_manager.stop()


async def handle_root(request: Request):
    """根路径，便于检查服务是否存活"""
    return JSONResponse({
        "service": "nestbrowse-mcp-browser",
        "version": "1.0.0",
        "endpoints": {"sse": "/sse", "message": "/message (POST)"},
    })


app = Starlette(
    debug=True,
    routes=[
        Route("/", handle_root, methods=["GET"]),
        Route("/sse", handle_sse, methods=["GET"]),
        Route("/message", handle_message, methods=["GET", "POST"]),
        Route("/messages", handle_message, methods=["GET", "POST"]),
    ],
    lifespan=lifespan,
)


def main():
    """主入口"""
    parser = argparse.ArgumentParser(description='NestBrowse MCP Browser Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=8080, help='Port to bind')
    parser.add_argument('--headless', action='store_true', default=True, help='Run browser in headless mode')
    parser.add_argument('--no-headless', action='store_false', dest='headless', help='Run browser with GUI')
    
    args = parser.parse_args()
    
    os.environ['BROWSER_HEADLESS'] = str(args.headless).lower()
    
    print(f"Starting MCP Browser Server on {args.host}:{args.port}")
    print(f"Headless mode: {args.headless}")
    print(f"SSE endpoint: http://{args.host}:{args.port}/sse")
    
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
