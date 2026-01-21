#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Crawl4AI 通用爬虫服务
=====================

基于配置驱动的模块化爬虫实现，支持：
- 动态页面展开（自动点击加载更多）
- 正则表达式 URL 过滤
- 磁盘缓存避免重复抓取
- JSONL 格式结果存储

Author: Auto-generated
Date: 2026-01-20
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import yaml
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode


# =============================================================================
# 配置管理模块
# =============================================================================

class ConfigManager:
    """配置管理器：负责读取和验证 YAML 配置文件"""
    
    DEFAULT_CONFIG = {
        "target_url": "",
        "regex_pattern": ".*",
        "click_selector": "",
        "wait_for_selector": "",
        "max_clicks": 50,
        "click_delay_ms": 1000,
        "cache_path": "./.crawl4ai_cache",
        "enable_cache": True,
        "output_file": "results.jsonl",
        "browser": {
            "headless": True,
            "verbose": True,
            "use_magic_mode": True
        },
        "crawler": {
            "timeout": 60000,
            "wait_until_stable": True,
            "stable_check_timeout": 10
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = Path(config_path)
        self.config: dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load(self) -> dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
            
        Raises:
            FileNotFoundError: 配置文件不存在
            yaml.YAMLError: YAML 解析错误
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                user_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"YAML 解析错误: {e}")
        
        # 合并默认配置和用户配置
        self.config = self._merge_config(self.DEFAULT_CONFIG, user_config)
        self._validate()
        
        self.logger.info(f"配置加载成功: {self.config_path}")
        return self.config
    
    def _merge_config(self, default: dict, user: dict) -> dict:
        """递归合并配置"""
        result = default.copy()
        for key, value in user.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result
    
    def _validate(self) -> None:
        """验证配置有效性"""
        if not self.config.get("target_url"):
            raise ValueError("配置错误: target_url 不能为空")
        
        # 验证正则表达式
        try:
            re.compile(self.config.get("regex_pattern", ".*"))
        except re.error as e:
            raise ValueError(f"配置错误: 无效的正则表达式 - {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项"""
        keys = key.split(".")
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default


# =============================================================================
# 数据处理模块
# =============================================================================

class URLProcessor:
    """URL 处理器：负责正则过滤和链接清洗"""
    
    def __init__(self, regex_pattern: str, base_url: str):
        """
        初始化 URL 处理器
        
        Args:
            regex_pattern: 过滤用的正则表达式
            base_url: 用于转换相对路径的基础 URL
        """
        self.pattern = re.compile(regex_pattern)
        self.base_url = base_url
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def filter_urls(self, urls: list[str]) -> list[str]:
        """
        过滤并转换 URL 列表
        
        Args:
            urls: 原始 URL 列表
            
        Returns:
            过滤后的完整 URL 列表（已去重）
        """
        matched_urls = set()
        
        # 调试：显示前 10 个链接样本
        self.logger.info(f"正则表达式: {self.pattern.pattern}")
        self.logger.info(f"链接样本（前10个）:")
        for i, url in enumerate(urls[:10]):
            absolute_url = urljoin(self.base_url, url)
            self.logger.info(f"  [{i+1}] {absolute_url}")
        
        for url in urls:
            # 转换为绝对路径
            absolute_url = urljoin(self.base_url, url)
            
            # 正则匹配
            if self.pattern.search(absolute_url):
                matched_urls.add(absolute_url)
        
        self.logger.info(f"URL 过滤完成: {len(urls)} -> {len(matched_urls)}")
        return list(matched_urls)
    
    def extract_links_from_html(self, html: str) -> list[str]:
        """
        从 HTML 中提取所有链接
        
        Args:
            html: HTML 内容
            
        Returns:
            链接列表
        """
        # 匹配 href 属性中的链接
        href_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
        links = href_pattern.findall(html)
        
        self.logger.debug(f"从 HTML 中提取到 {len(links)} 个链接")
        return links
    
    def extract_dois_from_html(self, html: str) -> list[str]:
        """
        从 HTML 中直接提取所有 DOI 格式的字符串
        用于 ACM 等网站，DOI 可能存储在数据属性而非链接中
        
        Args:
            html: HTML 内容
            
        Returns:
            DOI URL 列表
        """
        # 匹配 DOI 格式: 10.数字/数字.数字
        doi_pattern = re.compile(r'10\.\d{4,}/\d+\.\d+')
        dois = doi_pattern.findall(html)
        
        # 转换为完整 URL 并去重
        doi_urls = list(set(f"https://dl.acm.org/doi/{doi}" for doi in dois))
        
        self.logger.debug(f"从 HTML 中提取到 {len(doi_urls)} 个 DOI")
        return doi_urls


# =============================================================================
# 数据存储模块
# =============================================================================

class JSONLStorage:
    """JSONL 存储器：负责结果的持久化存储"""
    
    def __init__(self, output_file: str):
        """
        初始化存储器
        
        Args:
            output_file: 输出文件路径
        """
        self.output_path = Path(output_file)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 确保父目录存在
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def save_results(
        self,
        matched_urls: list[str],
        source_url: str,
        page_title: str
    ) -> int:
        """
        保存匹配结果到 JSONL 文件
        
        Args:
            matched_urls: 匹配到的 URL 列表
            source_url: 来源 URL
            page_title: 页面标题
            
        Returns:
            保存的记录数
        """
        timestamp = datetime.now().isoformat()
        saved_count = 0
        
        with open(self.output_path, "a", encoding="utf-8") as f:
            for url in matched_urls:
                record = {
                    "timestamp": timestamp,
                    "source_url": source_url,
                    "matched_url": url,
                    "page_title": page_title
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                saved_count += 1
        
        self.logger.info(f"已保存 {saved_count} 条记录到 {self.output_path}")
        return saved_count
    
    def load_existing_urls(self) -> set[str]:
        """
        加载已存在的 URL（用于去重）
        
        Returns:
            已存在的 URL 集合
        """
        existing_urls = set()
        
        if self.output_path.exists():
            with open(self.output_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        existing_urls.add(record.get("matched_url", ""))
                    except json.JSONDecodeError:
                        continue
        
        self.logger.debug(f"已加载 {len(existing_urls)} 个已存在的 URL")
        return existing_urls


# =============================================================================
# 爬虫服务模块
# =============================================================================

class CrawlerService:
    """爬虫服务：核心爬取逻辑实现"""
    
    def __init__(self, config: ConfigManager):
        """
        初始化爬虫服务
        
        Args:
            config: 配置管理器实例
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化组件
        self.url_processor = URLProcessor(
            regex_pattern=config.get("regex_pattern"),
            base_url=config.get("target_url")
        )
        self.storage = JSONLStorage(config.get("output_file"))
    
    def _build_js_code(self) -> str:
        """
        构建动态展开页面的 JavaScript 代码
        
        Returns:
            JavaScript 代码字符串
        """
        click_selector = self.config.get("click_selector", "")
        max_clicks = self.config.get("max_clicks", 50)
        click_delay = self.config.get("click_delay_ms", 1000)
        
        # 构建脚本：等待 Cloudflare 验证、处理弹窗、展开页面
        js_code = f"""
        (async () => {{
            const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
            
            // ========== 第零步：等待 Cloudflare 验证完成 ==========
            console.log('等待 Cloudflare 验证...');
            
            // 检测是否还在 Cloudflare 验证页面
            let waitCount = 0;
            const maxWait = 30;  // 最多等待 30 秒
            
            while (waitCount < maxWait) {{
                const title = document.title;
                const bodyText = document.body?.innerText || '';
                
                // 检查是否还在验证页面
                if (title.includes('请稍候') || title.includes('Just a moment') ||
                    title.includes('Checking') || title.includes('Verify') ||
                    bodyText.includes('Checking if the site connection is secure') ||
                    bodyText.includes('正在验证')) {{
                    console.log(`等待验证完成... (${{waitCount + 1}}s)`);
                    await sleep(1000);
                    waitCount++;
                }} else {{
                    console.log('Cloudflare 验证已完成');
                    break;
                }}
            }}
            
            if (waitCount >= maxWait) {{
                console.log('警告: Cloudflare 验证超时，继续执行...');
            }}
            
            // 额外等待页面渲染
            await sleep(3000);
            
            // ========== 第一步：自动处理 Cookie 弹窗 ==========
            console.log('检查并处理 Cookie 弹窗...');
            
            // 等待页面稳定
            await sleep(2000);
            
            // 常见的 Cookie 接受按钮选择器
            const cookieSelectors = [
                'button[id*="accept"]',
                'button[class*="accept"]',
                'button[id*="cookie"]',
                'button[class*="cookie"]',
                'a[id*="accept"]',
                '[data-cookiebanner="accept_button"]',
                '.cookie-accept',
                '#onetrust-accept-btn-handler',
                '.cc-accept',
                '.cc-allow',
                // Cookiebot 特定选择器
                '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
                '#CybotCookiebotDialogBodyButtonAccept',
                'button[data-cookieconsent="accept"]',
                // 通用文本匹配
                'button:contains("Allow all")',
                'button:contains("Accept all")',
                'button:contains("同意")',
                'button:contains("接受")'
            ];
            
            for (const selector of cookieSelectors) {{
                try {{
                    const btn = document.querySelector(selector);
                    if (btn && btn.offsetParent !== null) {{
                        btn.click();
                        console.log('已点击 Cookie 接受按钮:', selector);
                        await sleep(1000);
                        break;
                    }}
                }} catch (e) {{
                    // 忽略选择器错误
                }}
            }}
            
            // 尝试通过文本内容查找按钮
            const allButtons = document.querySelectorAll('button');
            for (const btn of allButtons) {{
                const text = btn.textContent.toLowerCase();
                if (text.includes('allow all') || text.includes('accept all') || 
                    text.includes('accept cookies') || text.includes('allow cookies')) {{
                    if (btn.offsetParent !== null) {{
                        btn.click();
                        console.log('已点击 Cookie 按钮（通过文本匹配）');
                        await sleep(1000);
                        break;
                    }}
                }}
            }}
            
            // ========== 第二步：动态展开页面 ==========
            const clickSelector = "{click_selector}";
            const maxClicks = {max_clicks};
            const delay = {click_delay};
            
            if (!clickSelector) {{
                console.log('未配置点击选择器，跳过展开');
                return;
            }}
            
            let clickCount = 0;
            let previousHeight = 0;
            let noChangeCount = 0;
            
            console.log('开始展开页面...');
            
            while (clickCount < maxClicks) {{
                // 查找所有匹配的按钮（支持多选择器）
                const buttons = document.querySelectorAll(clickSelector);
                
                if (buttons.length === 0) {{
                    console.log('未找到展开按钮，停止');
                    break;
                }}
                
                let clicked = false;
                for (const button of buttons) {{
                    // 检查按钮是否可见
                    const rect = button.getBoundingClientRect();
                    const isVisible = rect.width > 0 && rect.height > 0 && button.offsetParent !== null;
                    
                    if (isVisible) {{
                        button.click();
                        clicked = true;
                        clickCount++;
                        console.log(`点击展开按钮，累计: ${{clickCount}}`);
                        await sleep(delay);
                    }}
                }}
                
                if (!clicked) {{
                    console.log('没有可点击的按钮，停止');
                    break;
                }}
                
                // 检查页面高度是否增加
                const currentHeight = document.body.scrollHeight;
                
                if (currentHeight === previousHeight) {{
                    noChangeCount++;
                    if (noChangeCount >= 3) {{
                        console.log('页面内容不再增加，停止');
                        break;
                    }}
                }} else {{
                    noChangeCount = 0;
                    previousHeight = currentHeight;
                }}
                
                // 滚动到页面底部以触发懒加载
                window.scrollTo(0, document.body.scrollHeight);
                await sleep(500);
            }}
            
            console.log(`展开完成，共点击 ${{clickCount}} 次`);
        }})();
        """
        
        return js_code
    
    def _get_browser_config(self) -> BrowserConfig:
        """
        获取浏览器配置
        
        Returns:
            BrowserConfig 实例
        """
        browser_cfg = self.config.get("browser", {})
        cache_path = self.config.get("cache_path", "./.crawl4ai_cache")
        
        # 持久化浏览器数据目录（保存 cookies，帮助绕过 Cloudflare）
        user_data_dir = Path(cache_path) / "browser_data"
        user_data_dir.mkdir(parents=True, exist_ok=True)
        
        return BrowserConfig(
            headless=browser_cfg.get("headless", True),
            verbose=browser_cfg.get("verbose", True),
            browser_type="chromium",
            ignore_https_errors=True,
            java_script_enabled=True,
            # 额外的浏览器参数，帮助绕过检测
            extra_args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--no-first-run",
            ]
        )
    
    def _get_crawler_config(self) -> CrawlerRunConfig:
        """
        获取爬取配置
        
        Returns:
            CrawlerRunConfig 实例
        """
        crawler_cfg = self.config.get("crawler", {})
        js_code = self._build_js_code()
        
        # 缓存模式 - 强制绕过缓存以确保真正访问页面
        cache_mode = CacheMode.BYPASS  # 总是重新获取，但不更新缓存
        
        # 等待策略: domcontentloaded, load, networkidle, commit
        # 对于复杂网站推荐使用 domcontentloaded 或 load，避免 networkidle 超时
        wait_until = crawler_cfg.get("wait_until", "domcontentloaded")
        
        config_kwargs = {
            "cache_mode": cache_mode,
            "page_timeout": crawler_cfg.get("timeout", 120000),  # 默认2分钟
            "wait_until": wait_until,
            "scan_full_page": True,  # 扫描整个页面
            "scroll_delay": 0.5,
            "delay_before_return_html": crawler_cfg.get("delay_before_return", 3.0),  # 返回前等待
        }
        
        # 添加 JavaScript 代码
        if js_code:
            config_kwargs["js_code"] = js_code
        
        # 等待特定元素出现
        wait_for_selector = self.config.get("wait_for_selector", "")
        if wait_for_selector:
            config_kwargs["wait_for"] = f"css:{wait_for_selector}"
        
        return CrawlerRunConfig(**config_kwargs)
    
    async def crawl(self) -> dict[str, Any]:
        """
        执行爬取任务
        
        Returns:
            爬取结果摘要
        """
        target_url = self.config.get("target_url")
        cache_path = self.config.get("cache_path", "./.crawl4ai_cache")
        
        self.logger.info(f"开始爬取: {target_url}")
        
        # 确保缓存目录存在
        Path(cache_path).mkdir(parents=True, exist_ok=True)
        
        browser_config = self._get_browser_config()
        crawler_config = self._get_crawler_config()
        
        result_summary = {
            "success": False,
            "target_url": target_url,
            "total_links": 0,
            "matched_links": 0,
            "saved_links": 0,
            "page_title": "",
            "error": None
        }
        
        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                self.logger.info("浏览器启动成功")
                
                # 执行爬取
                result = await crawler.arun(
                    url=target_url,
                    config=crawler_config
                )
                
                if not result.success:
                    raise RuntimeError(f"爬取失败: {result.error_message}")
                
                self.logger.info("页面爬取完成")
                
                # 获取页面信息
                page_title = result.metadata.get("title", "") if result.metadata else ""
                html_content = result.html or ""
                
                # 提取链接
                # 优先使用 crawl4ai 提取的链接
                all_links = []
                if result.links:
                    # result.links 包含内部和外部链接
                    if hasattr(result.links, 'internal'):
                        all_links.extend([link.get('href', '') for link in result.links.get('internal', [])])
                    if hasattr(result.links, 'external'):
                        all_links.extend([link.get('href', '') for link in result.links.get('external', [])])
                    
                    # 如果 links 是字典格式
                    if isinstance(result.links, dict):
                        all_links.extend([link.get('href', '') for link in result.links.get('internal', [])])
                        all_links.extend([link.get('href', '') for link in result.links.get('external', [])])
                
                # 如果没有从 result.links 获取到，则从 HTML 中提取
                if not all_links:
                    all_links = self.url_processor.extract_links_from_html(html_content)
                
                # 额外从 HTML 中直接提取 DOI（用于 ACM 等特殊网站）
                # DOI 可能存储在数据属性而非链接中
                doi_urls = self.url_processor.extract_dois_from_html(html_content)
                if doi_urls:
                    self.logger.info(f"从 HTML 中直接提取到 {len(doi_urls)} 个 DOI")
                    all_links.extend(doi_urls)
                
                # 去重
                all_links = list(set(all_links))
                
                result_summary["total_links"] = len(all_links)
                self.logger.info(f"共提取到 {len(all_links)} 个链接（含 DOI）")
                
                # 过滤 URL
                matched_urls = self.url_processor.filter_urls(all_links)
                result_summary["matched_links"] = len(matched_urls)
                
                # 去除已存在的 URL
                existing_urls = self.storage.load_existing_urls()
                new_urls = [url for url in matched_urls if url not in existing_urls]
                
                self.logger.info(f"新发现 {len(new_urls)} 个 URL（去重后）")
                
                # 保存结果
                if new_urls:
                    saved_count = self.storage.save_results(
                        matched_urls=new_urls,
                        source_url=target_url,
                        page_title=page_title
                    )
                    result_summary["saved_links"] = saved_count
                
                result_summary["success"] = True
                result_summary["page_title"] = page_title
                
        except asyncio.TimeoutError:
            error_msg = "爬取超时"
            self.logger.error(error_msg)
            result_summary["error"] = error_msg
            
        except Exception as e:
            error_msg = f"爬取异常: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            result_summary["error"] = error_msg
        
        return result_summary


# =============================================================================
# 日志配置
# =============================================================================

def setup_logging(config: ConfigManager) -> None:
    """
    配置日志系统
    
    Args:
        config: 配置管理器实例
    """
    log_config = config.get("logging", {})
    level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)
    log_format = log_config.get(
        "format",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


# =============================================================================
# 主入口
# =============================================================================

async def main(config_path: str = "config.yaml") -> dict[str, Any]:
    """
    主函数
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        爬取结果摘要
    """
    # 加载配置
    config = ConfigManager(config_path)
    
    try:
        config.load()
    except FileNotFoundError as e:
        print(f"错误: {e}")
        print("请确保 config.yaml 文件存在于当前目录")
        sys.exit(1)
    except (yaml.YAMLError, ValueError) as e:
        print(f"配置错误: {e}")
        sys.exit(1)
    
    # 配置日志
    setup_logging(config)
    logger = logging.getLogger("Main")
    
    logger.info("=" * 60)
    logger.info("Crawl4AI 通用爬虫服务启动")
    logger.info("=" * 60)
    
    # 创建爬虫服务并执行
    service = CrawlerService(config)
    result = await service.crawl()
    
    # 输出结果摘要
    logger.info("=" * 60)
    logger.info("爬取结果摘要")
    logger.info("=" * 60)
    logger.info(f"  目标 URL: {result['target_url']}")
    logger.info(f"  页面标题: {result['page_title']}")
    logger.info(f"  总链接数: {result['total_links']}")
    logger.info(f"  匹配链接: {result['matched_links']}")
    logger.info(f"  新保存数: {result['saved_links']}")
    logger.info(f"  执行状态: {'成功' if result['success'] else '失败'}")
    
    if result['error']:
        logger.error(f"  错误信息: {result['error']}")
    
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Crawl4AI 通用爬虫服务")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="配置文件路径 (默认: config.yaml)"
    )
    
    args = parser.parse_args()
    
    # 运行爬虫
    asyncio.run(main(args.config))
