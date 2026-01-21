#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PDF 深度下载器
==============

功能：
1. 读取之前爬取的链接列表 (results.jsonl)
2. 访问每个详情页，提取 PDF 下载链接
3. 按年份分类下载并存储 PDF
4. 记录下载结果到 final_papers.jsonl

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
from urllib.parse import urlparse, unquote, urljoin

import yaml
from playwright.async_api import async_playwright, Page, Browser


# =============================================================================
# 配置管理
# =============================================================================

class DownloaderConfig:
    """下载器配置管理"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self.config: dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load(self) -> dict[str, Any]:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}
        
        # 获取下载器配置
        self.downloader = self.config.get("downloader", {})
        
        # 设置默认值
        self.input_file = self.downloader.get("input_file", "results.jsonl")
        self.output_file = self.downloader.get("output_file", "final_papers.jsonl")
        self.download_dir = Path(self.downloader.get("download_dir", "./downloads"))
        self.max_concurrent = self.downloader.get("max_concurrent", 5)
        self.request_delay = self.downloader.get("request_delay", 1.0)
        self.download_timeout = self.downloader.get("download_timeout", 120)
        self.default_year = self.downloader.get("default_year", "Unknown")
        self.max_retries = self.downloader.get("max_retries", 3)
        self.retry_delay = self.downloader.get("retry_delay", 10.0)
        
        # PDF 匹配正则
        self.pdf_patterns = [
            re.compile(p) for p in self.downloader.get("pdf_patterns", [
                r'https?://[^"\'\s]+\.pdf'
            ])
        ]
        
        # 年份匹配正则
        self.year_patterns = [
            re.compile(p) for p in self.downloader.get("year_patterns", [
                r'/paper/(\d{4})/',
                r'/(\d{4})/',
            ])
        ]
        
        # 浏览器配置
        browser_cfg = self.config.get("browser", {})
        self.headless = browser_cfg.get("headless", True)
        self.verbose = browser_cfg.get("verbose", True)
        
        # 爬虫配置
        crawler_cfg = self.config.get("crawler", {})
        self.timeout = crawler_cfg.get("timeout", 60000)
        self.wait_until = crawler_cfg.get("wait_until", "domcontentloaded")
        
        self.logger.info(f"配置加载完成: {self.config_path}")
        return self.config


# =============================================================================
# 年份提取器
# =============================================================================

class YearExtractor:
    """年份提取器"""
    
    def __init__(self, patterns: list[re.Pattern], default: str = "Unknown"):
        self.patterns = patterns
        self.default = default
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract(self, url: str, html: str = "") -> str:
        """
        从 URL 或 HTML 中提取年份
        
        Args:
            url: 页面 URL
            html: 页面 HTML 内容
            
        Returns:
            提取到的年份或默认值
        """
        # 先从 URL 中提取
        for pattern in self.patterns:
            match = pattern.search(url)
            if match:
                year = match.group(1)
                if 1990 <= int(year) <= 2030:  # 合理年份范围
                    self.logger.debug(f"从 URL 提取年份: {year}")
                    return year
        
        # 再从 HTML 中提取
        if html:
            # 尝试从 meta 标签提取
            meta_patterns = [
                r'<meta[^>]*name=["\']citation_year["\'][^>]*content=["\'](\d{4})["\']',
                r'<meta[^>]*content=["\'](\d{4})["\'][^>]*name=["\']citation_year["\']',
                r'Published:?\s*(\d{4})',
                r'Year:?\s*(\d{4})',
            ]
            
            for pattern in meta_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    year = match.group(1)
                    if 1990 <= int(year) <= 2030:
                        self.logger.debug(f"从 HTML 提取年份: {year}")
                        return year
        
        return self.default


# =============================================================================
# PDF 链接提取器
# =============================================================================

class PDFLinkExtractor:
    """PDF 链接提取器"""
    
    def __init__(self, patterns: list[re.Pattern]):
        self.patterns = patterns
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract(self, html: str, base_url: str = "") -> list[str]:
        """
        从 HTML 中提取所有 PDF 链接
        
        Args:
            html: 页面 HTML 内容
            base_url: 基础 URL（用于转换相对路径）
            
        Returns:
            PDF 链接列表
        """
        pdf_links = set()
        
        for pattern in self.patterns:
            matches = pattern.findall(html)
            for match in matches:
                # findall 可能返回元组（多分组时），取第一个
                link = (match[0] if isinstance(match, tuple) else match).strip().rstrip('"\'>')
                if link:
                    # 相对路径转绝对（如 ACM /doi/pdf/10.1145/xxx）
                    if link.startswith('/') and base_url:
                        link = urljoin(base_url, link)
                    pdf_links.add(link)
        
        # 尝试从 href 属性中提取
        href_pattern = re.compile(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', re.IGNORECASE)
        for match in href_pattern.findall(html):
            # 处理相对路径
            if match.startswith('/'):
                parsed = urlparse(base_url)
                link = f"{parsed.scheme}://{parsed.netloc}{match}"
            elif match.startswith('http'):
                link = match
            else:
                continue
            pdf_links.add(link)
        
        self.logger.debug(f"提取到 {len(pdf_links)} 个 PDF 链接")
        return list(pdf_links)


# =============================================================================
# 文件名处理器
# =============================================================================

class FilenameProcessor:
    """文件名处理器"""
    
    # 非法字符
    ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
    
    @classmethod
    def sanitize(cls, filename: str, max_length: int = 200) -> str:
        """
        清洗文件名，去除非法字符
        
        Args:
            filename: 原始文件名
            max_length: 最大长度
            
        Returns:
            清洗后的文件名
        """
        # URL 解码
        filename = unquote(filename)
        
        # 去除非法字符
        filename = cls.ILLEGAL_CHARS.sub('_', filename)
        
        # 去除首尾空格和点
        filename = filename.strip(' .')
        
        # 限制长度
        if len(filename) > max_length:
            name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
            filename = name[:max_length - len(ext) - 1] + '.' + ext if ext else name[:max_length]
        
        return filename or "unnamed.pdf"
    
    @classmethod
    def extract_from_url(cls, url: str) -> str:
        """
        从 URL 中提取文件名
        
        Args:
            url: PDF URL
            
        Returns:
            文件名
        """
        parsed = urlparse(url)
        path = parsed.path
        
        # 获取路径最后一部分
        filename = path.split('/')[-1]
        
        # 如果没有扩展名，添加 .pdf
        if not filename.lower().endswith('.pdf'):
            filename += '.pdf'
        
        return cls.sanitize(filename)


# =============================================================================
# 下载管理器
# =============================================================================

class DownloadManager:
    """异步下载管理器"""
    
    def __init__(self, config: DownloaderConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 统计
        self.success_count = 0
        self.error_count = 0


# =============================================================================
# 主下载服务
# =============================================================================

class PDFDownloaderService:
    """PDF 下载服务"""
    
    def __init__(self, config: DownloaderConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化组件
        self.year_extractor = YearExtractor(config.year_patterns, config.default_year)
        self.pdf_extractor = PDFLinkExtractor(config.pdf_patterns)
        self.download_manager = DownloadManager(config)
        
        # 结果记录
        self.results: list[dict] = []
    
    def load_input_urls(self) -> list[dict]:
        """
        加载输入的 URL 列表
        
        Returns:
            URL 记录列表
        """
        input_path = Path(self.config.input_file)
        
        if not input_path.exists():
            raise FileNotFoundError(f"输入文件不存在: {input_path}")
        
        records = []
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        if record.get("matched_url"):
                            records.append(record)
                    except json.JSONDecodeError:
                        continue
        
        self.logger.info(f"加载了 {len(records)} 个 URL")
        return records
    
    
    def _extract_title(self, html: str) -> str:
        """从 HTML 中提取标题"""
        patterns = [
            r'<meta[^>]*name=["\']citation_title["\'][^>]*content=["\']([^"\']+)["\']',
            r'<title>([^<]+)</title>',
            r'<h1[^>]*>([^<]+)</h1>',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                # 清理标题
                title = re.sub(r'\s+', ' ', title)
                if title and len(title) > 5:
                    return title
        
        return ""
    
    def save_results(self) -> None:
        """保存下载结果"""
        output_path = Path(self.config.output_file)
        
        with open(output_path, "w", encoding="utf-8") as f:
            for record in self.results:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        self.logger.info(f"结果已保存到: {output_path}")
    
    async def _handle_cloudflare(self, page: Page, max_attempts: int = 3) -> bool:
        """
        自动处理 Cloudflare 验证
        
        Args:
            page: Playwright 页面对象
            max_attempts: 最大尝试次数
            
        Returns:
            是否成功通过验证
        """
        for attempt in range(max_attempts):
            # 检查是否是 Cloudflare 验证页面
            title = await page.title()
            content = await page.content()
            
            # 检查页面标题或内容是否包含 Cloudflare 标识
            is_cloudflare = (
                "请稍候" in title or 
                "Just a moment" in title or
                "Cloudflare" in content or
                "确认您是真人" in content or
                "Verify you are human" in content
            )
            
            if not is_cloudflare:
                self.logger.info("页面已通过 Cloudflare 验证")
                return True
            
            self.logger.info(f"检测到 Cloudflare 验证页面，尝试自动验证 (尝试 {attempt + 1}/{max_attempts})...")
            
            try:
                # 方法1: 尝试点击 Cloudflare turnstile iframe 中的复选框
                # Cloudflare 验证框通常在 iframe 中
                frames = page.frames
                
                for frame in frames:
                    try:
                        # 查找验证复选框（可能的选择器）
                        checkbox_selectors = [
                            'input[type="checkbox"]',
                            '#cf-turnstile-response',
                            '.cf-turnstile',
                            '[data-action="managed-challenge"]',
                            'iframe[src*="challenges.cloudflare.com"]',
                        ]
                        
                        for selector in checkbox_selectors:
                            element = await frame.query_selector(selector)
                            if element:
                                # 如果是 iframe，进入它
                                if 'iframe' in selector:
                                    cf_frame = await element.content_frame()
                                    if cf_frame:
                                        # 在 iframe 中查找复选框
                                        cb = await cf_frame.query_selector('input[type="checkbox"]')
                                        if cb:
                                            await cb.click()
                                            self.logger.info("已点击 Cloudflare 验证框")
                                else:
                                    await element.click()
                                    self.logger.info("已点击验证元素")
                                break
                    except Exception as e:
                        continue
                
                # 方法2: 直接在主页面查找并点击
                try:
                    # 查找 Cloudflare iframe
                    cf_iframe = await page.query_selector('iframe[src*="challenges.cloudflare.com"]')
                    if cf_iframe:
                        cf_frame = await cf_iframe.content_frame()
                        if cf_frame:
                            # 等待复选框出现并点击
                            await cf_frame.wait_for_selector('input[type="checkbox"]', timeout=5000)
                            await cf_frame.click('input[type="checkbox"]')
                            self.logger.info("已点击 Cloudflare iframe 中的验证框")
                except Exception:
                    pass
                
                # 方法3: 使用 JavaScript 触发点击
                try:
                    await page.evaluate('''
                        () => {
                            // 尝试找到并点击所有可能的验证框
                            const iframes = document.querySelectorAll('iframe');
                            for (const iframe of iframes) {
                                try {
                                    const doc = iframe.contentDocument || iframe.contentWindow.document;
                                    const checkbox = doc.querySelector('input[type="checkbox"]');
                                    if (checkbox) {
                                        checkbox.click();
                                    }
                                } catch (e) {}
                            }
                            
                            // 直接在页面上查找
                            const checkbox = document.querySelector('input[type="checkbox"]');
                            if (checkbox) {
                                checkbox.click();
                            }
                        }
                    ''')
                except Exception:
                    pass
                
                # 等待验证完成
                self.logger.info("等待验证完成...")
                await asyncio.sleep(5)
                
                # 等待页面跳转或内容变化
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=15000)
                except Exception:
                    pass
                    
            except Exception as e:
                self.logger.warning(f"自动验证尝试失败: {e}")
            
            await asyncio.sleep(2)
        
        # 如果自动验证失败，提示用户手动验证
        print("\n" + "=" * 60)
        print("自动验证失败，请在浏览器中手动完成 Cloudflare 验证")
        print("完成后按 Enter 键继续...")
        print("=" * 60 + "\n")
        
        await asyncio.get_event_loop().run_in_executor(None, input)
        return True
    
    async def run(self) -> dict[str, Any]:
        """
        运行下载服务 - 使用 Playwright 浏览器直接下载 PDF
        
        Returns:
            执行统计
        """
        # 加载 URL 列表
        records = self.load_input_urls()
        
        if not records:
            self.logger.warning("没有需要处理的 URL")
            return {"total": 0, "success": 0, "failed": 0}
        
        total = len(records)
        
        self.logger.info("启动 Playwright 浏览器...")
        
        async with async_playwright() as p:
            # 启动浏览器（非 headless 以便通过 Cloudflare）
            browser = await p.chromium.launch(
                headless=self.config.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
            
            # 创建上下文（保持 cookies）
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                accept_downloads=True,
            )
            
            page = await context.new_page()
            
            # 首先访问 ACM 触发 Cloudflare 验证
            self.logger.info("正在通过 Cloudflare 验证...")
            init_url = records[0].get("matched_url", "https://dl.acm.org/")
            
            try:
                await page.goto(init_url, wait_until="domcontentloaded", timeout=120000)
                
                # 自动处理 Cloudflare 验证
                await self._handle_cloudflare(page)
                
                self.logger.info("Cloudflare 验证完成，开始下载...")
            except Exception as e:
                self.logger.warning(f"初始页面加载异常: {e}")
            
            self.logger.info(f"开始处理 {total} 个 URL...")
            
            # 顺序处理每个 URL
            for i, record in enumerate(records):
                result = await self.download_pdf_with_browser(page, record, i, total)
                if result:
                    self.results.append(result)
                
                # 每处理一定数量保存一次结果
                if (i + 1) % 50 == 0:
                    self.save_results()
                    self.logger.info(f"进度: {i + 1}/{total}, 已保存 {len(self.results)} 条记录")
                
                # 请求间隔
                await asyncio.sleep(self.config.request_delay)
            
            await browser.close()
        
        # 最终保存结果
        self.save_results()
        
        # 统计
        stats = {
            "total": total,
            "processed": len(self.results),
            "downloaded": sum(1 for r in self.results if r.get("status") == "downloaded"),
            "exists": sum(1 for r in self.results if r.get("status") == "exists"),
            "failed": sum(1 for r in self.results if r.get("status") == "failed"),
        }
        
        return stats
    
    async def download_pdf_with_browser(
        self,
        page: Page,
        record: dict,
        index: int,
        total: int
    ) -> Optional[dict]:
        """
        使用 Playwright 浏览器直接下载 PDF
        """
        url = record.get("matched_url", "")
        source_title = record.get("page_title", "")
        
        self.logger.info(f"[{index + 1}/{total}] 处理: {url}")
        
        try:
            # 构造 PDF URL
            if "dl.acm.org/doi/" in url and "/doi/pdf/" not in url:
                pdf_url = url.replace("/doi/", "/doi/pdf/", 1)
            else:
                pdf_url = url
            
            # 从 URL 提取 DOI 作为文件名
            doi_match = re.search(r'10\.\d+/(\d+\.\d+)', pdf_url)
            if doi_match:
                filename = f"{doi_match.group(1)}.pdf"
            else:
                filename = FilenameProcessor.extract_from_url(pdf_url)
            
            # 年份
            year_match = re.search(r'(\d{4})\s+(CHI|Conference|ICML|NeurIPS)', source_title)
            year = year_match.group(1) if year_match else "2025"
            
            save_dir = self.config.download_dir / year
            save_path = save_dir / filename
            
            # 检查是否已存在
            if save_path.exists():
                self.logger.info(f"[{year}] 已存在: {filename}")
                return {
                    "title": source_title or "Untitled",
                    "year": year,
                    "pdf_url": pdf_url,
                    "local_path": str(save_path),
                    "status": "exists"
                }
            
            # 确保目录存在
            save_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"[{year}] Downloading: {filename} ...")
            
            # 方法1: 使用 page.goto 获取 PDF 响应
            try:
                response = await page.goto(pdf_url, wait_until="load", timeout=self.config.download_timeout * 1000)
                
                if response and response.ok:
                    content_type = response.headers.get('content-type', '')
                    
                    # 获取响应内容
                    body = await response.body()
                    
                    # 检查是否是 PDF
                    if 'pdf' in content_type or (body and body[:4] == b'%PDF'):
                        with open(save_path, 'wb') as f:
                            f.write(body)
                        self.logger.info(f"[{year}] 下载完成: {filename} ({len(body)} bytes)")
                        return {
                            "title": source_title or "Untitled",
                            "year": year,
                            "pdf_url": pdf_url,
                            "local_path": str(save_path),
                            "status": "downloaded"
                        }
                    else:
                        self.logger.warning(f"非 PDF 内容 ({content_type}): {pdf_url}")
                elif response:
                    self.logger.error(f"HTTP {response.status}: {pdf_url}")
                else:
                    self.logger.error(f"无响应: {pdf_url}")
                    
            except Exception as e:
                self.logger.error(f"下载异常: {pdf_url} - {str(e)}")
            
            return {
                "title": source_title or "Untitled",
                "year": year,
                "pdf_url": pdf_url,
                "local_path": "",
                "status": "failed"
            }
                
        except Exception as e:
            self.logger.error(f"处理失败: {url} - {str(e)}")
            return None


# =============================================================================
# 日志配置
# =============================================================================

def setup_logging(level: str = "INFO") -> None:
    """配置日志"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )


# =============================================================================
# 主入口
# =============================================================================

async def main(config_path: str = "config.yaml") -> None:
    """主函数"""
    print("=" * 60)
    print("PDF 深度下载器")
    print("=" * 60)
    
    # 加载配置
    config = DownloaderConfig(config_path)
    
    try:
        config.load()
    except FileNotFoundError as e:
        print(f"错误: {e}")
        sys.exit(1)
    
    # 配置日志
    log_level = config.config.get("logging", {}).get("level", "INFO")
    setup_logging(log_level)
    
    logger = logging.getLogger("Main")
    
    # 运行下载服务
    service = PDFDownloaderService(config)
    
    start_time = datetime.now()
    stats = await service.run()
    end_time = datetime.now()
    
    # 输出统计
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("下载完成!")
    print("=" * 60)
    print(f"  总 URL 数: {stats['total']}")
    print(f"  处理成功: {stats['processed']}")
    print(f"  新下载数: {stats['downloaded']}")
    print(f"  已存在数: {stats['exists']}")
    print(f"  失败数量: {stats['failed']}")
    print(f"  耗时: {duration:.1f} 秒")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="PDF 深度下载器")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="配置文件路径 (默认: config.yaml)"
    )
    
    args = parser.parse_args()
    
    asyncio.run(main(args.config))
