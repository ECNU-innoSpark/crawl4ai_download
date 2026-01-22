#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
页面截图/HTML保存工具
=====================

功能：
1. 从 JSONL 文件读取 URL 列表
2. 对每个页面进行截图保存
3. 保存页面 HTML 内容
4. 支持全页面截图和可视区域截图
5. 记录结果到 JSONL 文件

Author: Auto-generated
Date: 2026-01-21
"""

import asyncio
import hashlib
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import yaml
from playwright.async_api import async_playwright, Page, Browser


# =============================================================================
# 配置管理
# =============================================================================

class CapturerConfig:
    """截图工具配置管理"""
    
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
        
        # 获取截图工具配置
        capturer = self.config.get("capturer", {})
        
        # 输入配置（支持 JSONL 文件或单个 URL）
        self.input = capturer.get("input", capturer.get("input_file", "results.jsonl"))
        self.url_field = capturer.get("url_field", "matched_url")
        
        # 输出配置
        self.output_dir = Path(capturer.get("output_dir", "./captures"))
        self.output_file = capturer.get("output_file", "capture_results.jsonl")
        
        # 保存选项
        self.save_screenshot = capturer.get("save_screenshot", True)
        self.save_html = capturer.get("save_html", True)
        
        # 截图配置
        screenshot_cfg = capturer.get("screenshot", {})
        self.full_page = screenshot_cfg.get("full_page", True)
        self.screenshot_format = screenshot_cfg.get("format", "png")
        self.screenshot_quality = screenshot_cfg.get("quality", 80)
        self.max_screenshot_height = screenshot_cfg.get("max_height", 30000)
        
        # HTML 配置
        html_cfg = capturer.get("html", {})
        self.save_clean_html = html_cfg.get("save_clean", False)
        
        # 文件命名配置
        naming_cfg = capturer.get("naming", {})
        self.use_title = naming_cfg.get("use_title", True)
        self.use_hash = naming_cfg.get("use_hash", True)
        self.max_filename_length = naming_cfg.get("max_filename_length", 100)
        
        # 执行控制
        self.request_delay = capturer.get("request_delay", 2.0)
        self.page_load_timeout = capturer.get("page_load_timeout", 60000)
        self.wait_after_load = capturer.get("wait_after_load", 3.0)
        self.max_retries = capturer.get("max_retries", 2)
        
        # 浏览器配置
        browser_cfg = self.config.get("browser", {})
        self.headless = browser_cfg.get("headless", True)
        self.verbose = browser_cfg.get("verbose", True)
        
        self.logger.info(f"配置加载完成: {self.config_path}")
        return self.config


# =============================================================================
# 文件名处理器
# =============================================================================

class FilenameProcessor:
    """文件名处理器"""
    
    # 非法字符
    ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
    
    @classmethod
    def sanitize(cls, filename: str, max_length: int = 100) -> str:
        """
        清洗文件名，去除非法字符
        
        Args:
            filename: 原始文件名
            max_length: 最大长度
            
        Returns:
            清洗后的文件名
        """
        # 去除非法字符
        filename = cls.ILLEGAL_CHARS.sub('_', filename)
        
        # 去除首尾空格和点
        filename = filename.strip(' .')
        
        # 替换连续空格/下划线
        filename = re.sub(r'[\s_]+', '_', filename)
        
        # 限制长度
        if len(filename) > max_length:
            filename = filename[:max_length]
        
        return filename or "unnamed"
    
    @classmethod
    def generate_from_url(cls, url: str) -> str:
        """
        从 URL 生成文件名（使用 MD5 哈希）
        
        Args:
            url: URL
            
        Returns:
            基于 URL 的文件名
        """
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        
        # 尝试从 URL 路径提取有意义的部分
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        
        if path_parts:
            # 取最后一部分
            name_part = cls.sanitize(path_parts[-1], max_length=50)
            return f"{name_part}_{url_hash}"
        
        return url_hash
    
    @classmethod
    def generate_from_title(cls, title: str, url: str, max_length: int = 100) -> str:
        """
        从页面标题生成文件名
        
        Args:
            title: 页面标题
            url: URL（用于生成哈希后缀）
            max_length: 最大长度
            
        Returns:
            文件名
        """
        if not title or len(title.strip()) < 3:
            return cls.generate_from_url(url)
        
        # 清洗标题
        clean_title = cls.sanitize(title, max_length=max_length - 13)  # 留出哈希后缀空间
        
        # 添加 URL 哈希后缀以确保唯一性
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        
        return f"{clean_title}_{url_hash}"


# =============================================================================
# 页面截图服务
# =============================================================================

class PageCapturerService:
    """页面截图/HTML保存服务"""
    
    def __init__(self, config: CapturerConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 结果记录
        self.results: list[dict] = []
        
        # 统计
        self.success_count = 0
        self.failed_count = 0
    
    def _is_url(self, text: str) -> bool:
        """判断是否为 URL"""
        return text.startswith("http://") or text.startswith("https://")
    
    def load_input_urls(self) -> list[dict]:
        """
        加载输入的 URL 列表
        支持 JSONL 文件或单个 URL
        
        Returns:
            URL 记录列表
        """
        input_value = self.config.input
        
        # 判断是单个 URL 还是 JSONL 文件
        if self._is_url(input_value):
            # 单个 URL 模式
            self.logger.info(f"单个 URL 模式: {input_value}")
            return [{
                self.config.url_field: input_value,
                "page_title": ""
            }]
        
        # JSONL 文件模式
        input_path = Path(input_value)
        
        if not input_path.exists():
            raise FileNotFoundError(f"输入文件不存在: {input_path}")
        
        records = []
        seen_urls = set()
        
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        url = record.get(self.config.url_field, "")
                        
                        if url and url not in seen_urls:
                            records.append(record)
                            seen_urls.add(url)
                    except json.JSONDecodeError:
                        continue
        
        self.logger.info(f"从 JSONL 加载了 {len(records)} 个 URL")
        return records
    
    def save_results(self) -> None:
        """保存结果"""
        output_path = Path(self.config.output_file)
        
        with open(output_path, "w", encoding="utf-8") as f:
            for record in self.results:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        
        self.logger.info(f"结果已保存到: {output_path}")
    
    async def _handle_cloudflare(self, page: Page) -> bool:
        """处理 Cloudflare 验证"""
        for attempt in range(3):
            title = await page.title()
            content = await page.content()
            
            is_cloudflare = (
                "请稍候" in title or 
                "Just a moment" in title or
                "Cloudflare" in content or
                "Verify you are human" in content
            )
            
            if not is_cloudflare:
                return True
            
            self.logger.info(f"检测到 Cloudflare 验证，等待中... (尝试 {attempt + 1}/3)")
            await asyncio.sleep(5)
        
        # 提示用户手动验证
        print("\n" + "=" * 60)
        print("请在浏览器中手动完成 Cloudflare 验证")
        print("完成后按 Enter 键继续...")
        print("=" * 60 + "\n")
        
        await asyncio.get_event_loop().run_in_executor(None, input)
        return True
    
    def _clean_html(self, html: str) -> str:
        """
        清理 HTML（移除脚本、样式等）
        
        Args:
            html: 原始 HTML
            
        Returns:
            清理后的 HTML
        """
        # 移除 script 标签
        html = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', html, flags=re.IGNORECASE)
        
        # 移除 style 标签
        html = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', html, flags=re.IGNORECASE)
        
        # 移除注释
        html = re.sub(r'<!--[\s\S]*?-->', '', html)
        
        return html
    
    async def capture_page(
        self,
        page: Page,
        record: dict,
        index: int,
        total: int
    ) -> Optional[dict]:
        """
        截取单个页面
        
        Args:
            page: Playwright 页面对象
            record: URL 记录
            index: 当前索引
            total: 总数
            
        Returns:
            结果记录
        """
        url = record.get(self.config.url_field, "")
        source_title = record.get("page_title", "")
        
        self.logger.info(f"[{index + 1}/{total}] 处理: {url}")
        
        result = {
            "url": url,
            "source_title": source_title,
            "timestamp": datetime.now().isoformat(),
            "status": "pending",
            "screenshot_path": "",
            "html_path": "",
            "page_title": "",
            "error": ""
        }
        
        for retry in range(self.config.max_retries + 1):
            try:
                # 访问页面
                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.config.page_load_timeout
                )
                
                if not response or not response.ok:
                    if response:
                        result["error"] = f"HTTP {response.status}"
                    else:
                        result["error"] = "无响应"
                    continue
                
                # 等待页面加载完成
                await asyncio.sleep(self.config.wait_after_load)
                
                # 获取页面标题
                page_title = await page.title()
                result["page_title"] = page_title
                
                # 生成文件名
                if self.config.use_title and page_title:
                    base_filename = FilenameProcessor.generate_from_title(
                        page_title, url, self.config.max_filename_length
                    )
                else:
                    base_filename = FilenameProcessor.generate_from_url(url)
                
                # 确保输出目录存在
                self.config.output_dir.mkdir(parents=True, exist_ok=True)
                
                # 保存截图
                if self.config.save_screenshot:
                    screenshot_ext = self.config.screenshot_format
                    
                    # 检查页面高度，防止超长页面导致内存溢出
                    page_height = await page.evaluate("document.body.scrollHeight")
                    viewport_height = 1080  # 视口高度
                    
                    if self.config.full_page and page_height > self.config.max_screenshot_height:
                        # 超长页面：分段截图
                        self.logger.info(f"  页面过长 ({page_height}px)，使用分段截图")
                        
                        screenshot_paths = []
                        # 使用 max_height 作为分段高度
                        segment_height = self.config.max_screenshot_height
                        num_segments = (page_height + segment_height - 1) // segment_height
                        
                        # 限制最大分段数，防止过多截图
                        max_segments = 20
                        if num_segments > max_segments:
                            self.logger.warning(f"  分段数 ({num_segments}) 过多，限制为 {max_segments} 段")
                            num_segments = max_segments
                        
                        # 第一步：预加载整个页面（快速滚动一遍触发懒加载）
                        self.logger.info(f"  预加载页面内容...")
                        preload_step = viewport_height * 2  # 每次滚动 2 个视口高度
                        preload_positions = range(0, page_height, preload_step)
                        for y_pos in preload_positions:
                            await page.evaluate(f"window.scrollTo(0, {y_pos})")
                            await asyncio.sleep(0.2)  # 快速滚动
                        
                        # 等待所有内容加载完成
                        await asyncio.sleep(2)
                        
                        # 第二步：滚动回顶部
                        await page.evaluate("window.scrollTo(0, 0)")
                        await asyncio.sleep(0.5)
                        
                        # 获取页面宽度
                        page_width = await page.evaluate("document.body.scrollWidth")
                        
                        # 第三步：逐段截图（使用 clip 参数截取指定区域）
                        for seg_idx in range(num_segments):
                            # 计算当前段的位置和高度
                            y_offset = seg_idx * segment_height
                            # 最后一段可能不足 segment_height
                            current_height = min(segment_height, page_height - y_offset)
                            
                            # 截图文件名加上序号
                            seg_filename = f"{base_filename}_part{seg_idx + 1}.{screenshot_ext}"
                            seg_path = self.config.output_dir / seg_filename
                            
                            screenshot_options = {
                                "path": str(seg_path),
                                "full_page": True,  # 需要 full_page 才能使用 clip
                                "type": self.config.screenshot_format,
                                "clip": {
                                    "x": 0,
                                    "y": y_offset,
                                    "width": page_width,
                                    "height": current_height
                                }
                            }
                            
                            if self.config.screenshot_format == "jpeg":
                                screenshot_options["quality"] = self.config.screenshot_quality
                            
                            await page.screenshot(**screenshot_options)
                            screenshot_paths.append(str(seg_path))
                            self.logger.info(f"  截图已保存: {seg_filename} ({seg_idx + 1}/{num_segments})")
                        
                        # 滚动回顶部
                        await page.evaluate("window.scrollTo(0, 0)")
                        
                        result["screenshot_path"] = screenshot_paths  # 保存为列表
                        result["screenshot_segments"] = num_segments
                    else:
                        # 普通截图
                        screenshot_path = self.config.output_dir / f"{base_filename}.{screenshot_ext}"
                        
                        screenshot_options = {
                            "path": str(screenshot_path),
                            "full_page": self.config.full_page,
                            "type": self.config.screenshot_format,
                        }
                        
                        if self.config.screenshot_format == "jpeg":
                            screenshot_options["quality"] = self.config.screenshot_quality
                        
                        await page.screenshot(**screenshot_options)
                        result["screenshot_path"] = str(screenshot_path)
                        self.logger.info(f"  截图已保存: {screenshot_path.name}")
                
                # 保存 HTML
                if self.config.save_html:
                    html_content = await page.content()
                    
                    if self.config.save_clean_html:
                        html_content = self._clean_html(html_content)
                    
                    html_path = self.config.output_dir / f"{base_filename}.html"
                    
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    
                    result["html_path"] = str(html_path)
                    self.logger.info(f"  HTML 已保存: {html_path.name}")
                
                result["status"] = "success"
                self.success_count += 1
                return result
                
            except asyncio.TimeoutError:
                result["error"] = "页面加载超时"
                self.logger.warning(f"  超时 (重试 {retry + 1}/{self.config.max_retries + 1})")
                
            except Exception as e:
                error_msg = str(e)
                result["error"] = error_msg
                self.logger.warning(f"  错误: {e} (重试 {retry + 1}/{self.config.max_retries + 1})")
                
                # 检测浏览器崩溃
                if "closed" in error_msg.lower() or "crash" in error_msg.lower():
                    # 返回特殊标记，让调用者重建浏览器
                    result["browser_crashed"] = True
                    return result
            
            if retry < self.config.max_retries:
                await asyncio.sleep(2)
        
        result["status"] = "failed"
        self.failed_count += 1
        return result
    
    async def _create_browser(self, playwright):
        """创建浏览器和页面"""
        browser = await playwright.chromium.launch(
            headless=self.config.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",  # 减少内存使用
            ]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        
        page = await context.new_page()
        return browser, context, page
    
    async def run(self) -> dict[str, Any]:
        """
        运行截图服务
        
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
            browser, context, page = await self._create_browser(p)
            
            # 首先访问第一个 URL 触发 Cloudflare 验证
            self.logger.info("正在检查 Cloudflare 验证...")
            init_url = records[0].get(self.config.url_field, "")
            
            try:
                await page.goto(init_url, wait_until="domcontentloaded", timeout=60000)
                await self._handle_cloudflare(page)
            except Exception as e:
                self.logger.warning(f"初始页面加载异常: {e}")
            
            self.logger.info(f"开始处理 {total} 个 URL...")
            
            # 处理每个 URL
            i = 0
            while i < total:
                record = records[i]
                result = await self.capture_page(page, record, i, total)
                
                if result:
                    # 检查是否浏览器崩溃
                    if result.get("browser_crashed"):
                        self.logger.warning("检测到浏览器崩溃，正在重启...")
                        
                        # 尝试关闭旧浏览器
                        try:
                            await browser.close()
                        except:
                            pass
                        
                        # 等待一下再重启
                        await asyncio.sleep(3)
                        
                        # 重新创建浏览器
                        browser, context, page = await self._create_browser(p)
                        self.logger.info("浏览器已重启，继续处理...")
                        
                        # 重新处理当前 URL（不增加 i）
                        continue
                    
                    self.results.append(result)
                
                # 定期保存结果
                if (i + 1) % 20 == 0:
                    self.save_results()
                    self.logger.info(f"进度: {i + 1}/{total}")
                
                # 请求间隔
                if i < total - 1:
                    await asyncio.sleep(self.config.request_delay)
                
                i += 1
            
            try:
                await browser.close()
            except:
                pass
        
        # 最终保存结果
        self.save_results()
        
        return {
            "total": total,
            "success": self.success_count,
            "failed": self.failed_count,
        }


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
    print("页面截图/HTML保存工具")
    print("=" * 60)
    
    # 加载配置
    config = CapturerConfig(config_path)
    
    try:
        config.load()
    except FileNotFoundError as e:
        print(f"错误: {e}")
        sys.exit(1)
    
    # 配置日志
    log_level = config.config.get("logging", {}).get("level", "INFO")
    setup_logging(log_level)
    
    logger = logging.getLogger("Main")
    
    logger.info(f"输入: {config.input}")
    logger.info(f"输出目录: {config.output_dir}")
    logger.info(f"保存截图: {config.save_screenshot}")
    logger.info(f"保存HTML: {config.save_html}")
    logger.info(f"全页面截图: {config.full_page}")
    
    # 运行服务
    service = PageCapturerService(config)
    
    start_time = datetime.now()
    stats = await service.run()
    end_time = datetime.now()
    
    # 输出统计
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("处理完成!")
    print("=" * 60)
    print(f"  总 URL 数: {stats['total']}")
    print(f"  成功数量: {stats['success']}")
    print(f"  失败数量: {stats['failed']}")
    print(f"  耗时: {duration:.1f} 秒")
    print(f"  输出目录: {config.output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="页面截图/HTML保存工具")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="配置文件路径 (默认: config.yaml)"
    )
    
    args = parser.parse_args()
    
    asyncio.run(main(args.config))
