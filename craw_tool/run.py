#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
论文爬取一键运行脚本
====================

功能：
1. 按顺序执行多次爬虫任务
2. 支持页面截图
3. 支持 PDF 下载
4. 支持多种配置文件（NeurIPS、CHI 等）

Usage:
    python run.py                              # 使用默认配置 (config_nips.yaml)
    python run.py -c config_chi.yaml           # 使用 CHI 配置
    python run.py -c config_nips.yaml          # 使用 NeurIPS 配置
    python run.py -c config_chi.yaml --crawl-only      # 只运行爬虫任务
    python run.py -c config_chi.yaml --capture-only    # 只运行截图任务
    python run.py -c config_chi.yaml --download-only   # 只运行下载任务
    python run.py -c config_chi.yaml --task 1          # 只运行第1个爬虫任务
    python run.py -c config_chi.yaml --task 1,2        # 只运行第1、2个爬虫任务

Author: Auto-generated
Date: 2026-01-21
"""

import argparse
import asyncio
import logging
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


# =============================================================================
# 配置加载
# =============================================================================

def load_config(config_path: str = "config_nips.yaml") -> dict:
    """加载配置文件"""
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")
    
    with open(config_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def generate_temp_config(global_config: dict, task_config: dict) -> str:
    """
    根据任务配置生成临时配置文件
    
    Args:
        global_config: 全局配置
        task_config: 任务配置
        
    Returns:
        临时配置文件路径
    """
    # 合并配置
    config = {
        "target_url": task_config.get("target_url", ""),
        "regex_pattern": task_config.get("regex_pattern", ".*"),
        "output_file": task_config.get("output_file", "results.jsonl"),
        "click_selector": task_config.get("click_selector", ""),
        "wait_for_selector": task_config.get("wait_for_selector", ""),
        "max_clicks": task_config.get("max_clicks", 0),
        "click_delay_ms": task_config.get("click_delay_ms", 1000),
        "cache_path": global_config.get("cache_path", "./.crawl4ai_cache"),
        "enable_cache": global_config.get("enable_cache", True),
        "jsonl_input": task_config.get("jsonl_input", {
            "url_field": "matched_url",
            "delay_between_urls": 2.0
        }),
        "browser": global_config.get("browser", {}),
        "crawler": global_config.get("crawler", {}),
        "logging": global_config.get("logging", {}),
    }
    
    # 写入临时文件
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        delete=False,
        encoding="utf-8"
    )
    
    yaml.dump(config, temp_file, allow_unicode=True, default_flow_style=False)
    temp_file.close()
    
    return temp_file.name


def generate_capturer_config(global_config: dict, capture_config: dict) -> str:
    """生成截图任务的临时配置文件"""
    config = {
        "browser": global_config.get("browser", {}),
        "logging": global_config.get("logging", {}),
        "capturer": {
            "input": capture_config.get("input", ""),
            "url_field": capture_config.get("url_field", "matched_url"),
            "output_dir": capture_config.get("output_dir", "./captures"),
            "output_file": capture_config.get("output_file", "capture_results.jsonl"),
            "save_screenshot": capture_config.get("save_screenshot", True),
            "save_html": capture_config.get("save_html", True),
            "screenshot": capture_config.get("screenshot", {}),
            "html": capture_config.get("html", {}),
            "naming": capture_config.get("naming", {}),
            "request_delay": capture_config.get("request_delay", 2.0),
            "page_load_timeout": capture_config.get("page_load_timeout", 60000),
            "wait_after_load": capture_config.get("wait_after_load", 3.0),
            "max_retries": capture_config.get("max_retries", 2),
        }
    }
    
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        delete=False,
        encoding="utf-8"
    )
    
    yaml.dump(config, temp_file, allow_unicode=True, default_flow_style=False)
    temp_file.close()
    
    return temp_file.name


def generate_downloader_config(global_config: dict, download_config: dict) -> str:
    """生成下载任务的临时配置文件"""
    config = {
        "browser": global_config.get("browser", {}),
        "crawler": global_config.get("crawler", {}),
        "logging": global_config.get("logging", {}),
        "downloader": {
            "input": download_config.get("input", ""),
            "url_field": download_config.get("url_field", "matched_url"),
            "output_file": download_config.get("output_file", "download_results.jsonl"),
            "download_dir": download_config.get("download_dir", "./downloads"),
            "max_concurrent": download_config.get("max_concurrent", 2),
            "request_delay": download_config.get("request_delay", 3.0),
            "download_timeout": download_config.get("download_timeout", 120),
            "max_retries": download_config.get("max_retries", 3),
            "retry_delay": download_config.get("retry_delay", 10.0),
            "pdf_patterns": download_config.get("pdf_patterns", []),
            "year_patterns": download_config.get("year_patterns", []),
            "default_year": download_config.get("default_year", "Unknown"),
        }
    }
    
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        delete=False,
        encoding="utf-8"
    )
    
    yaml.dump(config, temp_file, allow_unicode=True, default_flow_style=False)
    temp_file.close()
    
    return temp_file.name


# =============================================================================
# 任务执行
# =============================================================================

async def run_crawl_task(task_config: dict, global_config: dict, task_index: int) -> dict:
    """
    执行单个爬虫任务
    
    Args:
        task_config: 任务配置
        global_config: 全局配置
        task_index: 任务索引
        
    Returns:
        执行结果
    """
    task_name = task_config.get("name", f"Task {task_index + 1}")
    
    print(f"\n{'='*60}")
    print(f"[爬虫任务 {task_index + 1}] {task_name}")
    print(f"{'='*60}")
    
    # 检查输入文件是否存在（如果是 JSONL 文件）
    target_url = task_config.get("target_url", "")
    if target_url.endswith(".jsonl") and not Path(target_url).exists():
        print(f"  跳过：输入文件不存在 - {target_url}")
        return {"success": False, "error": "输入文件不存在", "task": task_name}
    
    # 生成临时配置文件
    temp_config = generate_temp_config(global_config, task_config)
    
    try:
        # 动态导入 crawler_service
        from crawler_service import main as crawler_main
        
        print(f"  目标: {target_url}")
        print(f"  输出: {task_config.get('output_file', 'results.jsonl')}")
        print(f"  正则: {task_config.get('regex_pattern', '.*')}")
        
        result = await crawler_main(temp_config)
        
        return {"success": True, "result": result, "task": task_name}
        
    except Exception as e:
        print(f"  错误: {e}")
        return {"success": False, "error": str(e), "task": task_name}
    finally:
        # 清理临时文件
        Path(temp_config).unlink(missing_ok=True)


async def run_capture_task(capture_config: dict, global_config: dict) -> dict:
    """执行截图任务"""
    print(f"\n{'='*60}")
    print(f"[截图任务]")
    print(f"{'='*60}")
    
    # 检查输入
    input_value = capture_config.get("input", "")
    if input_value.endswith(".jsonl") and not Path(input_value).exists():
        print(f"  跳过：输入文件不存在 - {input_value}")
        return {"success": False, "error": "输入文件不存在"}
    
    # 生成临时配置文件
    temp_config = generate_capturer_config(global_config, capture_config)
    
    try:
        from page_capturer import main as capturer_main
        
        print(f"  输入: {input_value}")
        print(f"  输出目录: {capture_config.get('output_dir', './captures')}")
        
        await capturer_main(temp_config)
        
        return {"success": True}
        
    except Exception as e:
        print(f"  错误: {e}")
        return {"success": False, "error": str(e)}
    finally:
        Path(temp_config).unlink(missing_ok=True)


async def run_download_task(download_config: dict, global_config: dict) -> dict:
    """执行下载任务"""
    print(f"\n{'='*60}")
    print(f"[下载任务]")
    print(f"{'='*60}")
    
    # 检查输入
    input_value = download_config.get("input", "")
    if input_value.endswith(".jsonl") and not Path(input_value).exists():
        print(f"  跳过：输入文件不存在 - {input_value}")
        return {"success": False, "error": "输入文件不存在"}
    
    # 生成临时配置文件
    temp_config = generate_downloader_config(global_config, download_config)
    
    try:
        from downloader_pdf import main as downloader_main
        
        print(f"  输入: {input_value}")
        print(f"  下载目录: {download_config.get('download_dir', './downloads')}")
        
        await downloader_main(temp_config)
        
        return {"success": True}
        
    except Exception as e:
        print(f"  错误: {e}")
        return {"success": False, "error": str(e)}
    finally:
        Path(temp_config).unlink(missing_ok=True)


# =============================================================================
# 主函数
# =============================================================================

async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="论文爬取一键运行脚本")
    parser.add_argument(
        "-c", "--config",
        default="config_nips.yaml",
        help="配置文件路径 (默认: config_nips.yaml)"
    )
    parser.add_argument(
        "--crawl-only",
        action="store_true",
        help="只运行爬虫任务"
    )
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="只运行截图任务"
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="只运行下载任务"
    )
    parser.add_argument(
        "--task",
        type=str,
        default="",
        help="指定运行的爬虫任务编号（如: 1 或 1,2,3）"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("论文爬取一键运行脚本")
    print(f"配置文件: {args.config}")
    print("=" * 60)
    
    # 加载配置
    try:
        config = load_config(args.config)
    except FileNotFoundError as e:
        print(f"错误: {e}")
        sys.exit(1)
    
    global_config = config.get("global", {})
    crawl_tasks = config.get("crawl_tasks", [])
    capture_task = config.get("capture_task", {})
    download_task = config.get("download_task", {})
    
    start_time = datetime.now()
    results = []
    
    # 解析要运行的任务编号
    task_indices = None
    if args.task:
        task_indices = [int(t.strip()) - 1 for t in args.task.split(",")]
    
    # 运行爬虫任务
    if not args.capture_only and not args.download_only:
        print(f"\n找到 {len(crawl_tasks)} 个爬虫任务")
        
        for i, task in enumerate(crawl_tasks):
            # 检查是否指定了特定任务
            if task_indices is not None and i not in task_indices:
                continue
            
            # 检查是否启用
            if not task.get("enabled", True):
                print(f"\n[爬虫任务 {i + 1}] {task.get('name', '')} - 已禁用，跳过")
                continue
            
            result = await run_crawl_task(task, global_config, i)
            results.append(result)
            
            # 任务之间等待
            if i < len(crawl_tasks) - 1:
                print("\n等待 3 秒后继续下一个任务...")
                await asyncio.sleep(3)
    
    # 运行截图任务
    if (not args.crawl_only and not args.download_only and 
        capture_task.get("enabled", False)) or args.capture_only:
        result = await run_capture_task(capture_task, global_config)
        results.append({"type": "capture", **result})
    
    # 运行下载任务
    if (not args.crawl_only and not args.capture_only and 
        download_task.get("enabled", False)) or args.download_only:
        result = await run_download_task(download_task, global_config)
        results.append({"type": "download", **result})
    
    # 输出统计
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("执行完成!")
    print("=" * 60)
    print(f"  总耗时: {duration:.1f} 秒")
    print(f"  成功任务: {sum(1 for r in results if r.get('success', False))}")
    print(f"  失败任务: {sum(1 for r in results if not r.get('success', False))}")
    
    # 显示失败的任务
    failed = [r for r in results if not r.get("success", False)]
    if failed:
        print("\n失败的任务:")
        for r in failed:
            print(f"  - {r.get('task', r.get('type', 'Unknown'))}: {r.get('error', 'Unknown error')}")
    
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
