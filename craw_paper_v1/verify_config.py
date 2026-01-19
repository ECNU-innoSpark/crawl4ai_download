"""
验证配置文件的正则表达式是否正确
"""
import asyncio
import json
import re
import time
from pathlib import Path
from typing import Dict, Any, List
from urllib.parse import urljoin
import yaml


class ConfigValidator:
    """配置验证器"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    async def validate_and_extract(self):
        """验证配置并提取各层级URL"""
        levels = self.config.get('target', {}).get('levels', [])
        base_url = self.config.get('target', {}).get('base_url', 'https://papers.nips.cc/')
        
        if not levels:
            print("❌ 配置中没有找到 levels")
            return
        
        print(f"{'='*80}")
        print(f"开始验证配置: {self.config_path}")
        print(f"基础URL: {base_url}")
        print(f"层级数量: {len(levels)}")
        print(f"⚠️  注意: 将处理和保存所有提取到的URL，可能需要较长时间")
        print(f"{'='*80}\n")
        
        # 从第一层开始
        current_urls = [base_url]
        all_results = {}
        
        try:
            from crawl4ai import AsyncWebCrawler
            
            async with AsyncWebCrawler(verbose=False) as crawler:
                for level_config in levels:
                    level = level_config.get('level')
                    level_name = level_config.get('name', f'Level{level}')
                    extract_pattern = level_config.get('extract_pattern', '')
                    filter_pattern = level_config.get('filter_pattern', '')
                    url_pattern = level_config.get('url_pattern', '')
                    description = level_config.get('description', '')
                    
                    print(f"\n{'─'*80}")
                    print(f"📊 层级 {level}: {level_name}")
                    print(f"{'─'*80}")
                    print(f"URL匹配模式: {url_pattern}")
                    print(f"提取模式:    {extract_pattern}")
                    print(f"过滤模式:    {filter_pattern}")
                    print(f"说明:        {description}")
                    print(f"\n当前待处理URL数: {len(current_urls)}")
                    
                    level_results = []
                    next_urls = []
                    extracted_count = 0
                    filtered_count = 0
                    
                    # 处理所有URL
                    total_to_process = len(current_urls)
                    estimated_time = total_to_process * 0.5 / 60  # 估计时间（分钟）
                    print(f"  开始处理 {total_to_process} 个URL (预计需要约 {estimated_time:.1f} 分钟)...")
                    
                    start_time = time.time()
                    
                    for idx, source_url in enumerate(current_urls, 1):
                        # 每10个URL显示一次进度
                        if idx % 10 == 1 or len(current_urls) <= 10:
                            print(f"\n  [{idx}/{len(current_urls)}] 爬取: {source_url[:80]}...")
                        
                        try:
                            result = await crawler.arun(url=source_url, bypass_cache=True)
                            
                            if result.success:
                                # 使用提取模式提取链接
                                if extract_pattern:
                                    raw_links = re.findall(extract_pattern, result.html, re.IGNORECASE)
                                    extracted_count += len(raw_links)
                                    
                                    if idx % 10 == 1 or len(current_urls) <= 10:
                                        print(f"      ✓ 提取到 {len(raw_links)} 个链接")
                                    
                                    # 去重
                                    raw_links = list(set(raw_links))
                                    
                                    # 转换为绝对URL并应用过滤
                                    for link in raw_links:
                                        # 处理相对路径
                                        if link.startswith('http'):
                                            full_url = link
                                        elif link.startswith('/'):
                                            full_url = urljoin(base_url, link)
                                        else:
                                            full_url = urljoin(source_url, link)
                                        
                                        # 应用过滤规则
                                        if filter_pattern:
                                            # 尝试匹配完整URL或相对路径
                                            if re.match(filter_pattern, full_url) or re.match(filter_pattern, link):
                                                filtered_count += 1
                                                
                                                # 保存结果
                                                level_results.append({
                                                    'level': level,
                                                    'level_name': level_name,
                                                    'url': full_url,
                                                    'source_url': source_url,
                                                    'extract_pattern': extract_pattern,
                                                    'filter_pattern': filter_pattern,
                                                    'matched_text': link
                                                })
                                                
                                                # 传递给下一层（不限制数量）
                                                next_urls.append(full_url)
                                        else:
                                            # 没有过滤规则，全部保留
                                            filtered_count += 1
                                            level_results.append({
                                                'level': level,
                                                'level_name': level_name,
                                                'url': full_url,
                                                'source_url': source_url,
                                                'extract_pattern': extract_pattern,
                                                'filter_pattern': filter_pattern,
                                                'matched_text': link
                                            })
                                            
                                            # 传递给下一层（不限制数量）
                                            next_urls.append(full_url)
                                else:
                                    if idx % 10 == 1 or len(current_urls) <= 10:
                                        print(f"      ⚠ 没有提取模式，跳过")
                            else:
                                if idx % 10 == 1 or len(current_urls) <= 10:
                                    print(f"      ✗ 爬取失败")
                            
                            # 延迟避免请求过快
                            await asyncio.sleep(0.5)
                            
                        except Exception as e:
                            if idx % 10 == 1 or len(current_urls) <= 10:
                                print(f"      ✗ 错误: {str(e)[:50]}")
                    
                    # 去重：按URL去重，保留第一次出现的记录
                    seen_urls = set()
                    deduplicated_results = []
                    for item in level_results:
                        if item['url'] not in seen_urls:
                            seen_urls.add(item['url'])
                            deduplicated_results.append(item)
                    level_results = deduplicated_results
                    
                    # 去重下一层URL
                    next_urls = list(set(next_urls))
                    
                    elapsed_time = time.time() - start_time
                    
                    print(f"\n  ✅ 层级 {level} 处理完成 (用时 {elapsed_time/60:.1f} 分钟)")
                    print(f"  统计:")
                    print(f"    - 处理的源URL数:      {len(current_urls)}")
                    print(f"    - 提取到的原始链接数: {extracted_count}")
                    print(f"    - 过滤后的链接数:     {filtered_count}")
                    print(f"    - 去重后保留链接数:   {len(level_results)}")
                    print(f"    - 传递给下一层数:     {len(next_urls)}")
                    
                    # 保存当前层级结果
                    all_results[f'level_{level}'] = level_results
                    
                    # 保存到JSONL
                    output_file = f"{self.config_path.stem}_level{level}.jsonl"
                    self._save_jsonl(output_file, level_results)
                    print(f"    - 已保存到: {output_file}")
                    
                    # 更新下一层的URL
                    current_urls = next_urls
                    
                    # 如果没有更多URL，停止
                    if not current_urls:
                        print(f"\n  ⚠️ 层级 {level} 后无更多链接，验证结束")
                        break
                
                # 输出总结
                self._print_summary(all_results)
                
        except ImportError:
            print("❌ 错误: 需要安装 crawl4ai")
            print("   pip install crawl4ai")
        except Exception as e:
            print(f"❌ 验证过程出错: {e}")
            import traceback
            traceback.print_exc()
    
    def _save_jsonl(self, filename: str, data: List[Dict]):
        """保存到JSONL文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            for item in data:
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')
    
    def _print_summary(self, all_results: Dict[str, List]):
        """打印总结"""
        print(f"\n{'='*80}")
        print(f"验证完成！")
        print(f"{'='*80}")
        
        total_urls = 0
        for level_key, results in all_results.items():
            level_num = level_key.split('_')[1]
            level_name = results[0]['level_name'] if results else '未知'
            count = len(results)
            total_urls += count
            
            print(f"  层级 {level_num} ({level_name}): {count} 个URL")
        
        print(f"\n  总计: {total_urls} 个URL")
        print(f"{'='*80}\n")


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="验证爬虫配置文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 验证配置文件
  python verify_config.py config1.yaml
  
  # 验证默认配置
  python verify_config.py
        """
    )
    
    parser.add_argument(
        'config_file',
        nargs='?',
        default='config1.yaml',
        help='配置文件路径 (默认: config1.yaml)'
    )
    
    args = parser.parse_args()
    
    validator = ConfigValidator(args.config_file)
    await validator.validate_and_extract()


if __name__ == "__main__":
    asyncio.run(main())
