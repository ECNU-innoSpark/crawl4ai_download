import asyncio
import argparse
import os
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
import aiohttp
from crawl4ai import AsyncWebCrawler


def extract_year_from_url(url: str) -> str:
    """
    从 URL 中提取年份
    例如: /paper_files/paper/2024/file/xxx.pdf -> 2024
    """
    match = re.search(r'/(\d{4})/', url)
    return match.group(1) if match else "unknown"


async def download_file(url: str, save_path: str, session: aiohttp.ClientSession, year: str = None):
    """
    下载单个文件
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status == 200:
                # 从 URL 中提取文件名
                filename = os.path.basename(urlparse(url).path)
                if not filename:
                    filename = f"downloaded_{hash(url)}.pdf"
                
                # 如果指定了年份，创建年份子目录
                if year:
                    year_dir = os.path.join(save_path, year)
                    Path(year_dir).mkdir(parents=True, exist_ok=True)
                    filepath = os.path.join(year_dir, filename)
                else:
                    filepath = os.path.join(save_path, filename)
                
                # 检查文件是否已存在
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    if year:
                        print(f"⏭️  已存在: {year}/{filename} ({file_size / 1024:.2f} KB)")
                    else:
                        print(f"⏭️  已存在: {filename} ({file_size / 1024:.2f} KB)")
                    return filepath
                
                # 保存文件
                with open(filepath, 'wb') as f:
                    f.write(await response.read())
                
                file_size = os.path.getsize(filepath)
                if year:
                    print(f"✅ 下载成功: {year}/{filename} ({file_size / 1024:.2f} KB)")
                else:
                    print(f"✅ 下载成功: {filename} ({file_size / 1024:.2f} KB)")
                return filepath
            else:
                print(f"❌ 下载失败: {url} (状态码: {response.status})")
                return None
    except Exception as e:
        print(f"❌ 下载出错: {url} - {str(e)}")
        return None


async def crawl_and_download_pdfs(url: str, output_dir: str = "downloaded_pdfs", max_concurrent: int = 5):
    """
    爬取网页并下载所有 PDF 文件
    
    Args:
        url: 要爬取的网页 URL
        output_dir: PDF 文件保存目录
        max_concurrent: 最大并发下载数 (默认: 5)
    """
    # 创建输出目录
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"🚀 开始爬取网页: {url}")
    
    # 初始化爬虫
    async with AsyncWebCrawler(verbose=False) as crawler:
        # 爬取网页
        result = await crawler.arun(
            url=url,
            bypass_cache=True,
        )
        
        if not result.success:
            print(f"❌ 爬取失败: {result.error_message}")
            return
        
        print(f"✅ 网页爬取成功")
        
        # 提取所有链接
        if not hasattr(result, 'links') or not result.links:
            print("⚠️  未找到任何链接，尝试从内容中提取 PDF 链接...")
            # 从 HTML 中手动提取 PDF 链接
            import re
            pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
            pdf_links = re.findall(pdf_pattern, result.html, re.IGNORECASE)
        else:
            # 筛选 PDF 链接 - result.links 返回的是字典列表，每个字典包含 href 和 text
            all_links = result.links.get('external', []) + result.links.get('internal', [])
            pdf_links = []
            for link in all_links:
                if isinstance(link, dict):
                    href = link.get('href', '')
                    if href and href.lower().endswith('.pdf'):
                        pdf_links.append(href)
                elif isinstance(link, str) and link.lower().endswith('.pdf'):
                    pdf_links.append(link)
        
        if not pdf_links:
            print("❌ 未找到任何 PDF 文件链接")
            return
        
        # 转换为绝对 URL
        absolute_pdf_links = []
        for link in pdf_links:
            if link.startswith('http://') or link.startswith('https://'):
                absolute_pdf_links.append(link)
            else:
                absolute_pdf_links.append(urljoin(url, link))
        
        # 去重
        absolute_pdf_links = list(set(absolute_pdf_links))
        
        print(f"📄 找到 {len(absolute_pdf_links)} 个 PDF 文件:")
        for i, link in enumerate(absolute_pdf_links, 1):
            print(f"  {i}. {link}")
        
        # 统计年份分布
        year_count = {}
        for link in absolute_pdf_links:
            year = extract_year_from_url(link)
            year_count[year] = year_count.get(year, 0) + 1
        
        print(f"\n📊 年份分布:")
        for year in sorted(year_count.keys()):
            print(f"  {year}: {year_count[year]} 个文件")
        
        print(f"\n⬇️  开始下载到目录: {os.path.abspath(output_dir)}")
        print(f"⚙️  并发数: {max_concurrent}")
        
        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_with_semaphore(pdf_url):
            async with semaphore:
                async with aiohttp.ClientSession() as session:
                    return await download_file(pdf_url, output_dir, session, extract_year_from_url(pdf_url))
        
        # 异步下载所有 PDF
        tasks = [download_with_semaphore(pdf_url) for pdf_url in absolute_pdf_links]
        results = await asyncio.gather(*tasks)
        
        # 统计下载结果
        successful = sum(1 for r in results if r is not None)
        print(f"\n🎉 下载完成! 成功: {successful}/{len(absolute_pdf_links)}")


async def process_jsonl(jsonl_path: str, output_dir: str = "downloaded_pdfs", url_field: str = 'url', max_concurrent: int = 20):
    """
    从 JSONL 文件读取 URL 列表，并发爬取和下载
    
    Args:
        jsonl_path: JSONL 文件路径
        output_dir: PDF 文件保存目录
        url_field: JSONL 中 URL 字段名
        max_concurrent: 最大并发爬取URL数 (默认: 20)
    """
    print(f"{'='*80}")
    print(f"📂 读取 JSONL 文件: {jsonl_path}")
    print(f"{'='*80}\n")
    
    # 读取 JSONL 文件中的 URL
    urls = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                url = data.get(url_field)
                if url:
                    urls.append(url)
            except json.JSONDecodeError as e:
                print(f"⚠️  跳过第 {line_num} 行 (JSON 解析失败): {str(e)[:50]}")
    
    if not urls:
        print("❌ 没有找到任何 URL")
        return
    
    print(f"✅ 读取到 {len(urls)} 个 URL")
    print(f"⚙️  并发爬取数: {max_concurrent}")
    print(f"{'='*80}\n")
    
    # 使用信号量控制并发URL爬取数
    semaphore = asyncio.Semaphore(max_concurrent)
    total_processed = 0
    total_urls = len(urls)
    
    async def crawl_with_semaphore(url, idx):
        nonlocal total_processed
        async with semaphore:
            print(f"\n{'─'*80}")
            print(f"[{idx}/{total_urls}] 爬取: {url[:70]}...")
            print(f"{'─'*80}")
            await crawl_and_download_pdfs(url, output_dir, max_concurrent=10)
            total_processed += 1
            print(f"✓ 已完成 {total_processed}/{total_urls}")
    
    # 并发爬取所有 URL
    tasks = [crawl_with_semaphore(url, idx) for idx, url in enumerate(urls, 1)]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print(f"\n{'='*80}")
    print(f"🎉 全部完成! 已处理 {total_urls} 个 URL")
    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(
        description="从网页或 JSONL 文件爬取并下载所有 PDF 文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 从 JSONL 文件读取 URL 列表（默认并发20个URL）
  python download_pdf.py --jsonl config1_level2.jsonl
  
  # 从单个网页爬取
  python download_pdf.py --url https://example.com/papers
  
  # 指定输出目录和并发爬取数
  python download_pdf.py --jsonl urls.jsonl --output my_pdfs --max-concurrent 10
  
  # 高并发爬取（同时处理50个URL）
  python download_pdf.py --jsonl urls.jsonl --max-concurrent 50
  
注意:
  - max-concurrent 控制同时爬取的URL数量
  - 每个URL爬取到的PDF文件内部并发10个下载
  - 建议根据网络状况和服务器性能调整并发数
        """
    )
    parser.add_argument(
        "--jsonl",
       default="config1_level2.jsonl",
        help="JSONL 文件路径（每行一个包含 URL 的 JSON 对象）"
    )
    parser.add_argument(
        "--url-field",
        type=str,
        default="url",
        help="JSONL 中 URL 字段名 (默认: url)"
    )
    parser.add_argument(
        "--url",
        type=str,
        help="要爬取的网页 URL（单个 URL 模式）"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="downloaded_pdfs",
        help="PDF 文件保存目录 (默认: downloaded_pdfs)"
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=20,
        help="最大并发爬取URL数 (默认: 20，每个URL下载PDF时内部并发10个)"
    )
    
    args = parser.parse_args()
    
    # 判断使用哪种模式
    if args.jsonl:
        # JSONL 模式：从文件读取 URL 列表
        asyncio.run(process_jsonl(args.jsonl, args.output, args.url_field, args.max_concurrent))
    elif args.url:
        # 单个 URL 模式
        asyncio.run(crawl_and_download_pdfs(args.url, args.output, args.max_concurrent))
    else:
        parser.print_help()
        print("\n❌ 错误: 必须指定 --jsonl 或 --url 参数")


if __name__ == "__main__":
    main()
