"""
使用 LLM 自动生成爬虫配置
"""
import asyncio
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from openai import AsyncOpenAI


class ConfigGenerator:
    """LLM 配置生成器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.client = None
        
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def _init_client(self):
        """初始化 LLM 客户端"""
        if not self.client:
            llm_config = self.config.get('llm', {})
            self.client = AsyncOpenAI(
                api_key=llm_config.get('api_key'),
                base_url=llm_config.get('base_url')
            )
    
    async def analyze_website(self, url: str, sample_html: str = None) -> Dict[str, Any]:
        """
        使用 LLM 分析网站结构并生成配置
        
        Args:
            url: 网站 URL
            sample_html: 示例 HTML（可选）
        
        Returns:
            生成的配置字典
        """
        self._init_client()
        
        prompt = self._build_analysis_prompt(url, sample_html)
        
        llm_config = self.config.get('llm', {})
        response = await self.client.chat.completions.create(
            model=llm_config.get('model', 'gpt-4'),
            messages=[
                {"role": "system", "content": "你是一位精通网页结构分析和正则表达式的爬虫专家。"},
                {"role": "user", "content": prompt}
            ],
            temperature=llm_config.get('temperature', 0.3),
            max_tokens=llm_config.get('max_tokens', 16384)  # 增加到 4000
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # 解析 LLM 返回的配置
        config_data = self._parse_llm_response(result_text)
        
        return config_data
    
    def _build_analysis_prompt(self, url: str, sample_html: str = None) -> str:
        """构建分析提示词"""
        prompt = f"""# Role
你是一位精通自动化数据采集的资深爬虫工程师，擅长分析复杂网站的 URL 拓扑结构，并能编写高精度的正则表达式。

# Task
请分析目标网站 {url} 的结构，并按照递归爬取的逻辑生成一份 YAML 格式的层级配置。

# Analysis Strategy
1. **拓扑分析**：识别网站从“入口”到“最终PDF文件”的逻辑路径（通常为：主页 -> 分类/年份列表 -> 论文列表页 -> 摘要详情页 -> PDF下载链接）。
2. **正则精度**：提取模式 (extract_pattern) 应尽可能捕获潜在链接；过滤模式 (filter_pattern) 必须使用锚点（如 ^ 和 $）确保路径纯净，排除无用的参数或非目标文件。
3. **路径兼容**：需同时考虑绝对路径 (https://...) 和相对路径 (/paper_files/...) 的匹配。

# Output Format (YAML)
```yaml
levels:
  - level: 1
    name: "起始页/索引页"
    url_pattern: "匹配当前层级的正则表达式"
    extract_pattern: "提取下一级链接的正则 (需捕获关键路径特征)"
    filter_pattern: "过滤正则 (确保只保留下一级目标的合法URL)"
    description: "描述当前层级的特征及跳转逻辑"

  - level: n (以此类推，直到最终 PDF 链接层)
    name: "PDF 下载层"
    url_pattern: "匹配 PDF 所在页面的正则"
    extract_pattern: "提取 .pdf 结尾的链接正则"
    filter_pattern: "过滤正则 (排除 Metadata, Bibtex 等干扰项)"
    description: "描述如何获取最终的 PDF 原始文件"
"""
        
        if sample_html:
            prompt += f"\n示例 HTML 片段：\n```html\n{sample_html}\n```\n"
        
        prompt += "\n请直接返回 YAML 格式的配置，不要包含其他文字。"
        
        return prompt
    
    def _parse_llm_response(self, response: str) -> Dict[str, Any]:
        """解析 LLM 返回的配置"""
        # 尝试提取 YAML 代码块
        import re
        yaml_match = re.search(r'```ya?ml\s*\n(.*?)\n```', response, re.DOTALL)
        if yaml_match:
            yaml_content = yaml_match.group(1)
        else:
            yaml_content = response
        
        try:
            config_data = yaml.safe_load(yaml_content)
            return config_data
        except yaml.YAMLError as e:
            print(f"⚠️  YAML 解析失败: {e}")
            print(f"LLM 返回内容:\n{response}")
            return {}
    
    def update_config(self, new_config: Dict[str, Any], merge: bool = True):
        """
        更新配置文件
        
        Args:
            new_config: 新配置
            merge: 是否合并（True）或覆盖（False）
        """
        if merge and self.config:
            # 合并配置
            self._deep_update(self.config, new_config)
        else:
            self.config = new_config
        
        # 保存到文件
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, allow_unicode=True, default_flow_style=False, indent=2)
        
        print(f"✅ 配置已更新: {self.config_path}")
    
    def _deep_update(self, base: Dict, update: Dict):
        """深度更新字典"""
        for key, value in update.items():
            if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                self._deep_update(base[key], value)
            else:
                base[key] = value


async def generate_config_from_url(url: str, config_path: str = "config.yaml"):
    """
    从 URL 生成配置的便捷函数
    
    Args:
        url: 目标网站 URL
        config_path: 配置文件路径
    """
    generator = ConfigGenerator(config_path)
    
    print(f"🤖 正在使用 LLM 分析网站: {url}")
    
    # 首先爬取网站获取示例 HTML
    try:
        from crawl4ai import AsyncWebCrawler
        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url, bypass_cache=True)
            if result.success:
                sample_html = result.html  # 取前5000字符
                print("✅ 网站爬取成功，开始分析...")
            else:
                sample_html = None
                print("⚠️  网站爬取失败，使用无示例分析...")
    except Exception as e:
        print(f"⚠️  爬取出错: {e}，使用无示例分析...")
        sample_html = None
    
    # 使用 LLM 分析
    config_data = await generator.analyze_website(url, sample_html)
    
    if config_data:
        # 更新配置
        generator.update_config({'target': config_data}, merge=True)
        print("🎉 配置生成完成！")
    else:
        print("❌ 配置生成失败")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="使用 LLM 自动生成爬虫配置文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 分析网站并生成配置
  python config_generator.py --url https://papers.nips.cc/
  
  # 指定输出配置文件
  python config_generator.py --url https://example.com --output custom_config.yaml
  
  # 使用自定义 API
  python config_generator.py --url https://example.com --api-key sk-xxx --model gpt-4
        """
    )
    
    parser.add_argument(
        '--url',
        type=str,
        default='https://papers.nips.cc/',
        help='要分析的网站 URL'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='config1.yaml',
        help='输出配置文件路径 (默认: config.yaml)'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        default="sk-eEHuvDfMPJf3mKQOmdDVHDq30RsA9RXKd4LhUtGxNgiXYtPq",
        help='OpenAI API Key（覆盖配置文件）'
    )
    
    parser.add_argument(
        '--base-url',
        type=str,
        default="http://49.51.37.239:3006/v1",
        help='API Base URL（覆盖配置文件）'
    )
    
    parser.add_argument(
        '--model',
        type=str,
        default="gemini-3-pro-preview-thinking",
        help='模型名称（覆盖配置文件）'
    )
    
    args = parser.parse_args()
    
    # 如果提供了 API 参数，更新配置
    if args.api_key or args.base_url or args.model:
        import yaml
        from pathlib import Path
        
        config_path = Path(args.output)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
        
        if 'llm' not in config:
            config['llm'] = {}
        
        if args.api_key:
            config['llm']['api_key'] = args.api_key
        if args.base_url:
            config['llm']['base_url'] = args.base_url
        if args.model:
            config['llm']['model'] = args.model
        
        # 保存更新的配置
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False, indent=2)
    
    asyncio.run(generate_config_from_url(args.url, args.output))
