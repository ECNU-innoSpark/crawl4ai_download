"""
RAGFlow 连接测试脚本

快速测试 RAGFlow 服务是否可访问
"""

import requests
import os
from pathlib import Path

# 加载 .env
def load_env():
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        # 尝试多种编码
        for encoding in ['utf-8', 'utf-8-sig', 'gbk', 'latin-1']:
            try:
                with open(env_path, 'r', encoding=encoding) as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip().strip('"').strip("'")
                            os.environ[key] = value
                print(f"[配置] 成功加载 .env (编码: {encoding})")
                break
            except UnicodeDecodeError:
                if encoding == 'latin-1':  # latin-1 是最后的保底方案
                    raise
                continue

load_env()

# 读取配置
RAGFLOW_API_URL = os.getenv("RAGFLOW_API_URL")
RAGFLOW_API_KEY = os.getenv("RAGFLOW_API_KEY", "")
RAGFLOW_KB_ID = os.getenv("RAGFLOW_KB_ID", "")

print("=" * 60)
print("RAGFlow 连接测试")
print("=" * 60)
print(f"\n配置信息:")
print(f"  API URL: {RAGFLOW_API_URL}")
print(f"  API Key: {RAGFLOW_API_KEY[:10]}...{RAGFLOW_API_KEY[-4:]}" if len(RAGFLOW_API_KEY) > 14 else f"  API Key: {RAGFLOW_API_KEY}")
print(f"  KB ID: {RAGFLOW_KB_ID}")

# 测试不同的 URL
test_urls = [
    RAGFLOW_API_URL,
    "http://localhost:19380",
    "http://localhost:8080",
    "http://host.docker.internal:19380",
    "http://127.0.0.1:19380",
]

print(f"\n尝试连接测试...")
for url in test_urls:
    print(f"\n测试 URL: {url}")
    try:
        # 尝试健康检查端点
        response = requests.get(f"{url}/api/health", timeout=3)
        print(f"  ✓ 连接成功! 状态码: {response.status_code}")
        if response.status_code == 200:
            print(f"    正确的 URL 是: {url}")
            break
    except requests.exceptions.ConnectionError:
        print(f"  ✗ 连接被拒绝 (服务未运行)")
    except requests.exceptions.Timeout:
        print(f"  ✗ 连接超时")
    except Exception as e:
        print(f"  ✗ 错误: {e}")

print("\n" + "=" * 60)
print("建议:")
print("1. 确保 RAGFlow 服务正在运行")
print("2. 检查 .env 中的 RAGFLOW_API_URL 配置")
print("3. 如果使用 Docker，确保端口映射正确")
print("=" * 60)

