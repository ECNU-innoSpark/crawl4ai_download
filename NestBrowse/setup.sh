#!/bin/bash
# NestBrowse 环境安装脚本
# 用法: bash setup.sh

# set -e

echo "=========================================="
echo "NestBrowse 环境安装"
echo "=========================================="

# 检查 Python
if ! command -v python &> /dev/null; then
    echo "[错误] 未找到 Python，请先安装 Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python --version 2>&1)
echo "[信息] 检测到 $PYTHON_VERSION"

# 安装 Python 依赖
echo ""
echo "[1/2] 安装 Python 依赖..."
pip install -r requirements.txt

# 安装 Playwright 浏览器
echo ""
echo "[2/2] 安装 Playwright Chromium 浏览器..."
playwright install chromium

echo ""
echo "=========================================="
echo "安装完成！"
echo "=========================================="
echo ""
echo "下一步："
echo "  1. 配置 infer_async_nestbrowse.py 中的 API 设置"
echo "  2. 运行: bash run_sample.sh"
echo ""
