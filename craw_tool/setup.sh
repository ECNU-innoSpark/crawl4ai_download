#!/bin/bash

# =============================================================================
# Crawl4AI 爬虫工具 - 一键环境安装脚本
# =============================================================================
# 功能：
#   1. 创建/激活 Conda 虚拟环境
#   2. 安装所有 Python 依赖
#   3. 安装 Playwright 浏览器
#   4. 验证安装
# =============================================================================

set -euo pipefail

# 配置
ENV_NAME="crawl4ai_env"
PYTHON_VERSION="3.10"
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo ""
echo "============================================================"
echo "  Crawl4AI 爬虫工具 - 环境安装"
echo "============================================================"
echo ""

# =============================================================================
# 1. 检测包管理器（Conda 或 pip）
# =============================================================================

USE_CONDA=false

# 检查 Conda 是否可用
if command -v conda &> /dev/null; then
    log_info "检测到 Conda 安装"
    USE_CONDA=true
    
    # 初始化 Conda
    CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
    if [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
        source "$CONDA_BASE/etc/profile.d/conda.sh"
    else
        log_warn "找不到 conda.sh，尝试直接使用 conda"
    fi
else
    log_warn "未检测到 Conda，将使用 pip + venv"
fi

# =============================================================================
# 2. 创建虚拟环境
# =============================================================================

cd "$SCRIPT_DIR"

if [ "$USE_CONDA" = true ]; then
    # 使用 Conda
    log_info "检查 Conda 环境 '$ENV_NAME'..."
    
    if conda info --envs | grep -qE "^$ENV_NAME\s"; then
        log_info "环境 '$ENV_NAME' 已存在"
    else
        log_info "创建 Conda 环境: $ENV_NAME (Python $PYTHON_VERSION)"
        conda create -n "$ENV_NAME" python="$PYTHON_VERSION" -y
    fi
    
    log_info "激活环境: $ENV_NAME"
    conda activate "$ENV_NAME"
else
    # 使用 venv
    VENV_DIR="$SCRIPT_DIR/.venv"
    
    if [ -d "$VENV_DIR" ]; then
        log_info "虚拟环境已存在: $VENV_DIR"
    else
        log_info "创建虚拟环境: $VENV_DIR"
        python3 -m venv "$VENV_DIR"
    fi
    
    log_info "激活虚拟环境"
    source "$VENV_DIR/bin/activate"
fi

# =============================================================================
# 3. 安装 Python 依赖
# =============================================================================

log_info "升级 pip..."
pip install --upgrade pip

log_info "安装 Python 依赖..."
pip install -r requirements.txt

# =============================================================================
# 4. 安装 Playwright 浏览器
# =============================================================================

log_info "安装 Playwright 浏览器 (Chromium)..."
playwright install chromium

# 可选：安装系统依赖（Linux）
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    log_info "安装 Playwright 系统依赖..."
    playwright install-deps chromium || log_warn "系统依赖安装失败，可能需要 sudo 权限"
fi

# =============================================================================
# 5. 安装 Crawl4AI 浏览器
# =============================================================================

log_info "安装 Crawl4AI 浏览器..."
crawl4ai-setup || log_warn "Crawl4AI 浏览器安装跳过（可能已安装）"

# =============================================================================
# 6. 验证安装
# =============================================================================

echo ""
echo "============================================================"
echo "  安装验证"
echo "============================================================"

log_info "Python 版本: $(python --version)"
log_info "pip 版本: $(pip --version)"

# 验证核心包
echo ""
log_info "验证核心依赖..."

python -c "import crawl4ai; print(f'  crawl4ai: {crawl4ai.__version__}')" 2>/dev/null || log_error "crawl4ai 未安装"
python -c "import playwright; print(f'  playwright: OK')" 2>/dev/null || log_error "playwright 未安装"
python -c "import yaml; print(f'  pyyaml: OK')" 2>/dev/null || log_error "pyyaml 未安装"
python -c "import httpx; print(f'  httpx: OK')" 2>/dev/null || log_error "httpx 未安装"

echo ""
echo "============================================================"
echo "  安装完成！"
echo "============================================================"
echo ""
echo "使用方法："
echo ""

if [ "$USE_CONDA" = true ]; then
    echo "  1. 激活环境:    conda activate $ENV_NAME"
else
    echo "  1. 激活环境:    source .venv/bin/activate"
fi

echo "  2. 运行爬虫:    python crawler_service.py"
echo "  3. 下载 PDF:    python downloader_pdf.py"
echo ""
echo "配置文件: config.yaml"
echo ""
