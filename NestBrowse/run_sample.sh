#!/bin/bash
# NestBrowse 运行脚本 (sample 数据集)
# 用法: bash run_sample.sh
#
# 也可以直接用命令行参数运行其他数据集:
#   python infer_async_nestbrowse.py sample
#   python infer_async_nestbrowse.py hard
#   python infer_async_nestbrowse.py mini

# set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MCP_PORT=8080
MCP_LOG="/tmp/mcp_browser_server.log"

echo "=========================================="
echo "NestBrowse 运行脚本 (sample)"
echo "=========================================="

# 清理函数：脚本退出时关闭 MCP 服务器
cleanup() {
    if [ -n "$MCP_PID" ] && kill -0 "$MCP_PID" 2>/dev/null; then
        echo ""
        echo "[清理] 关闭 MCP 浏览器服务器 (PID: $MCP_PID)..."
        kill "$MCP_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# 检查端口是否被占用
check_port() {
    python -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('localhost',$1)); s.close()" 2>/dev/null
}

# 启动 MCP 浏览器服务器（如果尚未运行）
if check_port $MCP_PORT; then
    echo "[1/2] MCP 浏览器服务器已在端口 $MCP_PORT 运行"
else
    echo "[1/2] 启动 MCP 浏览器服务器 (端口 $MCP_PORT)..."
    python mcp_browser_server.py --port $MCP_PORT > "$MCP_LOG" 2>&1 &
    MCP_PID=$!
    echo "[信息] MCP 服务器后台启动 (PID: $MCP_PID)"
    
    # 等待服务器就绪
    echo -n "[信息] 等待服务器就绪"
    for i in {1..30}; do
        if check_port $MCP_PORT; then
            echo " OK"
            break
        fi
        if [ $i -eq 30 ]; then
            echo ""
            echo "[错误] MCP 服务器启动超时，查看日志: $MCP_LOG"
            exit 1
        fi
        echo -n "."
        sleep 1
    done
fi

# 运行推理脚本
echo ""
echo "[2/2] 运行推理 (benchmark: sample)..."
echo "=========================================="
echo ""

python infer_async_nestbrowse.py sample

echo ""
echo "=========================================="
echo "运行完成！结果保存在: results/"
echo "=========================================="
