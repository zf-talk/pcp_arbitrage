#!/usr/bin/env bash
# scripts/setup.sh — 初始化脚本
# 功能：安装 supervisor、创建日志目录、部署 supervisord 配置
# 用法：bash scripts/setup.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> 检测 supervisor..."
if ! command -v supervisord &>/dev/null; then
    echo "==> 安装 supervisor..."
    if command -v apt-get &>/dev/null; then
        sudo apt-get update -q && sudo apt-get install -y supervisor
    elif command -v yum &>/dev/null; then
        sudo yum install -y supervisor
    elif command -v brew &>/dev/null; then
        brew install supervisor
    else
        echo "无法自动安装 supervisor，请手动安装后重试"
        exit 1
    fi
else
    echo "==> supervisor 已安装：$(supervisord --version)"
fi

echo "==> 创建日志目录..."
mkdir -p "$ROOT_DIR/data/logs"

echo "==> 检查 uv & 依赖..."
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # 刷新 PATH（仅当前 shell）
    export PATH="$HOME/.local/bin:$PATH"
fi
cd "$ROOT_DIR"
uv sync

echo ""
echo "==> 初始化完成！"
echo ""
echo "使用方式："
echo "  make up     # 启动 pcp_arbitrage（通过 supervisorctl）"
echo "  make down   # 停止 pcp_arbitrage"
echo "  supervisorctl -c etc/supervisord.conf status   # 查看状态"
echo "  supervisorctl -c etc/supervisord.conf tail -f pcp_arbitrage   # 查看实时日志"
echo ""
echo "提示：首次运行前请确认 config.yaml 中的 API 密钥和 web_dashboard 配置已填写正确。"
