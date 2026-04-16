.PHONY: init install run dev lint fmt clean up down restart status log logf daemon supervisor-shutdown kill

# make 有时在非登录 shell 下运行，PATH 不含 ~/.local/bin；uv 官方安装默认放这里
export PATH := $(HOME)/.local/bin:/opt/homebrew/bin:$(PATH)
UV ?= uv

SUPERVISOR_CONF_DIR ?= /etc/supervisor/conf.d
APP_CONF             = etc/supervisord.conf
APP_NAME             = pcp_arbitrage
LOG_DIR = data/logs
LOG_OUT = $(LOG_DIR)/pcp_arbitrage.log

# 服务器上首次部署：若无 uv 则安装，再同步依赖
init:
	@command -v $(UV) >/dev/null 2>&1 || curl -LsSf https://astral.sh/uv/install.sh | sh
	$(UV) sync

install:
	$(UV) sync

# 默认启动：含 web dashboard（在 config.yaml 中配置地址/端口）
run:
	$(UV) run pcp-arbitrage

# 开发模式：监听 src/ 与 config.yaml，变更自动重启（需要 watchfiles）
dev:
	$(UV) run watchfiles "$(UV) run pcp-arbitrage" src/ config.yaml

kill:
	@echo "Stopping pcp-arbitrage related processes..."
	@pkill -f "watchfiles .*pcp-arbitrage" 2>/dev/null || true
	@pkill -f "/pcp-arbitrage" 2>/dev/null || true
	@echo "Done."

lint:
	$(UV) run ruff check src tests

fmt:
	$(UV) run ruff format src tests

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

# ── Supervisor 管理 ────────────────────────────────────────────────────────────
# 须在本文件所在项目根目录执行（与 config.yaml 同级）。
# 顺序：1) make daemon 启动 supervisord（只装一次即可，会创建 /tmp/pcp_arbitrage_supervisor.sock）
#       2) make up / make down / make restart / make status
# 若报 unix:///tmp/pcp_arbitrage_supervisor.sock no such file，说明未跑过 daemon 或 supervisord 已退出。

daemon:
	supervisord -c $(APP_CONF)

supervisor-shutdown:
	supervisorctl -c $(APP_CONF) shutdown

up:
	supervisorctl -c $(APP_CONF) start $(APP_NAME)

down:
	supervisorctl -c $(APP_CONF) stop $(APP_NAME)

restart:
	supervisorctl -c $(APP_CONF) restart $(APP_NAME)

status:
	supervisorctl -c $(APP_CONF) status $(APP_NAME)

log:
	tail -f data/logs/*