import argparse
import asyncio
import datetime
import logging
import logging.handlers
import os
from pathlib import Path

from pcp_arbitrage.config import AppConfig, load_config
from pcp_arbitrage.exchanges.binance import BinanceRunner
from pcp_arbitrage.exchanges.deribit import DeribitLinearRunner, DeribitRunner
from pcp_arbitrage.exchanges.okx import OKXRunner
from pcp_arbitrage.signal_output import (
    configure_monitoring_barrier,
    configure_signal_output,
    dashboard_enabled,
    run_dashboard_loop,
    run_heartbeat_loop,
    run_opportunity_csv_loop,
    run_opportunity_sqlite_loop,
    run_startup_revalidation_loop,
    run_triplet_db_refresh_loop,
    run_web_dashboard_loop,
)
from pcp_arbitrage import web_dashboard as _wd

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=_fmt)

_LOG_MAX_BYTES = 50 * 1024 * 1024


class _DatedSizeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """当前始终写入固定文件名；达到 maxBytes 后重命名为带日期时间的归档文件，再新建当前文件。"""

    def doRollover(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None
        base = self.baseFilename
        if os.path.exists(base):
            stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            dname, fname = os.path.split(base)
            stem, ext = os.path.splitext(fname)
            dfn = os.path.join(dname, f"{stem}.{stamp}{ext}")
            n = 0
            while os.path.exists(dfn):
                n += 1
                dfn = os.path.join(dname, f"{stem}.{stamp}.{n}{ext}")
            os.rename(base, dfn)
        if not self.delay:
            self.stream = self._open()


def _add_file_log_handler() -> None:
    """写入 data/logs/pcp_arbitrage.log；单文件最大 50MB，满后归档为 pcp_arbitrage.YYYY-MM-DD_HHMMSS.log。"""
    root = Path(__file__).resolve().parent.parent.parent
    log_dir = root / "data" / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return
    path = log_dir / "pcp_arbitrage.log"
    fh = _DatedSizeRotatingFileHandler(
        path, maxBytes=_LOG_MAX_BYTES, backupCount=0, encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter(_fmt))
    logging.getLogger().addHandler(fh)


_add_file_log_handler()
logger = logging.getLogger(__name__)

def _apply_dashboard_log_quiet(cfg: AppConfig) -> None:
    """Reduce scrollback spam: stderr INFO from runners when using the fullscreen table."""
    if not dashboard_enabled() or not cfg.dashboard_quiet_exchanges:
        return
    if "pcp_arbitrage.exchanges" not in cfg.log_levels:
        logging.getLogger("pcp_arbitrage.exchanges").setLevel(logging.WARNING)
    if "pcp_arbitrage.okx_client" not in cfg.log_levels:
        logging.getLogger("pcp_arbitrage.okx_client").setLevel(logging.WARNING)


RUNNERS = {
    "okx": OKXRunner,
    "binance": BinanceRunner,
    "deribit": DeribitRunner,
    "deribit_linear": DeribitLinearRunner,
}


async def _run(cfg_path: str = "config.yaml", web_dashboard_override: str | None = None) -> None:
    cfg = load_config(cfg_path)
    # CLI --web-dashboard HOST:PORT overrides config
    if web_dashboard_override:
        parts = web_dashboard_override.rsplit(":", 1)
        if len(parts) == 2:
            cfg.web_dashboard_host = parts[0]
            cfg.web_dashboard_port = int(parts[1])
        cfg.web_dashboard_enabled = True
    configure_signal_output(cfg)
    # 应用日志等级配置
    logging.getLogger().setLevel(cfg.log_level)
    for module, level in cfg.log_levels.items():
        logging.getLogger(module).setLevel(level)
    _apply_dashboard_log_quiet(cfg)
    if cfg.proxy:
        logger.info("[main] Using proxy: %s", cfg.proxy)
    enabled: list[str] = []
    for name, ex_cfg in cfg.exchanges.items():
        if not ex_cfg.enabled:
            continue
        if name not in RUNNERS:
            logger.error("[main] Unknown exchange: %s", name)
            continue
        enabled.append(name)

    configure_monitoring_barrier(len(enabled))

    tasks: list[asyncio.Task[None]] = []
    for name in enabled:
        tasks.append(asyncio.create_task(RUNNERS[name](cfg.exchanges[name], cfg).run()))
    if not tasks:
        logger.error("[main] No enabled exchanges found in config")
        return
    if cfg.opportunity_csv_enabled:
        tasks.append(asyncio.create_task(run_opportunity_csv_loop()))
    if cfg.sqlite_path:
        tasks.append(asyncio.create_task(run_opportunity_sqlite_loop()))
        tasks.append(asyncio.create_task(run_triplet_db_refresh_loop()))
        tasks.append(asyncio.create_task(run_heartbeat_loop()))
    if dashboard_enabled():
        tasks.append(asyncio.create_task(run_dashboard_loop()))
        tasks.append(asyncio.create_task(run_startup_revalidation_loop()))
    if cfg.web_dashboard_enabled:
        _wd.set_app_config(cfg)
        tasks.append(asyncio.create_task(
            run_web_dashboard_loop(cfg.web_dashboard_host, cfg.web_dashboard_port, cfg.web_dashboard_width)
        ))
    if cfg.sqlite_path:
        from pcp_arbitrage import position_tracker
        tasks.append(asyncio.create_task(position_tracker.run_position_tracker_loop(cfg)))
        tasks.append(asyncio.create_task(
            position_tracker.exit_monitor_loop(cfg),
        ))
    # Periodic account balance fetch
    async def _balance_fetch_loop():
        import random
        from pcp_arbitrage import account_fetcher
        await asyncio.sleep(random.uniform(5, 15))  # 启动错开，避免与交易所握手撞车
        while True:
            try:
                balances = await account_fetcher.fetch_all_balances(
                    cfg, skip_exchanges={"deribit", "deribit_linear"}
                )
                _wd.update_account_balances(balances)
            except Exception as exc:
                logger.warning("[main] Balance fetch failed: %s", exc)
            await asyncio.sleep(300)  # 5 分钟拉一次，避免频繁认证触发 429
    tasks.append(asyncio.create_task(_balance_fetch_loop()))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception):
            logger.error("[main] task %s exited with error: %s", task.get_name(), result)


def main() -> None:
    parser = argparse.ArgumentParser(description="PCP arbitrage monitor")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument(
        "--web-dashboard",
        metavar="HOST:PORT",
        default=None,
        help="Enable web dashboard on HOST:PORT (overrides config)",
    )
    args = parser.parse_args()
    asyncio.run(_run(args.config, args.web_dashboard))
