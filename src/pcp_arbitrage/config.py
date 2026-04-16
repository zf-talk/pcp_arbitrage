from dataclasses import dataclass, field
from pathlib import Path
import copy

import yaml

# 默认合约数量（期权/交割合约 legs 共用），与标的对应；可在 YAML contracts.lot_size 中按符号覆盖
DEFAULT_LOT_SIZES: dict[str, float] = {
    "BTC": 0.01,
    "ETH": 0.1,
    "BNB": 0.1,
    "SOL": 1.0,
    "XRP": 10.0,
    "DOGE": 100.0,
    "AVAX": 1.0,
    "TRX": 100.0,
}

EXCHANGE_MARGIN_TYPES: dict[str, str] = {
    "okx": "coin",
    "binance": "usdt",
    "deribit": "coin",
    "deribit_linear": "usdc",
}


def _margin_type_for_exchange(exchange_name: str) -> str:
    return EXCHANGE_MARGIN_TYPES.get(exchange_name, "coin")


def _merge_lot_sizes(overrides: dict | None) -> dict[str, float]:
    out = copy.deepcopy(DEFAULT_LOT_SIZES)
    if not overrides:
        return out
    for k, v in overrides.items():
        key = str(k).strip()
        if not key:
            continue
        try:
            out[key] = float(v)
        except (TypeError, ValueError):
            continue
    return out


@dataclass
class TelegramConfig:
    bot_token: str = ""
    chat_id: str = ""


@dataclass
class ExchangeConfig:
    name: str
    enabled: bool
    margin_type: str       # "coin" | "usdt"
    api_key: str
    secret_key: str
    passphrase: str        # OKX-only; "" for Binance
    arbitrage_enabled: bool = False  # auto-trade on this exchange (requires api_key set)


@dataclass
class AppConfig:
    exchanges: dict[str, ExchangeConfig]
    symbols: list[str]
    min_annualized_rate: float
    order_min_annualized_rate: float
    atm_range: float
    min_days_to_expiry: float  # drop options with T < this (days); 0 = any positive T
    stale_threshold_ms: int
    lot_size: dict[str, float]
    proxy: str | None = None           # e.g. "http://127.0.0.1:7890"
    log_level: str = "INFO"            # global log level
    log_levels: dict[str, str] = None  # per-module overrides
    tick_interval_sec: float = 0.0   # 0 = off; periodic DEBUG while WS runs (e.g. Deribit)
    dashboard_quiet_exchanges: bool = True  # when dashboard: WARNING for exchange WS noise (see main)
    pairing_log_dir: str = "data/pairings"  # per-exchange pairing summary (overwrite each run); empty = disable
    opportunity_csv_enabled: bool = True
    opportunity_csv_path: str = "data/opportunities.csv"
    opportunity_csv_interval_sec: float = 5.0
    web_dashboard_enabled: bool = False
    web_dashboard_host: str = "127.0.0.1"
    web_dashboard_port: int = 8765
    # 反向代理子路径前缀（无前导斜杠亦可），如 pcp 表示 API 为 /pcp/api/...；直连留空
    web_dashboard_api_base: str = ""
    sqlite_path: str = "data/pcp_arbitrage.db"
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    pnl_alert_threshold_pct: float = 0.05  # 5% move triggers Telegram alert
    exit_days_before_expiry: int = 1       # close position N days before expiry
    exit_target_profit_pct: float = 0.50   # close when floating profit >= 50%
    # 开平仓统一追单：先 maker，超时后切 taker
    maker_chase_secs: int = 60             # maker 轮询窗口（秒），到期后切换 taker
    maker_chase_max_minutes: int = 3       # 开/平仓连续追单最长时长（分钟）
    exit_monitor_interval_secs: int = 10   # 后台守护扫描间隔（秒）
    # 开仓仓位控制
    entry_max_trade_pct: float = 0.20      # 单笔最多占用账户总权益的比例
    entry_reserve_pct: float = 0.10        # 账户预留比例（不可动用）
    # 保留字段：OKX 开仓门槛已改为 REST `GET /account/max-size`（保证金规则由交易所计算）。
    # 仍可从 YAML 读取，供后续扩展或文档兼容。
    entry_fund_usable_pct: float = 0.90
    # 相同合约 (exchange+label，不含方向)：窗口内年化变动 ≤ max_delta 则视为重复，不写 CSV / 少打日志
    opportunity_ann_suppress_window_sec: float = 300.0
    opportunity_ann_suppress_max_delta: float = 0.01  # 1 个百分点，年化小数差 ≤0.01

    def __post_init__(self):
        if self.log_levels is None:
            self.log_levels = {}


def load_config(path: Path | str = "config.yaml") -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    exchanges = {}
    for name, ex in raw["exchanges"].items():
        exchanges[name] = ExchangeConfig(
            name=name,
            enabled=ex.get("enabled", False),
            margin_type=_margin_type_for_exchange(name),
            api_key=ex.get("api_key", ""),
            secret_key=ex.get("secret_key", ""),
            passphrase=ex.get("passphrase", ""),
            arbitrage_enabled=ex.get("arbitrage_enabled", False),
        )
    arb = raw["arbitrage"]
    log_cfg = raw.get("logging", {})
    tick_raw = log_cfg.get("tick_interval_sec", 0)
    try:
        tick_interval_sec = float(tick_raw or 0)
    except (TypeError, ValueError):
        tick_interval_sec = 0.0
    try:
        min_days_to_expiry = float(arb.get("min_days_to_expiry", 1.0))
    except (TypeError, ValueError):
        min_days_to_expiry = 1.0
    if min_days_to_expiry < 0:
        min_days_to_expiry = 0.0
    dashboard_quiet_exchanges = bool(arb.get("dashboard_quiet_exchanges", True))
    contracts = raw.get("contracts") or {}
    lot_size = _merge_lot_sizes(contracts.get("lot_size"))
    trace = raw.get("tracing") or {}
    pairing_log_dir = str(trace.get("pairing_log_dir", "data/pairings")).strip()
    opportunity_csv_enabled = bool(trace.get("opportunity_csv_enabled", True))
    opportunity_csv_path = str(trace.get("opportunity_csv_path", "data/opportunities.csv")).strip()
    try:
        opportunity_csv_interval_sec = float(trace.get("opportunity_csv_interval_sec", 5.0))
    except (TypeError, ValueError):
        opportunity_csv_interval_sec = 5.0
    opportunity_csv_interval_sec = max(1.0, opportunity_csv_interval_sec)
    web_cfg = raw.get("web_dashboard") or {}
    web_dashboard_enabled = bool(web_cfg.get("enabled", False))
    web_dashboard_host = str(web_cfg.get("host", "127.0.0.1")).strip()
    web_dashboard_port = int(web_cfg.get("port", 8765))
    web_dashboard_api_base = str(web_cfg.get("api_base", "") or "").strip().rstrip("/")
    if web_dashboard_api_base and not web_dashboard_api_base.startswith("/"):
        web_dashboard_api_base = "/" + web_dashboard_api_base
    sqlite_path = str(trace.get("sqlite_path", "data/pcp_arbitrage.db")).strip()
    tg_cfg = raw.get("telegram") or {}
    telegram = TelegramConfig(
        bot_token=str(tg_cfg.get("bot_token", "") or ""),
        chat_id=str(tg_cfg.get("chat_id", "") or ""),
    )
    try:
        order_min_annualized_rate = float(arb.get("order_min_annualized_rate", 0.05))
    except (TypeError, ValueError):
        order_min_annualized_rate = 0.05
    try:
        pnl_alert_threshold_pct = float(arb.get("pnl_alert_threshold_pct", 0.05))
    except (TypeError, ValueError):
        pnl_alert_threshold_pct = 0.05
    try:
        exit_days_before_expiry = int(arb.get("exit_days_before_expiry", 1))
    except (TypeError, ValueError):
        exit_days_before_expiry = 1
    if exit_days_before_expiry < 0:
        exit_days_before_expiry = 0
    try:
        exit_target_profit_pct = float(arb.get("exit_target_profit_pct", 0.50))
    except (TypeError, ValueError):
        exit_target_profit_pct = 0.50
    maker_chase_raw = arb.get(
        "maker_chase_secs",
        arb.get("entry_maker_chase_secs", arb.get("exit_maker_chase_secs", 60)),
    )
    try:
        maker_chase_secs = int(maker_chase_raw)
    except (TypeError, ValueError):
        maker_chase_secs = 60
    if maker_chase_secs <= 0:
        maker_chase_secs = 60
    try:
        exit_monitor_interval_secs = int(arb.get("exit_monitor_interval_secs", 10))
    except (TypeError, ValueError):
        exit_monitor_interval_secs = 10
    if exit_monitor_interval_secs < 0:
        exit_monitor_interval_secs = 10
    try:
        entry_max_trade_pct = float(arb.get("entry_max_trade_pct", 0.20))
    except (TypeError, ValueError):
        entry_max_trade_pct = 0.20
    if entry_max_trade_pct < 0:
        entry_max_trade_pct = 0.20
    if entry_max_trade_pct > 1.0:
        entry_max_trade_pct = 1.0
    try:
        entry_reserve_pct = float(arb.get("entry_reserve_pct", 0.10))
    except (TypeError, ValueError):
        entry_reserve_pct = 0.10
    if entry_reserve_pct < 0:
        entry_reserve_pct = 0.10
    if entry_reserve_pct > 1.0:
        entry_reserve_pct = 1.0
    maker_chase_max_raw = arb.get(
        "maker_chase_max_minutes",
        arb.get("entry_maker_chase_max_minutes", 3),
    )
    try:
        maker_chase_max_minutes = int(maker_chase_max_raw)
    except (TypeError, ValueError):
        maker_chase_max_minutes = 3
    if maker_chase_max_minutes <= 0:
        maker_chase_max_minutes = 3
    try:
        entry_fund_usable_pct = float(arb.get("entry_fund_usable_pct", 0.90))
    except (TypeError, ValueError):
        entry_fund_usable_pct = 0.90
    if entry_fund_usable_pct <= 0:
        entry_fund_usable_pct = 0.90
    if entry_fund_usable_pct > 1.0:
        entry_fund_usable_pct = 1.0
    try:
        opportunity_ann_suppress_window_sec = float(arb.get("opportunity_ann_suppress_window_sec", 300.0))
    except (TypeError, ValueError):
        opportunity_ann_suppress_window_sec = 300.0
    if opportunity_ann_suppress_window_sec < 0:
        opportunity_ann_suppress_window_sec = 300.0
    try:
        opportunity_ann_suppress_max_delta = float(arb.get("opportunity_ann_suppress_max_delta", 0.01))
    except (TypeError, ValueError):
        opportunity_ann_suppress_max_delta = 0.01
    if opportunity_ann_suppress_max_delta < 0:
        opportunity_ann_suppress_max_delta = 0.01
    return AppConfig(
        exchanges=exchanges,
        symbols=arb["symbols"],
        min_annualized_rate=arb["min_annualized_rate"],
        order_min_annualized_rate=order_min_annualized_rate,
        atm_range=arb["atm_range"],
        min_days_to_expiry=min_days_to_expiry,
        stale_threshold_ms=arb["stale_threshold_ms"],
        lot_size=lot_size,
        proxy=raw.get("proxy") or None,
        log_level=log_cfg.get("level", "INFO"),
        log_levels=log_cfg.get("levels", {}),
        tick_interval_sec=tick_interval_sec,
        dashboard_quiet_exchanges=dashboard_quiet_exchanges,
        pairing_log_dir=pairing_log_dir,
        opportunity_csv_enabled=opportunity_csv_enabled,
        opportunity_csv_path=opportunity_csv_path,
        opportunity_csv_interval_sec=opportunity_csv_interval_sec,
        web_dashboard_enabled=web_dashboard_enabled,
        web_dashboard_host=web_dashboard_host,
        web_dashboard_port=web_dashboard_port,
        web_dashboard_api_base=web_dashboard_api_base,
        sqlite_path=sqlite_path,
        telegram=telegram,
        pnl_alert_threshold_pct=pnl_alert_threshold_pct,
        exit_days_before_expiry=exit_days_before_expiry,
        exit_target_profit_pct=exit_target_profit_pct,
        maker_chase_secs=maker_chase_secs,
        maker_chase_max_minutes=maker_chase_max_minutes,
        exit_monitor_interval_secs=exit_monitor_interval_secs,
        entry_max_trade_pct=entry_max_trade_pct,
        entry_reserve_pct=entry_reserve_pct,
        entry_fund_usable_pct=entry_fund_usable_pct,
        opportunity_ann_suppress_window_sec=opportunity_ann_suppress_window_sec,
        opportunity_ann_suppress_max_delta=opportunity_ann_suppress_max_delta,
    )
