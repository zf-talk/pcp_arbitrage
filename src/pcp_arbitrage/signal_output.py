"""Route arbitrage results to classic multi-line print or dashboard table."""

from __future__ import annotations

import asyncio
import datetime
import logging
import sys
from typing import TYPE_CHECKING

from pcp_arbitrage import notifier
from pcp_arbitrage.config import AppConfig
from pcp_arbitrage.exchange_symbols import format_strike_display
from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet
from pcp_arbitrage.pcp_calculator import ArbitrageSignal, calculate_forward, calculate_reverse
from pcp_arbitrage.signal_printer import print_signal
from pcp_arbitrage.tracing import (
    clear_opportunity_snap,
    flush_opportunities_csv,
    record_opportunity_snap,
)

if TYPE_CHECKING:
    from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard


logger = logging.getLogger(__name__)

_dash: OpportunityDashboard | None = None
_mode: str = "classic"
_cfg: AppConfig | None = None

# Tracks keys that have already been notified (since last inactive)
# Key: (exchange, label, direction) -> bool (True = already notified this activation)
_notified_keys: dict[tuple[str, str, str], bool] = {}

_monitoring_needed: int = 0
_monitoring_ready_count: int = 0
_dashboard_start_event: asyncio.Event | None = None
_startup_market_sweeps: list[object] = []  # list[Callable[[], Awaitable[None]]]


def register_startup_market_sweep(fn: object) -> None:
    """Register an exchange hook: after all monitors are ready, refresh books then run once."""
    _startup_market_sweeps.append(fn)


def configure_signal_output(cfg: AppConfig) -> None:
    """Call once after load_config. Dashboard is only used when signal_ui=dashboard and stdout is a TTY."""
    global _dash, _mode, _cfg, _startup_market_sweeps, _notified_keys
    _cfg = cfg
    _startup_market_sweeps = []
    _notified_keys = {}
    _mode = (cfg.signal_ui or "classic").strip().lower()
    if _mode not in ("classic", "dashboard"):
        _mode = "classic"
    _dash = None
    needs_dash = (_mode == "dashboard") or cfg.web_dashboard_enabled
    if needs_dash:
        if _mode == "dashboard" and not sys.stdout.isatty():
            logger.warning("signal_ui=dashboard requires a TTY; falling back to classic")
            _mode = "classic"
        from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard

        sqlite_path = cfg.sqlite_path or None
        mass_close_ended_utc: str | None = None
        if sqlite_path:
            from pcp_arbitrage.db import close_open_opportunity_sessions, init_db

            init_db(sqlite_path)
            closed, mass_close_ended_utc = close_open_opportunity_sessions(sqlite_path)
            if closed:
                logger.info("[db] Closed %d open opportunity_sessions from previous run", closed)

        _dash = OpportunityDashboard(
            max_rows=cfg.signal_dashboard_max_rows,
            min_annualized_rate=cfg.min_annualized_rate,
            atm_range=cfg.atm_range,
            min_days_to_expiry=cfg.min_days_to_expiry,
            symbols=cfg.symbols,
            sqlite_path=sqlite_path,
        )
        # Restore persisted opportunity rows from DB on startup
        if sqlite_path:
            _restore_opportunity_rows_from_db(
                sqlite_path, mass_close_ended_utc=mass_close_ended_utc
            )


def _restore_opportunity_rows_from_db(
    sqlite_path: str, *, mass_close_ended_utc: str | None = None
) -> None:
    """Load persisted opportunity_current rows back into _dash._rows on startup."""
    if _dash is None:
        return
    try:
        from pcp_arbitrage.db import coalesce_per_leg_fees, init_db, load_opportunity_current
        from pcp_arbitrage.opportunity_dashboard import _Row

        init_db(sqlite_path)
        db_rows = load_opportunity_current(sqlite_path)
        if not db_rows:
            return
        import time

        now = time.time()
        for r in db_rows:
            key = (
                r["exchange"],
                r["contract"].split("-")[0] if "-" in r["contract"] else "",
                "",
                0.0,
                r["direction"],
            )
            # reconstruct key properly: (exchange, symbol, expiry, strike, direction)
            # label format: SYMBOL-EXPIRY-STRIKE
            parts = r["contract"].split("-")
            sym = parts[0] if parts else ""
            expiry = parts[1] if len(parts) > 1 else ""
            strike_s = parts[2] if len(parts) > 2 else "0"
            try:
                strike = float(strike_s.replace(",", ""))
            except ValueError:
                strike = 0.0
            dir_key = _direction_key_from_db(r["direction"])
            key = (r["exchange"], sym, expiry, strike, dir_key)
            dur_sec = r.get("duration_sec")
            active = bool(r["active"])
            if active:
                if dur_sec is not None and float(dur_sec) >= 1:
                    first_active = now - float(dur_sec)
                else:
                    first_active = now
                frozen = None
            else:
                frozen = float(dur_sec) if dur_sec is not None else None
                # first_active 仅用于 fallback 计算，inactive 行无法精确还原；
                # 若 DB 有 last_active_eval 可粗略推算（duration 未知时用 0）
                last_ae = r.get("last_active_eval")
                if last_ae is not None and frozen is not None:
                    first_active = float(last_ae) - frozen
                elif last_ae is not None:
                    first_active = float(last_ae)
                else:
                    first_active = now
            cf, pf, ff = coalesce_per_leg_fees(
                r["fee_usdt"],
                r.get("call_fee_usdt"),
                r.get("put_fee_usdt"),
                r.get("fut_fee_usdt"),
            )
            row = _Row(
                exchange=r["exchange"],
                label=r["contract"],
                direction_cn=r["direction"],
                active=active,
                first_active=first_active,
                last_eval=now,
                gross=r["gross_usdt"],
                days_to_expiry=r["days_to_exp"],
                ann_pct=r["ann_pct"],
                max_ann_pct=r["ann_pct_max"],
                net=r["net_usdt"],
                fee=r["fee_usdt"],
                tradeable=r["tradeable"],
                last_active_eval=r.get("last_active_eval"),
                strike=r.get("strike", strike),
                call_px_usdt=r.get("call_px_usdt"),
                put_px_usdt=r.get("put_px_usdt"),
                fut_px_usdt=r.get("fut_px_usdt"),
                call_fee=cf,
                put_fee=pf,
                fut_fee=ff,
                index_price_usdt=r.get("index_price_usdt"),
                frozen_active_duration_sec=frozen,
                active_session_id=None,
            )
            _dash._rows[key] = row
            if row.active:
                _dash.note_startup_revalidate_key(key)
                sid = None
                if mass_close_ended_utc:
                    from pcp_arbitrage.db import reopen_last_session_if_mass_closed

                    sid = reopen_last_session_if_mass_closed(
                        sqlite_path,
                        exchange=r["exchange"],
                        contract=r["contract"],
                        direction=r["direction"],
                        mass_close_ended_utc=mass_close_ended_utc,
                    )
                if sid is not None:
                    row.active_session_id = sid
                else:
                    _dash._begin_opportunity_session(row)
        logger.info("[db] Restored %d opportunity rows from DB", len(db_rows))
    except Exception as e:
        logger.warning("[db] Failed to restore opportunity rows: %s", e)


def _direction_key_from_db(direction: str) -> str:
    if direction == "正向":
        return "forward"
    if direction == "反向":
        return "reverse"
    return direction


def configure_monitoring_barrier(enabled_runner_count: int) -> None:
    """Call before starting runners: Rich Live waits until this many notify_monitoring_ready()."""
    global _monitoring_needed, _monitoring_ready_count, _dashboard_start_event
    _monitoring_needed = max(0, enabled_runner_count)
    _monitoring_ready_count = 0
    if _dash is not None and _monitoring_needed > 0:
        _dashboard_start_event = asyncio.Event()
    else:
        _dashboard_start_event = None


def notify_monitoring_ready(exchange: str) -> None:
    """Call once per exchange when REST+triplets are done and WebSocket monitoring is about to start."""
    global _monitoring_ready_count
    if _monitoring_needed <= 0:
        return
    _monitoring_ready_count += 1
    logger.info(
        "[signal_output] monitoring ready %s (%d/%d)",
        exchange,
        _monitoring_ready_count,
        _monitoring_needed,
    )
    if _dashboard_start_event is not None and _monitoring_ready_count >= _monitoring_needed:
        _dashboard_start_event.set()


def dashboard_enabled() -> bool:
    return _dash is not None


def register_dashboard_runner_meta(
    exchange: str,
    *,
    option_taker_rate: float,
    option_maker_rate: float,
    future_taker_rate: float,
    future_maker_rate: float,
    n_triplets: int,
    settle_type: str = "",
    symbols: list[str] | None = None,
    triplets: list | None = None,
) -> None:
    """After REST init: fee tiers + triplet count + triplets for the static dashboard header."""
    if _dash is not None:
        _dash.set_runner_meta(
            exchange,
            option_taker_rate=option_taker_rate,
            option_maker_rate=option_maker_rate,
            future_taker_rate=future_taker_rate,
            future_maker_rate=future_maker_rate,
            n_triplets=n_triplets,
            settle_type=settle_type,
            symbols=symbols,
            triplets=triplets,
        )
    if _cfg is not None and _cfg.sqlite_path and triplets is not None:
        from pcp_arbitrage.db import init_db, upsert_triplets

        try:
            init_db(_cfg.sqlite_path)
            upsert_triplets(_cfg.sqlite_path, exchange, triplets, settle_type)
        except Exception as e:
            logger.warning("[db] upsert_triplets failed for %s: %s", exchange, e)


def update_dashboard_index_price(symbol: str, price: float) -> None:
    """Called by exchange runners when a new index price tick arrives."""
    if _dash is not None:
        _dash.update_index_price(symbol, price)


def update_dashboard_leg_price(
    exchange: str,
    inst_id: str,
    bid: float | None,
    ask: float | None,
) -> None:
    """Called by exchange runners on every book tick to keep snap prices fresh."""
    if _dash is not None:
        _dash.update_leg_price(exchange, inst_id, bid, ask)


def _triplet_label(t: Triplet) -> str:
    return f"{t.symbol}-{t.expiry}-{format_strike_display(t.symbol, t.strike)}"


def _trace_evaluation(
    exchange: str,
    triplet: Triplet,
    direction: str,
    sig: ArbitrageSignal | None,
    min_annualized_rate: float,
) -> None:
    if _cfg is None or not _cfg.opportunity_csv_enabled:
        return
    label = _triplet_label(triplet)
    if sig is None:
        clear_opportunity_snap(exchange, label, direction)
        return
    qualifies = sig.annualized_return >= min_annualized_rate
    record_opportunity_snap(
        exchange,
        label,
        direction,
        gross=sig.gross_profit,
        fee=sig.total_fee,
        net=sig.net_profit,
        ann_pct=sig.annualized_return * 100.0,
        qualifies=qualifies,
    )


def emit_opportunity_evaluation(
    exchange: str,
    triplet: Triplet,
    direction: str,
    sig: ArbitrageSignal | None,
    min_annualized_rate: float,
    signal_id: int | None = None,
) -> None:
    _trace_evaluation(exchange, triplet, direction, sig, min_annualized_rate)
    is_active = sig is not None and sig.annualized_return >= min_annualized_rate
    label = _triplet_label(triplet)
    notif_key = (exchange, label, direction)

    if is_active:
        assert sig is not None
        order_threshold = _cfg.order_min_annualized_rate if _cfg is not None else None
        meets_order = order_threshold is not None and sig.annualized_return >= order_threshold
        if not _notified_keys.get(notif_key, False):
            logger.info(
                "[%s %s %s] 年化 %.1f%%，下单阈值 %s，%s",
                exchange,
                _triplet_label(triplet),
                direction,
                sig.annualized_return * 100,
                f"{order_threshold * 100:.1f}%" if order_threshold is not None else "未配置",
                "触发下单 ✅" if meets_order else "未达下单阈值",
            )
        if (
            _cfg is not None
            and sig.annualized_return >= _cfg.order_min_annualized_rate
            and not _notified_keys.get(notif_key, False)
        ):
            _notified_keys[notif_key] = True
            direction_str = "正向" if direction == "forward" else "反向"
            msg = (
                f"🚨 套利信号达到下单阈值\n"
                f"交易所: {exchange}  标的: {label}  方向: {direction}\n"
                f"年化: {sig.annualized_return * 100:.1f}%  净利润: {sig.net_profit:.2f} USDT  量: {sig.tradeable_qty}"
            )
            try:
                asyncio.create_task(notifier.send_telegram(_cfg.telegram, msg))
            except RuntimeError:
                pass  # No running event loop (e.g., during testing without async context)
            try:
                from pcp_arbitrage import web_dashboard as _wd

                _wd.push_notification(
                    {
                        "exchange": exchange,
                        "label": label,
                        "direction": direction_str,
                        "ann_pct": round(sig.annualized_return * 100, 1),
                        "net_profit": round(sig.net_profit, 2),
                    }
                )
            except Exception:
                pass
            from pcp_arbitrage import order_manager as _om

            try:
                asyncio.create_task(
                    _om.submit_entry(triplet, sig, signal_id, _cfg, _cfg.sqlite_path)
                )
            except RuntimeError:
                pass
    # Key is only reset when the opportunity goes fully inactive (below display threshold).
    # If the signal dips below order_min_annualized_rate but stays above min_annualized_rate,
    # we intentionally do NOT re-notify — this avoids alert spam for noisy signals.
    else:
        _notified_keys.pop(notif_key, None)

    if _dash is not None:
        _dash.record_evaluation(exchange, triplet, direction, sig, min_annualized_rate)
        _dash.clear_startup_revalidate_key(exchange, triplet, direction)
        return
    if _mode == "classic" and sig is not None and sig.annualized_return >= min_annualized_rate:
        print_signal(sig)


def emit_triplet_if_books_ready(
    exchange: str,
    triplet: Triplet,
    books: dict[str, BookSnapshot | None],
    *,
    fee_rates: FeeRates,
    lot_size: float,
    days_to_expiry: float,
    spot_price: float,
    stale_threshold_ms: int,
    index_for_fee: float,
    min_annualized_rate: float,
    future_inverse_usd_face: float | None = None,
) -> None:
    """Run forward+reverse evaluation when all three legs have snapshots (startup sweep)."""
    books_clean = {k: v for k, v in books.items() if v is not None}
    if len(books_clean) < 3:
        return
    for calc, direction in (
        (calculate_forward, "forward"),
        (calculate_reverse, "reverse"),
    ):
        sig = calc(
            triplet=triplet,
            books=books_clean,
            fee_rates=fee_rates,
            lot_size=lot_size,
            days_to_expiry=days_to_expiry,
            spot_price=spot_price,
            stale_threshold_ms=stale_threshold_ms,
            index_for_fee=index_for_fee,
            future_inverse_usd_face=future_inverse_usd_face,
        )
        emit_opportunity_evaluation(exchange, triplet, direction, sig, min_annualized_rate)


async def run_startup_revalidation_loop() -> None:
    """After all exchange WS monitors are ready, re-evaluate triplets once; fix stale restored actives."""
    if _dashboard_start_event is not None:
        await _dashboard_start_event.wait()
        await asyncio.sleep(1.75)
        for sweep in list(_startup_market_sweeps):
            try:
                await sweep()  # type: ignore[misc]
            except Exception as exc:
                logger.warning("[startup] market sweep failed: %s", exc)
        if _dash is not None:
            _dash.finalize_startup_revalidate_stale()
    elif _dash is not None:
        # 无启用 runner 时 barrier 未创建；仍可能从 DB 恢复了行，稍等后按库存价格重算手续费
        await asyncio.sleep(2.0)
    if _dash is not None and _cfg is not None:
        _dash.recalculate_stored_leg_fees(_cfg.lot_size)


async def run_opportunity_sqlite_loop() -> None:
    """Upsert current opportunity state to opportunity_current every 10 seconds."""
    if _cfg is None or not _cfg.sqlite_path:
        return
    from pcp_arbitrage.db import init_db, upsert_opportunity_current

    init_db(_cfg.sqlite_path)
    while True:
        await asyncio.sleep(10)
        if _dash is not None and _cfg is not None and _cfg.sqlite_path:
            try:
                upsert_opportunity_current(_cfg.sqlite_path, list(_dash._rows.values()))
            except Exception as e:
                logger.warning("[db] upsert_opportunity_current failed: %s", e)


async def run_triplet_db_refresh_loop() -> None:
    """Re-upsert all exchange triplets at midnight local time."""
    if _cfg is None or not _cfg.sqlite_path:
        return
    from pcp_arbitrage.db import upsert_triplets

    while True:
        now = datetime.datetime.now()
        tomorrow = (now + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        await asyncio.sleep((tomorrow - now).total_seconds())
        if _dash is None or _cfg is None or not _cfg.sqlite_path:
            continue
        for exchange, (triplets, settle_type) in _dash.get_all_triplets().items():
            if not triplets:
                continue
            try:
                upsert_triplets(_cfg.sqlite_path, exchange, triplets, settle_type)
            except Exception as e:
                logger.warning("[db] midnight upsert_triplets failed for %s: %s", exchange, e)


async def run_opportunity_csv_loop() -> None:
    if _cfg is None or not _cfg.opportunity_csv_enabled:
        return
    interval = _cfg.opportunity_csv_interval_sec
    path = _cfg.opportunity_csv_path
    while True:
        await asyncio.sleep(interval)
        try:
            flush_opportunities_csv(path)
        except OSError as e:
            logger.warning("[tracing] CSV flush failed: %s", e)


async def run_dashboard_loop() -> None:
    if _dash is None or _mode != "dashboard":
        return
    if _dashboard_start_event is not None:
        await _dashboard_start_event.wait()
    from rich.console import Console
    from rich.live import Live

    console = Console()
    with Live(
        _dash.build_rich_table(),
        console=console,
        refresh_per_second=4,
        screen=True,
    ) as live:
        while True:
            await asyncio.sleep(0.25)
            live.update(_dash.build_rich_table())


async def run_web_dashboard_loop(host: str, port: int, width: int) -> None:
    if _dash is None:
        return
    from pcp_arbitrage.web_dashboard import run_web_dashboard_loop as _impl

    await _impl(_dash, host, port, width, _dashboard_start_event)
