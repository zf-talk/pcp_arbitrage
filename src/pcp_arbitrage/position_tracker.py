"""
Polls open positions from DB and OKX REST, tracks P&L, sends Telegram alerts on large moves.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import sqlite3
import time

import aiohttp

from pcp_arbitrage.config import AppConfig
from pcp_arbitrage.db import _utc_now_iso
from pcp_arbitrage.notifier import send_telegram

logger = logging.getLogger(__name__)

# Module-level cache updated after each poll; consumed by web_dashboard
_open_positions_cache: list[dict] = []

# Per-position per-leg live prices: {pos_id: {"call": {"bid1": x, "ask1": x}, ...}}
_leg_prices_cache: dict[int, dict] = {}

# Per-symbol last seen mark and periodic log throttle state.
_symbol_last_mark_cache: dict[str, float] = {}
_symbol_price_log_last_ts: float = 0.0
_SYMBOL_PRICE_LOG_INTERVAL_SEC: float = 180.0


def update_positions_cache(positions: list[dict]) -> None:
    """Update the module-level positions cache (called by _check_positions after each poll)."""
    global _open_positions_cache
    _open_positions_cache = list(positions)


def get_positions_cache() -> list[dict]:
    """Return a copy of the current positions cache."""
    return list(_open_positions_cache)


def get_leg_prices_cache() -> dict:
    """Return a copy of the per-position leg prices cache (bid1/ask1)."""
    return dict(_leg_prices_cache)


def _log_symbol_price_changes_if_due(symbol_latest_marks: dict[str, float]) -> None:
    """Log per-symbol mark price change every fixed interval."""
    global _symbol_price_log_last_ts

    if not symbol_latest_marks:
        return

    now_ts = time.time()
    if _symbol_price_log_last_ts > 0 and (now_ts - _symbol_price_log_last_ts) < _SYMBOL_PRICE_LOG_INTERVAL_SEC:
        for symbol, latest in symbol_latest_marks.items():
            _symbol_last_mark_cache[symbol] = latest
        return

    parts: list[str] = []
    for symbol in sorted(symbol_latest_marks):
        latest = symbol_latest_marks[symbol]
        prev = _symbol_last_mark_cache.get(symbol)
        if prev is not None and prev > 0:
            diff = latest - prev
            pct = (diff / prev) * 100.0
            parts.append(f"{symbol}: {latest:.4f} ({diff:+.4f}, {pct:+.2f}%)")
        else:
            parts.append(f"{symbol}: {latest:.4f} (baseline)")
        _symbol_last_mark_cache[symbol] = latest

    logger.info("[position_tracker] 币种价格变化(3m): %s", " | ".join(parts))
    _symbol_price_log_last_ts = now_ts


def push_leg_price_ws(pos_id: int, leg: str, update: dict) -> None:
    """Update _leg_prices_cache for a single leg and push to web dashboard WS clients."""
    if pos_id not in _leg_prices_cache:
        _leg_prices_cache[pos_id] = {}
    existing = _leg_prices_cache[pos_id].get(leg, {})
    existing.update(update)
    _leg_prices_cache[pos_id][leg] = existing
    # Notify web dashboard to push a leg_prices patch to WS clients
    try:
        from pcp_arbitrage import web_dashboard as _wd
        _wd.push_leg_prices_patch({pos_id: {leg: dict(existing)}})
    except Exception:
        pass


async def run_position_tracker_loop(cfg: AppConfig) -> None:
    """Poll open positions every 30 seconds. Run as asyncio task."""
    while True:
        await asyncio.sleep(30)
        await _check_positions(cfg)


async def exit_monitor_loop(cfg: "AppConfig") -> None:
    """
    后台守护：每 exit_monitor_interval_secs 秒扫描一次 partial_failed 仓位，
    对不在 _exit_active 集合中的仓位重新发起升级平仓流程。
    """
    from pcp_arbitrage import order_manager as _om
    interval = cfg.exit_monitor_interval_secs  # 默认 10s
    while True:
        await asyncio.sleep(interval)
        try:
            with sqlite3.connect(cfg.sqlite_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM positions "
                    "WHERE COALESCE(target_state,'open')='closed' AND status!='closed'"
                ).fetchall()
            for row in rows:
                pid = row["id"]
                if pid in _om._exit_active:
                    continue  # 已有内联循环在处理

                # 检查是否有任何入场单实际成交；若无则说明交易所未建仓，无需平仓
                from pcp_arbitrage import db as _db
                with sqlite3.connect(cfg.sqlite_path) as _chk_conn:
                    filled_entry_count = _chk_conn.execute(
                        "SELECT COUNT(*) FROM orders "
                        "WHERE position_id=? AND action='open' AND status='filled'",
                        (pid,),
                    ).fetchone()[0]
                if filled_entry_count == 0:
                    logger.warning(
                        "[exit_monitor] 仓位 %d 入场单全部未成交，交易所无实际仓位，"
                        "标记为 failed 跳过平仓",
                        pid,
                    )
                    with sqlite3.connect(cfg.sqlite_path) as _upd_conn:
                        for o in _upd_conn.execute(
                            "SELECT id, last_error FROM orders "
                            "WHERE position_id=? AND action='open'",
                            (pid,),
                        ):
                            oid = int(o[0])
                            existing = o[1]
                            if existing and str(existing).strip():
                                continue
                            _db.set_order_last_error(
                                _upd_conn, oid, "入场单全部未成交，无需平仓",
                            )
                        _db.update_position_status(_upd_conn, pid, "failed")
                    continue

                pos_dict = dict(row)
                if str(pos_dict.get("exchange", "")).lower() == "okx":
                    allowed = await _om.okx_exit_allowed_by_exchange(
                        cfg.sqlite_path, pos_dict, cfg,
                    )
                    if not allowed:
                        logger.debug(
                            "[exit_monitor] 跳过仓位 %d：OKX 侧无与数据库一致的三腿持仓",
                            pid,
                        )
                        continue

                logger.info("[exit_monitor] 接管目标为 closed 的仓位 %d", pid)
                _om._exit_active.add(pid)

                async def _run_exit(pos_dict: dict, _pid: int = pid) -> None:
                    try:
                        await _om.submit_exit(pos_dict, cfg, cfg.sqlite_path)
                    finally:
                        # 无论成功/失败/异常，都清除锁，让下一轮扫描可以重新接管
                        _om._exit_active.discard(_pid)

                asyncio.create_task(
                    _run_exit(dict(row)),
                    name=f"exit_monitor_{pid}",
                )
        except Exception as exc:
            logger.warning("[exit_monitor] 扫描异常: %s", exc)


async def _fetch_mark_price(
    session: aiohttp.ClientSession,
    inst_id: str,
    proxy: str | None = None,
) -> float | None:
    """Fetch mark price from OKX public endpoint. Returns None on any error."""
    url = "https://www.okx.com/api/v5/market/mark-price"
    # OKX requires instType to correctly query options vs futures
    if inst_id.endswith("-C") or inst_id.endswith("-P"):
        inst_type = "OPTION"
    else:
        inst_type = "FUTURES"
    params = {"instId": inst_id, "instType": inst_type}
    try:
        async with session.get(
            url,
            params=params,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.debug("[position_tracker] mark-price HTTP %d for %s", resp.status, inst_id)
                return None
            data = await resp.json()
            items = data.get("data", [])
            if not items:
                return None
            val = float(items[0].get("markPx", 0) or 0)
            return val if val > 0 else None
    except Exception as exc:
        logger.debug("[position_tracker] mark-price error for %s: %s", inst_id, exc)
        return None


async def _fetch_ticker(
    session: aiohttp.ClientSession,
    inst_id: str,
    proxy: str | None = None,
) -> dict | None:
    """Fetch bid1/ask1/bid_sz/ask_sz from OKX ticker endpoint."""
    url = "https://www.okx.com/api/v5/market/ticker"
    params = {"instId": inst_id}
    try:
        async with session.get(
            url,
            params=params,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.debug("[position_tracker] ticker HTTP %d for %s", resp.status, inst_id)
                return None
            data = await resp.json()
            items = data.get("data", [])
            if not items:
                return None
            item = items[0]
            bid1 = float(item.get("bidPx") or 0) or None
            ask1 = float(item.get("askPx") or 0) or None
            if bid1 is None and ask1 is None:
                return None
            bid_sz = float(item.get("bidSz") or 0) or None
            ask_sz = float(item.get("askSz") or 0) or None
            return {"bid1": bid1, "ask1": ask1, "bid_sz": bid_sz, "ask_sz": ask_sz}
    except Exception as exc:
        logger.debug("[position_tracker] ticker error for %s: %s", inst_id, exc)
        return None


async def _fetch_and_cache_leg_prices_for_legs(
    session: aiohttp.ClientSession,
    pos_id: int,
    leg_map: dict[str, str],
    *,
    proxy: str | None,
) -> None:
    """Fetch bid1/ask1/mark for legs (inst_id from entry orders) into _leg_prices_cache."""
    if not leg_map:
        return
    active_legs = [(leg, iid) for leg, iid in leg_map.items() if iid]
    ticker_coros = [_fetch_ticker(session, iid, proxy=proxy) for _, iid in active_legs]
    mark_coros = [_fetch_mark_price(session, iid, proxy=proxy) for _, iid in active_legs]
    all_results = await asyncio.gather(*ticker_coros, *mark_coros, return_exceptions=True)
    n = len(active_legs)
    ticker_results = all_results[:n]
    mark_results = all_results[n:]
    pos_prices: dict[str, dict] = {}
    for (leg, _), ticker, mark_px in zip(active_legs, ticker_results, mark_results):
        entry: dict = {}
        if isinstance(ticker, dict):
            entry.update(ticker)
        if isinstance(mark_px, float):
            entry["mark_price"] = mark_px
        if entry:
            pos_prices[leg] = entry
    if pos_prices:
        _leg_prices_cache[pos_id] = pos_prices


def _build_future_inst_id(pos: dict) -> str:
    """Construct OKX future instId from position fields, e.g. BTC-USD-250328."""
    symbol = pos["symbol"]
    expiry = pos["expiry"]  # e.g. "250328" or "2025-03-28"
    # Normalise expiry: strip dashes and take last 6 chars
    exp_clean = expiry.replace("-", "")
    if len(exp_clean) == 8:
        # YYYYMMDD -> YYMMDD
        exp_clean = exp_clean[2:]
    return f"{symbol}-USD-{exp_clean}"


def _days_to_expiry(expiry: str) -> float:
    """Parse expiry string ('250328' or '20250328') and return float days remaining."""
    exp_clean = expiry.replace("-", "").strip()
    try:
        if len(exp_clean) == 6:
            # YYMMDD: assume 2000s
            exp_date = datetime.date(2000 + int(exp_clean[:2]), int(exp_clean[2:4]), int(exp_clean[4:6]))
        elif len(exp_clean) == 8:
            exp_date = datetime.date(int(exp_clean[:4]), int(exp_clean[4:6]), int(exp_clean[6:8]))
        else:
            logger.warning("_days_to_expiry: unrecognised expiry format '%s'", expiry)
            return float("inf")
    except (ValueError, IndexError) as exc:
        logger.warning("_days_to_expiry: parse error for '%s': %s", expiry, exc)
        return float("inf")
    today = datetime.date.today()
    return (exp_date - today).days + (exp_date - today).seconds / 86400


def _calc_pnl_pct(pos: dict) -> float:
    """止盈比例判定占位：未实现盈亏% 未接入前返回 0。

    旧逻辑 realized_pnl_usdt/current_mark 非「收益率」，会误触自动平仓。
    """
    return 0.0


async def _check_positions(cfg: AppConfig) -> None:
    """Fetch mark prices for all open positions, update DB, alert on large P&L moves."""
    if not cfg.sqlite_path:
        return

    conn = sqlite3.connect(cfg.sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        from pcp_arbitrage.db import get_open_positions, update_position_mark

        positions = get_open_positions(conn)
    finally:
        conn.close()

    updated_positions: list[dict] = []
    symbol_latest_marks: dict[str, float] = {}
    now_iso = _utc_now_iso()

    if not positions:
        update_positions_cache([])

    async with aiohttp.ClientSession() as session:
        if positions:
            for pos in positions:
                inst_id = _build_future_inst_id(pos)
                mark_px = await _fetch_mark_price(session, inst_id, proxy=cfg.proxy)

                if mark_px is None:
                    updated_positions.append(dict(pos))
                    continue

                # Simplified P&L: change in future mark price * qty
                # We use current_mark_usdt as "previous" mark; first poll sets baseline
                prev_mark = pos.get("current_mark_usdt")

                # Update DB in a new connection (positions may be fetched again)
                conn2 = sqlite3.connect(cfg.sqlite_path)
                try:
                    with conn2:
                        from pcp_arbitrage.db import update_position_mark
                        update_position_mark(conn2, pos["id"], mark_px, now_iso)
                finally:
                    conn2.close()

                # Build updated dict
                pos_dict = dict(pos)
                pos_dict["current_mark_usdt"] = mark_px
                pos_dict["last_updated"] = now_iso
                updated_positions.append(pos_dict)
                symbol = str(pos.get("symbol") or "").upper()
                if symbol:
                    symbol_latest_marks[symbol] = mark_px

                # P&L alert: compare new vs previous mark
                if prev_mark is not None and prev_mark > 0:
                    pnl = mark_px - prev_mark  # simplified: +1 contract
                    pct = pnl / prev_mark
                    threshold = getattr(cfg, "pnl_alert_threshold_pct", 0.05)
                    if abs(pct) >= threshold:
                        symbol = pos["symbol"]
                        expiry = pos["expiry"]
                        strike = pos["strike"]
                        direction = pos["direction"]
                        pnl_usdt = pnl  # proxy (not scaled by qty here without order fill data)
                        msg = (
                            f"<b>持仓 P&amp;L 变动提醒</b>\n"
                            f"{symbol} {expiry} {strike} {direction}\n"
                            f"浮动盈亏: {pnl_usdt:+.2f} USDT ({pct * 100:+.1f}%)"
                        )
                        logger.info(
                            "[position_tracker] P&L alert: pos=%d pct=%.1f%%", pos["id"], pct * 100
                        )
                        await send_telegram(cfg.telegram, msg)

                # --- Check exit conditions ---
                if pos_dict.get("status") == "open":
                    days_left = _days_to_expiry(pos_dict["expiry"])
                    pnl_pct = _calc_pnl_pct(pos_dict)
                    exit_days = getattr(cfg, "exit_days_before_expiry", 1)
                    exit_pct = getattr(cfg, "exit_target_profit_pct", 0.50)

                    should_exit = (
                        days_left <= exit_days
                        or pnl_pct >= exit_pct
                    )

                    if should_exit:
                        logger.info(
                            "[position_tracker] exit triggered for pos=%d days_left=%.2f pnl_pct=%.2f",
                            pos_dict["id"], days_left, pnl_pct,
                        )
                        # Mark as 'closing' to prevent re-triggering
                        conn3 = sqlite3.connect(cfg.sqlite_path)
                        try:
                            with conn3:
                                from pcp_arbitrage.db import update_position_status
                                update_position_status(conn3, pos_dict["id"], "closing")
                                conn3.execute(
                                    "UPDATE positions SET target_state='closed' WHERE id=?",
                                    (pos_dict["id"],),
                                )
                        finally:
                            conn3.close()
                        pos_dict["status"] = "closing"

                        from pcp_arbitrage import order_manager
                        asyncio.create_task(
                            order_manager.submit_exit(pos_dict, cfg, cfg.sqlite_path)
                        )

        # 各腿盘口：由入场订单 status 决定拉哪些 inst（见 db.get_entry_leg_quotes_map），与仓位 status 无关
        try:
            from pcp_arbitrage.db import get_entry_leg_quotes_map

            conn_o = sqlite3.connect(cfg.sqlite_path)
            try:
                leg_maps = get_entry_leg_quotes_map(conn_o)
            finally:
                conn_o.close()
            for pos_id, leg_map in leg_maps.items():
                await _fetch_and_cache_leg_prices_for_legs(
                    session, pos_id, leg_map, proxy=cfg.proxy
                )
        except Exception as exc:
            logger.debug("[position_tracker] entry-order leg prices: %s", exc)

    update_positions_cache(updated_positions)
    _log_symbol_price_changes_if_due(symbol_latest_marks)
    # Also update the web dashboard positions cache if the module is available
    try:
        from pcp_arbitrage import web_dashboard as _wd
        _wd.update_positions_cache(updated_positions)
    except Exception:
        pass
    logger.debug("[position_tracker] polled %d open positions", len(updated_positions))
