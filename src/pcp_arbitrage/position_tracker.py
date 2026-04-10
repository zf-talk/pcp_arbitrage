"""
Polls open positions from DB and OKX REST, tracks P&L, sends Telegram alerts on large moves.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import sqlite3

import aiohttp

from pcp_arbitrage.config import AppConfig
from pcp_arbitrage.notifier import send_telegram

logger = logging.getLogger(__name__)

# Module-level cache updated after each poll; consumed by web_dashboard
_open_positions_cache: list[dict] = []


def update_positions_cache(positions: list[dict]) -> None:
    """Update the module-level positions cache (called by _check_positions after each poll)."""
    global _open_positions_cache
    _open_positions_cache = list(positions)


def get_positions_cache() -> list[dict]:
    """Return a copy of the current positions cache."""
    return list(_open_positions_cache)


async def run_position_tracker_loop(cfg: AppConfig) -> None:
    """Poll open positions every 30 seconds. Run as asyncio task."""
    while True:
        await asyncio.sleep(30)
        await _check_positions(cfg)


async def _fetch_mark_price(
    session: aiohttp.ClientSession,
    inst_id: str,
    proxy: str | None = None,
) -> float | None:
    """Fetch mark price from OKX public endpoint. Returns None on any error."""
    url = "https://www.okx.com/api/v5/market/mark-price"
    params = {"instId": inst_id}
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
            return float(items[0].get("markPx", 0) or 0) or None
    except Exception as exc:
        logger.debug("[position_tracker] mark-price error for %s: %s", inst_id, exc)
        return None


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
    """Estimate floating profit percentage using current_mark_usdt vs entry.

    Uses mark price change as a proxy for position P&L.  Returns 0.0 when data
    is unavailable.
    """
    current_mark = pos.get("current_mark_usdt")
    if not current_mark or current_mark <= 0:
        return 0.0
    # We use the position's limit_px from its first entry order as entry reference,
    # but since we only have the mark price here we compare current vs a baseline.
    # As a heuristic: if the position has a stored "entry" mark we compare against it.
    # For now we use realized_pnl / abs(current_mark) if available, else 0.
    realized = pos.get("realized_pnl_usdt")
    if realized is not None and current_mark > 0:
        return realized / current_mark
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

    if not positions:
        update_positions_cache([])
        return

    updated_positions: list[dict] = []
    now_iso = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    async with aiohttp.ClientSession() as session:
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
                    finally:
                        conn3.close()
                    pos_dict["status"] = "closing"

                    from pcp_arbitrage import order_manager
                    asyncio.create_task(
                        order_manager.submit_exit(pos_dict, cfg, cfg.sqlite_path)
                    )

    update_positions_cache(updated_positions)
    # Also update the web dashboard positions cache if the module is available
    try:
        from pcp_arbitrage import web_dashboard as _wd
        _wd.update_positions_cache(updated_positions)
    except Exception:
        pass
    logger.debug("[position_tracker] polled %d open positions", len(updated_positions))
