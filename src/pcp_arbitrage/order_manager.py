"""
Three-leg limit order entry for PCP arbitrage positions (OKX only, Phase 2).
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import TYPE_CHECKING

import aiohttp

from pcp_arbitrage import db as _db
from pcp_arbitrage import notifier as _notifier
from pcp_arbitrage.okx_client import _sign, _timestamp

if TYPE_CHECKING:
    from pcp_arbitrage.config import AppConfig
    from pcp_arbitrage.models import Triplet
    from pcp_arbitrage.pcp_calculator import ArbitrageSignal

logger = logging.getLogger(__name__)

_OKX_REST_BASE = "https://www.okx.com"
_POLL_INTERVAL_SEC = 2.0
_POLL_TIMEOUT_SEC = 30.0
_SUBMIT_TIMEOUT_SEC = 30.0


def _auth_headers(
    api_key: str,
    secret: str,
    passphrase: str,
    method: str,
    path: str,
    body: str = "",
    is_paper: bool = False,
) -> dict[str, str]:
    ts = _timestamp()
    sig = _sign(secret, ts, method, path, body)
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sig,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    if is_paper:
        headers["x-simulated-trading"] = "1"
    return headers


async def _place_order(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    td_mode: str,
    side: str,
    ord_type: str,
    px: float,
    sz: float,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
) -> str:
    """Place a single limit order; returns the exchange order ID string."""
    path = "/api/v5/trade/order"
    payload = {
        "instId": inst_id,
        "tdMode": td_mode,
        "side": side,
        "ordType": ord_type,
        "px": str(px),
        "sz": str(sz),
    }
    body = json.dumps(payload)
    headers = _auth_headers(api_key, secret, passphrase, "POST", path, body, is_paper)
    async with session.post(path, data=body, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
    orders = data.get("data", [])
    if not orders:
        raise RuntimeError(f"Empty data in order response: {data}")
    ord_id = orders[0].get("ordId", "")
    if not ord_id:
        raise RuntimeError(f"No ordId in order response: {data}")
    return str(ord_id)


async def _poll_order_fill(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    ord_id: str,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
    poll_interval: float = _POLL_INTERVAL_SEC,
    poll_timeout: float = _POLL_TIMEOUT_SEC,
) -> dict | None:
    """Poll until order is filled or timeout. Returns order data dict or None on timeout."""
    deadline = asyncio.get_event_loop().time() + poll_timeout
    params_str = f"instId={inst_id}&ordId={ord_id}"
    path = f"/api/v5/trade/order?{params_str}"
    while asyncio.get_event_loop().time() < deadline:
        headers = _auth_headers(api_key, secret, passphrase, "GET", path, "", is_paper)
        async with session.get(
            "/api/v5/trade/order",
            params={"instId": inst_id, "ordId": ord_id},
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        orders = data.get("data", [])
        if orders:
            order_data = orders[0]
            state = order_data.get("state", "")
            if state == "filled":
                return order_data
            if state in ("canceled", "partially_filled"):
                return None
        await asyncio.sleep(poll_interval)
    return None


async def _cancel_order(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    ord_id: str,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
) -> None:
    """Cancel an order. Errors are logged but not raised."""
    path = "/api/v5/trade/cancel-order"
    payload = {"instId": inst_id, "ordId": ord_id}
    body = json.dumps(payload)
    headers = _auth_headers(api_key, secret, passphrase, "POST", path, body, is_paper)
    try:
        async with session.post(path, data=body, headers=headers) as resp:
            resp.raise_for_status()
    except Exception as exc:
        logger.warning("cancel_order failed inst=%s ord=%s: %s", inst_id, ord_id, exc)


async def submit_entry(
    triplet: "Triplet",
    signal: "ArbitrageSignal",
    signal_id: int | None,
    cfg: "AppConfig",
    sqlite_path: str,
) -> tuple[bool, str]:
    """Submit a three-leg entry for a PCP arbitrage signal.

    1. Guard against duplicate open positions.
    2. Create DB records.
    3. Submit 3 limit orders in parallel (OKX only).
    4. Poll for fill status.
    5. Update DB and send Telegram notification.
    Never raises — all exceptions are caught and logged.
    Returns (ok, message).
    """
    try:
        msg = await _submit_entry_inner(triplet, signal, signal_id, cfg, sqlite_path)
        return (True, msg or "下单成功")
    except Exception as exc:
        logger.exception("submit_entry failed: %s", exc)
        return (False, str(exc))


async def _submit_entry_inner(
    triplet: "Triplet",
    signal: "ArbitrageSignal",
    signal_id: int | None,
    cfg: "AppConfig",
    sqlite_path: str,
) -> str:
    exchange_name = triplet.exchange if hasattr(triplet, "exchange") else "OKX"
    exc_cfg = cfg.exchanges.get(exchange_name.upper()) or cfg.exchanges.get(exchange_name.lower())
    if exc_cfg is None:
        raise RuntimeError(f"{exchange_name} 未配置")

    # --- Guard: check for existing open position ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        if _db.has_open_position(
            conn,
            exchange_name,
            triplet.symbol,
            triplet.expiry,
            triplet.strike,
            signal.direction,
        ):
            logger.info(
                "submit_entry: open position already exists for %s %s %s %s, skipping",
                exchange_name,
                triplet.symbol,
                triplet.expiry,
                signal.direction,
            )
            raise RuntimeError("已有持仓")

        # --- Create position record ---
        with conn:
            position_id = _db.create_position(
                conn,
                signal_id=signal_id,
                exchange=exchange_name,
                symbol=triplet.symbol,
                expiry=triplet.expiry,
                strike=triplet.strike,
                direction=signal.direction,
            )
    finally:
        conn.close()

    qty = signal.tradeable_qty

    # Determine order specs based on direction
    # forward: buy call, sell put, sell future
    # reverse: sell call, buy put, buy future
    if signal.direction == "forward":
        legs = [
            {"leg": "call", "inst_id": triplet.call_id, "side": "buy", "px": signal.call_price_coin},
            {"leg": "put", "inst_id": triplet.put_id, "side": "sell", "px": signal.put_price_coin},
            {"leg": "future", "inst_id": triplet.future_id, "side": "sell", "px": signal.future_price},
        ]
    else:
        legs = [
            {"leg": "call", "inst_id": triplet.call_id, "side": "sell", "px": signal.call_price_coin},
            {"leg": "put", "inst_id": triplet.put_id, "side": "buy", "px": signal.put_price_coin},
            {"leg": "future", "inst_id": triplet.future_id, "side": "buy", "px": signal.future_price},
        ]

    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key
    passphrase = exc_cfg.passphrase
    is_paper = exc_cfg.is_paper_trading

    # Create DB order records (pending)
    conn = sqlite3.connect(sqlite_path)
    order_ids_db: list[int] = []
    try:
        with conn:
            for leg_spec in legs:
                oid = _db.create_order(
                    conn,
                    position_id=position_id,
                    leg=leg_spec["leg"],
                    order_type="limit",
                    side=leg_spec["side"],
                    limit_px=leg_spec["px"],
                    qty=qty,
                )
                order_ids_db.append(oid)
    finally:
        conn.close()

    # --- Submit all 3 orders in parallel ---
    exch_order_ids: list[str | None] = [None] * 3
    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[
                        _place_order(
                            session,
                            inst_id=leg["inst_id"],
                            td_mode="cash",
                            side=leg["side"],
                            ord_type="limit",
                            px=leg["px"],
                            sz=qty,
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            is_paper=is_paper,
                        )
                        for leg in legs
                    ],
                    return_exceptions=True,
                ),
                timeout=_SUBMIT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            logger.error("submit_entry: order submission timed out for position %d", position_id)
            await _mark_partial_failed(
                sqlite_path, position_id, order_ids_db, cfg, exchange_name, triplet, legs, session,
                api_key, secret, passphrase, is_paper, exch_order_ids
            )
            raise RuntimeError("下单超时，已取消")

        # Record exchange order IDs and check for errors
        submit_errors: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                submit_errors.append(f"leg {legs[i]['leg']}: {result}")
            else:
                exch_order_ids[i] = result

        if submit_errors:
            logger.error("submit_entry: order submission errors: %s", submit_errors)
            # Update DB with exchange IDs we got, cancel any that were submitted
            conn = sqlite3.connect(sqlite_path)
            try:
                with conn:
                    for i, oid_db in enumerate(order_ids_db):
                        if exch_order_ids[i]:
                            conn.execute(
                                "UPDATE orders SET exchange_order_id=? WHERE id=?",
                                (exch_order_ids[i], oid_db),
                            )
            finally:
                conn.close()
            await _mark_partial_failed(
                sqlite_path, position_id, order_ids_db, cfg, exchange_name, triplet, legs, session,
                api_key, secret, passphrase, is_paper, exch_order_ids
            )
            raise RuntimeError("下单提交失败: " + "; ".join(submit_errors))

        # Store exchange order IDs in DB
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid_db in enumerate(order_ids_db):
                    conn.execute(
                        "UPDATE orders SET exchange_order_id=? WHERE id=?",
                        (exch_order_ids[i], oid_db),
                    )
        finally:
            conn.close()

        # --- Poll for fills ---
        fill_results = await asyncio.gather(
            *[
                _poll_order_fill(
                    session,
                    inst_id=leg["inst_id"],
                    ord_id=exch_order_ids[i],  # type: ignore[arg-type]
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    is_paper=is_paper,
                )
                for i, leg in enumerate(legs)
            ],
            return_exceptions=True,
        )

    # Evaluate fills
    all_filled = True
    filled_data: list[dict | None] = []
    for i, fr in enumerate(fill_results):
        if isinstance(fr, Exception) or fr is None:
            all_filled = False
            filled_data.append(None)
        else:
            filled_data.append(fr)

    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            if all_filled:
                for i, oid_db in enumerate(order_ids_db):
                    fd = filled_data[i]
                    _db.update_order_status(
                        conn,
                        oid_db,
                        "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0),  # type: ignore[union-attr]
                        filled_at=_db._utc_now_iso(),
                    )
                _db.update_position_status(conn, position_id, "open")
            else:
                # Mark unfilled orders as failed; cancel them at exchange
                for i, oid_db in enumerate(order_ids_db):
                    fd = filled_data[i]
                    if fd is None:
                        _db.update_order_status(conn, oid_db, "failed")
                    else:
                        _db.update_order_status(
                            conn,
                            oid_db,
                            "filled",
                            filled_px=float(fd.get("avgPx") or fd.get("px") or 0),
                            filled_at=_db._utc_now_iso(),
                        )
                _db.update_position_status(conn, position_id, "partial_failed")
    finally:
        conn.close()

    # Cancel unfilled orders at exchange (best effort)
    if not all_filled:
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session2:
            for i, fr in enumerate(fill_results):
                if (isinstance(fr, Exception) or fr is None) and exch_order_ids[i]:
                    await _cancel_order(
                        session2,
                        inst_id=legs[i]["inst_id"],
                        ord_id=exch_order_ids[i],  # type: ignore[arg-type]
                        api_key=api_key,
                        secret=secret,
                        passphrase=passphrase,
                        is_paper=is_paper,
                    )

    # Send Telegram notification
    label = f"{triplet.symbol}-{triplet.expiry}-{int(triplet.strike)}"
    if all_filled:
        msg = (
            f"\u2705 PCP套利下单成功\n"
            f"交易所: {exchange_name}  标的: {label}  方向: {signal.direction}\n"
            f"数量: {qty}  仓位ID: {position_id}"
        )
    else:
        msg = (
            f"\u274c PCP套利下单失败 (partial/timeout)\n"
            f"交易所: {exchange_name}  标的: {label}  方向: {signal.direction}\n"
            f"数量: {qty}  仓位ID: {position_id}"
        )
    try:
        await _notifier.send_telegram(cfg.telegram, msg)
    except Exception as exc:
        logger.warning("submit_entry: telegram notification failed: %s", exc)

    if not all_filled:
        raise RuntimeError(f"部分腿未成交 (partial_failed)，仓位 ID: {position_id}")
    return f"下单成功，仓位 ID: {position_id}，数量: {qty}"


async def _mark_partial_failed(
    sqlite_path: str,
    position_id: int,
    order_ids_db: list[int],
    cfg: "AppConfig",
    exchange_name: str,
    triplet: "Triplet",
    legs: list[dict],
    session: aiohttp.ClientSession,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
    exch_order_ids: list[str | None],
) -> None:
    """Mark position as partial_failed and cancel any submitted orders."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for oid_db in order_ids_db:
                _db.update_order_status(conn, oid_db, "failed")
            _db.update_position_status(conn, position_id, "partial_failed")
    finally:
        conn.close()

    for i, exch_oid in enumerate(exch_order_ids):
        if exch_oid:
            await _cancel_order(
                session,
                inst_id=legs[i]["inst_id"],
                ord_id=exch_oid,
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
                is_paper=is_paper,
            )


async def _fetch_order_book_top(
    session: aiohttp.ClientSession,
    inst_id: str,
    *,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
) -> tuple[float | None, float | None]:
    """Fetch top-of-book (best_bid, best_ask) from OKX.

    Returns (bid, ask) or (None, None) on failure.
    """
    path = "/api/v5/market/books"
    params = {"instId": inst_id, "sz": "1"}
    params_str = f"instId={inst_id}&sz=1"
    full_path = f"{path}?{params_str}"
    headers = _auth_headers(api_key, secret, passphrase, "GET", full_path, "", is_paper)
    try:
        async with session.get(path, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
        books = data.get("data", [])
        if not books:
            return None, None
        book = books[0]
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        bid = float(bids[0][0]) if bids else None
        ask = float(asks[0][0]) if asks else None
        return bid, ask
    except Exception as exc:
        logger.warning("_fetch_order_book_top failed inst=%s: %s", inst_id, exc)
        return None, None


async def submit_exit(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> None:
    """Submit reverse 3-leg orders to close an open position. Never raises."""
    try:
        await _submit_exit_inner(position, cfg, sqlite_path)
    except Exception as exc:
        logger.exception("submit_exit failed pos=%s: %s", position.get("id"), exc)


async def _submit_exit_inner(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> None:
    exchange_name = "OKX"
    exc_cfg = cfg.exchanges.get(exchange_name)
    if exc_cfg is None:
        logger.warning("submit_exit: OKX exchange not configured, skipping")
        return

    position_id = position["id"]
    symbol = position["symbol"]
    expiry = position["expiry"]
    strike = position["strike"]
    direction = position["direction"]

    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key
    passphrase = exc_cfg.passphrase
    is_paper = exc_cfg.is_paper_trading

    # --- Fetch entry orders from DB ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders = _db.get_position_orders(conn, position_id, order_type="entry")
    finally:
        conn.close()

    # Build inst_id map from entry orders by leg
    leg_map: dict[str, dict] = {o["leg"]: o for o in entry_orders}

    # --- Fetch current order book tops for exit pricing ---
    # Reconstruct inst IDs from position fields
    exp_clean = expiry.replace("-", "")
    if len(exp_clean) == 8:
        exp_clean = exp_clean[2:]
    call_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-C"
    put_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-P"
    future_inst_id = f"{symbol}-USD-{exp_clean}"

    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        # Fetch order book tops for all 3 legs in parallel
        book_results = await asyncio.gather(
            _fetch_order_book_top(session, call_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase, is_paper=is_paper),
            _fetch_order_book_top(session, put_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase, is_paper=is_paper),
            _fetch_order_book_top(session, future_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase, is_paper=is_paper),
            return_exceptions=True,
        )

        # Unpack results; fall back to entry limit_px on error
        def _safe_book(result, leg_name: str, use_bid: bool) -> float:
            if isinstance(result, Exception) or result == (None, None):
                fallback = leg_map.get(leg_name, {}).get("limit_px", 0.0) or 0.0
                logger.warning("submit_exit: order book fetch failed for %s, using entry px %s", leg_name, fallback)
                return fallback
            bid, ask = result
            px = bid if use_bid else ask
            if px is None:
                fallback = leg_map.get(leg_name, {}).get("limit_px", 0.0) or 0.0
                logger.warning("submit_exit: no %s px for %s, using entry px %s",
                               "bid" if use_bid else "ask", leg_name, fallback)
                return fallback
            return px

        # Exit logic (reverse of entry):
        # forward entry: BUY call, SELL put, SELL future
        #   → exit: SELL call at bid, BUY put at ask, BUY future at ask
        # reverse entry: SELL call, BUY put, BUY future
        #   → exit: BUY call at ask, SELL put at bid, SELL future at bid
        if direction == "forward":
            call_px = _safe_book(book_results[0], "call", use_bid=True)   # sell at bid
            put_px = _safe_book(book_results[1], "put", use_bid=False)    # buy at ask
            future_px = _safe_book(book_results[2], "future", use_bid=False)  # buy at ask
            exit_legs = [
                {"leg": "call", "inst_id": call_inst_id, "side": "sell", "px": call_px},
                {"leg": "put", "inst_id": put_inst_id, "side": "buy", "px": put_px},
                {"leg": "future", "inst_id": future_inst_id, "side": "buy", "px": future_px},
            ]
        else:
            call_px = _safe_book(book_results[0], "call", use_bid=False)  # buy at ask
            put_px = _safe_book(book_results[1], "put", use_bid=True)     # sell at bid
            future_px = _safe_book(book_results[2], "future", use_bid=True)  # sell at bid
            exit_legs = [
                {"leg": "call", "inst_id": call_inst_id, "side": "buy", "px": call_px},
                {"leg": "put", "inst_id": put_inst_id, "side": "sell", "px": put_px},
                {"leg": "future", "inst_id": future_inst_id, "side": "sell", "px": future_px},
            ]

        # Determine qty from entry orders (use first entry order qty, fallback 1)
        qty = float(next(iter(entry_orders), {}).get("qty", 1.0) or 1.0)

        # Create DB exit order records
        conn = sqlite3.connect(sqlite_path)
        exit_order_ids_db: list[int] = []
        try:
            with conn:
                for leg_spec in exit_legs:
                    oid = _db.create_order(
                        conn,
                        position_id=position_id,
                        leg=leg_spec["leg"],
                        order_type="exit",
                        side=leg_spec["side"],
                        limit_px=leg_spec["px"],
                        qty=qty,
                    )
                    exit_order_ids_db.append(oid)
        finally:
            conn.close()

        # --- Submit all 3 exit orders in parallel ---
        exch_order_ids: list[str | None] = [None] * 3
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[
                        _place_order(
                            session,
                            inst_id=leg["inst_id"],
                            td_mode="cash",
                            side=leg["side"],
                            ord_type="limit",
                            px=leg["px"],
                            sz=qty,
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            is_paper=is_paper,
                        )
                        for leg in exit_legs
                    ],
                    return_exceptions=True,
                ),
                timeout=_SUBMIT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            reason = "exit order submission timed out"
            logger.error("submit_exit: %s for position %d", reason, position_id)
            _update_exit_failed(sqlite_path, position_id, exit_order_ids_db)
            await _send_exit_failure_telegram(cfg, position, reason)
            return

        submit_errors: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                submit_errors.append(f"leg {exit_legs[i]['leg']}: {result}")
            else:
                exch_order_ids[i] = result

        if submit_errors:
            reason = f"order submission errors: {'; '.join(submit_errors)}"
            logger.error("submit_exit: %s", reason)
            _update_exit_failed(sqlite_path, position_id, exit_order_ids_db)
            await _send_exit_failure_telegram(cfg, position, reason)
            return

        # Store exchange order IDs
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid_db in enumerate(exit_order_ids_db):
                    conn.execute(
                        "UPDATE orders SET exchange_order_id=? WHERE id=?",
                        (exch_order_ids[i], oid_db),
                    )
        finally:
            conn.close()

        # --- Poll for fills ---
        fill_results = await asyncio.gather(
            *[
                _poll_order_fill(
                    session,
                    inst_id=exit_legs[i]["inst_id"],
                    ord_id=exch_order_ids[i],  # type: ignore[arg-type]
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    is_paper=is_paper,
                )
                for i in range(3)
            ],
            return_exceptions=True,
        )

    # Evaluate fills
    all_filled = True
    filled_data: list[dict | None] = []
    for fr in fill_results:
        if isinstance(fr, Exception) or fr is None:
            all_filled = False
            filled_data.append(None)
        else:
            filled_data.append(fr)

    if all_filled:
        # Calculate realized PnL
        # For each leg: (exit_filled_px - entry_limit_px) * qty with direction sign
        # forward: entry was buy call (+), sell put (-), sell future (-)
        #          exit is sell call (-), buy put (+), buy future (+)
        # net PnL = (entry_call_px - exit_call_px) + (exit_put_px - entry_put_px)
        #           + (exit_future_px - entry_future_px)  [all in coin terms]
        # We use filled prices where available
        realized_pnl = 0.0
        for i, leg_spec in enumerate(exit_legs):
            fd = filled_data[i]
            exit_px = float(fd.get("avgPx") or fd.get("px") or leg_spec["px"]) if fd else leg_spec["px"]  # type: ignore[union-attr]
            entry_order = leg_map.get(leg_spec["leg"])
            entry_px = float(entry_order.get("filled_px") or entry_order.get("limit_px") or 0.0) if entry_order else 0.0
            # Sign: exit sell = -qty, exit buy = +qty (from perspective of PnL)
            # forward exit: sell call (we previously bought), sell price > buy price = gain
            #   PnL component = (exit_px - entry_px) * qty  with appropriate sign
            if leg_spec["side"] == "sell":
                realized_pnl += (exit_px - entry_px) * qty
            else:
                realized_pnl += (entry_px - exit_px) * qty

        # Update DB orders as filled
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid_db in enumerate(exit_order_ids_db):
                    fd = filled_data[i]
                    _db.update_order_status(
                        conn,
                        oid_db,
                        "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0) if fd else 0.0,  # type: ignore[union-attr]
                        filled_at=_db._utc_now_iso(),
                    )
                _db.update_position_status(
                    conn,
                    position_id,
                    "closed",
                    realized_pnl_usdt=realized_pnl,
                    closed_at=_db._utc_now_iso(),
                )
        finally:
            conn.close()

        # Send success Telegram
        label = f"{symbol} {expiry} {int(strike)} {direction}"
        msg = (
            f"\U0001f4b0 止盈出场成功\n"
            f"{label}\n"
            f"实现盈亏: {realized_pnl:+.2f} USDT"
        )
        try:
            await _notifier.send_telegram(cfg.telegram, msg)
        except Exception as exc:
            logger.warning("submit_exit: telegram notification failed: %s", exc)
    else:
        reason = "one or more exit orders did not fill within timeout"
        _update_exit_failed(sqlite_path, position_id, exit_order_ids_db)
        await _send_exit_failure_telegram(cfg, position, reason)
        # Cancel unfilled exit orders at exchange
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session2:
            for i, fr in enumerate(fill_results):
                if (isinstance(fr, Exception) or fr is None) and exch_order_ids[i]:
                    await _cancel_order(
                        session2,
                        inst_id=exit_legs[i]["inst_id"],
                        ord_id=exch_order_ids[i],  # type: ignore[arg-type]
                        api_key=api_key,
                        secret=secret,
                        passphrase=passphrase,
                        is_paper=is_paper,
                    )


def _update_exit_failed(
    sqlite_path: str,
    position_id: int,
    exit_order_ids_db: list[int],
) -> None:
    """Mark exit orders as failed and position as partial_failed."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for oid_db in exit_order_ids_db:
                _db.update_order_status(conn, oid_db, "failed")
            _db.update_position_status(conn, position_id, "partial_failed")
    finally:
        conn.close()


async def _send_exit_failure_telegram(
    cfg: "AppConfig",
    position: dict,
    reason: str,
) -> None:
    """Send a Telegram notification about exit failure."""
    symbol = position.get("symbol", "")
    expiry = position.get("expiry", "")
    strike = position.get("strike", "")
    direction = position.get("direction", "")
    try:
        strike_int = int(strike)
    except (TypeError, ValueError):
        strike_int = strike
    label = f"{symbol} {expiry} {strike_int} {direction}"
    msg = (
        f"\u26a0\ufe0f 出场失败\n"
        f"{label}\n"
        f"原因: {reason}"
    )
    try:
        await _notifier.send_telegram(cfg.telegram, msg)
    except Exception as exc:
        logger.warning("submit_exit: telegram failure notification failed: %s", exc)
