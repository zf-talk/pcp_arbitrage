"""
Three-leg limit order entry for PCP arbitrage positions (OKX + Deribit, Phase 2).
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import TYPE_CHECKING

import math

import aiohttp

from pcp_arbitrage import db as _db
from pcp_arbitrage import notifier as _notifier
from pcp_arbitrage.okx_client import _sign, _timestamp

if TYPE_CHECKING:
    from pcp_arbitrage.config import AppConfig
    from pcp_arbitrage.exchanges.deribit import DeribitRestClient
    from pcp_arbitrage.models import Triplet
    from pcp_arbitrage.pcp_calculator import ArbitrageSignal

logger = logging.getLogger(__name__)

_OKX_REST_BASE = "https://www.okx.com"


def _entry_opportunity_tag(triplet: "Triplet", signal: "ArbitrageSignal") -> str:
    """Short label for logs/errors: exchange symbol-expiry-strike direction."""
    return (
        f"{triplet.exchange} {triplet.symbol}-{triplet.expiry}-{int(triplet.strike)} "
        f"{signal.direction}"
    )
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


_OKX_EXEC_EXCHANGES = {"okx"}
_SUPPORTED_EXEC_EXCHANGES = {"okx", "deribit"}


def _okx_fee_type(order_data: dict) -> str | None:
    """Infer maker/taker from OKX order fill response.

    OKX返回:
      rebate: 做市返佣（正数 → maker）
      fee:    手续费（负数 → taker）
    """
    try:
        if float(order_data.get("rebate") or 0) != 0:
            return "maker"
        if float(order_data.get("fee") or 0) < 0:
            return "taker"
    except (ValueError, TypeError):
        pass
    return None


def _okx_fee(order_data: dict) -> tuple[float | None, str | None]:
    """Return (actual_fee, fee_ccy) from OKX order fill response.

    OKX fee is negative (cost); rebate is positive. Net = fee + rebate.
    """
    try:
        fee = float(order_data.get("fee") or 0)
        rebate = float(order_data.get("rebate") or 0)
        net = fee + rebate          # typically negative or zero
        ccy = order_data.get("feeCcy") or order_data.get("rebateCcy") or None
        return (abs(net), ccy)
    except (ValueError, TypeError):
        return (None, None)


def _deribit_fee_type(trades: list[dict]) -> str | None:
    """Infer maker/taker from Deribit trade records.

    Deribit trade 的 liquidity 字段: 'M' = maker, 'T' = taker.
    任意一笔为 taker 则整体视为 taker（保守估计费用）。
    """
    if not trades:
        return None
    for t in trades:
        if t.get("liquidity") == "T":
            return "taker"
    return "maker"


def _deribit_fee(trades: list[dict]) -> tuple[float | None, str | None]:
    """Return (actual_fee, fee_currency) summed across all trades for one order."""
    if not trades:
        return (None, None)
    total = 0.0
    ccy = None
    for t in trades:
        try:
            total += float(t.get("fee") or 0)
        except (ValueError, TypeError):
            pass
        if ccy is None:
            ccy = t.get("fee_currency") or None
    return (total if total != 0.0 else None, ccy)


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
    3. Submit 3 limit orders in parallel.
    4. Poll for fill status.
    5. Update DB and send Telegram notification.
    Never raises — all exceptions are caught and logged.
    Returns (ok, message).
    """
    tag = _entry_opportunity_tag(triplet, signal)
    if triplet.exchange.lower() not in _SUPPORTED_EXEC_EXCHANGES:
        logger.info(
            "submit_entry [%s]: %s 暂不支持自动下单，跳过",
            tag,
            triplet.exchange,
        )
        return (False, f"{triplet.exchange} 暂不支持自动下单")
    try:
        if triplet.exchange.lower() in _OKX_EXEC_EXCHANGES:
            msg = await _submit_entry_inner(triplet, signal, signal_id, cfg, sqlite_path)
        else:
            msg = await _submit_entry_deribit_inner(triplet, signal, signal_id, cfg, sqlite_path)
        return (True, msg or "下单成功")
    except Exception as exc:
        logger.exception("submit_entry [%s] failed: %s", tag, exc)
        return (False, str(exc))


async def _submit_entry_inner(
    triplet: "Triplet",
    signal: "ArbitrageSignal",
    signal_id: int | None,
    cfg: "AppConfig",
    sqlite_path: str,
) -> str:
    tag = _entry_opportunity_tag(triplet, signal)
    exchange_name = triplet.exchange
    exc_cfg = cfg.exchanges.get(exchange_name.upper()) or cfg.exchanges.get(exchange_name.lower())
    if exc_cfg is None:
        raise RuntimeError(f"[{tag}] {exchange_name} 未配置")

    # --- Validate qty before any DB writes ---
    qty = signal.tradeable_qty
    lot_size = cfg.lot_size.get(triplet.symbol, 0.1)
    if lot_size > 0:
        qty = math.floor(qty / lot_size) * lot_size
        qty = round(qty, 8)  # strip float precision noise (e.g. 0.30000000000000004 → 0.3)
    if qty <= 0:
        raise RuntimeError(f"[{tag}] 下单数量为零（市场深度不足或不满足最小手数）")

    # --- Guard: block only live open or in-flight opening (partial_failed 可重试) ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        blocked = _db.blocking_entry_status(
            conn,
            exchange_name,
            triplet.symbol,
            triplet.expiry,
            triplet.strike,
            signal.direction,
        )
        if blocked == "open":
            logger.info(
                "submit_entry [%s]: 已有成交持仓，跳过",
                tag,
            )
            raise RuntimeError(f"[{tag}] 已有持仓")
        if blocked == "opening":
            logger.info(
                "submit_entry [%s]: 已有在途开仓，跳过",
                tag,
            )
            raise RuntimeError(f"[{tag}] 正在开仓中，请稍候")
        if blocked == "closing":
            logger.info("submit_entry [%s]: 正在平仓中，跳过新开仓", tag)
            raise RuntimeError(f"[{tag}] 正在平仓中，请稍候")

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
                call_inst_id=triplet.call_id,
                put_inst_id=triplet.put_id,
                future_inst_id=triplet.future_id,
            )
    finally:
        conn.close()

    # Determine order specs based on direction
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
    # OKX derivatives (options/futures) require "cross"; "cash" is spot-only
    td_mode = "cross"

    logger.info(
        "[submit_entry %s] 准备下单 | 仓位 pos=%d | 方向: %s | 数量: %s\n"
        "  call  %s  %s  @ %.6f BTC\n"
        "  put   %s  %s  @ %.6f BTC\n"
        "  future %s  %s  @ %.2f USDT",
        tag,
        position_id, signal.direction, qty,
        triplet.call_id,   legs[0]["side"], legs[0]["px"],
        triplet.put_id,    legs[1]["side"], legs[1]["px"],
        triplet.future_id, legs[2]["side"], legs[2]["px"],
    )

    # Create DB order records (pending)
    conn = sqlite3.connect(sqlite_path)
    order_ids_db: list[int] = []
    try:
        with conn:
            for leg_spec in legs:
                oid = _db.create_order(
                    conn,
                    signal_id=signal_id,
                    position_id=position_id,
                    inst_id=leg_spec["inst_id"],
                    leg=leg_spec["leg"],
                    action="open",
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
                            td_mode="cross",
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
            logger.error(
                "submit_entry [%s]: order submission timed out for position %d",
                tag,
                position_id,
            )
            await _mark_partial_failed(
                sqlite_path, position_id, order_ids_db, cfg, exchange_name, triplet, legs, session,
                api_key, secret, passphrase, is_paper, exch_order_ids,
                reason=f"[{tag}] 下单超时，已取消",
            )
            raise RuntimeError(f"[{tag}] 下单超时，已取消")

        # Record exchange order IDs and check for errors
        submit_errors: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                submit_errors.append(f"leg {legs[i]['leg']}: {result}")
            else:
                exch_order_ids[i] = result

        if submit_errors:
            logger.error("submit_entry [%s]: order submission errors: %s", tag, submit_errors)
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
            err_msg = f"[{tag}] 下单提交失败: " + "; ".join(submit_errors)
            await _mark_partial_failed(
                sqlite_path, position_id, order_ids_db, cfg, exchange_name, triplet, legs, session,
                api_key, secret, passphrase, is_paper, exch_order_ids,
                reason=err_msg,
            )
            raise RuntimeError(err_msg)

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
                    _fee, _ccy = _okx_fee(fd)  # type: ignore[arg-type]
                    # OKX may execute a limit order as market; use actual ordType from response
                    _ord_type = str(fd.get("ordType") or "").lower() or None  # type: ignore[union-attr]
                    _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0) if fd else 0.0  # type: ignore[union-attr]
                    _db.update_order_status(
                        conn,
                        oid_db,
                        "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0),  # type: ignore[union-attr]
                        filled_qty=_filled_qty if _filled_qty > 0 else None,
                        fee_type=_okx_fee_type(fd),  # type: ignore[arg-type]
                        actual_fee=_fee,
                        fee_ccy=_ccy,
                        filled_at=_db._utc_now_iso(),
                        order_type=_ord_type,
                    )
                _db.update_position_status(conn, position_id, "open")
            else:
                # Mark unfilled orders as failed; cancel them at exchange
                for i, oid_db in enumerate(order_ids_db):
                    fd = filled_data[i]
                    if fd is None:
                        _db.update_order_status(conn, oid_db, "failed")
                    else:
                        _fee, _ccy = _okx_fee(fd)
                        _ord_type = str(fd.get("ordType") or "").lower() or None
                        _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0) if fd else 0.0
                        _db.update_order_status(
                            conn,
                            oid_db,
                            "filled",
                            filled_px=float(fd.get("avgPx") or fd.get("px") or 0),
                            filled_qty=_filled_qty if _filled_qty > 0 else None,
                            fee_type=_okx_fee_type(fd),
                            actual_fee=_fee,
                            fee_ccy=_ccy,
                            filled_at=_db._utc_now_iso(),
                            order_type=_ord_type,
                        )
                bad_legs = [legs[i]["leg"] for i in range(3) if filled_data[i] is None]
                pf_msg = (
                    f"[{tag}] 部分腿未成交: {', '.join(bad_legs)}"
                    if bad_legs
                    else f"[{tag}] 部分腿未成交 (partial_failed)"
                )
                _db.update_position_status(
                    conn, position_id, "partial_failed", last_error=pf_msg,
                )
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
        logger.warning("submit_entry [%s]: telegram notification failed: %s", tag, exc)

    if not all_filled:
        raise RuntimeError(f"[{tag}] 部分腿未成交 (partial_failed)，仓位 ID: {position_id}")
    return f"下单成功，仓位 ID: {position_id}，数量: {qty}"


async def _submit_entry_deribit_inner(
    triplet: "Triplet",
    signal: "ArbitrageSignal",
    signal_id: int | None,
    cfg: "AppConfig",
    sqlite_path: str,
) -> str:
    from pcp_arbitrage.exchanges.deribit import DeribitRestClient, _internal_to_deribit
    from pcp_arbitrage.pcp_calculator import DERIBIT_INVERSE_FUT_USD_FACE

    tag = _entry_opportunity_tag(triplet, signal)
    exchange_name = triplet.exchange
    exc_cfg = cfg.exchanges.get(exchange_name.upper()) or cfg.exchanges.get(exchange_name.lower())
    if exc_cfg is None:
        raise RuntimeError(f"[{tag}] {exchange_name} 未配置")

    # --- Validate qty before any DB writes ---
    qty = signal.tradeable_qty
    lot_size = cfg.lot_size.get(triplet.symbol, 0.1)
    if lot_size > 0:
        qty = math.floor(qty / lot_size) * lot_size
    if qty <= 0:
        raise RuntimeError(f"[{tag}] 下单数量为零（市场深度不足或不满足最小手数）")

    # --- Guard: block only live open or in-flight opening ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        blocked = _db.blocking_entry_status(
            conn, exchange_name, triplet.symbol, triplet.expiry, triplet.strike, signal.direction,
        )
        if blocked == "open":
            logger.info("submit_entry [%s]: 已有成交持仓，跳过", tag)
            raise RuntimeError(f"[{tag}] 已有持仓")
        if blocked == "opening":
            logger.info("submit_entry [%s]: 已有在途开仓，跳过", tag)
            raise RuntimeError(f"[{tag}] 正在开仓中，请稍候")
        if blocked == "closing":
            logger.info("submit_entry [%s]: 正在平仓中，跳过新开仓", tag)
            raise RuntimeError(f"[{tag}] 正在平仓中，请稍候")
        with conn:
            position_id = _db.create_position(
                conn, signal_id=signal_id, exchange=exchange_name,
                symbol=triplet.symbol, expiry=triplet.expiry,
                strike=triplet.strike, direction=signal.direction,
                call_inst_id=triplet.call_id,
                put_inst_id=triplet.put_id,
                future_inst_id=triplet.future_id,
            )
    finally:
        conn.close()

    # Deribit: options amount in BTC/ETH, futures amount in USD contracts
    contract_size = DERIBIT_INVERSE_FUT_USD_FACE.get(triplet.symbol, 10.0)
    fut_amount = round(qty * signal.future_price / contract_size) * contract_size
    if fut_amount <= 0:
        fut_amount = contract_size

    if signal.direction == "forward":
        legs = [
            {"leg": "call",   "inst_id": _internal_to_deribit(triplet.call_id),   "side": "buy",  "px": signal.call_price_coin, "amount": qty},
            {"leg": "put",    "inst_id": _internal_to_deribit(triplet.put_id),    "side": "sell", "px": signal.put_price_coin,  "amount": qty},
            {"leg": "future", "inst_id": _internal_to_deribit(triplet.future_id), "side": "sell", "px": signal.future_price,    "amount": fut_amount},
        ]
    else:
        legs = [
            {"leg": "call",   "inst_id": _internal_to_deribit(triplet.call_id),   "side": "sell", "px": signal.call_price_coin, "amount": qty},
            {"leg": "put",    "inst_id": _internal_to_deribit(triplet.put_id),    "side": "buy",  "px": signal.put_price_coin,  "amount": qty},
            {"leg": "future", "inst_id": _internal_to_deribit(triplet.future_id), "side": "buy",  "px": signal.future_price,    "amount": fut_amount},
        ]

    # Create DB order records
    conn = sqlite3.connect(sqlite_path)
    order_ids_db: list[int] = []
    try:
        with conn:
            for leg_spec in legs:
                oid = _db.create_order(
                    conn, signal_id=signal_id, position_id=position_id,
                    inst_id=leg_spec["inst_id"],
                    leg=leg_spec["leg"],
                    action="open", side=leg_spec["side"],
                    limit_px=leg_spec["px"], qty=leg_spec["amount"],
                )
                order_ids_db.append(oid)
    finally:
        conn.close()

    exch_order_ids: list[str | None] = [None] * 3
    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key

    async with DeribitRestClient(api_key=api_key, secret=secret) as dclient:
        await dclient._authenticate()

        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[dclient.place_order(leg["side"], leg["inst_id"], leg["amount"], leg["px"])
                      for leg in legs],
                    return_exceptions=True,
                ),
                timeout=_SUBMIT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            logger.error(
                "submit_entry [%s]: deribit order submission timed out for position %d",
                tag,
                position_id,
            )
            conn = sqlite3.connect(sqlite_path)
            try:
                with conn:
                    for oid_db in order_ids_db:
                        _db.update_order_status(conn, oid_db, "failed")
                    _db.update_position_status(
                        conn,
                        position_id,
                        "partial_failed",
                        last_error=f"[{tag}] 下单超时，已取消",
                    )
            finally:
                conn.close()
            for oid in exch_order_ids:
                if oid:
                    await dclient.cancel_order(oid)
            raise RuntimeError(f"[{tag}] 下单超时，已取消")

        submit_errors: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                submit_errors.append(f"leg {legs[i]['leg']}: {result}")
            else:
                exch_order_ids[i] = result

        if submit_errors:
            logger.error("submit_entry [%s]: deribit order submission errors: %s", tag, submit_errors)
            err_msg = f"[{tag}] 下单提交失败: " + "; ".join(submit_errors)
            conn = sqlite3.connect(sqlite_path)
            try:
                with conn:
                    for i, oid_db in enumerate(order_ids_db):
                        if exch_order_ids[i]:
                            conn.execute(
                                "UPDATE orders SET exchange_order_id=? WHERE id=?",
                                (exch_order_ids[i], oid_db),
                            )
                    for oid_db in order_ids_db:
                        _db.update_order_status(conn, oid_db, "failed")
                    _db.update_position_status(
                        conn, position_id, "partial_failed", last_error=err_msg,
                    )
            finally:
                conn.close()
            for oid in exch_order_ids:
                if oid:
                    await dclient.cancel_order(oid)
            raise RuntimeError(err_msg)

        # Store exchange order IDs
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

        # Poll for fills
        fill_results = await asyncio.gather(
            *[_poll_order_fill_deribit(dclient, exch_order_ids[i]) for i in range(3)],
            return_exceptions=True,
        )

        # Fetch trades for fee_type while still authenticated
        fee_types: list[str | None] = []
        for i, fr in enumerate(fill_results):
            if isinstance(fr, Exception) or fr is None:
                fee_types.append(None)
            else:
                try:
                    trades = await dclient.get_trades_by_order(exch_order_ids[i])  # type: ignore[arg-type]
                    fee_types.append(_deribit_fee_type(trades))
                except Exception as exc:
                    logger.warning("get_trades_by_order failed ord=%s: %s", exch_order_ids[i], exc)
                    fee_types.append(None)

    # Evaluate fills
    all_filled = True
    filled_data: list[dict | None] = []
    for fr in fill_results:
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
                    _filled_qty = float(fd.get("filled_amount") or fd.get("amount") or 0) if fd else 0.0  # type: ignore[union-attr]
                    _db.update_order_status(
                        conn, oid_db, "filled",
                        filled_px=float(fd.get("average_price") or fd.get("price") or 0),  # type: ignore[union-attr]
                        filled_qty=_filled_qty if _filled_qty > 0 else None,
                        fee_type=fee_types[i],
                        filled_at=_db._utc_now_iso(),
                    )
                _db.update_position_status(conn, position_id, "open")
            else:
                for i, oid_db in enumerate(order_ids_db):
                    fd = filled_data[i]
                    if fd is None:
                        _db.update_order_status(conn, oid_db, "failed")
                    else:
                        _filled_qty = float(fd.get("filled_amount") or fd.get("amount") or 0) if fd else 0.0
                        _db.update_order_status(
                            conn, oid_db, "filled",
                            filled_px=float(fd.get("average_price") or fd.get("price") or 0),
                            filled_qty=_filled_qty if _filled_qty > 0 else None,
                            fee_type=fee_types[i],
                            filled_at=_db._utc_now_iso(),
                        )
                bad_legs = [legs[i]["leg"] for i in range(3) if filled_data[i] is None]
                pf_msg = (
                    f"[{tag}] 部分腿未成交: {', '.join(bad_legs)}"
                    if bad_legs
                    else f"[{tag}] 部分腿未成交 (partial_failed)"
                )
                _db.update_position_status(
                    conn, position_id, "partial_failed", last_error=pf_msg,
                )
    finally:
        conn.close()

    # Cancel unfilled orders
    if not all_filled:
        async with DeribitRestClient(api_key=api_key, secret=secret) as dclient2:
            await dclient2._authenticate()
            for i, fr in enumerate(fill_results):
                if (isinstance(fr, Exception) or fr is None) and exch_order_ids[i]:
                    await dclient2.cancel_order(exch_order_ids[i])  # type: ignore[arg-type]

    # Telegram notification
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
        logger.warning("submit_entry [%s]: telegram notification failed: %s", tag, exc)

    if not all_filled:
        raise RuntimeError(f"[{tag}] 部分腿未成交 (partial_failed)，仓位 ID: {position_id}")
    return f"下单成功，仓位 ID: {position_id}，数量: {qty}"


async def _poll_order_fill_deribit(
    client: "DeribitRestClient",
    order_id: str | None,
    poll_interval: float = _POLL_INTERVAL_SEC,
    poll_timeout: float = _POLL_TIMEOUT_SEC,
) -> dict | None:
    """Poll until Deribit order fills or times out. Returns order dict or None."""
    if not order_id:
        return None
    deadline = asyncio.get_event_loop().time() + poll_timeout
    while asyncio.get_event_loop().time() < deadline:
        order = await client.get_order_state(order_id)
        state = order.get("order_state", "")
        if state == "filled":
            return order
        if state in ("cancelled", "rejected"):
            return None
        await asyncio.sleep(poll_interval)
    return None



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
    *,
    reason: str,
) -> None:
    """Mark position as partial_failed and cancel any submitted orders."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for oid_db in order_ids_db:
                _db.update_order_status(conn, oid_db, "failed")
            _db.update_position_status(
                conn, position_id, "partial_failed", last_error=reason,
            )
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
        exchange = str(position.get("exchange", "")).lower()
        if exchange in _OKX_EXEC_EXCHANGES:
            await _submit_exit_inner(position, cfg, sqlite_path)
        elif exchange == "deribit":
            await _submit_exit_deribit_inner(position, cfg, sqlite_path)
        else:
            logger.warning(
                "submit_exit: unsupported exchange '%s' for position %s, skipping",
                exchange, position.get("id"),
            )
    except Exception as exc:
        logger.exception("submit_exit failed pos=%s: %s", position.get("id"), exc)


async def _submit_exit_inner(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> None:
    exchange_name = str(position.get("exchange", "OKX"))
    exc_cfg = cfg.exchanges.get(exchange_name.upper()) or cfg.exchanges.get(exchange_name.lower())
    if exc_cfg is None:
        logger.warning("submit_exit: exchange %s not configured, skipping", exchange_name)
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
        entry_orders = _db.get_position_orders(conn, position_id, action="open")
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

        # Exit logic (reverse of entry), using PASSIVE (maker) prices:
        # forward entry: BUY call, SELL put, SELL future
        #   → exit: SELL call at ask (passive), BUY put at bid (passive), BUY future at bid (passive)
        # reverse entry: SELL call, BUY put, BUY future
        #   → exit: BUY call at bid (passive), SELL put at ask (passive), SELL future at ask (passive)
        if direction == "forward":
            call_px = _safe_book(book_results[0], "call", use_bid=False)    # sell at ask (passive maker)
            put_px = _safe_book(book_results[1], "put", use_bid=True)       # buy at bid (passive maker)
            future_px = _safe_book(book_results[2], "future", use_bid=True) # buy at bid (passive maker)
            exit_legs = [
                {"leg": "call", "inst_id": call_inst_id, "side": "sell", "px": call_px},
                {"leg": "put", "inst_id": put_inst_id, "side": "buy", "px": put_px},
                {"leg": "future", "inst_id": future_inst_id, "side": "buy", "px": future_px},
            ]
        else:
            call_px = _safe_book(book_results[0], "call", use_bid=True)      # buy at bid (passive maker)
            put_px = _safe_book(book_results[1], "put", use_bid=False)        # sell at ask (passive maker)
            future_px = _safe_book(book_results[2], "future", use_bid=False)  # sell at ask (passive maker)
            exit_legs = [
                {"leg": "call", "inst_id": call_inst_id, "side": "buy", "px": call_px},
                {"leg": "put", "inst_id": put_inst_id, "side": "sell", "px": put_px},
                {"leg": "future", "inst_id": future_inst_id, "side": "sell", "px": future_px},
            ]

        # Determine qty from entry orders (use first entry order qty, fallback 1)
        qty = float(next(iter(entry_orders), {}).get("qty", 1.0) or 1.0)

        logger.info(
            "[submit_exit] 准备平仓 | 仓位 pos=%d | 方向: %s | 数量: %s\n"
            "  call  %s  %s  @ %.6f\n"
            "  put   %s  %s  @ %.6f\n"
            "  future %s  %s  @ %.2f",
            position_id, direction, qty,
            exit_legs[0]["inst_id"], exit_legs[0]["side"], exit_legs[0]["px"],
            exit_legs[1]["inst_id"], exit_legs[1]["side"], exit_legs[1]["px"],
            exit_legs[2]["inst_id"], exit_legs[2]["side"], exit_legs[2]["px"],
        )

        # Create DB exit order records
        conn = sqlite3.connect(sqlite_path)
        exit_order_ids_db: list[int] = []
        try:
            with conn:
                for leg_spec in exit_legs:
                    oid = _db.create_order(
                        conn,
                        signal_id=position.get("signal_id"),
                        position_id=position_id,
                        inst_id=leg_spec["inst_id"],
                        leg=leg_spec["leg"],
                        action="close",
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
                            td_mode="cross",
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
            _update_exit_failed(
                sqlite_path, position_id, exit_order_ids_db,
                reason="平仓下单超时",
            )
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
            _update_exit_failed(
                sqlite_path, position_id, exit_order_ids_db,
                reason="; ".join(submit_errors),
            )
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
                    _fee, _ccy = _okx_fee(fd) if fd else (None, None)
                    _db.update_order_status(
                        conn,
                        oid_db,
                        "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0) if fd else 0.0,  # type: ignore[union-attr]
                        fee_type=_okx_fee_type(fd) if fd else None,
                        actual_fee=_fee,
                        fee_ccy=_ccy,
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
        _update_exit_failed(
            sqlite_path, position_id, exit_order_ids_db,
            reason="平仓单未在时限内全部成交",
        )
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


async def _submit_exit_deribit_inner(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> None:
    from pcp_arbitrage.exchanges.deribit import DeribitRestClient, _internal_to_deribit
    from pcp_arbitrage.pcp_calculator import DERIBIT_INVERSE_FUT_USD_FACE

    exchange_name = str(position.get("exchange", "deribit"))
    exc_cfg = cfg.exchanges.get(exchange_name.upper()) or cfg.exchanges.get(exchange_name.lower())
    if exc_cfg is None:
        logger.warning("submit_exit: exchange %s not configured, skipping", exchange_name)
        return

    position_id = position["id"]
    symbol = position["symbol"]
    expiry = position["expiry"]
    strike = position["strike"]
    direction = position["direction"]
    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key

    # --- Fetch entry orders from DB ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders = _db.get_position_orders(conn, position_id, action="open")
    finally:
        conn.close()
    leg_map: dict[str, dict] = {o["leg"]: o for o in entry_orders}

    # Reconstruct Deribit instrument IDs from position fields
    exp_clean = expiry.replace("-", "")
    if len(exp_clean) == 8:
        exp_clean = exp_clean[2:]
    call_inst_id = _internal_to_deribit(f"{symbol}-USD-{exp_clean}-{int(strike)}-C")
    put_inst_id  = _internal_to_deribit(f"{symbol}-USD-{exp_clean}-{int(strike)}-P")
    future_inst_id = _internal_to_deribit(f"{symbol}-USD-{exp_clean}")

    # qty from entry orders; futures amount in USD contracts
    qty = float(next(iter(entry_orders), {}).get("qty", 1.0) or 1.0)
    contract_size = DERIBIT_INVERSE_FUT_USD_FACE.get(symbol, 10.0)

    async with DeribitRestClient(api_key=api_key, secret=secret) as dclient:
        await dclient._authenticate()

        # --- Fetch order book tops for exit pricing ---
        book_results = await asyncio.gather(
            dclient.get_order_book(call_inst_id),
            dclient.get_order_book(put_inst_id),
            dclient.get_order_book(future_inst_id),
            return_exceptions=True,
        )

        def _safe_book(result, leg_name: str, use_bid: bool) -> float:
            if isinstance(result, Exception):
                fallback = float(leg_map.get(leg_name, {}).get("limit_px", 0.0) or 0.0)
                logger.warning("submit_exit deribit: book fetch failed for %s, using entry px %s", leg_name, fallback)
                return fallback
            px = result.get("best_bid_price" if use_bid else "best_ask_price")
            if px is None:
                fallback = float(leg_map.get(leg_name, {}).get("limit_px", 0.0) or 0.0)
                logger.warning("submit_exit deribit: no px for %s, using entry px %s", leg_name, fallback)
                return fallback
            return float(px)

        # Exit is the reverse of entry
        if direction == "forward":
            # entry: buy call, sell put, sell future → exit: sell call, buy put, buy future
            fut_amount = round(qty * _safe_book(book_results[2], "future", use_bid=False) / contract_size) * contract_size or contract_size
            exit_legs = [
                {"leg": "call",   "inst_id": call_inst_id,   "side": "sell", "px": _safe_book(book_results[0], "call",   use_bid=True),  "amount": qty},
                {"leg": "put",    "inst_id": put_inst_id,    "side": "buy",  "px": _safe_book(book_results[1], "put",    use_bid=False), "amount": qty},
                {"leg": "future", "inst_id": future_inst_id, "side": "buy",  "px": _safe_book(book_results[2], "future", use_bid=False), "amount": fut_amount},
            ]
        else:
            # entry: sell call, buy put, buy future → exit: buy call, sell put, sell future
            fut_amount = round(qty * _safe_book(book_results[2], "future", use_bid=True) / contract_size) * contract_size or contract_size
            exit_legs = [
                {"leg": "call",   "inst_id": call_inst_id,   "side": "buy",  "px": _safe_book(book_results[0], "call",   use_bid=False), "amount": qty},
                {"leg": "put",    "inst_id": put_inst_id,    "side": "sell", "px": _safe_book(book_results[1], "put",    use_bid=True),  "amount": qty},
                {"leg": "future", "inst_id": future_inst_id, "side": "sell", "px": _safe_book(book_results[2], "future", use_bid=True),  "amount": fut_amount},
            ]

        # Create DB exit order records
        conn = sqlite3.connect(sqlite_path)
        exit_order_ids_db: list[int] = []
        try:
            with conn:
                for leg_spec in exit_legs:
                    oid = _db.create_order(
                        conn,
                        signal_id=position.get("signal_id"),
                        position_id=position_id,
                        inst_id=leg_spec["inst_id"],
                        leg=leg_spec["leg"],
                        action="close",
                        side=leg_spec["side"],
                        limit_px=leg_spec["px"],
                        qty=leg_spec["amount"],
                    )
                    exit_order_ids_db.append(oid)
        finally:
            conn.close()

        # Submit all 3 exit orders in parallel
        exch_order_ids: list[str | None] = [None] * 3
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[dclient.place_order(leg["side"], leg["inst_id"], leg["amount"], leg["px"])
                      for leg in exit_legs],
                    return_exceptions=True,
                ),
                timeout=_SUBMIT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            reason = "exit order submission timed out"
            logger.error("submit_exit deribit: %s for position %d", reason, position_id)
            _update_exit_failed(
                sqlite_path, position_id, exit_order_ids_db,
                reason="平仓下单超时",
            )
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
            logger.error("submit_exit deribit: %s", reason)
            _update_exit_failed(
                sqlite_path, position_id, exit_order_ids_db,
                reason="; ".join(submit_errors),
            )
            await _send_exit_failure_telegram(cfg, position, reason)
            for oid in exch_order_ids:
                if oid:
                    await dclient.cancel_order(oid)
            return

        # Store exchange order IDs
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid_db in enumerate(exit_order_ids_db):
                    conn.execute("UPDATE orders SET exchange_order_id=? WHERE id=?", (exch_order_ids[i], oid_db))
        finally:
            conn.close()

        # Poll for fills
        fill_results = await asyncio.gather(
            *[_poll_order_fill_deribit(dclient, exch_order_ids[i]) for i in range(3)],
            return_exceptions=True,
        )

        # Fetch trades for fee_type while still authenticated
        fee_types: list[str | None] = []
        trades_list: list[list[dict]] = []
        for i, fr in enumerate(fill_results):
            if isinstance(fr, Exception) or fr is None:
                fee_types.append(None)
                trades_list.append([])
            else:
                try:
                    trades = await dclient.get_trades_by_order(exch_order_ids[i])  # type: ignore[arg-type]
                    fee_types.append(_deribit_fee_type(trades))
                    trades_list.append(trades)
                except Exception as exc:
                    logger.warning("get_trades_by_order failed ord=%s: %s", exch_order_ids[i], exc)
                    fee_types.append(None)
                    trades_list.append([])

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
        realized_pnl = 0.0
        for i, leg_spec in enumerate(exit_legs):
            fd = filled_data[i]
            exit_px = float(fd.get("average_price") or fd.get("price") or leg_spec["px"]) if fd else leg_spec["px"]  # type: ignore[union-attr]
            entry_order = leg_map.get(leg_spec["leg"])
            entry_px = float(entry_order.get("filled_px") or entry_order.get("limit_px") or 0.0) if entry_order else 0.0
            if leg_spec["side"] == "sell":
                realized_pnl += (exit_px - entry_px) * qty
            else:
                realized_pnl += (entry_px - exit_px) * qty

        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid_db in enumerate(exit_order_ids_db):
                    fd = filled_data[i]
                    _fee, _ccy = _deribit_fee(trades_list[i]) if i < len(trades_list) else (None, None)
                    _db.update_order_status(
                        conn, oid_db, "filled",
                        filled_px=float(fd.get("average_price") or fd.get("price") or 0) if fd else 0.0,  # type: ignore[union-attr]
                        fee_type=fee_types[i],
                        actual_fee=_fee,
                        fee_ccy=_ccy,
                        filled_at=_db._utc_now_iso(),
                    )
                _db.update_position_status(
                    conn, position_id, "closed",
                    realized_pnl_usdt=realized_pnl,
                    closed_at=_db._utc_now_iso(),
                )
        finally:
            conn.close()

        label = f"{symbol} {expiry} {int(strike)} {direction}"
        msg = (
            f"\U0001f4b0 止盈出场成功\n"
            f"{label}\n"
            f"实现盈亏: {realized_pnl:+.2f} USDT"
        )
        try:
            await _notifier.send_telegram(cfg.telegram, msg)
        except Exception as exc:
            logger.warning("submit_exit deribit: telegram notification failed: %s", exc)
    else:
        reason = "one or more exit orders did not fill within timeout"
        _update_exit_failed(
            sqlite_path, position_id, exit_order_ids_db,
            reason="平仓单未在时限内全部成交",
        )
        await _send_exit_failure_telegram(cfg, position, reason)
        async with DeribitRestClient(api_key=api_key, secret=secret) as dclient2:
            await dclient2._authenticate()
            for i, fr in enumerate(fill_results):
                if (isinstance(fr, Exception) or fr is None) and exch_order_ids[i]:
                    await dclient2.cancel_order(exch_order_ids[i])  # type: ignore[arg-type]


def _update_exit_failed(
    sqlite_path: str,
    position_id: int,
    exit_order_ids_db: list[int],
    *,
    reason: str,
) -> None:
    """Mark exit orders as failed and position as partial_failed."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for oid_db in exit_order_ids_db:
                _db.update_order_status(conn, oid_db, "failed")
            _db.update_position_status(
                conn,
                position_id,
                "partial_failed",
                last_error=f"出场失败：{reason}",
            )
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
