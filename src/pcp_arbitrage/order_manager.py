"""
Three-leg limit order entry for PCP arbitrage positions (OKX + Deribit, Phase 2).
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import math

import aiohttp

from pcp_arbitrage import db as _db
from pcp_arbitrage import notifier as _notifier
from pcp_arbitrage.okx_client import _sign, _timestamp

if TYPE_CHECKING:
    from pcp_arbitrage.config import AppConfig, ExchangeConfig
    from pcp_arbitrage.exchanges.deribit import DeribitRestClient
    from pcp_arbitrage.models import Triplet
    from pcp_arbitrage.pcp_calculator import ArbitrageSignal

logger = logging.getLogger(__name__)

# 模块级，防止内联循环与守护协程双重操作同一仓位
_exit_active: set[int] = set()

_OKX_REST_BASE = "https://www.okx.com"

# instId -> public /instruments row（含 ctVal、lotSz）；进程内缓存
_okx_instrument_cache: dict[str, dict] = {}


def _entry_opportunity_tag(triplet: "Triplet", signal: "ArbitrageSignal") -> str:
    """Short label for logs/errors: exchange symbol-expiry-strike direction."""
    return (
        f"{triplet.exchange} {triplet.symbol}-{triplet.expiry}-{int(triplet.strike)} "
        f"{signal.direction}"
    )
_POLL_INTERVAL_SEC = 2.0
_POLL_TIMEOUT_SEC = 60.0
_TAKER_HARD_TIMEOUT_SEC = 600.0
_SUBMIT_TIMEOUT_SEC = 30.0

# OKX account/positions vs DB entry: allow small float / rounding drift
_OKX_POSITION_QTY_TOL = 1e-5
_OKX_POSITION_REL_TOL = 0.02


def _okx_inst_type_for_inst_id(inst_id: str) -> str:
    u = inst_id.upper()
    if u.endswith("-C") or u.endswith("-P"):
        return "OPTION"
    return "FUTURES"


def _okx_uly_from_inst_id(inst_id: str) -> str | None:
    parts = inst_id.split("-")
    if len(parts) < 2:
        return None
    return f"{parts[0]}-{parts[1]}"


async def _okx_get_instrument(session: aiohttp.ClientSession, inst_id: str) -> dict:
    """GET /api/v5/public/instruments 单行；期权需带 uly。"""
    if inst_id in _okx_instrument_cache:
        return _okx_instrument_cache[inst_id]
    inst_type = _okx_inst_type_for_inst_id(inst_id)
    params: dict[str, str] = {"instType": inst_type, "instId": inst_id}
    if inst_type == "OPTION":
        uly = _okx_uly_from_inst_id(inst_id)
        if uly:
            params["uly"] = uly
    path = "/api/v5/public/instruments"
    async with session.get(path, params=params) as resp:
        resp.raise_for_status()
        data = await resp.json()
    if str(data.get("code", "0")) != "0":
        raise RuntimeError(f"OKX instruments: {data.get('msg') or data}")
    rows = data.get("data") or []
    if not rows:
        raise RuntimeError(f"OKX instruments: empty data for {inst_id}")
    inst = rows[0]
    _okx_instrument_cache[inst_id] = inst
    return inst


def _okx_contracts_from_qty_btc(
    inst: dict,
    qty_btc: float,
    *,
    future_mark_usd: float | None,
) -> float:
    """标的币数量 -> OKX 合约张数（未按 lotSz 取整）。"""
    inst_type = str(inst.get("instType") or "")
    ct_type = (inst.get("ctType") or "").lower()
    ccy = (inst.get("ctValCcy") or "").upper()
    ct_val = float(inst.get("ctVal") or 0)
    iid = inst.get("instId") or "?"

    def _parse_ct_mult() -> float:
        raw = inst.get("ctMult")
        if raw is None or raw == "":
            return float("nan")
        return float(raw)

    ct_mult = _parse_ct_mult()

    if inst_type == "OPTION":
        # OKX 期权：每张标的币 = ctVal×ctMult（常见 BTC-USD 为 1×0.01=0.01 BTC）。
        # 若接口未返回 ctMult，不可用「默认 1.0」——否则会当成 1 BTC/张 → sz 错成 0.01 张。
        if not math.isfinite(ct_mult) or ct_mult <= 0:
            ct_mult = 0.01 if ccy == "BTC" else 1.0
        coin_per = ct_val * ct_mult
        if coin_per <= 0:
            raise ValueError(f"OKX option ctVal*ctMult invalid for {iid}")
        # 若接口把 ctMult 标成 1，会得到 coin_per=1（1 BTC/张），与 OKX BTC-USD 期权常见 0.01 BTC/张 不符，
        # 会把 0.01 BTC 名义错算成 0.01 张；按每张 0.01 BTC 重算张数。
        if ccy == "BTC" and math.isclose(coin_per, 1.0, rel_tol=0.0, abs_tol=1e-12):
            coin_per = 0.01
        return qty_btc / coin_per

    if inst_type in ("FUTURES", "SWAP") and ct_type == "inverse" and ccy == "USD":
        if future_mark_usd is None or future_mark_usd <= 0:
            raise ValueError(f"inverse future {iid} needs positive USD mark for sizing")
        if ct_val <= 0:
            raise ValueError(f"OKX inverse ctVal invalid for {iid}")
        return qty_btc * future_mark_usd / ct_val

    if not math.isfinite(ct_mult) or ct_mult <= 0:
        ct_mult = 1.0
    coin_per = ct_val * ct_mult
    if coin_per > 0 and ccy in ("BTC", "ETH", "SOL"):
        return qty_btc / coin_per

    raise ValueError(
        f"unsupported OKX instrument for qty→contracts: {iid} "
        f"type={inst_type} ctType={ct_type} ctValCcy={ccy}"
    )


def _okx_floor_contracts_to_lot(raw: float, inst: dict) -> float:
    """按 lotSz 向下取整；若有 minSz 且结果在 (0, minSz) 则取 0。"""
    lot_sz = float(inst.get("lotSz") or "1")
    min_sz = float(inst.get("minSz") or lot_sz)
    if raw <= 0:
        return 0.0
    n = math.floor(raw / lot_sz + 1e-12)
    out = n * lot_sz
    if 0 < out < min_sz:
        return 0.0
    return out


def _format_okx_order_sz(sz: float) -> str:
    if not math.isfinite(sz) or sz <= 0:
        raise ValueError(f"invalid OKX sz: {sz}")
    s = f"{sz:.12f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _expected_signed_open_contracts(direction: str, leg: str, qty: float) -> float:
    """Signed contract count per leg after a fully filled triple open (long +)."""
    q = float(qty)
    if direction == "forward":
        if leg == "call":
            return q
        if leg == "put":
            return -q
        if leg == "future":
            return -q
    else:
        if leg == "call":
            return -q
        if leg == "put":
            return q
        if leg == "future":
            return q
    raise ValueError(f"bad leg/direction: {leg}/{direction}")


async def _okx_get_signed_position(
    session: aiohttp.ClientSession,
    inst_id: str,
    *,
    api_key: str,
    secret: str,
    passphrase: str,
) -> float | None:
    """Return signed OKX position (contracts); None on API/transport error."""
    inst_type = _okx_inst_type_for_inst_id(inst_id)
    path = "/api/v5/account/positions"
    params = {"instType": inst_type, "instId": inst_id}
    qs = urlencode(params)
    full_path = f"{path}?{qs}"
    headers = _auth_headers(api_key, secret, passphrase, "GET", full_path, "")
    try:
        async with session.get(path, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as exc:
        logger.warning("_okx_get_signed_position HTTP error inst=%s: %s", inst_id, exc)
        return None
    if str(data.get("code", "0")) != "0":
        logger.warning(
            "_okx_get_signed_position API code=%s inst=%s msg=%s",
            data.get("code"), inst_id, data.get("msg"),
        )
        return None
    rows = data.get("data") or []
    if not rows:
        return 0.0
    pos = rows[0].get("pos")
    try:
        return float(pos or 0)
    except (TypeError, ValueError):
        return 0.0


async def okx_db_entry_matches_exchange_positions(
    session: aiohttp.ClientSession,
    *,
    call_inst_id: str,
    put_inst_id: str,
    future_inst_id: str,
    direction: str,
    qty_call: float,
    qty_put: float,
    qty_future: float,
    api_key: str,
    secret: str,
    passphrase: str,
) -> tuple[bool, str]:
    """True if OKX /account/positions matches DB hedge (same sign & size per leg).

    qty_* 为各腿 OKX 合约张数（与 orders.qty / 交易所 pos 一致）。
    """
    qty_by_leg = {
        "call": qty_call,
        "put": qty_put,
        "future": qty_future,
    }
    for leg, iid in (
        ("call", call_inst_id),
        ("put", put_inst_id),
        ("future", future_inst_id),
    ):
        exp = _expected_signed_open_contracts(direction, leg, qty_by_leg[leg])
        actual = await _okx_get_signed_position(
            session, iid, api_key=api_key, secret=secret,
            passphrase=passphrase,
        )
        if actual is None:
            logger.warning(
                "[okx_exit_verify] leg=%s inst=%s expected=%g actual=NA verdict=QUERY_FAILED",
                leg, iid, exp,
            )
            return False, f"无法查询交易所持仓 {iid}"
        tol = max(_OKX_POSITION_QTY_TOL, abs(exp) * _OKX_POSITION_REL_TOL)
        diff = actual - exp
        ok_leg = abs(diff) <= tol
        logger.info(
            "[okx_exit_verify] leg=%s inst=%s expected=%g actual=%g diff=%g tol=%g verdict=%s",
            leg,
            iid,
            exp,
            actual,
            diff,
            tol,
            "PASS" if ok_leg else "FAIL",
        )
        if abs(actual - exp) > tol:
            return (
                False,
                f"{leg} {iid}: 期望 {exp:g} 张，交易所 {actual:g} 张",
            )
    return True, ""


async def okx_exit_allowed_by_exchange(
    sqlite_path: str,
    position: dict,
    cfg: "AppConfig",
) -> bool:
    """False = skip auto exit/retry (no matching hedge on OKX)."""
    if str(position.get("exchange", "")).lower() != "okx":
        return True
    pos_id = int(position.get("id") or 0)
    exc_name = str(position.get("exchange", "OKX"))
    exc_cfg = cfg.exchanges.get(exc_name.upper()) or cfg.exchanges.get(exc_name.lower())
    if exc_cfg is None:
        logger.warning("[okx_exit_verify] pos=%s exchange config missing", pos_id)
        return False
    fully_exited = False
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders = _db.get_position_orders(conn, int(position["id"]), action="open")
    finally:
        conn.close()
    if len(entry_orders) != 3 or any(o.get("status") != "filled" for o in entry_orders):
        logger.info(
            "[okx_exit_verify] pos=%s skip auto-exit verify: db entry orders not all filled "
            "(total=%d statuses=%s)",
            pos_id,
            len(entry_orders),
            [str(o.get("status")) for o in entry_orders],
        )
        return False
    leg_map = {o["leg"]: o for o in entry_orders}
    qc = leg_map.get("call") or {}
    qp = leg_map.get("put") or {}
    qf = leg_map.get("future") or {}
    qty_call = float(qc.get("qty", 1.0) or 1.0)
    qty_put = float(qp.get("qty", 1.0) or 1.0)
    qty_future = float(qf.get("qty", 1.0) or 1.0)
    call_inst_id = qc.get("inst_id") or ""
    put_inst_id = qp.get("inst_id") or ""
    future_inst_id = qf.get("inst_id") or ""
    if not (call_inst_id and put_inst_id and future_inst_id):
        logger.warning(
            "[okx_exit_verify] pos=%s missing inst_id call=%s put=%s future=%s",
            pos_id,
            bool(call_inst_id),
            bool(put_inst_id),
            bool(future_inst_id),
        )
        return False
    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        ok, reason = await okx_db_entry_matches_exchange_positions(
            session,
            call_inst_id=call_inst_id,
            put_inst_id=put_inst_id,
            future_inst_id=future_inst_id,
            direction=str(position.get("direction") or ""),
            qty_call=qty_call,
            qty_put=qty_put,
            qty_future=qty_future,
            api_key=exc_cfg.api_key,
            secret=exc_cfg.secret_key,
            passphrase=exc_cfg.passphrase,
        )
    if not ok:
        logger.warning("[okx_exit_verify] pos=%s verdict=FAIL reason=%s", pos_id, reason)
    else:
        logger.info("[okx_exit_verify] pos=%s verdict=PASS", pos_id)
    return ok


def _auth_headers(
    api_key: str,
    secret: str,
    passphrase: str,
    method: str,
    path: str,
    body: str = "",
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
    return headers


def _okx_order_error_detail(data: dict) -> str:
    """Human-readable error from OKX trade/order JSON (top-level msg + data[0] sCode/sMsg)."""
    parts: list[str] = []
    top = (data.get("msg") or "").strip()
    if top:
        parts.append(top)
    row0 = (data.get("data") or None)
    if isinstance(row0, list) and row0 and isinstance(row0[0], dict):
        r = row0[0]
        sc = (r.get("sCode") or "").strip()
        sm = (r.get("sMsg") or "").strip()
        if sc or sm:
            parts.append(f"sCode={sc} sMsg={sm}".strip())
    return " | ".join(parts) if parts else json.dumps(data, ensure_ascii=False)


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
    log_label: str | None = None,
) -> str:
    """Place a single limit order; returns the exchange order ID string."""
    path = "/api/v5/trade/order"
    payload = {
        "instId": inst_id,
        "tdMode": td_mode,
        "side": side,
        "ordType": ord_type,
        "px": str(px),
        "sz": _format_okx_order_sz(sz),
    }
    body = json.dumps(payload)
    tag = f"[{log_label}] " if log_label else ""
    logger.info(
        "%sOKX place_order request: %s",
        tag,
        body,
    )
    headers = _auth_headers(api_key, secret, passphrase, "POST", path, body)
    async with session.post(path, data=body, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
    logger.info(
        "%sOKX place_order response: %s",
        tag,
        json.dumps(data, ensure_ascii=False)[:4000],
    )
    if str(data.get("code", "0")) != "0":
        detail = _okx_order_error_detail(data)
        raise RuntimeError(f"OKX[{data.get('code')}]: {detail}")
    orders = data.get("data", [])
    if not orders:
        raise RuntimeError(f"Empty data in order response: {data}")
    ord_id = orders[0].get("ordId", "")
    if not ord_id:
        detail = _okx_order_error_detail(data)
        raise RuntimeError(f"No ordId in order response ({detail}): {data}")
    return str(ord_id)


async def _poll_order_fill(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    ord_id: str,
    api_key: str,
    secret: str,
    passphrase: str,
    poll_interval: float = _POLL_INTERVAL_SEC,
    poll_timeout: float | None = _POLL_TIMEOUT_SEC,
) -> dict | None:
    """Poll until order is filled or timeout. Returns order data dict or None on timeout.

    When poll_timeout is None, poll indefinitely until filled or canceled.
    """
    params_str = f"instId={inst_id}&ordId={ord_id}"
    path = f"/api/v5/trade/order?{params_str}"
    if poll_timeout is None:
        hard_deadline = asyncio.get_event_loop().time() + _TAKER_HARD_TIMEOUT_SEC
        deadline = None
    else:
        deadline = asyncio.get_event_loop().time() + poll_timeout
        hard_deadline = None
    while True:
        # Check hard deadline (taker mode: poll_timeout=None)
        if hard_deadline is not None and asyncio.get_event_loop().time() >= hard_deadline:
            logger.critical(
                "_poll_order_fill [%s %s]: taker hard timeout (10min), giving up",
                inst_id, ord_id,
            )
            return None
        # Check normal deadline (maker mode)
        if deadline is not None and asyncio.get_event_loop().time() >= deadline:
            return None
        headers = _auth_headers(api_key, secret, passphrase, "GET", path, "")
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
            if state == "canceled":
                return None
            # partially_filled: order still active, keep polling
        await asyncio.sleep(poll_interval)


async def _cancel_order(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    ord_id: str,
    api_key: str,
    secret: str,
    passphrase: str,
) -> None:
    """Cancel an order. Errors are logged but not raised."""
    path = "/api/v5/trade/cancel-order"
    payload = {"instId": inst_id, "ordId": ord_id}
    body = json.dumps(payload)
    headers = _auth_headers(api_key, secret, passphrase, "POST", path, body)
    try:
        async with session.post(path, data=body, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
        s_msg = ""
        s_code = ""
        rows = data.get("data") if isinstance(data, dict) else None
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            s_msg = str(rows[0].get("sMsg") or "")
            s_code = str(rows[0].get("sCode") or "")
        top_msg = (data.get("msg") or "") if isinstance(data, dict) else ""
        extra = []
        if s_code or s_msg:
            extra.append(f"sCode={s_code} sMsg={s_msg}".strip())
        if top_msg:
            extra.append(f"msg={top_msg}")
        logger.info(
            "cancel_order ok inst=%s ord=%s%s",
            inst_id,
            ord_id,
            (" " + " ".join(extra)) if extra else "",
        )
    except Exception as exc:
        logger.warning("cancel_order failed inst=%s ord=%s: %s", inst_id, ord_id, exc)


async def _okx_get_order_snapshot(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    ord_id: str,
    api_key: str,
    secret: str,
    passphrase: str,
) -> dict | None:
    """GET /api/v5/trade/order 单行，用于部分失败后按 ordId 拉取最终状态。"""
    params_str = f"instId={inst_id}&ordId={ord_id}"
    path = f"/api/v5/trade/order?{params_str}"
    headers = _auth_headers(api_key, secret, passphrase, "GET", path, "")
    try:
        async with session.get(
            "/api/v5/trade/order",
            params={"instId": inst_id, "ordId": ord_id},
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as exc:
        logger.warning("okx get order snapshot HTTP inst=%s ord=%s: %s", inst_id, ord_id, exc)
        return None
    if str(data.get("code", "0")) != "0":
        logger.warning(
            "okx get order snapshot API code=%s inst=%s ord=%s msg=%s",
            data.get("code"),
            inst_id,
            ord_id,
            data.get("msg"),
        )
        return None
    rows = data.get("data") or []
    if not rows or not isinstance(rows[0], dict):
        return None
    return rows[0]


def _okx_format_order_snapshot_text(row: dict) -> str:
    """Readable + compact raw JSON for DB last_error (truncated in set_order_last_error)."""
    prefer = (
        "instId",
        "ordId",
        "state",
        "side",
        "ordType",
        "px",
        "sz",
        "accFillSz",
        "fillSz",
        "avgPx",
        "fillPx",
        "notionalUsd",
        "fee",
        "feeCcy",
        "rebate",
        "cancelSource",
        "cTime",
        "uTime",
    )
    parts = [f"{k}={row.get(k)}" for k in prefer if row.get(k) not in (None, "")]
    headline = "; ".join(parts) if parts else "empty fields"
    try:
        raw = json.dumps(row, ensure_ascii=False, default=str)
    except TypeError:
        raw = str(row)
    if len(raw) > 1800:
        raw = raw[:1800] + "…"
    return headline + "\nraw=" + raw


async def _okx_backfill_open_entry_orders_detail(
    sqlite_path: str,
    *,
    position_id: int,
    order_ids_db: list[int],
    legs: list[dict],
    exch_order_ids: list[str | None],
    session: aiohttp.ClientSession,
    api_key: str,
    secret: str,
    passphrase: str,
    tag: str,
) -> None:
    """部分失败或提交异常后：按交易所 ordId 查询订单详情，追加写入各 orders.last_error。"""
    stamp = _db._utc_now_iso()
    for i, oid_db in enumerate(order_ids_db):
        if i >= len(legs):
            break
        eid = exch_order_ids[i] if i < len(exch_order_ids) else None
        if not eid:
            continue
        inst_id = legs[i]["inst_id"]
        leg = legs[i].get("leg", "?")
        snap = await _okx_get_order_snapshot(
            session,
            inst_id=inst_id,
            ord_id=eid,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
        )
        conn = sqlite3.connect(sqlite_path)
        try:
            cur = conn.execute("SELECT last_error FROM orders WHERE id=?", (oid_db,))
            row = cur.fetchone()
            prev = (row[0] or "").strip() if row else ""
            if snap is None:
                block = f"[OKX订单详情 @{stamp} leg={leg} ordId={eid}] 查询失败或无返回数据"
            else:
                block = (
                    f"[OKX订单详情 @{stamp} leg={leg} ordId={eid}]\n"
                    f"{_okx_format_order_snapshot_text(snap)}"
                )
            merged = f"{prev}\n{block}" if prev else block
            with conn:
                _db.set_order_last_error(conn, oid_db, merged)
        finally:
            conn.close()
    logger.info(
        "[submit_entry %s] pos=%d 已按 ordId 从 OKX 回填订单详情到 orders.last_error",
        tag,
        position_id,
    )


def _log_okx_open_poll_leg(
    tag: str,
    position_id: int,
    leg: dict,
    ord_id: str | None,
    fr: object,
) -> None:
    """Log one leg outcome after open-entry poll (filled / timeout / error)."""
    name = leg.get("leg", "?")
    iid = leg.get("inst_id", "?")
    oid = ord_id or "-"
    if isinstance(fr, Exception):
        logger.warning(
            "[submit_entry %s] pos=%d 开仓轮询 leg=%s inst=%s ordId=%s 结果=poll_error %s",
            tag,
            position_id,
            name,
            iid,
            oid,
            repr(fr),
        )
        return
    if fr is None:
        to = float(_POLL_TIMEOUT_SEC or 0.0)
        logger.warning(
            "[submit_entry %s] pos=%d 开仓轮询 leg=%s inst=%s ordId=%s "
            "结果=poll_timeout_or_canceled（约 %.0fs 内未 fully filled，或已为 canceled）",
            tag,
            position_id,
            name,
            iid,
            oid,
            to,
        )
        return
    fd = fr if isinstance(fr, dict) else {}
    state = fd.get("state", "")
    avg_px = fd.get("avgPx") or fd.get("px") or ""
    fill_sz = fd.get("fillSz") or fd.get("sz") or ""
    logger.info(
        "[submit_entry %s] pos=%d 开仓轮询 leg=%s inst=%s ordId=%s "
        "结果=filled state=%s avgPx=%s fillSz=%s",
        tag,
        position_id,
        name,
        iid,
        oid,
        state,
        avg_px,
        fill_sz,
    )


async def _noop_fill() -> None:
    """Placeholder coroutine for skipped poll slots."""
    return None


async def _submit_and_poll_exit_legs_okx(
    *,
    legs: list[dict],
    position_id: int,
    signal_id: int | None,
    leg_map: dict,
    api_key: str,
    secret: str,
    passphrase: str,
    sqlite_path: str,
    poll_timeout: float | None = _POLL_TIMEOUT_SEC,
) -> tuple[bool, float]:
    """Create orders, submit, poll for fills, update DB per-leg.

    Each leg dict must include ``qty`` (OKX 合约张数).
    Returns (all_filled, pnl_for_these_legs).
    Used by both the auto-retry path and the manual retry endpoint.
    """
    # Create DB records for the legs
    conn = sqlite3.connect(sqlite_path)
    db_ids: list[int] = []
    try:
        with conn:
            for leg in legs:
                oid = _db.create_order(
                    conn, signal_id=signal_id, position_id=position_id,
                    inst_id=leg["inst_id"], leg=leg["leg"], action="close",
                    side=leg["side"], limit_px=leg["px"], qty=float(leg["qty"]),
                )
                db_ids.append(oid)
    finally:
        conn.close()

    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as sess:
        # Submit
        exch_ids: list[str | None] = [None] * len(legs)
        try:
            submit_results = await asyncio.wait_for(
                asyncio.gather(
                    *[_place_order(
                        sess, inst_id=leg["inst_id"], td_mode="cross",
                        side=leg["side"], ord_type="limit", px=leg["px"],
                        sz=float(leg["qty"]), api_key=api_key, secret=secret,
                        passphrase=passphrase,
                        log_label=f"exit_retry pos={position_id} leg={leg.get('leg', '')}",
                    ) for leg in legs],
                    return_exceptions=True,
                ),
                timeout=_SUBMIT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            logger.error("[exit_legs] submit timed out pos=%d legs=%s",
                         position_id, [lg["leg"] for lg in legs])
            conn = sqlite3.connect(sqlite_path)
            try:
                with conn:
                    for oid in db_ids:
                        _db.update_order_status(conn, oid, "failed")
                        _db.set_order_last_error(conn, oid, "平仓下单超时")
            finally:
                conn.close()
            return False, 0.0

        for i, r in enumerate(submit_results):
            if not isinstance(r, Exception):
                exch_ids[i] = r

        # Store exchange IDs
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for i, oid in enumerate(db_ids):
                    if exch_ids[i]:
                        conn.execute("UPDATE orders SET exchange_order_id=? WHERE id=?",
                                     (exch_ids[i], oid))
        finally:
            conn.close()

        # Poll fills (use _noop_fill for legs whose submission failed)
        fill_results = await asyncio.gather(
            *[_poll_order_fill(sess, inst_id=legs[i]["inst_id"], ord_id=exch_ids[i],
                               api_key=api_key, secret=secret,
                               passphrase=passphrase,
                               poll_timeout=poll_timeout)
              if exch_ids[i] else _noop_fill()
              for i in range(len(legs))],
            return_exceptions=True,
        )

    # Per-leg DB update + PnL accumulation
    all_filled = True
    pnl = 0.0
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for i, oid in enumerate(db_ids):
                fr = fill_results[i]
                fd = fr if (not isinstance(fr, Exception) and fr is not None) else None
                if fd is not None:
                    _fee, _ccy = _okx_fee(fd)
                    _ord_type = str(fd.get("ordType") or "").lower() or None
                    _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0)
                    _db.update_order_status(
                        conn, oid, "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0),
                        filled_qty=_filled_qty if _filled_qty > 0 else None,
                        fee_type=_okx_fee_type(fd), actual_fee=_fee, fee_ccy=_ccy,
                        filled_at=_db._utc_now_iso(),
                        order_type=_ord_type,
                    )
                    leg = legs[i]
                    exit_px = float(fd.get("avgPx") or fd.get("px") or leg["px"])
                    entry_o = leg_map.get(leg["leg"])
                    entry_px = float(
                        (entry_o.get("filled_px") or entry_o.get("limit_px") or entry_o.get("entry_px") or 0.0)
                    ) if entry_o else 0.0
                    qleg = float(leg.get("qty") or 1.0)
                    if leg["side"] == "sell":
                        pnl += (exit_px - entry_px) * qleg
                    else:
                        pnl += (entry_px - exit_px) * qleg
                else:
                    all_filled = False
                    if isinstance(submit_results[i], Exception):
                        _db.update_order_status(conn, oid, "failed")
                        detail = "提交失败: " + str(submit_results[i])
                    elif isinstance(fr, Exception):
                        _db.update_order_status(conn, oid, "failed")
                        detail = "平仓失败: " + str(fr)
                    else:
                        _db.update_order_status(conn, oid, "pending")
                        detail = "轮询超时，等待成交中（已提交到交易所）"
                    _db.set_order_last_error(conn, oid, detail)
    finally:
        conn.close()

    return all_filled, pnl


async def _escalating_exit_loop_okx(
    *,
    session: aiohttp.ClientSession,
    failed_legs: list[dict],
    position_id: int,
    signal_id: int | None,
    api_key: str,
    secret: str,
    passphrase: str,
    sqlite_path: str,
    exit_started_at: float,    # time.monotonic() at first submit
    cfg: "AppConfig",
) -> tuple[bool, float]:
    """
    Escalating retry for failed exit legs: maker (one round) -> taker.
    Returns (all_filled, incremental_pnl).
    """
    maker_chase_secs = max(1, int(getattr(cfg, "maker_chase_secs", 60) or 60))
    max_chase_minutes = max(1, int(getattr(cfg, "maker_chase_max_minutes", 3) or 3))
    max_chase_secs = max_chase_minutes * 60.0

    # Record the highest existing close order ID before this cycle starts,
    # so the DB rebuild only looks at orders created in this escalation cycle.
    with sqlite3.connect(sqlite_path) as _init_conn:
        _baseline_row = _init_conn.execute(
            "SELECT COALESCE(MAX(id), 0) FROM orders WHERE position_id=? AND action='close'",
            (position_id,),
        ).fetchone()
        _baseline_order_id = _baseline_row[0] if _baseline_row else 0

    remaining = list(failed_legs)
    total_pnl = 0.0
    round_no = 1

    while remaining:
        elapsed = time.monotonic() - exit_started_at
        if elapsed >= max_chase_secs:
            logger.error(
                "exit_escalating [pos=%d]: exceed max chase window %.0fs",
                position_id,
                max_chase_secs,
            )
            break
        # 第一轮 maker；maker_chase_secs 超时后后续轮次全部 taker。
        use_taker = round_no > 1

        # Cancel stale orders (best effort)
        for leg in remaining:
            if leg.get("oid_db"):
                with sqlite3.connect(sqlite_path) as _conn:
                    with _conn:
                        _db.update_order_status(_conn, int(leg["oid_db"]), "canceled")
                        _db.set_order_last_error(
                            _conn,
                            int(leg["oid_db"]),
                            "策略撤单：maker 超时，已切换下一轮报价",
                        )
            if leg.get("exch_ord_id"):
                await _cancel_order(session, inst_id=leg["inst_id"],
                                    ord_id=leg["exch_ord_id"],
                                    api_key=api_key, secret=secret,
                                    passphrase=passphrase)

        # Refresh prices
        book_results = await asyncio.gather(
            *[_fetch_order_book_top(session, leg["inst_id"],
                                   api_key=api_key, secret=secret,
                                   passphrase=passphrase)
              for leg in remaining],
            return_exceptions=True,
        )

        retry_legs = []
        for i, leg in enumerate(remaining):
            br = book_results[i]
            if isinstance(br, Exception) or br is None:
                bid_px = ask_px = leg["last_px"]
            else:
                bid_px, ask_px = br
            if use_taker:
                px = ask_px if leg["side"] == "buy" else bid_px
            else:
                px = bid_px if leg["side"] == "buy" else ask_px
            retry_legs.append({**leg, "px": px, "exch_ord_id": None})

        # Update DB attempt tracking
        with sqlite3.connect(sqlite_path) as _conn:
            now_iso = _db._utc_now_iso()
            _conn.execute(
                "UPDATE positions SET exit_last_attempt_at=?, "
                "exit_attempt_count = exit_attempt_count + 1 "
                "WHERE id=?",
                (now_iso, position_id),
            )

        # Submit + poll
        poll_timeout = float(maker_chase_secs)
        filled, pnl = await _submit_and_poll_exit_legs_okx(
            legs=retry_legs,
            position_id=position_id,
            signal_id=signal_id,
            leg_map={lg["leg"]: lg for lg in retry_legs},
            api_key=api_key, secret=secret, passphrase=passphrase,
            sqlite_path=sqlite_path,
            poll_timeout=poll_timeout,
        )
        total_pnl += pnl

        if filled:
            remaining = []
        else:
            # Rebuild remaining from DB (source of truth for which legs are still failed)
            with sqlite3.connect(sqlite_path) as _conn:
                _conn.row_factory = sqlite3.Row
                failed_rows = _conn.execute(
                    "SELECT id, leg, inst_id, side FROM orders "
                    "WHERE position_id=? AND action='close' "
                    "AND status IN ('failed','pending') AND id > ?",
                    (position_id, _baseline_order_id),
                ).fetchall()
            leg_index = {lg["leg"]: lg for lg in retry_legs}
            db_remaining = [
                {**leg_index[r["leg"]], "oid_db": int(r["id"]), "exch_ord_id": None}
                for r in failed_rows
                if r["leg"] in leg_index
            ]
            # If DB has no failed rows (e.g. mock context or race), fall back to
            # retry_legs so we still escalate to taker on the next iteration.
            remaining = db_remaining if db_remaining else [
                {**lg, "exch_ord_id": None} for lg in retry_legs
            ]
        round_no += 1

    return len(remaining) == 0, total_pnl


_SUPPORTED_EXEC_EXCHANGES = {"okx", "deribit"}
_OKX_EXEC_EXCHANGES = {"okx"}


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


def _okx_parse_max_buy_sell(row: dict) -> tuple[float, float]:
    """Parse OKX /account/max-size data[0] maxBuy / maxSell (contract counts)."""

    def _f(k: str) -> float:
        v = row.get(k)
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    return _f("maxBuy"), _f("maxSell")


async def _okx_get_max_size_row(
    session: aiohttp.ClientSession,
    *,
    inst_id: str,
    td_mode: str,
    px: str,
    api_key: str,
    secret: str,
    passphrase: str,
) -> dict | None:
    """GET /api/v5/account/max-size — 交易所按统一账户与保证金规则返回当前可开张数。"""
    path = "/api/v5/account/max-size"
    params = {"instId": inst_id, "tdMode": td_mode, "px": px}
    qs = urlencode(params)
    full_path = f"{path}?{qs}"
    headers = _auth_headers(api_key, secret, passphrase, "GET", full_path, "")
    try:
        async with session.get(path, params=params, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except Exception as exc:
        logger.warning("max-size HTTP error inst=%s: %s", inst_id, exc)
        return None
    if str(data.get("code", "0")) != "0":
        logger.warning(
            "max-size API code=%s inst=%s msg=%s",
            data.get("code"),
            inst_id,
            data.get("msg"),
        )
        return None
    rows = data.get("data") or []
    return rows[0] if rows else None


async def _okx_preflight_max_size_legs(
    tag: str,
    session: aiohttp.ClientSession,
    legs: list[dict],
    sz: float,
    api_key: str,
    secret: str,
    passphrase: str,
) -> None:
    """开仓前：用 OKX max-size（含期权/期货保证金与组合规则）校验计划张数是否超过当前可开上限。

    说明：旧逻辑用 tradeable_qty×指数 当「名义」与权益比 —— tradeable_qty 是三腿深度换算的标的币数量，
    与「各腿统一张数 sz」下的保证金占用无关，会严重高估（例如 1 张 vs 近 1 BTC 名义）。
    官方文档：期权保证金公式见 https://www.okx.com/help/xiii-introduction-to-options-margin-calculation
    ；本处直接调用 GET /api/v5/account/max-size 与下单侧一致。
    """
    if sz <= 0:
        raise RuntimeError(f"[{tag}] 张数≤0，跳过")
    rows: list[dict | None] = await asyncio.gather(
        *[
            _okx_get_max_size_row(
                session,
                inst_id=leg["inst_id"],
                td_mode="cross",
                px=str(leg["px"]),
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
            )
            for leg in legs
        ]
    )
    if any(r is None for r in rows):
        logger.warning(
            "[%s] max-size 不可用，跳过开仓前可开张数校验（仍提交订单，由交易所拒单）",
            tag,
        )
        return
    tol = max(1e-9, sz * 1e-9)
    for leg, row in zip(legs, rows):
        assert row is not None
        max_buy, max_sell = _okx_parse_max_buy_sell(row)
        lim = max_buy if leg["side"] == "buy" else max_sell
        if sz > lim + tol:
            raise RuntimeError(
                f"[{tag}] {leg['leg']} {leg['inst_id']} 计划 {_format_okx_order_sz(sz)} 张，"
                f"超过账户当前可{'买' if leg['side'] == 'buy' else '卖'}上限 (≈{_format_okx_order_sz(lim)} 张)，跳过开仓"
            )


async def _check_and_cap_qty(
    qty: float,
    symbol: str,
    index_price_usdt: float,
    exchange_cfg: "ExchangeConfig",
    app_cfg: "AppConfig",
    lot_size: float,
) -> float:
    """
    根据账户余额约束截断 qty。
    - 名义价值 = qty × index_price_usdt
    - 上限 = total_eq_usdt × entry_max_trade_pct
    - 预留 = total_eq_usdt × entry_reserve_pct（可用权益必须超过此值）
    返回调整后的 qty（向下取整到 lot_size）。如果余额不足则返回 0.0。
    """
    from pcp_arbitrage import account_fetcher as _af

    bal = await _af.get_exchange_balance(exchange_cfg, app_cfg)
    if bal is None:
        logger.warning("[entry_sizing] 无法获取账户余额，跳过余额约束")
        return qty  # 无法获取时不限制，保持原逻辑

    total_eq = bal.get("total_eq_usdt", 0.0)
    adj_eq   = bal.get("adj_eq_usdt", 0.0)

    reserve_floor  = total_eq * app_cfg.entry_reserve_pct     # e.g. 10%
    trade_budget   = total_eq * app_cfg.entry_max_trade_pct   # e.g. 20%

    if adj_eq <= reserve_floor:
        logger.warning(
            "[entry_sizing] 可用权益 %.2f USDT ≤ 预留底线 %.2f USDT，跳过开仓",
            adj_eq, reserve_floor,
        )
        return 0.0

    usable = min(trade_budget, adj_eq - reserve_floor)
    max_qty = usable / index_price_usdt if index_price_usdt > 0 else 0.0

    if max_qty < qty:
        logger.info(
            "[entry_sizing] qty %.4f → %.4f（受余额约束: budget=%.2f USDT, index=%.2f）",
            qty, max_qty, usable, index_price_usdt,
        )

    capped = math.floor(max_qty / lot_size) * lot_size if lot_size > 0 else max_qty
    result = min(qty, capped)
    # Strip float precision noise (e.g., 0.30000000000000004 → 0.3)
    return round(result, 8)


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

    # --- 机会深度（张）；各腿统一 sz 在取整后由 OKX max-size 做保证金口径校验 ---
    depth_raw = float(signal.depth_contracts or 0.0)
    if depth_raw <= 0:
        depth_raw = 1.0
    if signal.tradeable_qty <= 0:
        raise RuntimeError(f"[{tag}] 可交易量为零（深度不足）")

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

    leg_contracts: list[float] = []
    try:
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as _sz_sess:
            inst_metas = await asyncio.gather(
                *[_okx_get_instrument(_sz_sess, leg["inst_id"]) for leg in legs],
            )
            floored: list[float] = []
            for i, leg in enumerate(legs):
                sz_i = _okx_floor_contracts_to_lot(depth_raw, inst_metas[i])
                floored.append(sz_i)
                logger.info(
                    "[okx sizing] pos=%d leg=%s inst=%s depth=%s -> sz=%s (lotSz=%s)",
                    position_id,
                    leg["leg"],
                    leg["inst_id"],
                    depth_raw,
                    sz_i,
                    inst_metas[i].get("lotSz"),
                )
            sz_common = min(floored)
            if sz_common <= 0:
                blocked: list[str] = []
                for i, leg in enumerate(legs):
                    if floored[i] > 0:
                        continue
                    lot_sz = inst_metas[i].get("lotSz")
                    blocked.append(
                        f"{leg['leg']}({leg['inst_id']}, lotSz={lot_sz})"
                    )
                raise RuntimeError(
                    "OKX 深度不足最小下单张数："
                    f"depth={depth_raw:g}，按 lot 取整后可下张数=0，"
                    f"受限腿={'; '.join(blocked) if blocked else 'unknown'}"
                )
            leg_contracts = [sz_common, sz_common, sz_common]
            await _okx_preflight_max_size_legs(
                tag,
                _sz_sess,
                legs,
                sz_common,
                api_key,
                secret,
                passphrase,
            )
    except Exception as exc:
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                _db.update_position_status(conn, position_id, "failed", last_error=str(exc))
        finally:
            conn.close()
        raise

    def _fmt_sz_zh(c: float) -> str:
        return f"{_format_okx_order_sz(c)} 张"

    qty = sz_common  # 各腿统一张数（与 DB / 对账一致）
    logger.info(
        "[submit_entry %s] 准备下单 | 仓位 pos=%d | 方向: %s | 机会深度(张)=%s | 标的币可交易量: %s | 各腿 sz=%s\n"
        "  call  %s  %s  %s  @ %.6f BTC\n"
        "  put   %s  %s  %s  @ %.6f BTC\n"
        "  future %s  %s  %s  @ %.2f USDT",
        tag,
        position_id,
        signal.direction,
        depth_raw,
        signal.tradeable_qty,
        _format_okx_order_sz(sz_common),
        triplet.call_id,
        legs[0]["side"],
        _fmt_sz_zh(leg_contracts[0]),
        legs[0]["px"],
        triplet.put_id,
        legs[1]["side"],
        _fmt_sz_zh(leg_contracts[1]),
        legs[1]["px"],
        triplet.future_id,
        legs[2]["side"],
        _fmt_sz_zh(leg_contracts[2]),
        legs[2]["px"],
    )

    # Create DB order records (pending)；qty 存 OKX 合约张数（与交易所 pos / fillSz 一致）
    conn = sqlite3.connect(sqlite_path)
    order_ids_db: list[int] = []
    try:
        with conn:
            for i, leg_spec in enumerate(legs):
                oid = _db.create_order(
                    conn,
                    signal_id=signal_id,
                    position_id=position_id,
                    inst_id=leg_spec["inst_id"],
                    leg=leg_spec["leg"],
                    action="open",
                    side=leg_spec["side"],
                    limit_px=leg_spec["px"],
                    qty=leg_contracts[i],
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
                            inst_id=legs[i]["inst_id"],
                            td_mode="cross",
                            side=legs[i]["side"],
                            ord_type="limit",
                            px=legs[i]["px"],
                            sz=leg_contracts[i],
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            log_label=f"open pos={position_id} leg={legs[i]['leg']}",
                        )
                        for i in range(3)
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
                api_key, secret, passphrase, exch_order_ids,
                reason=f"[{tag}] 下单超时，已取消",
                tag=tag,
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
            per_leg = [
                str(results[i]) if isinstance(results[i], Exception) else ""
                for i in range(3)
            ]
            await _mark_partial_failed(
                sqlite_path, position_id, order_ids_db, cfg, exchange_name, triplet, legs, session,
                api_key, secret, passphrase, exch_order_ids,
                reason=err_msg,
                per_order_reasons=per_leg,
                tag=tag,
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
                    poll_timeout=float(getattr(cfg, "maker_chase_secs", _POLL_TIMEOUT_SEC)),
                )
                for i, leg in enumerate(legs)
            ],
            return_exceptions=True,
        )

    for i, leg in enumerate(legs):
        _log_okx_open_poll_leg(tag, position_id, leg, exch_order_ids[i], fill_results[i])

    # Evaluate fills (first round)
    round_poll_timeout = float(getattr(cfg, "maker_chase_secs", _POLL_TIMEOUT_SEC))
    if round_poll_timeout <= 0:
        round_poll_timeout = float(_POLL_TIMEOUT_SEC)
    max_chase_minutes = int(getattr(cfg, "maker_chase_max_minutes", 3) or 3)
    if max_chase_minutes <= 0:
        max_chase_minutes = 3
    max_chase_rounds = max(1, int(math.ceil((max_chase_minutes * 60.0) / round_poll_timeout)))

    all_filled = True
    filled_data: list[dict | None] = []
    unfilled_idx: list[int] = []
    for i, fr in enumerate(fill_results):
        if isinstance(fr, Exception) or fr is None:
            all_filled = False
            filled_data.append(None)
            unfilled_idx.append(i)
        else:
            filled_data.append(fr)

    # persist first round
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for i, oid_db in enumerate(order_ids_db):
                fd = filled_data[i]
                if fd is None:
                    _db.update_order_status(conn, oid_db, "failed")
                    leg_name = legs[i]["leg"]
                    per_msg = f"[{tag}] 首轮{round_poll_timeout:.0f}s未成交，进入追单（{leg_name}）"
                    _db.set_order_last_error(conn, oid_db, per_msg)
                else:
                    _fee, _ccy = _okx_fee(fd)
                    _ord_type = str(fd.get("ordType") or "").lower() or None
                    _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0)
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
    finally:
        conn.close()

    if not all_filled:
        logger.info(
            "[submit_entry %s] pos=%d 首轮未全成，开始追单：每轮 %.0fs，最长 %d 分钟",
            tag,
            position_id,
            round_poll_timeout,
            max_chase_minutes,
        )
        round_no = 1
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session2:
            current_specs: list[dict] = [dict(legs[i]) for i in unfilled_idx]
            current_qtys: list[float] = [leg_contracts[i] for i in unfilled_idx]
            current_exch_ids: list[str | None] = [exch_order_ids[i] for i in unfilled_idx]
            current_order_ids_db: list[int] = [order_ids_db[i] for i in unfilled_idx]
            while current_specs and round_no <= max_chase_rounds:
                for i, spec in enumerate(current_specs):
                    if current_exch_ids[i]:
                        await _cancel_order(
                            session2,
                            inst_id=spec["inst_id"],
                            ord_id=current_exch_ids[i],  # type: ignore[arg-type]
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                        )
                await _okx_backfill_open_entry_orders_detail(
                    sqlite_path,
                    position_id=position_id,
                    order_ids_db=current_order_ids_db,
                    legs=current_specs,
                    exch_order_ids=current_exch_ids,
                    session=session2,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    tag=f"{tag}-chase-{round_no}",
                )

                retry_specs: list[dict] = []
                for spec in current_specs:
                    leg = dict(spec)
                    bid, ask = await _fetch_order_book_top(
                        session2,
                        leg["inst_id"],
                        api_key=api_key,
                        secret=secret,
                        passphrase=passphrase,
                    )
                    old_px = float(leg["px"])
                    if str(leg["side"]).lower() == "buy":
                        leg["px"] = float(ask) if ask is not None else old_px
                    else:
                        leg["px"] = float(bid) if bid is not None else old_px
                    retry_specs.append(leg)

                retry_order_ids_db = []
                conn = sqlite3.connect(sqlite_path)
                try:
                    with conn:
                        for i, leg in enumerate(retry_specs):
                            oid = _db.create_order(
                                conn,
                                signal_id=signal_id,
                                position_id=position_id,
                                inst_id=leg["inst_id"],
                                leg=leg["leg"],
                                action="open",
                                side=leg["side"],
                                limit_px=leg["px"],
                                qty=current_qtys[i],
                            )
                            retry_order_ids_db.append(oid)
                finally:
                    conn.close()

                retry_order_ids_exch: list[str | None] = [None] * len(retry_specs)
                submit_retry = await asyncio.gather(
                    *[
                        _place_order(
                            session2,
                            inst_id=retry_specs[i]["inst_id"],
                            td_mode="cross",
                            side=retry_specs[i]["side"],
                            ord_type="limit",
                            px=retry_specs[i]["px"],
                            sz=current_qtys[i],
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            log_label=f"open_chase#{round_no} pos={position_id} leg={retry_specs[i]['leg']}",
                        )
                        for i in range(len(retry_specs))
                    ],
                    return_exceptions=True,
                )
                for i, sr in enumerate(submit_retry):
                    if not isinstance(sr, Exception):
                        retry_order_ids_exch[i] = sr

                conn = sqlite3.connect(sqlite_path)
                try:
                    with conn:
                        for i, oid_db in enumerate(retry_order_ids_db):
                            if retry_order_ids_exch[i]:
                                conn.execute(
                                    "UPDATE orders SET exchange_order_id=? WHERE id=?",
                                    (retry_order_ids_exch[i], oid_db),
                                )
                finally:
                    conn.close()

                this_round_timeout = round_poll_timeout
                retry_fill_results = await asyncio.gather(
                    *[
                        _poll_order_fill(
                            session2,
                            inst_id=retry_specs[i]["inst_id"],
                            ord_id=retry_order_ids_exch[i],  # type: ignore[arg-type]
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            poll_timeout=this_round_timeout,
                        ) if retry_order_ids_exch[i] else _noop_fill()
                        for i in range(len(retry_specs))
                    ],
                    return_exceptions=True,
                )

                next_specs: list[dict] = []
                next_qtys: list[float] = []
                next_exch_ids: list[str | None] = []
                next_order_ids_db: list[int] = []
                conn = sqlite3.connect(sqlite_path)
                try:
                    with conn:
                        for i, oid_db in enumerate(retry_order_ids_db):
                            leg = retry_specs[i]
                            fr = retry_fill_results[i]
                            _log_okx_open_poll_leg(tag, position_id, leg, retry_order_ids_exch[i], fr)
                            fd = fr if (not isinstance(fr, Exception) and fr is not None) else None
                            if fd is not None:
                                _fee, _ccy = _okx_fee(fd)
                                _ord_type = str(fd.get("ordType") or "").lower() or None
                                _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0)
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
                            else:
                                _db.update_order_status(conn, oid_db, "failed")
                                _db.set_order_last_error(
                                    conn,
                                    oid_db,
                                    f"[{tag}] 追单第{round_no}轮 {this_round_timeout:.0f}s 未成交（{leg.get('leg', '?')}）",
                                )
                                next_specs.append(leg)
                                next_qtys.append(current_qtys[i])
                                next_exch_ids.append(retry_order_ids_exch[i])
                                next_order_ids_db.append(oid_db)
                finally:
                    conn.close()

                current_specs = next_specs
                current_qtys = next_qtys
                current_exch_ids = next_exch_ids
                current_order_ids_db = next_order_ids_db
                all_filled = len(current_specs) == 0
                if all_filled:
                    break
                round_no += 1

            if not all_filled and current_specs:
                for i, spec in enumerate(current_specs):
                    if current_exch_ids[i]:
                        await _cancel_order(
                            session2,
                            inst_id=spec["inst_id"],
                            ord_id=current_exch_ids[i],  # type: ignore[arg-type]
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                        )
                await _okx_backfill_open_entry_orders_detail(
                    sqlite_path,
                    position_id=position_id,
                    order_ids_db=current_order_ids_db,
                    legs=current_specs,
                    exch_order_ids=current_exch_ids,
                    session=session2,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    tag=f"{tag}-chase-final",
                )
                logger.warning(
                    "[submit_entry %s] pos=%d 开仓追单达到上限（%d 分钟 / %d 轮）仍未全部成交",
                    tag,
                    position_id,
                    max_chase_minutes,
                    max_chase_rounds,
                )

    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            _db.update_position_status(conn, position_id, "open" if all_filled else "partial_failed")
    finally:
        conn.close()

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

    # --- Check and cap qty by account balance ---
    qty = await _check_and_cap_qty(
        qty=qty,
        symbol=triplet.symbol,
        index_price_usdt=signal.index_for_fee_usdt,
        exchange_cfg=exc_cfg,
        app_cfg=cfg,
        lot_size=lot_size,
    )
    if qty <= 0:
        raise RuntimeError(f"[{tag}] 账户余额不足，跳过开仓")

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
                        _db.set_order_last_error(conn, oid_db, f"[{tag}] 下单超时，已取消")
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
                    for i, oid_db in enumerate(order_ids_db):
                        _db.update_order_status(conn, oid_db, "failed")
                        detail = str(results[i]) if isinstance(results[i], Exception) else err_msg
                        _db.set_order_last_error(conn, oid_db, detail)
                    _db.update_position_status(
                        conn, position_id, "partial_failed", last_error=err_msg
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
                        leg_name = legs[i]["leg"]
                        per_msg = f"[{tag}] 本腿未成交（{leg_name}）"
                        _db.set_order_last_error(conn, oid_db, per_msg)
                    else:
                        _filled_qty = float(fd.get("filled_amount") or fd.get("amount") or 0) if fd else 0.0
                        _db.update_order_status(
                            conn, oid_db, "filled",
                            filled_px=float(fd.get("average_price") or fd.get("price") or 0),
                            filled_qty=_filled_qty if _filled_qty > 0 else None,
                            fee_type=fee_types[i],
                            filled_at=_db._utc_now_iso(),
                        )
                _db.update_position_status(conn, position_id, "partial_failed")
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
    exch_order_ids: list[str | None],
    *,
    reason: str,
    per_order_reasons: list[str] | None = None,
    tag: str | None = None,
) -> None:
    """Mark position as partial_failed and cancel any submitted orders."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for i, oid_db in enumerate(order_ids_db):
                _db.update_order_status(conn, oid_db, "failed")
                msg = (
                    per_order_reasons[i]
                    if per_order_reasons and i < len(per_order_reasons) and per_order_reasons[i]
                    else reason
                )
                _db.set_order_last_error(conn, oid_db, msg)
            _db.update_position_status(
                conn, position_id, "partial_failed", last_error=reason
            )
    finally:
        conn.close()

    logger.info(
        "[submit_entry] pos=%d 提交阶段失败或超时，撤单已下订单 | %s",
        position_id,
        reason[:200] + ("…" if len(reason) > 200 else ""),
    )
    for i, exch_oid in enumerate(exch_order_ids):
        if exch_oid:
            await _cancel_order(
                session,
                inst_id=legs[i]["inst_id"],
                ord_id=exch_oid,
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
            )

    log_tag = tag if tag else exchange_name
    await _okx_backfill_open_entry_orders_detail(
        sqlite_path,
        position_id=position_id,
        order_ids_db=order_ids_db,
        legs=legs,
        exch_order_ids=exch_order_ids,
        session=session,
        api_key=api_key,
        secret=secret,
        passphrase=passphrase,
        tag=log_tag,
    )


async def _fetch_order_book_top(
    session: aiohttp.ClientSession,
    inst_id: str,
    *,
    api_key: str,
    secret: str,
    passphrase: str,
) -> tuple[float | None, float | None]:
    """Fetch top-of-book (best_bid, best_ask) from OKX.

    Returns (bid, ask) or (None, None) on failure.
    """
    path = "/api/v5/market/books"
    params = {"instId": inst_id, "sz": "1"}
    params_str = f"instId={inst_id}&sz=1"
    full_path = f"{path}?{params_str}"
    headers = _auth_headers(api_key, secret, passphrase, "GET", full_path, "")
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


async def reconcile_close_orders_status(
    *,
    position_id: int,
    cfg: "AppConfig",
    sqlite_path: str,
    statuses: tuple[str, ...] = ("pending", "failed"),
) -> dict[str, int]:
    """Reconcile close-order statuses with exchange snapshots.

    For OKX close orders in pending/failed, pull exchange state and map:
    - filled -> filled (write fill/fee/order_type)
    - live/partially_filled -> pending
    - canceled -> canceled
    """
    exchange = ""
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        prow = conn.execute(
            "SELECT exchange FROM positions WHERE id=?",
            (position_id,),
        ).fetchone()
        if prow and prow[0]:
            exchange = str(prow[0]).lower()
        placeholders = ",".join("?" for _ in statuses)
        rows = [dict(r) for r in conn.execute(
            "SELECT id, inst_id, exchange_order_id, status FROM orders "
            "WHERE position_id=? AND action='close' "
            f"AND status IN ({placeholders}) "
            "AND exchange_order_id IS NOT NULL AND exchange_order_id != '' "
            "AND inst_id IS NOT NULL AND inst_id != ''",
            (position_id, *statuses),
        ).fetchall()]
    finally:
        conn.close()

    if exchange not in _OKX_EXEC_EXCHANGES or not rows:
        return {"filled": 0, "pending": 0, "canceled": 0, "unchanged": 0}

    exc_cfg = cfg.exchanges.get(exchange.upper()) or cfg.exchanges.get(exchange)
    if exc_cfg is None:
        return {"filled": 0, "pending": 0, "canceled": 0, "unchanged": 0}

    stats = {"filled": 0, "pending": 0, "canceled": 0, "unchanged": 0}
    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        for od in rows:
            snap = await _okx_get_order_snapshot(
                session,
                inst_id=str(od["inst_id"]),
                ord_id=str(od["exchange_order_id"]),
                api_key=exc_cfg.api_key,
                secret=exc_cfg.secret_key,
                passphrase=exc_cfg.passphrase,
            )
            if not snap:
                stats["unchanged"] += 1
                continue
            state = str(snap.get("state") or "").lower()
            order_id = int(od["id"])
            conn2 = sqlite3.connect(sqlite_path)
            try:
                with conn2:
                    if state == "filled":
                        _fee, _ccy = _okx_fee(snap)
                        _ord_type = str(snap.get("ordType") or "").lower() or None
                        _filled_qty = float(snap.get("fillSz") or snap.get("sz") or 0)
                        _db.update_order_status(
                            conn2,
                            order_id,
                            "filled",
                            filled_px=float(snap.get("avgPx") or snap.get("px") or 0),
                            filled_qty=_filled_qty if _filled_qty > 0 else None,
                            fee_type=_okx_fee_type(snap),
                            actual_fee=_fee,
                            fee_ccy=_ccy,
                            filled_at=_db._utc_now_iso(),
                            order_type=_ord_type,
                        )
                        stats["filled"] += 1
                    elif state in ("live", "partially_filled"):
                        _db.update_order_status(conn2, order_id, "pending")
                        _db.set_order_last_error(conn2, order_id, "等待成交中（已提交到交易所）")
                        stats["pending"] += 1
                    elif state == "canceled":
                        _db.update_order_status(conn2, order_id, "canceled")
                        _db.set_order_last_error(conn2, order_id, "交易所显示已撤单")
                        stats["canceled"] += 1
                    else:
                        stats["unchanged"] += 1
            finally:
                conn2.close()
    return stats


async def retry_exit_position(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> tuple[bool, str]:
    """手动三腿平仓：优先重试失败平仓腿，否则对已成交开仓腿发起平仓。"""
    exchange = str(position.get("exchange", "")).lower()
    if exchange not in _OKX_EXEC_EXCHANGES:
        return False, f"暂不支持 {exchange} 的手动重试平仓"

    position_id = position["id"]
    conn_ts = sqlite3.connect(sqlite_path)
    try:
        with conn_ts:
            _db.update_position_target_state(conn_ts, int(position_id), "closed")
    finally:
        conn_ts.close()
    exc_cfg = cfg.exchanges.get(exchange.upper()) or cfg.exchanges.get(exchange)
    if exc_cfg is None:
        return False, f"{exchange} 未配置"

    # Find failed close orders and entry orders for PnL/fallback close.
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        failed_orders = [dict(r) for r in conn.execute(
            "SELECT leg, inst_id, side, qty FROM orders "
            "WHERE position_id=? AND action='close' AND status='failed' ORDER BY id",
            (position_id,),
        ).fetchall()]
        entry_orders = [dict(r) for r in conn.execute(
            "SELECT leg, inst_id, side, qty, filled_qty, status, limit_px, filled_px FROM orders "
            "WHERE position_id=? AND action='open'",
            (position_id,),
        ).fetchall()]
        close_filled_legs = {
            str(r[0])
            for r in conn.execute(
                "SELECT DISTINCT leg FROM orders "
                "WHERE position_id=? AND action='close' AND status='filled'",
                (position_id,),
            ).fetchall()
            if r[0]
        }
    finally:
        conn.close()

    # 全部平仓仅基于 open+filled 的腿，不处理 failed 的平仓委托单
    legs_from_open_filled: list[dict] = []
    for o in entry_orders:
        leg = str(o.get("leg") or "")
        if str(o.get("status") or "") != "filled":
            continue
        if leg in close_filled_legs:
            continue
        side_open = str(o.get("side") or "").lower()
        if side_open not in ("buy", "sell"):
            continue
        close_side = "sell" if side_open == "buy" else "buy"
        qty = float(o.get("filled_qty") or o.get("qty") or 0.0)
        if qty <= 0:
            continue
        legs_from_open_filled.append(
            {
                "leg": leg,
                "inst_id": o.get("inst_id"),
                "side": close_side,
                "qty": qty,
            }
        )
    failed_orders = [o for o in legs_from_open_filled if o.get("inst_id")]
    if not failed_orders:
        return False, "没有可平仓的已成交开仓腿"

    leg_map = {o["leg"]: o for o in entry_orders}
    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key
    passphrase = exc_cfg.passphrase
    from_failed_close = False

    # For fallback (open-filled -> close), verify each filled leg against exchange live position.
    # If exchange qty differs from DB, close the overlap qty only.
    if not from_failed_close:
        checked_orders: list[dict] = []
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as sess_chk:
            for o in failed_orders:
                inst_id = str(o.get("inst_id") or "")
                if not inst_id:
                    continue
                pos_signed = await _okx_get_signed_position(
                    sess_chk,
                    inst_id,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                )
                if pos_signed is None:
                    continue
                live_qty = abs(float(pos_signed))
                if live_qty <= _OKX_POSITION_QTY_TOL:
                    continue
                db_qty = float(o.get("qty") or 0.0)
                close_qty = min(db_qty, live_qty)
                if close_qty <= _OKX_POSITION_QTY_TOL:
                    continue
                close_side = "sell" if pos_signed > 0 else "buy"
                checked_orders.append(
                    {
                        **o,
                        "side": close_side,
                        "qty": close_qty,
                    }
                )
        failed_orders = checked_orders
        if not failed_orders:
            return False, "交易所未找到可对应平仓的 filled 腿持仓"

    # 不再要求三腿完全一致；仅对可匹配的 filled 腿执行平仓
    # Fetch fresh passive prices
    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as sess:
        book_results = await asyncio.gather(
            *[_fetch_order_book_top(sess, o["inst_id"], api_key=api_key,
                                   secret=secret, passphrase=passphrase)
              for o in failed_orders],
            return_exceptions=True,
        )

    retry_legs = []
    for i, o in enumerate(failed_orders):
        book = book_results[i]
        if isinstance(book, Exception) or book == (None, None):
            return False, f"无法获取 {o['inst_id']} 盘口数据，请稍后重试"
        bid, ask = book
        px = bid if o["side"] == "buy" else ask  # passive maker
        retry_legs.append(
            {
                "inst_id": o["inst_id"],
                "side": o["side"],
                "leg": o["leg"],
                "px": px,
                "qty": float(o.get("qty") or 1.0),
            }
        )

    all_filled, pnl = await _submit_and_poll_exit_legs_okx(
        legs=retry_legs,
        position_id=position_id,
        signal_id=position.get("signal_id"),
        leg_map=leg_map,
        api_key=api_key,
        secret=secret,
        passphrase=passphrase,
        sqlite_path=sqlite_path,
    )
    if not all_filled:
        with sqlite3.connect(sqlite_path) as _conn:
            _conn.row_factory = sqlite3.Row
            rem_rows = _conn.execute(
                "SELECT id, leg, inst_id, side, qty, limit_px, exchange_order_id "
                "FROM orders WHERE position_id=? AND action='close' "
                "AND status IN ('pending','failed') ORDER BY id DESC",
                (position_id,),
            ).fetchall()
        rem_by_leg: dict[str, sqlite3.Row] = {}
        for r in rem_rows:
            lg = str(r["leg"] or "")
            if lg and lg not in rem_by_leg:
                rem_by_leg[lg] = r
        failed_legs = []
        for leg in retry_legs:
            lg = str(leg.get("leg") or "")
            row_rem = rem_by_leg.get(lg)
            if row_rem is None:
                continue
            entry_ref = leg_map.get(lg) or {}
            failed_legs.append(
                {
                    "leg": lg,
                    "inst_id": str(row_rem["inst_id"]),
                    "side": str(row_rem["side"]),
                    "qty": float(row_rem["qty"] or leg.get("qty") or 0.0),
                    "entry_px": float(entry_ref.get("filled_px") or entry_ref.get("limit_px") or 0.0),
                    "oid_db": int(row_rem["id"]),
                    "last_px": float(row_rem["limit_px"] or leg.get("px") or 0.0),
                    "exch_ord_id": row_rem["exchange_order_id"],
                }
            )
        if failed_legs:
            async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as _loop_sess:
                loop_filled, loop_pnl = await _escalating_exit_loop_okx(
                    session=_loop_sess,
                    failed_legs=failed_legs,
                    position_id=position_id,
                    signal_id=position.get("signal_id"),
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    sqlite_path=sqlite_path,
                    exit_started_at=time.monotonic(),
                    cfg=cfg,
                )
            all_filled = loop_filled
            pnl += loop_pnl
    await reconcile_close_orders_status(
        position_id=position_id,
        cfg=cfg,
        sqlite_path=sqlite_path,
    )

    # Update position status
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        with conn:
            ef = {str(o["leg"]) for o in entry_orders if str(o.get("status") or "") == "filled"}
            cf = _close_filled_legs_set(conn, position_id)
            fully_exited = bool(ef) and ef.issubset(cf)
            existing = conn.execute(
                "SELECT realized_pnl_usdt FROM positions WHERE id=?", (position_id,)
            ).fetchone()
            existing_pnl = float(existing[0] or 0) if existing else 0.0
            if fully_exited:
                _db.update_position_status(
                    conn, position_id, "closed",
                    realized_pnl_usdt=existing_pnl + pnl,
                    closed_at=_db._utc_now_iso(),
                )
            else:
                for row in conn.execute(
                    "SELECT id, leg, status FROM orders "
                    "WHERE position_id=? AND action='close' AND status IN ('failed','pending')",
                    (position_id,),
                ):
                    leg = row[1] or "?"
                    st = str(row[2] or "")
                    msg = (
                        f"等待成交中（{leg}）"
                        if st == "pending"
                        else f"平仓未成交或已撤单（{leg}）"
                    )
                    _db.set_order_last_error(conn, int(row[0]), msg)
                _db.update_position_status(conn, position_id, "partial_failed")
    finally:
        conn.close()

    if fully_exited:
        return True, "三腿平仓成功，仓位已全部平仓"
    return False, "三腿平仓后仍有腿未成交，请查看订单详情"


def _close_filled_legs_set(conn: sqlite3.Connection, position_id: int) -> set[str]:
    return {
        str(r[0])
        for r in conn.execute(
            "SELECT DISTINCT leg FROM orders WHERE position_id=? AND action='close' AND status='filled'",
            (position_id,),
        ).fetchall()
        if r[0]
    }


async def submit_exit_single_leg(
    *,
    position_id: int,
    order_id: int,
    cfg: "AppConfig",
    sqlite_path: str,
) -> tuple[bool, str]:
    """对已成交的一条开仓腿提交平仓（OKX）。返回 (是否全部成交, 说明)。"""
    conn_ts = sqlite3.connect(sqlite_path)
    try:
        with conn_ts:
            _db.update_position_target_state(conn_ts, int(position_id), "closed")
    finally:
        conn_ts.close()

    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        orow = conn.execute(
            "SELECT id, position_id, leg, inst_id, action, status FROM orders WHERE id=?",
            (order_id,),
        ).fetchone()
        if orow is None:
            return False, f"订单 {order_id} 不存在"
        od = dict(orow)
        if int(od["position_id"]) != position_id:
            return False, "订单不属于该持仓"
        if od.get("action") != "open":
            return False, "只能对已开仓订单发起单腿平仓"
        if od.get("status") != "filled":
            return False, "只有已成交的开仓单可以单腿平仓"
        leg = str(od.get("leg") or "")
        if leg not in ("call", "put", "future"):
            return False, "无效的腿类型"
        dup = conn.execute(
            "SELECT 1 FROM orders WHERE position_id=? AND leg=? AND action='close' AND status='filled' LIMIT 1",
            (position_id, leg),
        ).fetchone()
        if dup:
            return False, "该腿已有已成交的平仓单"
        prow = conn.execute(
            "SELECT id, exchange, symbol, expiry, strike, direction, status, signal_id FROM positions WHERE id=?",
            (position_id,),
        ).fetchone()
        if prow is None:
            return False, "持仓不存在"
        pos = dict(prow)
        st = str(pos.get("status") or "")
        if st not in ("open", "partial_failed"):
            return False, f"持仓状态为 {st}，不能单腿平仓"
    finally:
        conn.close()

    if str(pos.get("exchange", "")).lower() not in _OKX_EXEC_EXCHANGES:
        return False, "暂仅支持 OKX 单腿平仓"

    exc_name = str(pos.get("exchange", "OKX"))
    exc_cfg = cfg.exchanges.get(exc_name.upper()) or cfg.exchanges.get(exc_name.lower())
    if exc_cfg is None:
        return False, "交易所未配置"

    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders = _db.get_position_orders(conn, position_id, action="open")
    finally:
        conn.close()
    leg_map: dict[str, dict] = {o["leg"]: o for o in entry_orders}
    if leg not in leg_map:
        return False, "找不到该腿的开仓记录"

    symbol = pos["symbol"]
    expiry = pos["expiry"]
    strike = pos["strike"]
    direction = pos["direction"]
    exp_clean = expiry.replace("-", "")
    if len(exp_clean) == 8:
        exp_clean = exp_clean[2:]
    call_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-C"
    put_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-P"
    future_inst_id = f"{symbol}-USD-{exp_clean}"
    call_inst_id = (leg_map.get("call") or {}).get("inst_id") or call_inst_id
    put_inst_id = (leg_map.get("put") or {}).get("inst_id") or put_inst_id
    future_inst_id = (leg_map.get("future") or {}).get("inst_id") or future_inst_id

    api_key = exc_cfg.api_key
    secret = exc_cfg.secret_key
    passphrase = exc_cfg.passphrase

    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        book_results = await asyncio.gather(
            _fetch_order_book_top(session, call_inst_id, api_key=api_key, secret=secret, passphrase=passphrase),
            _fetch_order_book_top(session, put_inst_id, api_key=api_key, secret=secret, passphrase=passphrase),
            _fetch_order_book_top(session, future_inst_id, api_key=api_key, secret=secret, passphrase=passphrase),
            return_exceptions=True,
        )

        def _safe_book(result: object, leg_name: str, use_bid: bool) -> float:
            if isinstance(result, Exception) or result == (None, None):
                fallback = float((leg_map.get(leg_name) or {}).get("limit_px") or 0.0) or 0.0
                logger.warning("submit_exit_single: book failed for %s, using %s", leg_name, fallback)
                return fallback
            bid, ask = result  # type: ignore[misc]
            px = bid if use_bid else ask
            if px is None:
                fallback = float((leg_map.get(leg_name) or {}).get("limit_px") or 0.0) or 0.0
                return fallback
            return float(px)

        if direction == "forward":
            specs = {
                "call": {"leg": "call", "inst_id": call_inst_id, "side": "sell", "px": _safe_book(book_results[0], "call", False)},
                "put": {"leg": "put", "inst_id": put_inst_id, "side": "buy", "px": _safe_book(book_results[1], "put", True)},
                "future": {"leg": "future", "inst_id": future_inst_id, "side": "buy", "px": _safe_book(book_results[2], "future", True)},
            }
        else:
            specs = {
                "call": {"leg": "call", "inst_id": call_inst_id, "side": "buy", "px": _safe_book(book_results[0], "call", True)},
                "put": {"leg": "put", "inst_id": put_inst_id, "side": "sell", "px": _safe_book(book_results[1], "put", False)},
                "future": {"leg": "future", "inst_id": future_inst_id, "side": "sell", "px": _safe_book(book_results[2], "future", False)},
            }
        leg_spec = specs[leg]
        qty = float(leg_map[leg].get("qty", 1.0) or 1.0)
        exit_leg = {**leg_spec, "qty": qty}

    all_filled, pnl = await _submit_and_poll_exit_legs_okx(
        legs=[exit_leg],
        position_id=position_id,
        signal_id=pos.get("signal_id"),
        leg_map=leg_map,
        api_key=api_key,
        secret=secret,
        passphrase=passphrase,
        sqlite_path=sqlite_path,
    )
    if not all_filled:
        with sqlite3.connect(sqlite_path) as _conn:
            _conn.row_factory = sqlite3.Row
            row_rem = _conn.execute(
                "SELECT id, leg, inst_id, side, qty, limit_px, exchange_order_id "
                "FROM orders WHERE position_id=? AND action='close' AND leg=? "
                "AND status IN ('pending','failed') ORDER BY id DESC LIMIT 1",
                (position_id, leg),
            ).fetchone()
        if row_rem is not None:
            entry_ref = leg_map.get(leg) or {}
            failed_leg = {
                "leg": str(row_rem["leg"]),
                "inst_id": str(row_rem["inst_id"]),
                "side": str(row_rem["side"]),
                "qty": float(row_rem["qty"] or exit_leg.get("qty") or 0.0),
                "entry_px": float(entry_ref.get("filled_px") or entry_ref.get("limit_px") or 0.0),
                "oid_db": int(row_rem["id"]),
                "last_px": float(row_rem["limit_px"] or exit_leg.get("px") or 0.0),
                "exch_ord_id": row_rem["exchange_order_id"],
            }
            async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as _loop_sess:
                loop_filled, loop_pnl = await _escalating_exit_loop_okx(
                    session=_loop_sess,
                    failed_legs=[failed_leg],
                    position_id=position_id,
                    signal_id=pos.get("signal_id"),
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    sqlite_path=sqlite_path,
                    exit_started_at=time.monotonic(),
                    cfg=cfg,
                )
            all_filled = loop_filled
            pnl += loop_pnl
    await reconcile_close_orders_status(
        position_id=position_id,
        cfg=cfg,
        sqlite_path=sqlite_path,
    )

    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders2 = _db.get_position_orders(conn, position_id, action="open")
        ef = {str(o["leg"]) for o in entry_orders2 if o.get("status") == "filled"}
        cf = _close_filled_legs_set(conn, position_id)
        row = conn.execute("SELECT realized_pnl_usdt FROM positions WHERE id=?", (position_id,)).fetchone()
        prev = float(row[0] or 0) if row else 0.0
        new_total = prev + pnl
        fully_exited = bool(ef) and ef.issubset(cf)
        with conn:
            if all_filled and fully_exited:
                _db.update_position_status(
                    conn, position_id, "closed",
                    realized_pnl_usdt=new_total,
                    closed_at=_db._utc_now_iso(),
                )
            else:
                _db.update_position_status(
                    conn, position_id, "partial_failed",
                    realized_pnl_usdt=new_total,
                    closed_at=None,
                )
    finally:
        conn.close()

    if all_filled:
        return True, f"单腿 {leg} 平仓已成交（累计盈亏约 {new_total:+.2f} USDT）"
    return False, "单腿平仓未完全成交，请查看订单状态"


async def cancel_close_order(
    *,
    position_id: int,
    order_id: int,
    cfg: "AppConfig",
    sqlite_path: str,
) -> tuple[bool, str]:
    """取消一条 pending 的 close 委托，并在本地标记为 canceled。"""
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id, position_id, action, status, inst_id, exchange_order_id "
            "FROM orders WHERE id=?",
            (order_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return False, f"订单 {order_id} 不存在"
    od = dict(row)
    if int(od.get("position_id") or 0) != int(position_id):
        return False, "订单不属于该持仓"
    if str(od.get("action") or "") != "close":
        return False, "只能取消平仓委托单"
    if str(od.get("status") or "") != "pending":
        return False, f"当前状态为 {od.get('status')}，仅 pending 可取消"

    inst_id = str(od.get("inst_id") or "")
    exch_order_id = str(od.get("exchange_order_id") or "")
    exchange = "okx"
    c2 = sqlite3.connect(sqlite_path)
    try:
        c2.row_factory = sqlite3.Row
        prow = c2.execute("SELECT exchange FROM positions WHERE id=?", (position_id,)).fetchone()
        if prow and prow[0]:
            exchange = str(prow[0]).lower()
    finally:
        c2.close()

    if exchange in _OKX_EXEC_EXCHANGES and inst_id and exch_order_id:
        exc_cfg = cfg.exchanges.get(exchange.upper()) or cfg.exchanges.get(exchange)
        if exc_cfg is None:
            return False, f"{exchange} 未配置"
        async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
            await _cancel_order(
                session,
                inst_id=inst_id,
                ord_id=exch_order_id,
                api_key=exc_cfg.api_key,
                secret=exc_cfg.secret_key,
                passphrase=exc_cfg.passphrase,
            )

    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            _db.update_order_status(conn, order_id, "canceled")
            _db.set_order_last_error(conn, order_id, "用户手动取消平仓委托")
    finally:
        conn.close()
    return True, "平仓委托已取消"


async def submit_exit(
    position: dict,
    cfg: "AppConfig",
    sqlite_path: str,
) -> None:
    """Submit reverse 3-leg orders to close an open position. Never raises."""
    try:
        pos_id = int(position.get("id") or 0)
        if pos_id > 0:
            conn_ts = sqlite3.connect(sqlite_path)
            try:
                with conn_ts:
                    _db.update_position_target_state(conn_ts, pos_id, "closed")
            finally:
                conn_ts.close()
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
    _exit_submit_start = time.monotonic()
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
    # --- Fetch entry orders from DB ---
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.row_factory = sqlite3.Row
        entry_orders = _db.get_position_orders(conn, position_id, action="open")
    finally:
        conn.close()

    # Build inst_id map from entry orders by leg
    leg_map: dict[str, dict] = {o["leg"]: o for o in entry_orders}

    # Reconstruct inst IDs from position fields (fallback if DB missing inst_id)
    exp_clean = expiry.replace("-", "")
    if len(exp_clean) == 8:
        exp_clean = exp_clean[2:]
    call_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-C"
    put_inst_id = f"{symbol}-USD-{exp_clean}-{int(strike)}-P"
    future_inst_id = f"{symbol}-USD-{exp_clean}"
    call_inst_id = (leg_map.get("call") or {}).get("inst_id") or call_inst_id
    put_inst_id = (leg_map.get("put") or {}).get("inst_id") or put_inst_id
    future_inst_id = (leg_map.get("future") or {}).get("inst_id") or future_inst_id

    if len(entry_orders) != 3:
        logger.error("submit_exit: pos=%d expected 3 open orders, got %d", position_id, len(entry_orders))
        return

    unfilled = [o for o in entry_orders if o.get("status") != "filled"]
    if unfilled:
        conn = sqlite3.connect(sqlite_path)
        try:
            with conn:
                for o in unfilled:
                    _db.update_order_status(conn, int(o["id"]), "failed")
                    _db.set_order_last_error(conn, int(o["id"]), "开仓单未成交，无法平仓")
                _db.update_position_status(conn, position_id, "partial_failed")
        finally:
            conn.close()
        return

    qty_call = float(leg_map["call"].get("qty", 1.0) or 1.0)
    qty_put = float(leg_map["put"].get("qty", 1.0) or 1.0)
    qty_future = float(leg_map["future"].get("qty", 1.0) or 1.0)
    exit_qty_by_leg = {"call": qty_call, "put": qty_put, "future": qty_future}

    async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as session:
        ok_ex, vrf_reason = await okx_db_entry_matches_exchange_positions(
            session,
            call_inst_id=call_inst_id,
            put_inst_id=put_inst_id,
            future_inst_id=future_inst_id,
            direction=str(direction),
            qty_call=qty_call,
            qty_put=qty_put,
            qty_future=qty_future,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
        )
        if not ok_ex:
            conn = sqlite3.connect(sqlite_path)
            try:
                with conn:
                    for o in entry_orders:
                        _db.set_order_last_error(conn, int(o["id"]), vrf_reason)
                    _db.update_position_status(conn, position_id, "partial_failed")
            finally:
                conn.close()
            logger.warning(
                "submit_exit: exchange has no matching hedge pos=%d: %s",
                position_id, vrf_reason,
            )
            return

        # Fetch order book tops for all 3 legs in parallel
        book_results = await asyncio.gather(
            _fetch_order_book_top(session, call_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase),
            _fetch_order_book_top(session, put_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase),
            _fetch_order_book_top(session, future_inst_id, api_key=api_key, secret=secret,
                                  passphrase=passphrase),
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

        conn_ac = sqlite3.connect(sqlite_path)
        try:
            already_closed = {
                str(r[0])
                for r in conn_ac.execute(
                    "SELECT DISTINCT leg FROM orders WHERE position_id=? AND action='close' AND status='filled'",
                    (position_id,),
                ).fetchall()
                if r[0]
            }
        finally:
            conn_ac.close()
        exit_legs = [e for e in exit_legs if e["leg"] not in already_closed]
        if not exit_legs:
            logger.info("submit_exit: pos=%d no remaining legs to close (all already closed)", position_id)
            conn = sqlite3.connect(sqlite_path)
            try:
                conn.row_factory = sqlite3.Row
                entry_chk = _db.get_position_orders(conn, position_id, action="open")
                ef = {o["leg"] for o in entry_chk if o.get("status") == "filled"}
                cf = _close_filled_legs_set(conn, position_id)
                row = conn.execute("SELECT realized_pnl_usdt FROM positions WHERE id=?", (position_id,)).fetchone()
                prev_only = float(row[0] or 0) if row else 0.0
                with conn:
                    if ef and ef.issubset(cf):
                        _db.update_position_status(
                            conn, position_id, "closed",
                            realized_pnl_usdt=prev_only,
                            closed_at=_db._utc_now_iso(),
                        )
            finally:
                conn.close()
            return

        logger.info(
            "[submit_exit] 准备平仓 | 仓位 pos=%d | 方向: %s | OKX张数 call/put/fut: %s/%s/%s | 本轮 %d 腿\n%s",
            position_id,
            direction,
            qty_call,
            qty_put,
            qty_future,
            len(exit_legs),
            "\n".join(
                f"  {e['leg']:6} {e['inst_id']} {e['side']} @ {e['px']}"
                for e in exit_legs
            ),
        )

        # Create DB exit order records
        conn = sqlite3.connect(sqlite_path)
        exit_order_ids_db: list[int] = []
        try:
            with conn:
                for leg_spec in exit_legs:
                    _q = exit_qty_by_leg[leg_spec["leg"]]
                    oid = _db.create_order(
                        conn,
                        signal_id=position.get("signal_id"),
                        position_id=position_id,
                        inst_id=leg_spec["inst_id"],
                        leg=leg_spec["leg"],
                        action="close",
                        side=leg_spec["side"],
                        limit_px=leg_spec["px"],
                        qty=_q,
                    )
                    exit_order_ids_db.append(oid)
        finally:
            conn.close()

        # --- Submit exit orders in parallel ---
        n_exit = len(exit_legs)
        exch_order_ids: list[str | None] = [None] * n_exit
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
                            sz=exit_qty_by_leg[leg["leg"]],
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            log_label=f"close pos={position_id} leg={leg['leg']}",
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
            per_leg = [
                str(results[i]) if isinstance(results[i], Exception) else ""
                for i in range(n_exit)
            ]
            _update_exit_failed(
                sqlite_path, position_id, exit_order_ids_db,
                reason="平仓下单失败",
                per_order_reasons=per_leg,
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
                )
                for i in range(n_exit)
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

    # --- Per-leg DB update (filled or failed), accumulate PnL ---
    realized_pnl = 0.0
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for i, oid_db in enumerate(exit_order_ids_db):
                fd = filled_data[i]
                if fd is not None:
                    _fee, _ccy = _okx_fee(fd)
                    _ord_type = str(fd.get("ordType") or "").lower() or None
                    _filled_qty = float(fd.get("fillSz") or fd.get("sz") or 0)
                    _db.update_order_status(
                        conn, oid_db, "filled",
                        filled_px=float(fd.get("avgPx") or fd.get("px") or 0),
                        filled_qty=_filled_qty if _filled_qty > 0 else None,
                        fee_type=_okx_fee_type(fd), actual_fee=_fee, fee_ccy=_ccy,
                        filled_at=_db._utc_now_iso(),
                        order_type=_ord_type,
                    )
                    leg_spec = exit_legs[i]
                    exit_px = float(fd.get("avgPx") or fd.get("px") or leg_spec["px"])
                    entry_order = leg_map.get(leg_spec["leg"])
                    entry_px = float(entry_order.get("filled_px") or entry_order.get("limit_px") or 0.0) if entry_order else 0.0
                    qx = exit_qty_by_leg[leg_spec["leg"]]
                    if leg_spec["side"] == "sell":
                        realized_pnl += (exit_px - entry_px) * qx
                    else:
                        realized_pnl += (entry_px - exit_px) * qx
                else:
                    _db.update_order_status(conn, oid_db, "pending")
                    _db.set_order_last_error(conn, oid_db, "maker 轮询超时，准备切换 taker")
    finally:
        conn.close()

    # --- Escalating exit retry loop ---
    if not all_filled:
        now_iso = _db._utc_now_iso()
        with sqlite3.connect(sqlite_path) as _conn:
            _conn.execute(
                "UPDATE positions SET "
                "exit_started_at = COALESCE(exit_started_at, ?), "
                "exit_attempt_count = exit_attempt_count + 1, "
                "exit_last_attempt_at = ? "
                "WHERE id=?",
                (now_iso, now_iso, position_id),
            )
        _exit_active.add(position_id)
        try:
            failed_legs = [
                {
                    "leg": exit_legs[i]["leg"],
                    "inst_id": exit_legs[i]["inst_id"],
                    "side": exit_legs[i]["side"],
                    "qty": exit_qty_by_leg[exit_legs[i]["leg"]],
                    "entry_px": (
                        float(
                            _eo.get("filled_px") or _eo.get("limit_px") or 0.0,
                        )
                        if (_eo := leg_map.get(exit_legs[i]["leg"]))
                        else 0.0
                    ),
                    "oid_db": exit_order_ids_db[i],
                    "last_px": exit_legs[i]["px"],
                    "exch_ord_id": exch_order_ids[i],
                }
                for i in range(len(exit_legs)) if filled_data[i] is None
            ]
            async with aiohttp.ClientSession(base_url=_OKX_REST_BASE) as _loop_sess:
                loop_filled, loop_pnl = await _escalating_exit_loop_okx(
                    session=_loop_sess,
                    failed_legs=failed_legs,
                    position_id=position_id,
                    signal_id=position.get("signal_id"),
                    api_key=api_key, secret=secret, passphrase=passphrase,
                    sqlite_path=sqlite_path,
                    exit_started_at=_exit_submit_start,
                    cfg=cfg,
                )
            if loop_filled:
                all_filled = True
                realized_pnl += loop_pnl
        finally:
            _exit_active.discard(position_id)

    # --- Update position status ---
    new_total = realized_pnl
    fully_exited = False
    conn = sqlite3.connect(sqlite_path)
    try:
        row = conn.execute("SELECT realized_pnl_usdt FROM positions WHERE id=?", (position_id,)).fetchone()
        prev_pnl = float(row[0] or 0) if row else 0.0
        new_total = prev_pnl + realized_pnl
        entry_filled_legs = {o["leg"] for o in entry_orders if o.get("status") == "filled"}
        cf_legs = {
            str(r[0])
            for r in conn.execute(
                "SELECT DISTINCT leg FROM orders WHERE position_id=? AND action='close' AND status='filled'",
                (position_id,),
            ).fetchall()
            if r[0]
        }
        fully_exited = bool(entry_filled_legs) and entry_filled_legs.issubset(cf_legs)
        with conn:
            if all_filled and fully_exited:
                _db.update_position_status(
                    conn, position_id, "closed",
                    realized_pnl_usdt=new_total,
                    closed_at=_db._utc_now_iso(),
                )
            else:
                if not all_filled:
                    for row in conn.execute(
                        "SELECT id, leg FROM orders WHERE position_id=? AND action='close' AND status='failed'",
                        (position_id,),
                    ):
                        leg = row[1] or "?"
                        _db.set_order_last_error(
                            conn,
                            int(row[0]),
                            f"出场失败：{leg} 腿在重试后仍未完全成交",
                        )
                _db.update_position_status(
                    conn, position_id, "partial_failed",
                    realized_pnl_usdt=new_total,
                    closed_at=None,
                )
    finally:
        conn.close()

    # --- Notifications ---
    label = f"{symbol} {expiry} {int(strike)} {direction}"
    if all_filled and fully_exited:
        msg = (
            f"\U0001f4b0 止盈出场成功\n"
            f"{label}\n"
            f"实现盈亏: {new_total:+.2f} USDT"
        )
        try:
            await _notifier.send_telegram(cfg.telegram, msg)
        except Exception as exc:
            logger.warning("submit_exit: telegram notification failed: %s", exc)
    elif not all_filled:
        await _send_exit_failure_telegram(cfg, position, "重试后仍有平仓单未成交")


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
        now_iso = _db._utc_now_iso()
        with sqlite3.connect(sqlite_path) as _conn:
            _conn.execute(
                "UPDATE positions SET "
                "exit_started_at = COALESCE(exit_started_at, ?), "
                "exit_attempt_count = exit_attempt_count + 1, "
                "exit_last_attempt_at = ? "
                "WHERE id=?",
                (now_iso, now_iso, position_id),
            )
        _exit_active.add(position_id)
        try:
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
        finally:
            _exit_active.discard(position_id)


def _update_exit_failed(
    sqlite_path: str,
    position_id: int,
    exit_order_ids_db: list[int],
    *,
    reason: str,
    per_order_reasons: list[str] | None = None,
) -> None:
    """Mark exit orders as failed and position as partial_failed (errors on orders, not position)."""
    conn = sqlite3.connect(sqlite_path)
    try:
        with conn:
            for i, oid_db in enumerate(exit_order_ids_db):
                _db.update_order_status(conn, oid_db, "failed")
                msg = (
                    per_order_reasons[i]
                    if per_order_reasons and i < len(per_order_reasons) and per_order_reasons[i]
                    else reason
                )
                _db.set_order_last_error(conn, oid_db, msg)
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
