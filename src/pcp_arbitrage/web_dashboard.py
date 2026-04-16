"""Web dashboard: HTML tables for opportunities and triplet overview, served via aiohttp + WebSocket."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape

from pcp_arbitrage.config import DEFAULT_LOT_SIZES
from pcp_arbitrage.exchange_symbols import format_strike_display

if TYPE_CHECKING:
    from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard, _Row

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
_jinja_env: Environment | None = None

_notification_queue: asyncio.Queue[dict] = asyncio.Queue()

# Queue for real-time leg price patches pushed to WS clients
_leg_prices_queue: asyncio.Queue[dict] = asyncio.Queue()

# Cache for open positions, updated by position_tracker after each poll
_open_positions_cache: list[dict] = []

# Cache for account balances, updated periodically
_account_balances_cache: dict[
    str, dict
] = {}  # {exchange: {total_eq_usdt, adj_eq_usdt, im_pct, mm_pct, ...}}

# Reference to AppConfig, set when the dashboard starts
_app_cfg = None  # type: ignore[assignment]

# Track order IDs currently being fee-backfilled to avoid duplicate requests
_fee_backfill_in_progress: set[int] = set()
# Track close-order IDs currently being status-reconciled
_close_reconcile_in_progress: set[int] = set()


def set_app_config(cfg) -> None:  # type: ignore[type-arg]
    """Store AppConfig reference for use in payload building."""
    global _app_cfg
    _app_cfg = cfg


def _position_contract_coin_mult(pos: dict) -> float:
    """每张合约对应的标的币数量（与 YAML contracts.lot_size / DEFAULT_LOT_SIZES 一致），用于「币」列与浮动盈亏。"""
    raw = pos.get("symbol")
    sym = str(raw or "").strip().upper()
    if _app_cfg and sym:
        lot = _app_cfg.lot_size
        for key in (raw, sym):
            if key is None or key == "":
                continue
            if key not in lot:
                continue
            try:
                v = float(lot[key])
            except (TypeError, ValueError):
                continue
            if v > 0:
                return v
    return float(DEFAULT_LOT_SIZES.get(sym, 1.0))


def _min_open_filled_contract_qty(open_orders: list[dict]) -> float | None:
    """三腿开仓单均已成交时，取各腿数量最小值（与深度约束一致）。"""
    qties: list[float] = []
    for leg in ("call", "put", "future"):
        legs = [o for o in open_orders if o.get("leg") == leg and o.get("status") == "filled"]
        if not legs:
            return None
        q = legs[0].get("filled_qty")
        if q is None:
            q = legs[0].get("qty")
        if q is None:
            return None
        try:
            qties.append(float(q))
        except (TypeError, ValueError):
            return None
    return min(qties)


def _direction_cn_for_opportunity_tables(direction_en: str) -> str:
    """positions 使用 forward/reverse；opportunity_current / sessions 使用 正向/反向。"""
    d = str(direction_en or "").strip().lower()
    return "正向" if d == "forward" else "反向"


def _fetch_opportunity_gross_fee_tradeable(
    path: str,
    signal_id: int | None,
    exchange: str,
    contract: str,
    direction: str,
) -> tuple[float | None, float | None, float | None]:
    """已结束 session（有 gross）或 opportunity_current 的价差 / 手续费 / 深度(张数)，用于持仓价差按张数缩放。"""
    import sqlite3

    con = sqlite3.connect(path)
    try:
        con.row_factory = sqlite3.Row
        if signal_id is not None:
            row = con.execute(
                "SELECT gross_usdt, fee_usdt, tradeable FROM opportunity_sessions WHERE id=?",
                (int(signal_id),),
            ).fetchone()
            if row is not None and row["gross_usdt"] is not None:
                try:
                    g = float(row["gross_usdt"])
                    f = float(row["fee_usdt"] or 0)
                    t_raw = row["tradeable"]
                    t_f = float(t_raw) if t_raw is not None else None
                    return g, f, t_f
                except (TypeError, ValueError):
                    pass
        dir_cn = _direction_cn_for_opportunity_tables(direction)
        row = con.execute(
            "SELECT gross_usdt, fee_usdt, tradeable FROM opportunity_current "
            "WHERE lower(exchange)=lower(?) AND contract=? AND direction=?",
            (exchange, contract, dir_cn),
        ).fetchone()
        if row is not None and row["gross_usdt"] is not None:
            try:
                g = float(row["gross_usdt"])
                f = float(row["fee_usdt"] or 0)
                t_raw = row["tradeable"]
                t_f = float(t_raw) if t_raw is not None else None
                return g, f, t_f
            except (TypeError, ValueError):
                pass
    finally:
        con.close()
    return None, None, None


def _fetch_linked_opportunity_session(
    path: str,
    signal_id: int | None,
    *,
    exchange: str,
    contract: str,
    direction_en: str,
) -> dict[str, float | None]:
    """持仓预计年化/收益：优先 opportunity_sessions（signal_id），空缺则用 opportunity_current 实时行。

    说明：未结束的 session 在 sessions 表里往往尚无 ann_pct/net（仅在 close 时写入），
    此时应从 opportunity_current 读取（与机会表主表一致）；direction 需用 正向/反向 匹配。
    """
    out: dict[str, float | None] = {
        "ann_pct": None,
        "net_usdt": None,
        "tradeable": None,
        "expected_max_usdt": None,
    }
    import sqlite3

    con = sqlite3.connect(path)
    try:
        con.row_factory = sqlite3.Row
        if signal_id is not None:
            row = con.execute(
                "SELECT ann_pct, net_usdt, tradeable, expected_max_usdt "
                "FROM opportunity_sessions WHERE id=?",
                (int(signal_id),),
            ).fetchone()
            if row is not None:
                for key in ("ann_pct", "net_usdt", "tradeable", "expected_max_usdt"):
                    raw = row[key]
                    if raw is None:
                        continue
                    try:
                        out[key] = float(raw)
                    except (TypeError, ValueError):
                        pass

        dir_cn = _direction_cn_for_opportunity_tables(direction_en)
        cur = con.execute(
            "SELECT ann_pct, net_usdt, tradeable, expected_max_usdt "
            "FROM opportunity_current "
            "WHERE lower(exchange)=lower(?) AND contract=? AND direction=?",
            (exchange, contract, dir_cn),
        ).fetchone()
        if cur is not None:
            for key in ("ann_pct", "net_usdt", "tradeable", "expected_max_usdt"):
                if out.get(key) is not None:
                    continue
                raw = cur[key]
                if raw is None:
                    continue
                try:
                    out[key] = float(raw)
                except (TypeError, ValueError):
                    pass
    finally:
        con.close()
    return out


def _fee_order_to_usdt(order: dict, index_usdt: float | None, symbol_upper: str) -> float | None:
    if order.get("status") != "filled":
        return 0.0
    afu = order.get("actual_fee_usdt")
    if afu is not None:
        try:
            v = float(afu)
            return v if v >= 0 else None
        except (TypeError, ValueError):
            pass
    af = order.get("actual_fee")
    if af is None:
        return None
    try:
        af = float(af)
    except (TypeError, ValueError):
        return None
    ccy = str(order.get("fee_ccy") or "").upper()
    if ccy == "USDT":
        return af
    if index_usdt and ccy and ccy == symbol_upper and float(index_usdt) > 0:
        return af * float(index_usdt)
    return None


def _sum_position_orders_fee_usdt(
    orders: list[dict], index_usdt: float | None, symbol: str
) -> float | None:
    symu = str(symbol or "").strip().upper()
    filled = [o for o in orders if o.get("status") == "filled"]
    if not filled:
        return None
    total = 0.0
    for o in filled:
        part = _fee_order_to_usdt(o, index_usdt, symu)
        if part is None:
            return None
        total += part
    return total


def _scaled_total_spread_usdt(
    gross: float | None, tradeable: float | None, qty: float | None
) -> float | None:
    if gross is None:
        return None
    if qty is not None and tradeable is not None and tradeable > 0:
        return gross * (qty / tradeable)
    return gross


def update_positions_cache(positions: list[dict]) -> None:
    """Update open positions cache for the web dashboard. Called by position_tracker."""
    global _open_positions_cache
    _open_positions_cache = list(positions)


def update_account_balances(balances: dict[str, dict]) -> None:
    """Update account balances cache for the web dashboard. Called periodically from main."""
    global _account_balances_cache
    _account_balances_cache = dict(balances)


def update_single_account_balance(name: str, bal: dict) -> None:
    """Merge one exchange's balance into cache without replacing other exchanges."""
    _account_balances_cache[name] = bal


def push_notification(notification: dict) -> None:
    """Enqueue a notification for broadcast to all web dashboard clients."""
    _notification_queue.put_nowait(notification)


def push_leg_prices_patch(patch: dict) -> None:
    """Enqueue a leg_prices patch for immediate WS broadcast. patch = {pos_id: {leg: {...}}}."""
    _leg_prices_queue.put_nowait(patch)


def _get_jinja_env() -> Environment:
    global _jinja_env
    if _jinja_env is None:
        _jinja_env = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
    return _jinja_env


def _render_index_html() -> str:
    api_base = ""
    if _app_cfg is not None:
        api_base = str(getattr(_app_cfg, "web_dashboard_api_base", "") or "").strip().rstrip("/")
        if api_base and not api_base.startswith("/"):
            api_base = "/" + api_base
    return _get_jinja_env().get_template("dashboard.html").render(pcp_api_base=api_base)


async def run_web_dashboard_loop(
    dash: "OpportunityDashboard",
    host: str,
    port: int,
    start_event: asyncio.Event | None,
) -> None:
    """Start aiohttp web server and stream JSON data over WebSocket."""
    try:
        from aiohttp import web
    except ImportError:
        logger.error("[web_dashboard] aiohttp is not installed; web dashboard disabled")
        return

    data_clients: set[web.WebSocketResponse] = set()

    # Queue for event-driven price pushes (set on dash so exchange runners can notify)
    price_queue: asyncio.Queue = asyncio.Queue()
    dash._price_event_queue = price_queue  # type: ignore[attr-defined]

    async def _index_handler(request: web.Request) -> web.Response:
        return web.Response(content_type="text/html", text=_render_index_html())

    async def _ws_data_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        data_clients.add(ws)
        logger.info("[web_dashboard] client connected (total=%d)", len(data_clients))
        # Send full snapshot immediately on connect
        try:
            payload = _build_payload(dash, full=True)
            await ws.send_str(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.warning("[web_dashboard] initial snapshot failed: %s", e)
        try:
            async for msg in ws:
                # handle client ping messages (ignore)
                pass
        finally:
            data_clients.discard(ws)
            logger.info("[web_dashboard] client disconnected (total=%d)", len(data_clients))
        return ws

    async def _triplet_summary_handler(request: web.Request) -> web.Response:
        all_triplets = dash.get_all_triplets()
        result = {}
        for exchange, (triplets, settle_type) in all_triplets.items():
            sym_data: dict = {}
            for t in triplets:
                sym = t.symbol
                if sym not in sym_data:
                    sym_data[sym] = {"index_price": dash._index_prices.get(sym), "expiries": {}}
                exp = t.expiry
                if exp not in sym_data[sym]["expiries"]:
                    sym_data[sym]["expiries"][exp] = []
                sym_data[sym]["expiries"][exp].append(t.strike)
            for sym in sym_data:
                sorted_expiries = {}
                for exp in sorted(sym_data[sym]["expiries"].keys()):
                    sorted_expiries[exp] = sorted(sym_data[sym]["expiries"][exp])
                sym_data[sym]["expiries"] = sorted_expiries
            result[exchange] = {
                "settle_type": settle_type,
                "symbols": {sym: sym_data[sym] for sym in sorted(sym_data.keys())},
            }
        return web.Response(
            content_type="application/json",
            text=json.dumps(result, ensure_ascii=False),
        )

    async def _opportunity_history_handler(request: web.Request) -> web.Response:
        sqlite_path = getattr(dash, "_sqlite_path", None)
        if not sqlite_path:
            return web.Response(
                content_type="application/json",
                text=json.dumps(
                    {"sessions": [], "error": "未配置 sqlite_path，无历史记录"},
                    ensure_ascii=False,
                ),
            )
        try:
            lim = int(request.query.get("limit", "500"))
        except ValueError:
            lim = 500
        from pcp_arbitrage.db import (
            aggregate_opportunity_sessions_stats,
            daily_expected_max_series_local,
            list_opportunity_sessions_history,
            get_session_ids_with_positions,
        )

        sessions = list_opportunity_sessions_history(sqlite_path, limit=lim)
        payload: dict = {"sessions": sessions}
        try:
            payload["session_position_map"] = get_session_ids_with_positions(sqlite_path)
        except Exception as exc:
            logger.warning("[web_dashboard] session_position_map: %s", exc)
            payload["session_position_map"] = {}
        try:
            payload["session_stats"] = aggregate_opportunity_sessions_stats(sqlite_path)
        except Exception as exc:
            logger.warning("[web_dashboard] opportunity-history session_stats: %s", exc)
            payload["session_stats"] = None
        try:
            payload["daily_expected_max"] = daily_expected_max_series_local(
                sqlite_path, days=30
            )
        except Exception as exc:
            logger.warning("[web_dashboard] opportunity-history daily_expected_max: %s", exc)
            payload["daily_expected_max"] = []
        return web.Response(
            content_type="application/json",
            text=json.dumps(payload, ensure_ascii=False),
        )

    async def _manual_entry_handler(request: web.Request) -> web.Response:
        if _app_cfg is None:
            return web.Response(
                content_type="application/json",
                status=503,
                text=json.dumps({"ok": False, "error": "config not ready"}, ensure_ascii=False),
            )
        try:
            body = await request.json()
        except Exception:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}, ensure_ascii=False),
            )

        exchange = str(body.get("exchange", ""))
        label = str(body.get("label", ""))
        direction_cn = str(body.get("direction", ""))  # "正向" or "反向"

        exc_cfg = _app_cfg.exchanges.get(exchange.upper()) or _app_cfg.exchanges.get(exchange.lower())
        if exc_cfg is None or not exc_cfg.api_key:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps({"ok": False, "error": "OKX 未配置 API key"}, ensure_ascii=False),
            )

        # --- Find the live row ---
        direction_key = "forward" if direction_cn == "正向" else "reverse"
        row = None
        for key, r in dash._rows.items():
            if (
                r.exchange == exchange
                and r.label == label
                and r.direction_cn == direction_cn
            ):
                row = r
                break
        if row is None:
            return web.Response(
                content_type="application/json",
                status=404,
                text=json.dumps(
                    {"ok": False, "error": "找不到该机会记录"}, ensure_ascii=False
                ),
            )

        # --- Check existing position (non-terminal) ---
        sqlite_path_entry = getattr(dash, "_sqlite_path", None)
        if sqlite_path_entry:
            from pcp_arbitrage.db import get_active_position_keys

            active_keys = get_active_position_keys(sqlite_path_entry)
            parts = label.split("-")
            sym = parts[0] if parts else ""
            expiry = parts[1] if len(parts) > 1 else ""
            try:
                strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
            except (ValueError, AttributeError):
                strike = float(row.strike) if row.strike is not None else 0.0
            if (exchange, sym, expiry, strike, direction_key) in active_keys:
                return web.Response(
                    content_type="application/json",
                    status=409,
                    text=json.dumps({"ok": False, "error": "已有持仓"}, ensure_ascii=False),
                )

        # --- Check OKX balance ---
        try:
            from pcp_arbitrage.account_fetcher import get_exchange_balance

            bal = await get_exchange_balance(exc_cfg, _app_cfg)
            if bal is None or bal.get("adj_eq_usdt", 0) <= 0:
                return web.Response(
                    content_type="application/json",
                    status=400,
                    text=json.dumps(
                        {"ok": False, "error": "OKX 余额不足或无法获取"}, ensure_ascii=False
                    ),
                )
        except Exception as exc:
            return web.Response(
                content_type="application/json",
                status=500,
                text=json.dumps({"ok": False, "error": f"余额查询失败: {exc}"}, ensure_ascii=False),
            )

        # --- Fetch triplet from DB (for call_id/put_id/future_id) ---
        if not sqlite_path_entry:
            return web.Response(
                content_type="application/json",
                status=503,
                text=json.dumps({"ok": False, "error": "sqlite_path 未配置"}, ensure_ascii=False),
            )
        import sqlite3 as _sqlite3

        conn = _sqlite3.connect(sqlite_path_entry)
        triplet_row = None
        try:
            conn.row_factory = _sqlite3.Row
            parts = label.split("-")
            sym = parts[0] if parts else ""
            expiry = parts[1] if len(parts) > 1 else ""
            try:
                strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
            except (ValueError, AttributeError):
                strike = float(row.strike) if row.strike is not None else 0.0
            cur = conn.execute(
                "SELECT exchange, symbol, expiry, strike, call_id, put_id, future_id FROM triplets "
                "WHERE exchange=? AND symbol=? AND expiry=? AND ABS(strike-?)<=0.01 LIMIT 1",
                (exchange, sym, expiry, strike),
            )
            triplet_row = cur.fetchone()
        finally:
            conn.close()

        if triplet_row is None:
            return web.Response(
                content_type="application/json",
                status=404,
                text=json.dumps(
                    {"ok": False, "error": "triplet 不在 DB 中，请稍后重试"}, ensure_ascii=False
                ),
            )

        import math as _math

        from pcp_arbitrage.models import Triplet
        from pcp_arbitrage.pcp_calculator import ArbitrageSignal

        triplet = Triplet(
            exchange=triplet_row["exchange"],
            symbol=triplet_row["symbol"],
            expiry=triplet_row["expiry"],
            strike=float(triplet_row["strike"]),
            call_id=triplet_row["call_id"],
            put_id=triplet_row["put_id"],
            future_id=triplet_row["future_id"],
        )

        # Rebuild ArbitrageSignal from stored row prices（优先本行冻结的指数价）
        ix = row.index_price_usdt
        if ix is not None and _math.isfinite(float(ix)) and float(ix) > 0:
            spot = float(ix)
        else:
            spot = dash._index_prices.get(sym, 0.0) or 0.0
        call_px_coin = (row.call_px_usdt / spot) if spot > 0 and row.call_px_usdt else 0.0
        put_px_coin = (row.put_px_usdt / spot) if spot > 0 and row.put_px_usdt else 0.0
        dc = float(getattr(row, "depth_contracts", 0) or 0)
        if dc <= 0:
            dc = 1.0
        signal = ArbitrageSignal(
            direction=direction_key,
            triplet=triplet,
            call_price=row.call_px_usdt or 0.0,
            put_price=row.put_px_usdt or 0.0,
            future_price=row.fut_px_usdt or 0.0,
            call_price_coin=call_px_coin,
            put_price_coin=put_px_coin,
            spot_price=spot,
            gross_profit=row.gross,
            total_fee=row.fee,
            net_profit=row.net,
            annualized_return=row.ann_pct / 100.0,
            days_to_expiry=row.days_to_expiry,
            index_for_fee_usdt=spot,
            tradeable_qty=float(row.tradeable) if row.tradeable else 1.0,
            depth_contracts=dc,
            call_fee=row.call_fee or 0.0,
            put_fee=row.put_fee or 0.0,
            fut_fee=row.fut_fee or 0.0,
        )

        # --- Submit entry (await for result) ---
        from pcp_arbitrage import order_manager as _om
        from pcp_arbitrage import web_dashboard as _self_wd

        ok, result_msg = await _om.submit_entry(triplet, signal, row.active_session_id, _app_cfg, sqlite_path_entry)
        direction_str = "正向" if direction_key == "forward" else "反向"
        if ok:
            _self_wd.push_notification(
                {
                    "exchange": exchange,
                    "label": label,
                    "direction": direction_str,
                    "type": "order_success",
                    "message": result_msg,
                }
            )
            return web.Response(
                content_type="application/json",
                text=json.dumps({"ok": True, "message": result_msg}, ensure_ascii=False),
            )
        else:
            _self_wd.push_notification(
                {
                    "exchange": exchange,
                    "label": label,
                    "direction": direction_str,
                    "type": "order_error",
                    "message": result_msg,
                }
            )
            return web.Response(
                content_type="application/json",
                status=500,
                text=json.dumps({"ok": False, "error": result_msg}, ensure_ascii=False),
            )

    async def _retry_exit_handler(request: web.Request) -> web.Response:
        """手动全部平仓：优先重试失败平仓腿，否则对已成交开仓腿发起反向平仓。"""
        if _app_cfg is None:
            return web.Response(content_type="application/json", status=503,
                                text=json.dumps({"ok": False, "error": "服务未就绪"}, ensure_ascii=False))
        sqlite_path_retry = _app_cfg.sqlite_path
        if not sqlite_path_retry:
            return web.Response(content_type="application/json", status=400,
                                text=json.dumps({"ok": False, "error": "未配置 sqlite_path"}, ensure_ascii=False))
        try:
            body = await request.json()
            position_id = int(body["position_id"])
        except Exception:
            return web.Response(content_type="application/json", status=400,
                                text=json.dumps({"ok": False, "error": "参数错误，需要 position_id"}, ensure_ascii=False))
        import sqlite3 as _sqlite3
        conn = _sqlite3.connect(sqlite_path_retry)
        try:
            conn.row_factory = _sqlite3.Row
            with conn:
                conn.execute(
                    "UPDATE positions SET target_state='closed' WHERE id=?",
                    (position_id,),
                )
            row = conn.execute(
                "SELECT id, exchange, symbol, expiry, strike, direction, status, signal_id, "
                "call_inst_id, put_inst_id, future_inst_id, target_state FROM positions WHERE id=?",
                (position_id,),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return web.Response(content_type="application/json", status=404,
                                text=json.dumps({"ok": False, "error": f"持仓 {position_id} 不存在"}, ensure_ascii=False))
        position = dict(row)
        from pcp_arbitrage import order_manager as _om
        ok, msg = await _om.retry_exit_position(position, _app_cfg, sqlite_path_retry)
        return web.Response(
            content_type="application/json",
            text=json.dumps({"ok": ok, "message": msg}, ensure_ascii=False),
        )

    async def _cancel_close_order_handler(request: web.Request) -> web.Response:
        """取消一条 pending 平仓委托单。"""
        if _app_cfg is None:
            return web.Response(
                content_type="application/json",
                status=503,
                text=json.dumps({"ok": False, "error": "服务未就绪"}, ensure_ascii=False),
            )
        sqlite_path = _app_cfg.sqlite_path
        if not sqlite_path:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps({"ok": False, "error": "未配置 sqlite_path"}, ensure_ascii=False),
            )
        try:
            body = await request.json()
            position_id = int(body["position_id"])
            order_id = int(body["order_id"])
        except Exception:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps({"ok": False, "error": "参数错误，需要 position_id 与 order_id"}, ensure_ascii=False),
            )
        from pcp_arbitrage import order_manager as _om
        ok, msg = await _om.cancel_close_order(
            position_id=position_id,
            order_id=order_id,
            cfg=_app_cfg,
            sqlite_path=sqlite_path,
        )
        return web.Response(
            content_type="application/json",
            text=json.dumps({"ok": ok, "message": msg}, ensure_ascii=False),
            status=200 if ok else 400,
        )

    async def _close_position_handler(request: web.Request) -> web.Response:
        """Deprecated: 组合级平仓统一走 /api/retry-exit，单腿平仓走 /api/close-position-leg。"""
        return web.Response(
            content_type="application/json",
            status=410,
            text=json.dumps(
                {
                    "ok": False,
                    "error": "该接口已停用：组合级请使用 /api/retry-exit，单腿请使用 /api/close-position-leg",
                },
                ensure_ascii=False,
            ),
        )

    async def _close_position_leg_handler(request: web.Request) -> web.Response:
        """对已成交的一条开仓腿提交单腿平仓（OKX）。"""
        if _app_cfg is None:
            return web.Response(
                content_type="application/json",
                status=503,
                text=json.dumps({"ok": False, "error": "服务未就绪"}, ensure_ascii=False),
            )
        sqlite_path_leg = _app_cfg.sqlite_path
        if not sqlite_path_leg:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps({"ok": False, "error": "未配置 sqlite_path"}, ensure_ascii=False),
            )
        try:
            body = await request.json()
            position_id = int(body["position_id"])
            order_id = int(body["order_id"])
        except Exception:
            return web.Response(
                content_type="application/json",
                status=400,
                text=json.dumps(
                    {"ok": False, "error": "参数错误，需要 position_id 与 order_id"},
                    ensure_ascii=False,
                ),
            )
        from pcp_arbitrage import order_manager as _om

        ok, msg = await _om.submit_exit_single_leg(
            position_id=position_id,
            order_id=order_id,
            cfg=_app_cfg,
            sqlite_path=sqlite_path_leg,
        )
        return web.Response(
            content_type="application/json",
            text=json.dumps({"ok": ok, "message": msg}, ensure_ascii=False),
            status=200 if ok else 400,
        )

    app = web.Application()
    app.router.add_get("/", _index_handler)
    app.router.add_get("/ws-data", _ws_data_handler)
    app.router.add_get("/api/triplet-summary", _triplet_summary_handler)
    app.router.add_get("/api/opportunity-history", _opportunity_history_handler)
    app.router.add_post("/api/manual-entry", _manual_entry_handler)
    app.router.add_post("/api/close-position", _close_position_handler)
    app.router.add_post("/api/close-position-leg", _close_position_leg_handler)
    app.router.add_post("/api/close-position-leg/", _close_position_leg_handler)
    app.router.add_post("/api/close_position_leg", _close_position_leg_handler)
    app.router.add_post("/api/retry-exit", _retry_exit_handler)
    app.router.add_post("/api/cancel-close-order", _cancel_close_order_handler)

    runner = web.AppRunner(app, access_log_format='%a "%r" %s %b')
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    for attempt in range(12):
        try:
            await site.start()
            break
        except OSError:
            if attempt == 11:
                logger.error("[web_dashboard] 端口 %d 持续被占用，放弃启动", port)
                await runner.cleanup()
                return
            logger.warning("[web_dashboard] 端口 %d 被占用，1s 后重试（%d/12）…", port, attempt + 1)
            await asyncio.sleep(1)
    logger.info("[web_dashboard] listening on http://%s:%d", host, port)

    if start_event is not None:
        await start_event.wait()

    async def _broadcast(msg: str) -> None:
        """Send msg to all connected clients, removing dead ones."""
        for ws in list(data_clients):
            try:
                await ws.send_str(msg)
            except Exception:
                data_clients.discard(ws)

    async def _price_push_loop() -> None:
        """Wake on price events and immediately push all pending prices."""
        while True:
            try:
                # Wait for at least one price event
                price_msg = await price_queue.get()
                if not data_clients:
                    # drain remaining without sending
                    while not price_queue.empty():
                        try:
                            price_queue.get_nowait()
                        except Exception:
                            break
                    continue
                # Merge any additional prices that arrived while we were processing
                merged: dict = dict(price_msg.get("prices", {}))
                while not price_queue.empty():
                    try:
                        extra = price_queue.get_nowait()
                        merged.update(extra.get("prices", {}))
                    except Exception:
                        break
                await _broadcast(json.dumps({"prices": merged}, ensure_ascii=False))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[web_dashboard] price push error: %s", exc)

    async def _broadcast_loop() -> None:
        """Periodic full-payload broadcast (meta + rows) every 2s."""
        tick = 0
        while True:
            try:
                await asyncio.sleep(2.0)
                if not data_clients:
                    tick += 1
                    continue
                tick += 1
                payload = _build_payload(dash, full=True)
                msg = json.dumps(payload, ensure_ascii=False)
                await _broadcast(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[web_dashboard] broadcast error: %s", exc)

    async def _notification_push_loop() -> None:
        while True:
            notif = await _notification_queue.get()
            await _broadcast(json.dumps({"type": "notification", **notif}, ensure_ascii=False))

    async def _leg_prices_push_loop() -> None:
        """Merge and push leg_prices patches to WS clients immediately."""
        while True:
            try:
                patch = await _leg_prices_queue.get()
                if not data_clients:
                    while not _leg_prices_queue.empty():
                        try:
                            _leg_prices_queue.get_nowait()
                        except Exception:
                            break
                    continue
                # Merge any additional patches that arrived concurrently
                merged: dict = {}
                for pos_id_s, legs in patch.items():
                    merged.setdefault(str(pos_id_s), {}).update(legs)
                while not _leg_prices_queue.empty():
                    try:
                        extra = _leg_prices_queue.get_nowait()
                        for pos_id_s, legs in extra.items():
                            merged.setdefault(str(pos_id_s), {}).update(legs)
                    except Exception:
                        break
                await _broadcast(json.dumps({"leg_prices": merged}, ensure_ascii=False))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[web_dashboard] leg_prices push error: %s", exc)

    asyncio.ensure_future(_price_push_loop())
    asyncio.ensure_future(_broadcast_loop())
    asyncio.ensure_future(_notification_push_loop())
    asyncio.ensure_future(_leg_prices_push_loop())

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


async def _backfill_order_fee(order: dict, sqlite_path: str) -> None:
    """Fetch actual fee from exchange for a filled order and write it to DB.

    Supports OKX (uses inst_id + exchange_order_id) and Deribit (uses exchange_order_id).
    On completion, removes the order_id from _fee_backfill_in_progress.
    """
    import sqlite3 as _sqlite3
    import aiohttp

    order_id: int = order["id"]
    exchange_order_id: str = order["exchange_order_id"]
    inst_id: str | None = order.get("inst_id")
    exchange: str = (order.get("exchange") or "").lower()

    try:
        from pcp_arbitrage import db as _db

        if exchange == "deribit":
            try:
                from pcp_arbitrage.exchanges.deribit import DeribitRestClient
                exc_cfg = _app_cfg.exchanges.get("DERIBIT") or _app_cfg.exchanges.get("deribit") if _app_cfg else None
                if exc_cfg is None:
                    return
                async with DeribitRestClient(api_key=exc_cfg.api_key, secret=exc_cfg.secret_key) as dclient:
                    await dclient._authenticate()
                    order_state = await dclient.get_order_state(exchange_order_id)
                commission = order_state.get("commission")
                fee_val = abs(float(commission)) if commission is not None else None
                # Infer currency from instrument name
                iname = order_state.get("instrument_name", "")
                fee_ccy = iname.split("-")[0] if iname else None
                avg_px_raw = order_state.get("average_price")
                avg_px = float(avg_px_raw) if avg_px_raw else None
                filled_amt = order_state.get("filled_amount")
                _filled_qty = float(filled_amt) if filled_amt else None
                # Get fee type from trades
                from pcp_arbitrage.order_manager import _deribit_fee_type
                trades = await dclient.get_trades_by_order(exchange_order_id)
                _fee_type = _deribit_fee_type(trades)
                # Only write filled_px/filled_qty if currently missing
                fp_to_write = avg_px if order.get("filled_px") is None else None
                fq_to_write = _filled_qty if order.get("filled_qty") is None else None
                conn = _sqlite3.connect(sqlite_path)
                try:
                    with conn:
                        _db.update_order_status(
                            conn, order_id, order.get("status", "filled"),
                            filled_px=fp_to_write,
                            filled_qty=fq_to_write,
                            fee_type=_fee_type,
                            actual_fee=fee_val, fee_ccy=fee_ccy,
                        )
                finally:
                    conn.close()
                logger.debug("[fee_backfill] deribit order %d avg_px=%s filled_qty=%s fee=%s %s fee_type=%s",
                             order_id, avg_px, _filled_qty, fee_val, fee_ccy, _fee_type)
            except Exception as exc:
                logger.warning("[fee_backfill] deribit order %d: %s", order_id, exc)
        else:
            # OKX
            if not inst_id or not _app_cfg:
                return
            exc_cfg = _app_cfg.exchanges.get("OKX") or _app_cfg.exchanges.get("okx")
            if exc_cfg is None:
                return
            from pcp_arbitrage.okx_client import _sign, _timestamp

            params_str = f"instId={inst_id}&ordId={exchange_order_id}"
            path = f"/api/v5/trade/order?{params_str}"
            ts = _timestamp()
            sig = _sign(exc_cfg.secret_key, ts, "GET", path, "")
            headers = {
                "OK-ACCESS-KEY": exc_cfg.api_key,
                "OK-ACCESS-SIGN": sig,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": exc_cfg.passphrase,
                "Content-Type": "application/json",
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        "https://www.okx.com" + path,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        data = await resp.json()
                orders_data = data.get("data", [])
                if not orders_data:
                    return
                od = orders_data[0]
                fee    = float(od.get("fee")    or 0)
                rebate = float(od.get("rebate") or 0)
                net_fee = abs(fee + rebate)
                fee_ccy = od.get("feeCcy") or od.get("rebateCcy") or None
                avg_px_raw = od.get("avgPx")
                avg_px = float(avg_px_raw) if avg_px_raw else None
                filled_sz = od.get("fillSz")
                _filled_qty = float(filled_sz) if filled_sz else None
                # Get fee type (maker/taker)
                from pcp_arbitrage.order_manager import _okx_fee_type
                _fee_type = _okx_fee_type(od)
                _ord_type_raw = str(od.get("ordType") or "").strip().lower()
                _ord_type = _ord_type_raw if _ord_type_raw else None
                # Only write filled_px/filled_qty if currently missing
                fp_to_write = avg_px if order.get("filled_px") is None else None
                fq_to_write = _filled_qty if order.get("filled_qty") is None else None
                conn = _sqlite3.connect(sqlite_path)
                try:
                    with conn:
                        _db.update_order_status(
                            conn, order_id, order.get("status", "filled"),
                            filled_px=fp_to_write,
                            filled_qty=fq_to_write,
                            fee_type=_fee_type,
                            actual_fee=net_fee, fee_ccy=fee_ccy,
                            order_type=_ord_type,
                        )
                finally:
                    conn.close()
                logger.debug("[fee_backfill] okx order %d avg_px=%s filled_qty=%s fee=%.6f %s fee_type=%s",
                             order_id, avg_px, _filled_qty, net_fee, fee_ccy, _fee_type)
            except Exception as exc:
                logger.warning("[fee_backfill] okx order %d: %s", order_id, exc)
    finally:
        _fee_backfill_in_progress.discard(order_id)


async def _reconcile_close_order_status(order: dict, position_id: int, sqlite_path: str) -> None:
    """Reconcile one close order status with exchange and write latest DB status."""
    order_id = int(order["id"])
    try:
        if not _app_cfg:
            return
        from pcp_arbitrage import order_manager as _om
        await _om.reconcile_close_orders_status(
            position_id=position_id,
            cfg=_app_cfg,
            sqlite_path=sqlite_path,
        )
    except Exception as exc:
        logger.debug("[close_reconcile] order %s pos=%s: %s", order_id, position_id, exc)
    finally:
        _close_reconcile_in_progress.discard(order_id)


def _utc_iso_to_unix(iso_s: str | None) -> float:
    """Parse DB / ISO timestamps (with optional Z) to Unix seconds."""
    if not iso_s or not str(iso_s).strip():
        return time.time()
    s = str(iso_s).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return time.time()


def _opp_row_payload_from_dashboard_r(
    r: "_Row",
    hist_peak: dict[tuple[str, str, str], dict[str, float | None]],
    active_pos_keys: set[tuple[str, str, str, float, str]],
) -> dict:
    """Single opportunity row dict for WebSocket (from live ``_Row``)."""
    from pcp_arbitrage.db import coalesce_per_leg_fees

    dur = None
    if not r.active:
        if r.frozen_active_duration_sec is not None:
            dur = r.frozen_active_duration_sec
        elif r.last_active_eval is not None:
            d = r.last_active_eval - r.first_active
            dur = d if d > 0 else None
    hp = hist_peak.get((r.exchange, r.label, r.direction_cn))
    cf, pf, ff = coalesce_per_leg_fees(r.fee, r.call_fee, r.put_fee, r.fut_fee)
    parts = r.label.split("-")
    row_symbol = parts[0] if parts else ""
    row_expiry = parts[1] if len(parts) > 1 else ""
    try:
        row_strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
    except (ValueError, AttributeError):
        row_strike = float(r.strike) if r.strike is not None else 0.0
    row_direction_key = "forward" if r.direction_cn == "正向" else "reverse"
    has_position = (
        r.exchange,
        row_symbol,
        row_expiry,
        row_strike,
        row_direction_key,
    ) in active_pos_keys
    return {
        "exchange": r.exchange,
        "label": r.label,
        "direction": r.direction_cn,
        "active": r.active,
        "first_active": float(r.first_active) if r.active else None,
        "frozen_duration": float(dur) if dur is not None else None,
        "last_eval": float(r.last_eval),
        "last_active_eval": None if r.last_active_eval is None else float(r.last_active_eval),
        "gross": r.gross,
        "fee": r.fee,
        "net": r.net,
        "tradeable": r.tradeable,
        "depth_contracts": getattr(r, "depth_contracts", 1.0),
        "days_to_expiry": r.days_to_expiry,
        "ann_pct": r.ann_pct,
        "hist_ann_pct": None if hp is None else hp.get("ann_pct"),
        "hist_duration_sec": None if hp is None else hp.get("duration_sec"),
        "strike": r.strike,
        "index_price_usdt": r.index_price_usdt,
        "call_px_usdt": r.call_px_usdt,
        "put_px_usdt": r.put_px_usdt,
        "fut_px_usdt": r.fut_px_usdt,
        "call_fee": cf,
        "put_fee": pf,
        "fut_fee": ff,
        "expected_max": r.net * r.tradeable if r.tradeable is not None else None,
        "has_position": has_position,
    }


def _opp_row_payload_from_current_dict(
    r: dict,
    hist_peak: dict[tuple[str, str, str], dict[str, float | None]],
    active_pos_keys: set[tuple[str, str, str, float, str]],
) -> dict:
    """WS row from ``opportunity_current`` when memory ``_rows`` is empty (e.g. restore gap)."""
    from pcp_arbitrage.db import coalesce_per_leg_fees

    exchange = str(r["exchange"])
    label = str(r["contract"])
    direction_cn = str(r["direction"])
    tnow = time.time()
    active = bool(r.get("active"))
    dur_sec = r.get("duration_sec")
    last_ae = r.get("last_active_eval")
    if active:
        if dur_sec is not None and float(dur_sec) >= 1:
            first_active = tnow - float(dur_sec)
        else:
            first_active = tnow
        frozen = None
    else:
        frozen = float(dur_sec) if dur_sec is not None else None
        if last_ae is not None and frozen is not None:
            first_active = float(last_ae) - frozen
        elif last_ae is not None:
            first_active = float(last_ae)
        else:
            first_active = tnow
    dur = None if active else frozen
    if not active and dur is None and last_ae is not None:
        d = float(last_ae) - first_active
        dur = d if d > 0 else None
    hp = hist_peak.get((exchange, label, direction_cn))
    cf, pf, ff = coalesce_per_leg_fees(
        float(r["fee_usdt"]),
        r.get("call_fee_usdt"),
        r.get("put_fee_usdt"),
        r.get("fut_fee_usdt"),
    )
    parts = label.split("-")
    row_symbol = parts[0] if parts else ""
    row_expiry = parts[1] if len(parts) > 1 else ""
    try:
        row_strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
    except (ValueError, AttributeError):
        raw = r.get("strike")
        row_strike = float(raw) if raw is not None else 0.0
    row_direction_key = "forward" if direction_cn == "正向" else "reverse"
    has_position = (
        exchange,
        row_symbol,
        row_expiry,
        row_strike,
        row_direction_key,
    ) in active_pos_keys
    last_eval = _utc_iso_to_unix(r.get("updated_at"))
    lae = float(last_ae) if last_ae is not None else None
    net = float(r["net_usdt"])
    tradeable = float(r["tradeable"])
    return {
        "exchange": exchange,
        "label": label,
        "direction": direction_cn,
        "active": active,
        "first_active": float(first_active) if active else None,
        "frozen_duration": float(dur) if dur is not None else None,
        "last_eval": last_eval,
        "last_active_eval": lae,
        "gross": float(r["gross_usdt"]),
        "fee": float(r["fee_usdt"]),
        "net": net,
        "tradeable": tradeable,
        "days_to_expiry": float(r["days_to_exp"]),
        "ann_pct": float(r["ann_pct"]),
        "hist_ann_pct": None if hp is None else hp.get("ann_pct"),
        "hist_duration_sec": None if hp is None else hp.get("duration_sec"),
        "strike": r.get("strike"),
        "index_price_usdt": r.get("index_price_usdt"),
        "call_px_usdt": r.get("call_px_usdt"),
        "put_px_usdt": r.get("put_px_usdt"),
        "fut_px_usdt": r.get("fut_px_usdt"),
        "call_fee": cf,
        "put_fee": pf,
        "fut_fee": ff,
        "expected_max": net * tradeable,
        "has_position": has_position,
    }


def _opp_row_payload_from_session_dict(
    s: dict,
    hist_peak: dict[tuple[str, str, str], dict[str, float | None]],
    active_pos_keys: set[tuple[str, str, str, float, str]],
) -> dict:
    """WS row from ``opportunity_sessions`` when both memory and ``opportunity_current`` are empty."""
    from pcp_arbitrage.db import coalesce_per_leg_fees

    exchange = str(s["exchange"])
    label = str(s["contract"])
    direction_cn = str(s["direction"])
    ended_raw = s.get("ended_utc")
    active = ended_raw is None
    started_ts = _utc_iso_to_unix(s.get("started_utc"))
    ended_ts = _utc_iso_to_unix(ended_raw) if ended_raw else None
    dur_sec = s.get("duration_sec")
    dur = float(dur_sec) if dur_sec is not None else None
    if not active and dur is None and ended_ts is not None:
        dur = max(0.0, ended_ts - started_ts)
    last_eval = ended_ts if ended_ts is not None else time.time()
    last_ae = ended_ts if not active else time.time()
    hp = hist_peak.get((exchange, label, direction_cn))
    gross = float(s["gross_usdt"]) if s.get("gross_usdt") is not None else 0.0
    fee = float(s["fee_usdt"]) if s.get("fee_usdt") is not None else 0.0
    net = float(s["net_usdt"]) if s.get("net_usdt") is not None else 0.0
    tradeable = float(s["tradeable"]) if s.get("tradeable") is not None else 0.0
    ann_pct = float(s["ann_pct"]) if s.get("ann_pct") is not None else 0.0
    days_exp = float(s["days_to_exp"]) if s.get("days_to_exp") is not None else 0.0
    cf, pf, ff = coalesce_per_leg_fees(fee, None, None, None)
    parts = label.split("-")
    row_symbol = parts[0] if parts else ""
    row_expiry = parts[1] if len(parts) > 1 else ""
    try:
        row_strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
    except (ValueError, AttributeError):
        row_strike = 0.0
    row_direction_key = "forward" if direction_cn == "正向" else "reverse"
    has_position = (
        exchange,
        row_symbol,
        row_expiry,
        row_strike,
        row_direction_key,
    ) in active_pos_keys
    return {
        "exchange": exchange,
        "label": label,
        "direction": direction_cn,
        "active": active,
        "first_active": float(started_ts) if active else None,
        "frozen_duration": None if active else dur,
        "last_eval": float(last_eval),
        "last_active_eval": float(last_ae),
        "gross": gross,
        "fee": fee,
        "net": net,
        "tradeable": tradeable,
        "days_to_expiry": days_exp,
        "ann_pct": ann_pct,
        "hist_ann_pct": None if hp is None else hp.get("ann_pct"),
        "hist_duration_sec": None if hp is None else hp.get("duration_sec"),
        "strike": row_strike,
        "index_price_usdt": None,
        "call_px_usdt": None,
        "put_px_usdt": None,
        "fut_px_usdt": None,
        "call_fee": cf,
        "put_fee": pf,
        "fut_fee": ff,
        "expected_max": net * tradeable if tradeable else None,
        "has_position": has_position,
    }


def _opp_rows_when_memory_empty(
    dash: "OpportunityDashboard",
    sqlite_path: str | None,
    hist_peak: dict[tuple[str, str, str], dict[str, float | None]],
    active_pos_keys: set[tuple[str, str, str, float, str]],
) -> list[dict]:
    """If live ``_rows`` is empty, show persisted current snapshot or recent sessions (history has rows)."""
    if dash._rows or not sqlite_path:
        return []
    from pcp_arbitrage.db import list_opportunity_sessions_history, load_opportunity_current

    try:
        cur = load_opportunity_current(sqlite_path)
    except OSError:
        cur = []
    if cur:
        return [_opp_row_payload_from_current_dict(r, hist_peak, active_pos_keys) for r in cur]
    try:
        sessions = list_opportunity_sessions_history(sqlite_path, limit=100)
    except OSError:
        return []
    return [_opp_row_payload_from_session_dict(s, hist_peak, active_pos_keys) for s in sessions]


def _dedupe_opp_rows_for_web(rows: list[dict]) -> list[dict]:
    """同一交易所+合约只保留一行：优先 active，其次更高实时年化，再其次更近的 last_eval。"""
    best: dict[str, dict] = {}
    for row in rows:
        key = f"{row['exchange']}\0{row['label']}"
        cur = best.get(key)
        if cur is None:
            best[key] = row
            continue
        r_a, c_a = row.get("active"), cur.get("active")
        if r_a and not c_a:
            best[key] = row
            continue
        if c_a and not r_a:
            continue
        r_ann = float(row.get("ann_pct") or 0.0)
        c_ann = float(cur.get("ann_pct") or 0.0)
        if r_ann > c_ann:
            best[key] = row
            continue
        if r_ann < c_ann:
            continue
        r_le = float(row.get("last_eval") or 0.0)
        c_le = float(cur.get("last_eval") or 0.0)
        if r_le > c_le:
            best[key] = row
    return list(best.values())


def _build_payload(dash: "OpportunityDashboard", *, full: bool) -> dict:
    """Build the JSON payload for /ws-data clients."""
    # build meta from runner_meta
    exchanges_meta = []
    all_symbols: list[str] = []
    for ex_name in sorted(dash._runner_meta.keys()):
        m = dash._runner_meta[ex_name]
        exchanges_meta.append(
            {
                "name": ex_name,
                "settle_type": m.settle_type,
                "option_taker_rate": m.option_taker_rate,
                "option_maker_rate": m.option_maker_rate,
                "future_taker_rate": m.future_taker_rate,
                "future_maker_rate": m.future_maker_rate,
                "n_triplets": m.n_triplets,
                "symbols": list(m.symbols),
            }
        )
        for s in m.symbols:
            if s not in all_symbols:
                all_symbols.append(s)
    payload: dict = {
        "prices": dict(dash._index_prices),
        "start_ts": min((r.first_active for r in dash._rows.values()), default=dash._started_at),
        "meta": {
            "min_ann": dash._min_annualized_rate,
            "atm_range": dash._atm_range,
            "min_dte": dash._min_days_to_expiry,
            "symbols": all_symbols,
            "exchanges": exchanges_meta,
            "order_min_ann": _app_cfg.order_min_annualized_rate if _app_cfg else None,
            "exit_target_profit_pct": _app_cfg.exit_target_profit_pct if _app_cfg else None,
        },
    }
    if full:
        from pcp_arbitrage.db import get_active_position_keys, history_peak_ann_session_by_key

        hist_peak: dict[tuple[str, str, str], dict[str, float | None]] = {}
        sqlite_path = getattr(dash, "_sqlite_path", None)
        active_pos_keys: set[tuple[str, str, str, float, str]] = set()
        if sqlite_path:
            try:
                hist_peak = history_peak_ann_session_by_key(sqlite_path)
            except OSError:
                pass
            try:
                active_pos_keys = get_active_position_keys(sqlite_path)
            except Exception:
                pass

        rows = [_opp_row_payload_from_dashboard_r(r, hist_peak, active_pos_keys) for r in dash._rows.values()]
        if not rows:
            rows = _opp_rows_when_memory_empty(dash, sqlite_path, hist_peak, active_pos_keys)
        payload["rows"] = _dedupe_opp_rows_for_web(rows)
    sqlite_path_pos = getattr(dash, "_sqlite_path", None)

    # Merge open (tracker cache) + partial_failed/failed + recent closed (all from DB for non-open)
    shown_pos_ids = {pos["id"] for pos in _open_positions_cache}
    all_pos_rows = list(_open_positions_cache)
    if sqlite_path_pos:
        try:
            import sqlite3 as _sqlite3
            from pcp_arbitrage.db import get_failed_positions, get_recent_closed_positions
            with _sqlite3.connect(sqlite_path_pos) as _conn:
                for fp in get_failed_positions(_conn):
                    if fp["id"] not in shown_pos_ids:
                        all_pos_rows.append(fp)
                        shown_pos_ids.add(fp["id"])
                for cp in get_recent_closed_positions(_conn):
                    if cp["id"] not in shown_pos_ids:
                        all_pos_rows.append(cp)
                        shown_pos_ids.add(cp["id"])
        except Exception:
            pass

    positions_out = []
    for pos in all_pos_rows:
        # Build per-leg entry info from orders
        leg_entry: dict[str, dict] = {}
        open_orders: list[dict] = []
        # Build position output
        entry: dict = {
            "id": pos["id"],
            "exchange": pos["exchange"],
            "symbol": pos["symbol"],
            "expiry": pos["expiry"],
            "strike": pos["strike"],
            "direction": pos["direction"],
            "target_state": pos.get("target_state", "open"),
            "status": pos["status"],
            "last_error": pos.get("last_error"),
            "current_mark_usdt": pos.get("current_mark_usdt"),
            "opened_at": pos["opened_at"],
            "call_inst_id": pos.get("call_inst_id"),
            "put_inst_id": pos.get("put_inst_id"),
            "future_inst_id": pos.get("future_inst_id"),
            "index_price_usdt": dash._index_prices.get(pos["symbol"]) if dash else None,
            "ct_val": _position_contract_coin_mult(pos),
            "orders": [],
            "total_spread_usdt": None,
            "total_fee_usdt": None,
            "total_float_profit_usdt": None,
            "expected_ann_pct": None,
            "expected_profit_usdt": None,
        }
        if sqlite_path_pos:
            try:
                import sqlite3 as _sqlite3
                from pcp_arbitrage.db import get_position_orders
                with _sqlite3.connect(sqlite_path_pos) as _conn:
                    open_orders = get_position_orders(_conn, pos["id"], action="open")
                    close_orders = get_position_orders(_conn, pos["id"], action="close")
                    entry["orders"] = open_orders + close_orders
                    # Build leg_entry from filled open orders
                    for o in open_orders:
                        if o.get("status") == "filled" and o.get("filled_px") is not None:
                            leg_entry[o["leg"]] = {
                                "filled_px": o["filled_px"],
                                "qty": o["qty"],
                                "filled_qty": o.get("filled_qty"),
                                "side": o["side"],
                                "fee_ccy": o.get("fee_ccy"),
                                "inst_id": o.get("inst_id"),
                            }
                    # Trigger backfill for filled orders with missing actual_fee or filled_px
                    sqlite_path_fee = sqlite_path_pos
                    for o in open_orders + close_orders:
                        if (
                            o.get("status") == "filled"
                            and (o.get("actual_fee") is None or o.get("filled_px") is None or o.get("filled_qty") is None or o.get("fee_type") is None)
                            and o.get("exchange_order_id")
                            and o["id"] not in _fee_backfill_in_progress
                        ):
                            o_with_exchange = dict(o)
                            o_with_exchange["exchange"] = pos["exchange"]
                            _fee_backfill_in_progress.add(o["id"])
                            try:
                                asyncio.ensure_future(
                                    _backfill_order_fee(o_with_exchange, sqlite_path_fee)
                                )
                            except RuntimeError:
                                # No running event loop (e.g., called from sync context)
                                _fee_backfill_in_progress.discard(o["id"])
                        elif (
                            o.get("action") == "close"
                            and o.get("status") in ("pending", "failed")
                            and o.get("exchange_order_id")
                            and o["id"] not in _close_reconcile_in_progress
                        ):
                            _close_reconcile_in_progress.add(o["id"])
                            try:
                                asyncio.ensure_future(
                                    _reconcile_close_order_status(
                                        dict(o), pos["id"], sqlite_path_fee
                                    )
                                )
                            except RuntimeError:
                                _close_reconcile_in_progress.discard(o["id"])
            except Exception:
                pass
        entry["leg_entry"] = leg_entry
        contract_label = (
            f"{pos['symbol']}-{pos['expiry']}-"
            f"{format_strike_display(pos['symbol'], float(pos['strike']))}"
        )
        qty_min = _min_open_filled_contract_qty(open_orders)
        gross_snap, tradeable_snap = None, None
        if sqlite_path_pos:
            gross_snap, _fee_snap, tradeable_snap = _fetch_opportunity_gross_fee_tradeable(
                sqlite_path_pos,
                pos.get("signal_id"),
                str(pos.get("exchange") or ""),
                contract_label,
                str(pos.get("direction") or ""),
            )
            sess = _fetch_linked_opportunity_session(
                sqlite_path_pos,
                pos.get("signal_id"),
                exchange=str(pos.get("exchange") or ""),
                contract=contract_label,
                direction_en=str(pos.get("direction") or ""),
            )
            entry["expected_ann_pct"] = sess["ann_pct"]
            net_u = sess["net_usdt"]
            tr_s = sess["tradeable"]
            em_s = sess["expected_max_usdt"]
            # 预计收益：历史会话中的净利 net_usdt × 实际开仓张数（与机会表净利润同口径）
            if net_u is not None and qty_min is not None:
                try:
                    entry["expected_profit_usdt"] = float(net_u) * float(qty_min)
                except (TypeError, ValueError):
                    entry["expected_profit_usdt"] = None
            elif (
                em_s is not None
                and tr_s is not None
                and tr_s > 0
                and qty_min is not None
            ):
                try:
                    entry["expected_profit_usdt"] = float(em_s) * (
                        float(qty_min) / float(tr_s)
                    )
                except (TypeError, ValueError):
                    entry["expected_profit_usdt"] = None
            elif em_s is not None:
                try:
                    entry["expected_profit_usdt"] = float(em_s)
                except (TypeError, ValueError):
                    entry["expected_profit_usdt"] = None
            elif net_u is not None and tr_s is not None:
                try:
                    entry["expected_profit_usdt"] = float(net_u) * float(tr_s)
                except (TypeError, ValueError):
                    entry["expected_profit_usdt"] = None
        entry["total_spread_usdt"] = _scaled_total_spread_usdt(
            gross_snap, tradeable_snap, qty_min
        )
        entry["total_fee_usdt"] = _sum_position_orders_fee_usdt(
            entry["orders"], entry.get("index_price_usdt"), str(pos.get("symbol") or "")
        )
        _ts, _tf = entry["total_spread_usdt"], entry["total_fee_usdt"]
        if _ts is not None and _tf is not None:
            try:
                entry["total_float_profit_usdt"] = float(_ts) - float(_tf)
            except (TypeError, ValueError):
                entry["total_float_profit_usdt"] = None
        # Attach live prices from tracker cache
        from pcp_arbitrage import position_tracker as _pt
        entry["leg_live"] = _pt.get_leg_prices_cache().get(pos["id"], {})
        positions_out.append(entry)
    payload["positions"] = positions_out
    # Add account balances and arbitrage_enabled status
    if _app_cfg:
        account_info = []
        for ex_name, bal in _account_balances_cache.items():
            ex_cfg = _app_cfg.exchanges.get(ex_name)
            account_info.append(
                {
                    "exchange": ex_name,
                    "total_eq_usdt": bal.get("total_eq_usdt", 0),
                    "adj_eq_usdt": bal.get("adj_eq_usdt", 0),
                    "im_pct": bal.get("im_pct", 0),
                    "mm_pct": bal.get("mm_pct", 0),
                    "arbitrage_enabled": ex_cfg.arbitrage_enabled if ex_cfg else False,
                }
            )
        payload["accounts"] = account_info
    else:
        payload["accounts"] = []
    return payload
