"""Web dashboard: HTML tables for opportunities and triplet overview, served via aiohttp + WebSocket."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape

if TYPE_CHECKING:
    from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
_jinja_env: Environment | None = None

_notification_queue: asyncio.Queue[dict] = asyncio.Queue()

# Cache for open positions, updated by position_tracker after each poll
_open_positions_cache: list[dict] = []

# Cache for account balances, updated periodically
_account_balances_cache: dict[str, dict] = {}  # {exchange: {total_eq_usdt, adj_eq_usdt, im_pct, mm_pct, ...}}

# Reference to AppConfig, set when the dashboard starts
_app_cfg = None  # type: ignore[assignment]


def set_app_config(cfg) -> None:  # type: ignore[type-arg]
    """Store AppConfig reference for use in payload building."""
    global _app_cfg
    _app_cfg = cfg


def update_positions_cache(positions: list[dict]) -> None:
    """Update open positions cache for the web dashboard. Called by position_tracker."""
    global _open_positions_cache
    _open_positions_cache = list(positions)


def update_account_balances(balances: dict[str, dict]) -> None:
    """Update account balances cache for the web dashboard. Called periodically from main."""
    global _account_balances_cache
    _account_balances_cache = dict(balances)


def push_notification(notification: dict) -> None:
    """Enqueue a notification for broadcast to all web dashboard clients."""
    _notification_queue.put_nowait(notification)


def _get_jinja_env() -> Environment:
    global _jinja_env
    if _jinja_env is None:
        _jinja_env = Environment(
            loader=FileSystemLoader(str(_TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
    return _jinja_env


def _render_index_html() -> str:
    return _get_jinja_env().get_template("dashboard.html").render()


async def run_web_dashboard_loop(
    dash: "OpportunityDashboard",
    host: str,
    port: int,
    width: int,
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
        except Exception:
            pass
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
        from pcp_arbitrage.db import list_opportunity_sessions_history

        sessions = list_opportunity_sessions_history(sqlite_path, limit=lim)
        return web.Response(
            content_type="application/json",
            text=json.dumps({"sessions": sessions}, ensure_ascii=False),
        )

    async def _manual_entry_handler(request: web.Request) -> web.Response:
        if _app_cfg is None:
            return web.Response(
                content_type="application/json", status=503,
                text=json.dumps({"ok": False, "error": "config not ready"}, ensure_ascii=False),
            )
        try:
            body = await request.json()
        except Exception:
            return web.Response(
                content_type="application/json", status=400,
                text=json.dumps({"ok": False, "error": "invalid JSON"}, ensure_ascii=False),
            )

        exchange = str(body.get("exchange", ""))
        label = str(body.get("label", ""))
        direction_cn = str(body.get("direction", ""))  # "正向" or "反向"

        # Only OKX supported
        if exchange.lower() != "okx":
            return web.Response(
                content_type="application/json", status=400,
                text=json.dumps({"ok": False, "error": "仅支持 OKX 手动下单"}, ensure_ascii=False),
            )

        exc_cfg = _app_cfg.exchanges.get("OKX") or _app_cfg.exchanges.get("okx")
        if exc_cfg is None or not exc_cfg.api_key:
            return web.Response(
                content_type="application/json", status=400,
                text=json.dumps({"ok": False, "error": "OKX 未配置 API key"}, ensure_ascii=False),
            )

        # --- Find the live row ---
        direction_key = "forward" if direction_cn == "正向" else "reverse"
        row = None
        for key, r in dash._rows.items():
            if r.exchange == exchange and r.label == label and r.direction_cn == direction_cn and r.active:
                row = r
                break
        if row is None:
            return web.Response(
                content_type="application/json", status=404,
                text=json.dumps({"ok": False, "error": "机会已消失或非激活状态"}, ensure_ascii=False),
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
                    content_type="application/json", status=409,
                    text=json.dumps({"ok": False, "error": "已有持仓"}, ensure_ascii=False),
                )

        # --- Check OKX balance ---
        try:
            from pcp_arbitrage.account_fetcher import get_exchange_balance
            bal = await get_exchange_balance(exc_cfg, _app_cfg)
            if bal is None or bal.get("adj_eq_usdt", 0) <= 0:
                return web.Response(
                    content_type="application/json", status=400,
                    text=json.dumps({"ok": False, "error": "OKX 余额不足或无法获取"}, ensure_ascii=False),
                )
        except Exception as exc:
            return web.Response(
                content_type="application/json", status=500,
                text=json.dumps({"ok": False, "error": f"余额查询失败: {exc}"}, ensure_ascii=False),
            )

        # --- Fetch triplet from DB (for call_id/put_id/future_id) ---
        if not sqlite_path_entry:
            return web.Response(
                content_type="application/json", status=503,
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
                "SELECT symbol, expiry, strike, call_id, put_id, future_id FROM triplets "
                "WHERE exchange=? AND symbol=? AND expiry=? AND ABS(strike-?)<=0.01 LIMIT 1",
                (exchange, sym, expiry, strike),
            )
            triplet_row = cur.fetchone()
        finally:
            conn.close()

        if triplet_row is None:
            return web.Response(
                content_type="application/json", status=404,
                text=json.dumps({"ok": False, "error": "triplet 不在 DB 中，请稍后重试"}, ensure_ascii=False),
            )

        from pcp_arbitrage.models import Triplet
        from pcp_arbitrage.pcp_calculator import ArbitrageSignal

        triplet = Triplet(
            symbol=triplet_row["symbol"],
            expiry=triplet_row["expiry"],
            strike=float(triplet_row["strike"]),
            call_id=triplet_row["call_id"],
            put_id=triplet_row["put_id"],
            future_id=triplet_row["future_id"],
        )

        # Rebuild ArbitrageSignal from stored row prices
        spot = dash._index_prices.get(sym, 0.0) or 0.0
        call_px_coin = (row.call_px_usdt / spot) if spot > 0 and row.call_px_usdt else 0.0
        put_px_coin = (row.put_px_usdt / spot) if spot > 0 and row.put_px_usdt else 0.0
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
            tradeable_qty=float(row.tradeable) if row.tradeable else 1.0,
            call_fee=row.call_fee or 0.0,
            put_fee=row.put_fee or 0.0,
            fut_fee=row.fut_fee or 0.0,
        )

        # --- Submit entry (non-blocking) ---
        from pcp_arbitrage import order_manager as _om
        asyncio.create_task(_om.submit_entry(triplet, signal, None, _app_cfg, sqlite_path_entry))

        return web.Response(
            content_type="application/json",
            text=json.dumps({"ok": True, "message": "下单已提交"}, ensure_ascii=False),
        )

    app = web.Application()
    app.router.add_get("/", _index_handler)
    app.router.add_get("/ws-data", _ws_data_handler)
    app.router.add_get("/api/triplet-summary", _triplet_summary_handler)
    app.router.add_get("/api/opportunity-history", _opportunity_history_handler)
    app.router.add_post("/api/manual-entry", _manual_entry_handler)


    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
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

    asyncio.ensure_future(_price_push_loop())
    asyncio.ensure_future(_broadcast_loop())
    asyncio.ensure_future(_notification_push_loop())

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


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
    from pcp_arbitrage.db import coalesce_per_leg_fees

    # build meta from runner_meta
    exchanges_meta = []
    all_symbols: list[str] = []
    for ex_name in sorted(dash._runner_meta.keys()):
        m = dash._runner_meta[ex_name]
        exchanges_meta.append({
            "name": ex_name,
            "settle_type": m.settle_type,
            "option_taker_rate": m.option_taker_rate,
            "option_maker_rate": m.option_maker_rate,
            "future_taker_rate": m.future_taker_rate,
            "future_maker_rate": m.future_maker_rate,
            "n_triplets": m.n_triplets,
            "symbols": list(m.symbols),
        })
        for s in m.symbols:
            if s not in all_symbols:
                all_symbols.append(s)
    payload: dict = {
        "prices": dict(dash._index_prices),
        "start_ts": dash._started_at,
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

        rows = []
        for r in dash._rows.values():
            dur = None
            if not r.active and r.frozen_active_duration_sec is not None:
                dur = r.frozen_active_duration_sec
            hp = hist_peak.get((r.exchange, r.label, r.direction_cn))
            cf, pf, ff = coalesce_per_leg_fees(r.fee, r.call_fee, r.put_fee, r.fut_fee)
            # Derive position-key fields from label (format: SYMBOL-EXPIRY-STRIKE)
            parts = r.label.split("-")
            row_symbol = parts[0] if parts else ""
            row_expiry = parts[1] if len(parts) > 1 else ""
            try:
                row_strike = float(parts[2].replace(",", "")) if len(parts) > 2 else 0.0
            except (ValueError, AttributeError):
                row_strike = float(r.strike) if r.strike is not None else 0.0
            row_direction_key = "forward" if r.direction_cn == "正向" else "reverse"
            has_position = (r.exchange, row_symbol, row_expiry, row_strike, row_direction_key) in active_pos_keys
            rows.append({
                "exchange":       r.exchange,
                "label":          r.label,
                "direction":      r.direction_cn,
                "active":         r.active,
                "first_active":   float(r.first_active) if r.active else None,
                "frozen_duration": float(dur) if dur is not None else None,
                "last_eval":      float(r.last_eval),
                "last_active_eval": None if r.last_active_eval is None else float(r.last_active_eval),
                "gross":          r.gross,
                "fee":            r.fee,
                "net":            r.net,
                "tradeable":      r.tradeable,
                "days_to_expiry": r.days_to_expiry,
                "ann_pct":        r.ann_pct,
                "hist_ann_pct": None if hp is None else hp.get("ann_pct"),
                "hist_duration_sec": None if hp is None else hp.get("duration_sec"),
                "strike":         r.strike,
                "call_px_usdt":   r.call_px_usdt,
                "put_px_usdt":    r.put_px_usdt,
                "fut_px_usdt":    r.fut_px_usdt,
                "call_fee":       cf,
                "put_fee":        pf,
                "fut_fee":        ff,
                "expected_max":   r.net * r.tradeable if r.tradeable is not None else None,
                "has_position":   has_position,
            })
        payload["rows"] = _dedupe_opp_rows_for_web(rows)
    payload["positions"] = [
        {
            "id": pos["id"],
            "exchange": pos["exchange"],
            "symbol": pos["symbol"],
            "expiry": pos["expiry"],
            "strike": pos["strike"],
            "direction": pos["direction"],
            "status": pos["status"],
            "current_mark_usdt": pos.get("current_mark_usdt"),
            "opened_at": pos["opened_at"],
        }
        for pos in _open_positions_cache
    ]
    # Add account balances and arbitrage_enabled status
    if _app_cfg:
        account_info = []
        for ex_name, bal in _account_balances_cache.items():
            ex_cfg = _app_cfg.exchanges.get(ex_name)
            account_info.append({
                "exchange": ex_name,
                "total_eq_usdt": bal.get("total_eq_usdt", 0),
                "adj_eq_usdt": bal.get("adj_eq_usdt", 0),
                "im_pct": bal.get("im_pct", 0),
                "mm_pct": bal.get("mm_pct", 0),
                "arbitrage_enabled": ex_cfg.arbitrage_enabled if ex_cfg else False,
            })
        payload["accounts"] = account_info
    else:
        payload["accounts"] = []
    return payload
