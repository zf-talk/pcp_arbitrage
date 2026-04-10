import logging
import math
import os
import time
from collections import defaultdict

from pcp_arbitrage.config import AppConfig, ExchangeConfig
from pcp_arbitrage.exchange_symbols import format_quote_display, format_strike_display, symbols_for_exchange
from pcp_arbitrage.fee_fetcher import fetch_fee_rates
from pcp_arbitrage.instruments import build_triplets, describe_unmatched_expiry
from pcp_arbitrage.market_data import MarketData
from pcp_arbitrage.models import BookSnapshot, FeeRates, top_of_book_contracts
from pcp_arbitrage.okx_client import OKXRestClient, OKXWebSocketClient
from pcp_arbitrage.pcp_calculator import OKX_INVERSE_FUT_USD_FACE
from pcp_arbitrage.signal_output import (
    dashboard_enabled,
    emit_triplet_if_books_ready,
    notify_monitoring_ready,
    register_dashboard_runner_meta,
    register_startup_market_sweep,
    update_dashboard_index_price,
    update_dashboard_leg_price,
)
from pcp_arbitrage.tracing import write_pairing_log

logger = logging.getLogger(__name__)


def format_triplet_summary(
    triplets: list,
    all_options: list[dict],
    all_futures: list[dict],
    symbols: list[str],
    margin_label: str,
    *,
    atm_prices: dict[str, float] | None = None,
    atm_range: float | None = None,
    now_ms: int | None = None,
    min_days_to_expiry: float = 1.0,
) -> str:
    fut_expiries: dict[str, set[str]] = defaultdict(set)
    for f in all_futures:
        parts = f["instId"].split("-")
        if len(parts) == 3 and parts[1] == margin_label and parts[0] in symbols:
            fut_expiries[parts[0]].add(parts[2])

    opt_expiries: dict[str, set[str]] = defaultdict(set)
    for o in all_options:
        parts = o["instId"].split("-")
        if len(parts) == 5 and parts[0] in symbols:
            opt_expiries[parts[0]].add(parts[2])

    matched: dict[str, set[str]] = defaultdict(set)
    matched_strikes: dict[tuple[str, str], list[float]] = defaultdict(list)
    for t in triplets:
        matched[t.symbol].add(t.expiry)
        matched_strikes[(t.symbol, t.expiry)].append(t.strike)

    lines: list[str] = []
    lines.append("\n合约匹配摘要")
    lines.append("─" * 60)
    for sym in symbols:
        all_exp = sorted(opt_expiries[sym])
        px = atm_prices.get(sym) if atm_prices else None
        try:
            pxf = float(px) if px is not None else float("nan")
        except (TypeError, ValueError):
            pxf = float("nan")
        if math.isfinite(pxf) and pxf > 0:
            px_s = format_quote_display(sym, pxf)
            lines.append(
                f"\n  {sym}  现指 {px_s}  (期权到期日共 {len(all_exp)} 个, "
                f"{margin_label} 交割合约 {len(fut_expiries[sym])} 个)"
            )
        else:
            lines.append(
                f"\n  {sym}  (期权到期日共 {len(all_exp)} 个, "
                f"{margin_label} 交割合约 {len(fut_expiries[sym])} 个)"
            )
        lines.append(f"  {'到期日':<10}  {'说明'}")
        lines.append(f"  {'─'*8}  {'─'*52}")
        for exp in all_exp:
            if exp in matched[sym]:
                strikes = sorted(matched_strikes[(sym, exp)])
                strike_str = "  ".join(format_strike_display(sym, k) for k in strikes)
                lines.append(f"  {exp:<10}  ✓ 匹配  {strike_str}")
            elif exp in fut_expiries[sym]:
                if atm_prices is not None and atm_range is not None and now_ms is not None:
                    detail = describe_unmatched_expiry(
                        sym,
                        exp,
                        all_options,
                        margin_label,
                        atm_prices,
                        atm_range,
                        now_ms,
                        min_days_to_expiry=min_days_to_expiry,
                    )
                    lines.append(f"  {exp:<10}  − {detail}")
                else:
                    lines.append(f"  {exp:<10}  − 未匹配（缺指数/时间参数）")
            else:
                lines.append(f"  {exp:<10}  ✗ 无交割合约")
    lines.append("\n" + "─" * 60)
    return "\n".join(lines)


def print_triplet_summary(
    exchange: str,
    triplets: list,
    all_options: list[dict],
    all_futures: list[dict],
    symbols: list[str],
    margin_label: str,
    *,
    pairing_log_dir: str | None = None,
    atm_prices: dict[str, float] | None = None,
    atm_range: float | None = None,
    now_ms: int | None = None,
    min_days_to_expiry: float = 1.0,
) -> None:
    text = format_triplet_summary(
        triplets,
        all_options,
        all_futures,
        symbols,
        margin_label,
        atm_prices=atm_prices,
        atm_range=atm_range,
        now_ms=now_ms,
        min_days_to_expiry=min_days_to_expiry,
    )
    print(text)
    if pairing_log_dir:
        write_pairing_log(pairing_log_dir, exchange, text)


def _build_ws_args(triplets: list) -> list[dict]:
    inst_ids: set[str] = set()
    for t in triplets:
        inst_ids.update([t.call_id, t.put_id, t.future_id])
    return [{"channel": "books5", "instId": inst_id} for inst_id in sorted(inst_ids)]


def _build_index_ticker_args(symbols: list[str]) -> list[dict]:
    """OKX index-tickers channel: one sub per symbol (e.g. BTC-USDT)."""
    return [{"channel": "index-tickers", "instId": f"{sym}-USDT"} for sym in symbols]


class OKXRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        ex = self._ex
        app = self._app

        # Apply PAPER_TRADING env-var override
        if os.environ.get("PAPER_TRADING", "").lower() == "true":
            import dataclasses
            ex = dataclasses.replace(ex, is_paper_trading=True)

        margin_type = ex.margin_type
        margin_label = "USD" if margin_type == "coin" else "USDT"
        symbols = symbols_for_exchange("okx", app.symbols)
        if not symbols:
            notify_monitoring_ready("okx")
            return

        async with OKXRestClient(
            api_key=ex.api_key,
            secret=ex.secret_key,
            passphrase=ex.passphrase,
            is_paper=ex.is_paper_trading,
        ) as rest:
            # 1. Fetch fee rates
            logger.info("[okx] Fetching fee rates...")
            fee_rates: FeeRates = await fetch_fee_rates(rest)

            # 2. Print account balance
            bal_info = await rest.get_balance()
            details = bal_info.get("details", []) if bal_info else []
            if details:
                bal_str = "  ".join(
                    f"{b['ccy']} {float(b.get('availBal') or 0):.6g} (≈${float(b.get('eqUsd') or 0):.2f})"
                    for b in details
                )
                logger.info("[okx] Account balance: %s", bal_str)
            else:
                logger.info("[okx] Account balance: (empty)")

            # 3. Fetch instruments
            logger.info("[okx] Fetching instruments for %s...", symbols)
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            for sym in symbols:
                uly = f"{sym}-{margin_label}"
                opts = await rest.get_instruments("OPTION", uly=uly)
                futs = await rest.get_instruments("FUTURES", uly=uly)
                all_options.extend(opts)
                all_futures.extend(futs)

                ticker = await rest.get_ticker(f"{sym}-USDT")
                atm_prices[sym] = float(ticker.get("last", 0))
                logger.info("[okx] %s ATM price: %.2f", sym, atm_prices[sym])

            # 4. Build triplets
            now_ms = int(time.time() * 1000)
            triplets = build_triplets(
                options=all_options,
                futures=all_futures,
                atm_prices=atm_prices,
                symbols=symbols,
                atm_range=app.atm_range,
                now_ms=now_ms,
                margin_type=margin_type,
                min_days_to_expiry=app.min_days_to_expiry,
            )
            logger.info("[okx] Built %d triplets", len(triplets))
            settle_type = "U本位(USDT)" if margin_type == "usdt" else "币本位(USD)"
            register_dashboard_runner_meta(
                "okx",
                option_taker_rate=fee_rates.option_taker_rate,
                option_maker_rate=fee_rates.option_maker_rate,
                future_taker_rate=fee_rates.future_taker_rate,
                future_maker_rate=fee_rates.future_maker_rate,
                n_triplets=len(triplets),
                settle_type=settle_type,
                symbols=symbols,
                triplets=triplets,
            )
            pairing_dir = app.pairing_log_dir.strip() if app.pairing_log_dir else ""
            print_triplet_summary(
                "okx",
                triplets,
                all_options,
                all_futures,
                symbols,
                margin_label,
                pairing_log_dir=pairing_dir or None,
                atm_prices=atm_prices,
                atm_range=app.atm_range,
                now_ms=now_ms,
                min_days_to_expiry=app.min_days_to_expiry,
            )
            if not triplets:
                logger.error("[okx] No triplets found — check config and OKX connectivity")
                notify_monitoring_ready("okx")
                return

            # 5. Market data store
            market = MarketData()
            exp_ms_by_call: dict[str, int] = {o["instId"]: int(o["expTime"]) for o in all_options}

            async def on_reconnect() -> None:
                logger.warning("[okx] WebSocket reconnected — clearing order book cache")
                market.clear()

            async def on_message(msg: dict) -> None:
                if msg.get("event"):
                    if msg.get("event") == "error":
                        logger.error("[ws] Error: %s", msg)
                    return

                arg = msg.get("arg", {})
                channel = arg.get("channel", "")
                inst_id = arg.get("instId", "")
                data = msg.get("data", [])
                if not data:
                    return

                # index-tickers: update dashboard index price
                if channel == "index-tickers":
                    tick = data[0]
                    idx_px = tick.get("idxPx")
                    if idx_px:
                        # instId is like "BTC-USDT" → symbol is "BTC"
                        sym = inst_id.split("-")[0]
                        update_dashboard_index_price(sym, float(idx_px))
                    return

                book = data[0]
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                ts = int(book.get("ts", 0))

                if not bids or not asks:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                    bid_sz=top_of_book_contracts(bids[0]),
                    ask_sz=top_of_book_contracts(asks[0]),
                )
                market.update(inst_id, snap)
                update_dashboard_leg_price("okx", inst_id, snap.bid, snap.ask)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = atm_prices[t.symbol] if margin_type == "coin" else 1.0
                    inv_face = (
                        OKX_INVERSE_FUT_USD_FACE.get(t.symbol)
                        if margin_type == "coin"
                        else None
                    )
                    emit_triplet_if_books_ready(
                        "okx",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=spot_price,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=atm_prices[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                        future_inverse_usd_face=inv_face,
                    )

            async def _startup_sweep_okx() -> None:
                now_ms = int(time.time() * 1000)
                for t in triplets:
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = atm_prices[t.symbol] if margin_type == "coin" else 1.0
                    inv_face = (
                        OKX_INVERSE_FUT_USD_FACE.get(t.symbol)
                        if margin_type == "coin"
                        else None
                    )
                    emit_triplet_if_books_ready(
                        "okx",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=spot_price,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=atm_prices[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                        future_inverse_usd_face=inv_face,
                    )

            # 6. Start WebSocket
            ws_args = _build_ws_args(triplets)
            idx_args = _build_index_ticker_args(symbols)
            logger.info("[okx] Subscribing to %d channels + %d index tickers", len(ws_args), len(idx_args))
            ws_url = (
                "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
                if ex.is_paper_trading
                else "wss://ws.okx.com:8443/ws/v5/public"
            )
            ws_client = OKXWebSocketClient(ws_url, on_message=on_message, on_reconnect=on_reconnect)
            ws_client.add_subscriptions(ws_args + idx_args)
            if dashboard_enabled():
                register_startup_market_sweep(_startup_sweep_okx)
            notify_monitoring_ready("okx")
            await ws_client.run()
