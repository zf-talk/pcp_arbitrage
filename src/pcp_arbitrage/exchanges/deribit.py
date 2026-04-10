import asyncio
import json
import logging
import math
import time

import aiohttp

from pcp_arbitrage.config import AppConfig, ExchangeConfig
from pcp_arbitrage.exchange_symbols import symbols_for_exchange
from pcp_arbitrage.exchanges.okx import print_triplet_summary

logger = logging.getLogger(__name__)

MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

# 月份反查表（模块级，避免每次调用重建）
_MONTH_REV = {v: k for k, v in MONTH_MAP.items()}


def _parse_deribit_date_segment(s: str) -> tuple[str, str, str] | None:
    """
    Deribit 到期段 → (日两位, 月三字母, 年两位)。文档: DMMMYY 或 DDMMMYY
    （1–9 日常为 6 字符如 6APR26，10–31 日为 7 字符如 10APR26）。
    """
    if len(s) == 7:
        if not s[:2].isdigit() or s[2:5] not in MONTH_MAP or not s[5:7].isdigit():
            return None
        return s[:2], s[2:5], s[5:7]
    if len(s) == 6:
        if s[0] == "0":
            return None
        if not s[0].isdigit() or s[1:4] not in MONTH_MAP or not s[4:6].isdigit():
            return None
        # 单日 1–9，无前导零
        return f"0{s[0]}", s[1:4], s[4:6]
    return None


def _is_deribit_expiry_token(s: str) -> bool:
    """True iff s is a Deribit dated segment (DDMMMYY or DMMMYY for days 1–9)."""
    return _parse_deribit_date_segment(s) is not None


def _deribit_day_token_for_api(dd: str) -> str:
    """内部 YYMMDD 的日部分 → Deribit 频道名中的日（1–9 不带前导零）。"""
    return str(int(dd, 10))


def _deribit_date_to_yymmdd(deribit_date: str) -> str:
    """'27JUN25' / '6APR26' → '250627' / '260406'"""
    parsed = _parse_deribit_date_segment(deribit_date)
    if parsed is None:
        raise ValueError(f"not a Deribit date segment: {deribit_date!r}")
    day, mon, year = parsed
    return f"{year}{MONTH_MAP[mon]}{day}"


def _canonical_deribit_strike_str(v: float) -> str:
    """String for instId stk: whole numbers without .0; keep decimals for USDC alts (~1.32)."""
    if math.isnan(v) or math.isinf(v) or v <= 0:
        return "0"
    # Large integer strikes (BTC/ETH) — avoid "70000.00000000001"
    if abs(v - round(v)) < 1e-7 * max(1.0, abs(v)):
        return str(int(round(v)))
    s = f"{v:.10f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _deribit_option_strike_str(o: dict, strike_token: str) -> str:
    """
    Strike from REST `strike` when present and positive; else from instrument_name segment.

    Must not truncate fractional strikes: Linear USDC alts use ~1.32 USDC strikes; int()
    would collapse 1.32→1 and break ATM pairing vs index.

    Deribit may return `strike` as 0 while `instrument_name` still has the strike.
    """
    raw = o.get("strike")
    api_v: float | None = None
    if raw is not None:
        try:
            api_v = float(raw)
        except (TypeError, ValueError):
            api_v = None
    try:
        named_v = float(strike_token)
    except (TypeError, ValueError):
        named_v = float("nan")

    if api_v is not None and not math.isnan(api_v) and api_v > 0:
        return _canonical_deribit_strike_str(api_v)
    if not math.isnan(named_v) and named_v > 0:
        return _canonical_deribit_strike_str(named_v)
    return "0"


def _normalize_instruments(
    raw_options: list[dict],
    raw_futures: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Deribit get_instruments result 列表 → 项目内部统一格式。
    settle 固定 'USD'（Deribit 币本位）。
    """
    normalized_options = []
    for o in raw_options:
        name = o["instrument_name"]          # e.g. "BTC-27JUN25-70000-C"
        parts = name.split("-")              # ["BTC", "27JUN25", "70000", "C"]
        if len(parts) != 4:
            continue
        date_part = parts[1]
        if not _is_deribit_expiry_token(date_part):
            continue
        sym = parts[0]
        exp = _deribit_date_to_yymmdd(date_part)
        stk = _deribit_option_strike_str(o, parts[2])
        opt_type = "C" if o["option_type"] == "call" else "P"
        inst_id = f"{sym}-USD-{exp}-{stk}-{opt_type}"
        normalized_options.append({
            "instId": inst_id,
            "stk": stk,
            "optType": opt_type,
            "expTime": str(o["expiration_timestamp"]),  # already ms
        })

    normalized_futures = []
    for f in raw_futures:
        name = f["instrument_name"]          # e.g. "BTC-27JUN25"
        parts = name.split("-")
        if len(parts) != 2:
            continue                         # skip multi-part names
        date_part = parts[1]
        if not _is_deribit_expiry_token(date_part):
            continue
        sym = parts[0]
        exp = _deribit_date_to_yymmdd(date_part)
        normalized_futures.append({
            "instId": f"{sym}-USD-{exp}",
        })

    return normalized_options, normalized_futures


def _internal_to_deribit(inst_id: str) -> str:
    """
    项目内部 instId → Deribit 原始 instrument_name，用于构建 WS 频道名。
    BTC-USD-250627-70000-C → BTC-27JUN25-70000-C
    BTC-USD-250627         → BTC-27JUN25
    """
    parts = inst_id.split("-")
    if len(parts) == 5:
        # option: SYM-USD-YYMMDD-STK-TYPE
        sym, _, yymmdd, stk, opt = parts
        yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
        mon = _MONTH_REV[mm]
        d_tok = _deribit_day_token_for_api(dd)
        return f"{sym}-{d_tok}{mon}{yy}-{stk}-{opt}"
    elif len(parts) == 3:
        # future: SYM-USD-YYMMDD
        sym, _, yymmdd = parts
        yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
        mon = _MONTH_REV[mm]
        d_tok = _deribit_day_token_for_api(dd)
        return f"{sym}-{d_tok}{mon}{yy}"
    return inst_id


class DeribitRestClient:
    BASE = "https://www.deribit.com"

    def __init__(self, api_key: str, secret: str) -> None:
        self._api_key = api_key
        self._secret = secret
        self._token: str = ""
        self._token_expires_at: float = 0.0
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(base_url=self.BASE)
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    async def _authenticate(self) -> None:
        assert self._session is not None
        payload = {
            "jsonrpc": "2.0",
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self._api_key,
                "client_secret": self._secret,
            },
            "id": 1,
        }
        async with self._session.post("/api/v2/public/auth", json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
        result = data["result"]
        self._token = result["access_token"]
        self._token_expires_at = time.time() + result["expires_in"]

    async def _refresh_token_if_needed(self) -> None:
        if time.time() >= self._token_expires_at - 60:
            await self._authenticate()

    async def get_instruments(self, currency: str, kind: str) -> list[dict]:
        """Public endpoint. Returns result list."""
        assert self._session is not None
        params = {"currency": currency, "kind": kind}
        async with self._session.get("/api/v2/public/get_instruments", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return data["result"]

    async def get_index_price(self, index_name: str) -> dict:
        """Public endpoint. Returns result dict."""
        assert self._session is not None
        params = {"index_name": index_name}
        async with self._session.get("/api/v2/public/get_index_price", params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return data["result"]

    async def get_account_summary(self, currency: str) -> dict:
        """Private endpoint. Returns result dict."""
        await self._refresh_token_if_needed()
        assert self._session is not None
        params = {"currency": currency, "extended": "true"}
        headers = {"Authorization": f"Bearer {self._token}"}
        async with self._session.get(
            "/api/v2/private/get_account_summary", params=params, headers=headers
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return data["result"]


class DeribitWebSocketClient:
    """Deribit JSON-RPC 2.0 WebSocket 客户端，单连接，指数退避重连。"""

    WS_URL = "wss://www.deribit.com/ws/api/v2"
    HEARTBEAT_INTERVAL = 5          # Deribit 要求 10s 内发一次消息
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60
    # https://docs.deribit.com/articles/market-data-collection-best-practices
    MAX_CHANNELS_PER_SUBSCRIBE = 500
    SUBSCRIBE_INTERVAL_SEC = 0.35   # public/subscribe 约 3.3/s，多批之间留间隔

    def __init__(self, on_message, on_reconnect=None) -> None:
        self._on_message = on_message
        self._on_reconnect = on_reconnect
        self._channels: list[str] = []

    def set_channels(self, channels: list[str]) -> None:
        self._channels = channels

    async def run(self) -> None:
        retry_delay = self.RECONNECT_BASE
        while True:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[deribit ws] Disconnected: %s — reconnecting in %ds", exc, retry_delay)
            else:
                logger.warning("[deribit ws] Connection closed — reconnecting in %ds", retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, self.RECONNECT_MAX)
            if self._on_reconnect:
                await self._on_reconnect()

    async def _connect_and_run(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL) as ws:
                logger.info("[deribit ws] Connected")
                await self._subscribe(ws)
                ping_task = asyncio.create_task(self._heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._on_message(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws) -> None:
        channels = self._channels
        n = len(channels)
        cap = self.MAX_CHANNELS_PER_SUBSCRIBE
        n_batches = (n + cap - 1) // cap
        if n_batches > 1:
            logger.info(
                "[deribit ws] Subscribing in %d batches (max %d channels per request, %d total)",
                n_batches,
                cap,
                n,
            )
        req_id = 1
        for i in range(0, n, cap):
            chunk = channels[i : i + cap]
            payload = {
                "jsonrpc": "2.0",
                "method": "public/subscribe",
                "params": {"channels": chunk},
                "id": req_id,
            }
            req_id += 1
            await ws.send_str(json.dumps(payload))
            if i + cap < n:
                await asyncio.sleep(self.SUBSCRIBE_INTERVAL_SEC)

    async def _heartbeat(self, ws) -> None:
        ping = json.dumps({"jsonrpc": "2.0", "method": "public/test", "id": 99})
        while True:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            await ws.send_str(ping)


async def _run_ws_with_tick(
    ws_client: DeribitWebSocketClient,
    *,
    tick_interval_sec: float,
    log_label: str,
    n_triplets: int,
    n_channels: int,
    touched_call_ids: set[str],
    touched_inst_ids: set[str],
) -> None:
    """与 WS 并行；tick_interval_sec > 0 时周期性打 DEBUG，统计本间隔内有 book 推送的 triplet / 频道数。"""
    if tick_interval_sec <= 0:
        await ws_client.run()
        return

    async def tick_loop() -> None:
        n = 0
        while True:
            await asyncio.sleep(tick_interval_sec)
            n += 1
            n_triplet_touch = len(touched_call_ids)
            n_channel_touch = len(touched_inst_ids)
            touched_call_ids.clear()
            touched_inst_ids.clear()
            logger.debug(
                "[%s] tick #%d — triplets %d/%d (changed/total), channels %d/%d (changed/total) (WS alive)",
                log_label,
                n,
                n_triplet_touch,
                n_triplets,
                n_channel_touch,
                n_channels,
            )

    await asyncio.gather(ws_client.run(), tick_loop())


def _normalize_instruments_linear(
    raw_options: list[dict],
    raw_futures: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Deribit Linear (USDC) get_instruments result → 项目内部统一格式。
    工具名格式: BTC_USDC-27JUN25-70000-C / BTC_USDC-27JUN25
    settle 固定 'USDC'.
    """
    normalized_options = []
    for o in raw_options:
        name = o["instrument_name"]          # e.g. "BTC_USDC-27JUN25-70000-C"
        parts = name.split("-")              # ["BTC_USDC", "27JUN25", "70000", "C"]
        if len(parts) != 4:
            continue
        date_part = parts[1]
        if not _is_deribit_expiry_token(date_part):
            continue
        # extract base symbol from "BTC_USDC"
        sym = parts[0].split("_")[0]         # "BTC"
        exp = _deribit_date_to_yymmdd(date_part)
        stk = _deribit_option_strike_str(o, parts[2])
        opt_type = "C" if o["option_type"] == "call" else "P"
        inst_id = f"{sym}-USDC-{exp}-{stk}-{opt_type}"
        normalized_options.append({
            "instId": inst_id,
            "stk": stk,
            "optType": opt_type,
            "expTime": str(o["expiration_timestamp"]),
        })

    normalized_futures = []
    for f in raw_futures:
        name = f["instrument_name"]          # e.g. "BTC_USDC-27JUN25"
        parts = name.split("-")              # ["BTC_USDC", "27JUN25"]
        if len(parts) != 2:
            continue
        date_part = parts[1]
        if not _is_deribit_expiry_token(date_part):
            continue
        sym = parts[0].split("_")[0]         # "BTC"
        exp = _deribit_date_to_yymmdd(date_part)
        normalized_futures.append({
            "instId": f"{sym}-USDC-{exp}",
        })

    return normalized_options, normalized_futures


def _internal_to_deribit_linear(inst_id: str) -> str:
    """
    项目内部 instId → Deribit Linear instrument_name，用于构建 WS 频道名。
    BTC-USDC-250627-70000-C → BTC_USDC-27JUN25-70000-C
    BTC-USDC-250627         → BTC_USDC-27JUN25
    """
    parts = inst_id.split("-")
    if len(parts) == 5:
        sym, _, yymmdd, stk, opt = parts
        yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
        mon = _MONTH_REV[mm]
        d_tok = _deribit_day_token_for_api(dd)
        return f"{sym}_USDC-{d_tok}{mon}{yy}-{stk}-{opt}"
    elif len(parts) == 3:
        sym, _, yymmdd = parts
        yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
        mon = _MONTH_REV[mm]
        d_tok = _deribit_day_token_for_api(dd)
        return f"{sym}_USDC-{d_tok}{mon}{yy}"
    return inst_id


def _deribit_book_channel(instrument_name: str) -> str:
    """
    聚合盘口频道 book.(instrument).(group).(depth).(interval)。
    Deribit 文档：bids/asks 为 [price, amount] 数对，与下面 BookSnapshot 解析一致。
    旧写法 book.{inst}.5 不是合法 API（interval 只能是 raw / 100ms / agg2），会导致无推送。
    """
    return f"book.{instrument_name}.none.1.100ms"


class DeribitLinearRunner:
    """Deribit Linear (USDC-settled) PCP arbitrage runner."""

    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        from pcp_arbitrage.instruments import build_triplets
        from pcp_arbitrage.market_data import MarketData
        from pcp_arbitrage.models import BookSnapshot, FeeRates, top_of_book_contracts
        from pcp_arbitrage.signal_output import (
            dashboard_enabled,
            emit_triplet_if_books_ready,
            notify_monitoring_ready,
            register_dashboard_runner_meta,
            register_startup_market_sweep,
            update_dashboard_index_price,
            update_dashboard_leg_price,
        )

        ex = self._ex
        app = self._app
        symbols = symbols_for_exchange("deribit_linear", app.symbols)
        if not symbols:
            notify_monitoring_ready("deribit_linear")
            return

        async with DeribitRestClient(api_key=ex.api_key, secret=ex.secret_key) as rest:
            # 1. 认证
            logger.info("[deribit-linear] Authenticating...")
            await rest._authenticate()

            # 2. 费率 — USDC currency
            logger.info("[deribit-linear] Fetching fee rates...")
            all_fees: dict[str, float] = {"option_taker": 0.0003, "option_maker": 0.0003, "future_taker": 0.0005, "future_maker": 0.0005}
            try:
                summary = await rest.get_account_summary("USDC")
                for fee in summary.get("fees", []):
                    if fee["instrument_type"] == "option":
                        all_fees["option_taker"] = float(fee["taker_commission"])
                        all_fees["option_maker"] = float(fee.get("maker_commission", fee["taker_commission"]))
                    elif fee["instrument_type"] == "future":
                        all_fees["future_taker"] = float(fee["taker_commission"])
                        all_fees["future_maker"] = float(fee.get("maker_commission", fee["taker_commission"]))
            except Exception as e:
                logger.warning("[deribit-linear] Fee fetch failed, using defaults: %s", e)
            fee_rates = FeeRates(
                option_taker_rate=all_fees["option_taker"],
                option_maker_rate=all_fees["option_maker"],
                future_taker_rate=all_fees["future_taker"],
                future_maker_rate=all_fees["future_maker"],
            )
            logger.info("[deribit-linear] Fee rates: option taker=%.4f maker=%.4f  future taker=%.4f maker=%.4f",
                        fee_rates.option_taker_rate, fee_rates.option_maker_rate,
                        fee_rates.future_taker_rate, fee_rates.future_maker_rate)

            # 3. 合约 — 统一用 currency="USDC"
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            raw_opts = await rest.get_instruments("USDC", "option")
            raw_futs = await rest.get_instruments("USDC", "future")

            for sym in symbols:
                sym_prefix = f"{sym}_USDC"
                sym_opts = [o for o in raw_opts if o["instrument_name"].startswith(sym_prefix)]
                sym_futs = [f for f in raw_futs if f["instrument_name"].startswith(sym_prefix)]
                opts, futs = _normalize_instruments_linear(sym_opts, sym_futs)
                all_options.extend(opts)
                all_futures.extend(futs)

                index_name = f"{sym.lower()}_usdc"
                price_data = await rest.get_index_price(index_name)
                atm_prices[sym] = float(price_data["index_price"])
                logger.info("[deribit-linear] %s index price: %.2f", sym, atm_prices[sym])

            # 4. Build triplets (margin_type="usdc" → margin_label="USDC")
            now_ms = int(time.time() * 1000)
            triplets = build_triplets(
                options=all_options,
                futures=all_futures,
                atm_prices=atm_prices,
                symbols=symbols,
                atm_range=app.atm_range,
                now_ms=now_ms,
                margin_type="usdc",
                min_days_to_expiry=app.min_days_to_expiry,
            )
            logger.info("[deribit-linear] Built %d triplets", len(triplets))
            register_dashboard_runner_meta(
                "deribit_linear",
                option_taker_rate=fee_rates.option_taker_rate,
                option_maker_rate=fee_rates.option_maker_rate,
                future_taker_rate=fee_rates.future_taker_rate,
                future_maker_rate=fee_rates.future_maker_rate,
                n_triplets=len(triplets),
                settle_type="U本位(USDC)",
                symbols=symbols,
                triplets=triplets,
            )
            pairing_dir = app.pairing_log_dir.strip() if app.pairing_log_dir else ""
            print_triplet_summary(
                "deribit_linear",
                triplets,
                all_options,
                all_futures,
                symbols,
                "USDC",
                pairing_log_dir=pairing_dir or None,
                atm_prices=atm_prices,
                atm_range=app.atm_range,
                now_ms=now_ms,
                min_days_to_expiry=app.min_days_to_expiry,
            )
            if not triplets:
                logger.error("[deribit-linear] No triplets — check config and Deribit connectivity")
                notify_monitoring_ready("deribit_linear")
                return

            # 5. exp_ms_by_call
            exp_ms_by_call: dict[str, int] = {
                o["instId"]: int(o["expTime"]) for o in all_options
            }

            # 6. WS 频道（使用 Linear instrument_name 格式）
            inst_ids: set[str] = set()
            for t in triplets:
                inst_ids.update([t.call_id, t.put_id, t.future_id])
            channels = [_deribit_book_channel(_internal_to_deribit_linear(i)) for i in sorted(inst_ids)]
            index_channels = [f"deribit_price_index.{sym.lower()}_usd" for sym in symbols]

            # 7. MarketData + WS 回调
            market = MarketData()
            touched_call_ids: set[str] = set()
            touched_inst_ids: set[str] = set()

            async def on_reconnect() -> None:
                logger.warning("[deribit-linear ws] Reconnected — clearing order book cache")
                market.clear()
                touched_call_ids.clear()
                touched_inst_ids.clear()

            async def on_message(msg: dict) -> None:
                if msg.get("method") != "subscription":
                    return
                params = msg.get("params", {})
                channel = params.get("channel", "")
                data = params.get("data", {})

                # index price channel
                if channel.startswith("deribit_price_index."):
                    idx_px = data.get("price")
                    if idx_px is not None:
                        suffix = channel[len("deribit_price_index."):]
                        sym = suffix.split("_")[0].upper()
                        update_dashboard_index_price(sym, float(idx_px))
                    return

                raw_name = data.get("instrument_name", "")
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if not bids or not asks:
                    return
                ts = int(data.get("timestamp", 0))

                # BTC_USDC-27JUN25-70000-C or BTC_USDC-27JUN25 → internal instId
                parts = raw_name.split("-")
                if len(parts) == 4:
                    # option: ["BTC_USDC", "27JUN25", "70000", "C"]
                    prefix, ddate, stk, opt = parts
                    if not _is_deribit_expiry_token(ddate):
                        return
                    sym = prefix.split("_")[0]
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USDC-{exp}-{stk}-{opt}"
                elif len(parts) == 2:
                    # future: ["BTC_USDC", "27JUN25"]
                    prefix, ddate = parts
                    if not _is_deribit_expiry_token(ddate):
                        return
                    sym = prefix.split("_")[0]
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USDC-{exp}"
                else:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                    bid_sz=top_of_book_contracts(bids[0]),
                    ask_sz=top_of_book_contracts(asks[0]),
                )
                market.update(inst_id, snap)
                update_dashboard_leg_price("deribit_linear", inst_id, snap.bid, snap.ask)
                touched_inst_ids.add(inst_id)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    touched_call_ids.add(t.call_id)
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    emit_triplet_if_books_ready(
                        "deribit_linear",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=1.0,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=atm_prices[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                    )

            async def _startup_sweep_deribit_linear() -> None:
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
                    emit_triplet_if_books_ready(
                        "deribit_linear",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=1.0,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=atm_prices[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                    )

            # 8. 启动 WS
            all_channels = channels + index_channels
            logger.info("[deribit-linear] Subscribing to %d channels + %d index channels",
                        len(channels), len(index_channels))
            ws_client = DeribitWebSocketClient(on_message=on_message, on_reconnect=on_reconnect)
            ws_client.set_channels(all_channels)
            if dashboard_enabled():
                register_startup_market_sweep(_startup_sweep_deribit_linear)
            notify_monitoring_ready("deribit_linear")
            await _run_ws_with_tick(
                ws_client,
                tick_interval_sec=app.tick_interval_sec,
                log_label="deribit-linear",
                n_triplets=len(triplets),
                n_channels=len(channels),
                touched_call_ids=touched_call_ids,
                touched_inst_ids=touched_inst_ids,
            )


class DeribitRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        from pcp_arbitrage.instruments import build_triplets
        from pcp_arbitrage.market_data import MarketData
        from pcp_arbitrage.models import BookSnapshot, FeeRates, top_of_book_contracts
        from pcp_arbitrage.pcp_calculator import DERIBIT_INVERSE_FUT_USD_FACE
        from pcp_arbitrage.signal_output import (
            dashboard_enabled,
            emit_triplet_if_books_ready,
            notify_monitoring_ready,
            register_dashboard_runner_meta,
            register_startup_market_sweep,
            update_dashboard_index_price,
            update_dashboard_leg_price,
        )

        ex = self._ex
        app = self._app
        margin_type = ex.margin_type          # "coin" for Deribit
        symbols = symbols_for_exchange("deribit", app.symbols)
        if not symbols:
            notify_monitoring_ready("deribit")
            return

        async with DeribitRestClient(api_key=ex.api_key, secret=ex.secret_key) as rest:
            # 1. 认证
            logger.info("[deribit] Authenticating...")
            await rest._authenticate()

            # 2. 费率
            logger.info("[deribit] Fetching fee rates...")
            all_fees: dict[str, float] = {"option_taker": 0.0003, "option_maker": 0.0003, "future_taker": 0.0005, "future_maker": 0.0005}
            try:
                summary = await rest.get_account_summary(symbols[0])
                for fee in summary.get("fees", []):
                    if fee["instrument_type"] == "option":
                        all_fees["option_taker"] = float(fee["taker_commission"])
                        all_fees["option_maker"] = float(fee.get("maker_commission", fee["taker_commission"]))
                    elif fee["instrument_type"] == "future":
                        all_fees["future_taker"] = float(fee["taker_commission"])
                        all_fees["future_maker"] = float(fee.get("maker_commission", fee["taker_commission"]))
            except Exception as e:
                logger.warning("[deribit] Fee fetch failed, using defaults: %s", e)
            fee_rates = FeeRates(
                option_taker_rate=all_fees["option_taker"],
                option_maker_rate=all_fees["option_maker"],
                future_taker_rate=all_fees["future_taker"],
                future_maker_rate=all_fees["future_maker"],
            )
            logger.info("[deribit] Fee rates: option taker=%.4f maker=%.4f  future taker=%.4f maker=%.4f",
                        fee_rates.option_taker_rate, fee_rates.option_maker_rate,
                        fee_rates.future_taker_rate, fee_rates.future_maker_rate)

            # 3. 合约 + 价格
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            for sym in symbols:
                raw_opts = await rest.get_instruments(sym, "option")
                raw_futs = await rest.get_instruments(sym, "future")
                opts, futs = _normalize_instruments(raw_opts, raw_futs)
                all_options.extend(opts)
                all_futures.extend(futs)

                index_name = f"{sym.lower()}_usd"
                price_data = await rest.get_index_price(index_name)
                atm_prices[sym] = float(price_data["index_price"])
                logger.info("[deribit] %s index price: %.2f", sym, atm_prices[sym])

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
            logger.info("[deribit] Built %d triplets", len(triplets))
            register_dashboard_runner_meta(
                "deribit",
                option_taker_rate=fee_rates.option_taker_rate,
                option_maker_rate=fee_rates.option_maker_rate,
                future_taker_rate=fee_rates.future_taker_rate,
                future_maker_rate=fee_rates.future_maker_rate,
                n_triplets=len(triplets),
                settle_type="币本位(USD)",
                symbols=symbols,
                triplets=triplets,
            )
            pairing_dir = app.pairing_log_dir.strip() if app.pairing_log_dir else ""
            print_triplet_summary(
                "deribit",
                triplets,
                all_options,
                all_futures,
                symbols,
                "USD",
                pairing_log_dir=pairing_dir or None,
                atm_prices=atm_prices,
                atm_range=app.atm_range,
                now_ms=now_ms,
                min_days_to_expiry=app.min_days_to_expiry,
            )
            if not triplets:
                logger.error("[deribit] No triplets — check config and Deribit connectivity")
                notify_monitoring_ready("deribit")
                return

            # 5. exp_ms_by_call
            exp_ms_by_call: dict[str, int] = {
                o["instId"]: int(o["expTime"]) for o in all_options
            }

            # 6. 构建 WS 频道
            inst_ids: set[str] = set()
            for t in triplets:
                inst_ids.update([t.call_id, t.put_id, t.future_id])
            channels = [_deribit_book_channel(_internal_to_deribit(i)) for i in sorted(inst_ids)]
            # Deribit index price channels: deribit_price_index.btc_usd, etc.
            index_channels = [f"deribit_price_index.{sym.lower()}_usd" for sym in symbols]

            # 7. MarketData + WS 回调
            market = MarketData()
            spot_price_cache = dict(atm_prices)
            touched_call_ids: set[str] = set()
            touched_inst_ids: set[str] = set()

            async def on_reconnect() -> None:
                logger.warning("[deribit ws] Reconnected — clearing order book cache")
                market.clear()
                touched_call_ids.clear()
                touched_inst_ids.clear()

            async def on_message(msg: dict) -> None:
                if msg.get("method") != "subscription":
                    return
                params = msg.get("params", {})
                channel = params.get("channel", "")
                data = params.get("data", {})

                # deribit_price_index.btc_usd → update dashboard index price
                if channel.startswith("deribit_price_index."):
                    idx_px = data.get("price")
                    if idx_px is not None:
                        # channel suffix: "btc_usd" → "BTC"
                        suffix = channel[len("deribit_price_index."):]
                        sym = suffix.split("_")[0].upper()
                        update_dashboard_index_price(sym, float(idx_px))
                    return

                raw_name = data.get("instrument_name", "")
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if not bids or not asks:
                    return
                ts = int(data.get("timestamp", 0))

                # Deribit 原始名 → 内部 instId
                parts = raw_name.split("-")
                if len(parts) == 4:
                    sym, ddate, stk, opt = parts
                    if not _is_deribit_expiry_token(ddate):
                        return
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USD-{exp}-{stk}-{opt}"
                elif len(parts) == 2:
                    sym, ddate = parts
                    if not _is_deribit_expiry_token(ddate):
                        return
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USD-{exp}"
                else:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                    bid_sz=top_of_book_contracts(bids[0]),
                    ask_sz=top_of_book_contracts(asks[0]),
                )
                market.update(inst_id, snap)
                update_dashboard_leg_price("deribit", inst_id, snap.bid, snap.ask)
                touched_inst_ids.add(inst_id)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    touched_call_ids.add(t.call_id)
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = spot_price_cache[t.symbol]   # coin margin
                    inv_face = (
                        DERIBIT_INVERSE_FUT_USD_FACE.get(t.symbol)
                        if margin_type == "coin"
                        else None
                    )
                    emit_triplet_if_books_ready(
                        "deribit",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=spot_price,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=spot_price_cache[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                        future_inverse_usd_face=inv_face,
                    )

            async def _startup_sweep_deribit() -> None:
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
                    sp = spot_price_cache[t.symbol]
                    inv_face = (
                        DERIBIT_INVERSE_FUT_USD_FACE.get(t.symbol)
                        if margin_type == "coin"
                        else None
                    )
                    emit_triplet_if_books_ready(
                        "deribit",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=sp,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=sp,
                        min_annualized_rate=app.min_annualized_rate,
                        future_inverse_usd_face=inv_face,
                    )

            # 8. 启动 WS
            all_channels = channels + index_channels
            logger.info("[deribit] Subscribing to %d channels + %d index channels",
                        len(channels), len(index_channels))
            ws_client = DeribitWebSocketClient(on_message=on_message, on_reconnect=on_reconnect)
            ws_client.set_channels(all_channels)
            if dashboard_enabled():
                register_startup_market_sweep(_startup_sweep_deribit)
            notify_monitoring_ready("deribit")
            await _run_ws_with_tick(
                ws_client,
                tick_interval_sec=app.tick_interval_sec,
                log_label="deribit",
                n_triplets=len(triplets),
                n_channels=len(channels),
                touched_call_ids=touched_call_ids,
                touched_inst_ids=touched_inst_ids,
            )
