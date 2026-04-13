import asyncio
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import urlencode

import aiohttp
from aiohttp_socks import ProxyConnector

from pcp_arbitrage.config import AppConfig, ExchangeConfig
from pcp_arbitrage.exchange_symbols import symbols_for_exchange

logger = logging.getLogger(__name__)


def _sign(secret: str, params: dict) -> str:
    """params 必须已包含 timestamp。对完整 query string 做 HMAC-SHA256。"""
    qs = urlencode(params)
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _eapi_option_account_from_margin(margin: dict, comm: dict | None) -> dict:
    """将 marginAccount（+ 可选 commission）拼成旧版 /eapi/v1/account 同级字段，兼容费率与余额解析。"""
    out = dict(margin)
    assets = margin.get("asset") or []
    usdt = None
    for a in assets:
        if str(a.get("asset", "")).upper() == "USDT":
            usdt = a
            break
    if usdt is None and assets:
        usdt = assets[0]
    if usdt:
        mb = usdt.get("marginBalance") or usdt.get("equity") or "0"
        out["totalWalletBalance"] = mb
        out["totalMarginBalance"] = mb
        out["availableBalance"] = usdt.get("available") or "0"
        out["totalInitialMargin"] = usdt.get("initialMargin") or "0"
        out["totalMaintenanceMargin"] = usdt.get("maintMargin") or "0"

    taker, maker = "0.0003", "0.0002"
    if comm and isinstance(comm.get("commissions"), list) and comm["commissions"]:
        first = comm["commissions"][0]
        taker = str(first.get("takerFee", taker))
        maker = str(first.get("makerFee", maker))
    out["optionCommissionRate"] = {"taker": taker, "maker": maker}
    return out


class BinanceRestClient:
    """Binance REST 客户端（eapi / fapi / api 三个 base URL）。"""

    BASE_EAPI = "https://eapi.binance.com"
    BASE_FAPI = "https://fapi.binance.com"
    BASE_API  = "https://api.binance.com"

    def __init__(self, api_key: str, secret: str, proxy: str | None = None) -> None:
        self._api_key = api_key
        self._secret = secret
        self._proxy = proxy
        self._eapi_session: aiohttp.ClientSession | None = None
        self._fapi_session: aiohttp.ClientSession | None = None
        self._api_session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._eapi_session = aiohttp.ClientSession(base_url=self.BASE_EAPI)
        self._fapi_session = aiohttp.ClientSession(base_url=self.BASE_FAPI)
        self._api_session  = aiohttp.ClientSession(base_url=self.BASE_API)
        return self

    async def __aexit__(self, *_):
        for s in (self._eapi_session, self._fapi_session, self._api_session):
            if s:
                await s.close()

    def _signed_params(self, extra: dict | None = None) -> dict:
        params = {"timestamp": int(time.time() * 1000)}
        if extra:
            params.update(extra)
        params["signature"] = _sign(self._secret, params)
        return params

    def _auth_headers(self) -> dict:
        return {"X-MBX-APIKEY": self._api_key}

    async def get_option_exchange_info(self) -> dict:
        assert self._eapi_session is not None
        async with self._eapi_session.get("/eapi/v1/exchangeInfo", proxy=self._proxy) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_option_account(self) -> dict:
        """期权账户信息：优先 legacy `/eapi/v1/account`；若 404 则改用官方 `GET /eapi/v1/marginAccount`。"""
        assert self._eapi_session is not None
        headers = self._auth_headers()
        params = self._signed_params()
        async with self._eapi_session.get(
            "/eapi/v1/account", params=params, headers=headers, proxy=self._proxy
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            if resp.status != 404:
                resp.raise_for_status()

        params_m = self._signed_params()
        async with self._eapi_session.get(
            "/eapi/v1/marginAccount", params=params_m, headers=headers, proxy=self._proxy
        ) as resp:
            resp.raise_for_status()
            margin = await resp.json()

        comm: dict | None = None
        try:
            params_c = self._signed_params()
            async with self._eapi_session.get(
                "/eapi/v1/commission", params=params_c, headers=headers, proxy=self._proxy
            ) as resp_c:
                if resp_c.status == 200:
                    comm = await resp_c.json()
        except (aiohttp.ClientError, json.JSONDecodeError, KeyError) as exc:
            logger.debug("[binance] option commission optional fetch: %s", exc)

        return _eapi_option_account_from_margin(margin, comm)

    async def get_spot_account(self) -> dict:
        """现货账户余额；UTA 统一账户用户可通过此接口获取合并后的 USDT 余额。"""
        assert self._api_session is not None
        params = self._signed_params()
        async with self._api_session.get(
            "/api/v3/account", params=params, headers=self._auth_headers(), proxy=self._proxy
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_futures_exchange_info(self) -> dict:
        assert self._fapi_session is not None
        async with self._fapi_session.get("/fapi/v1/exchangeInfo", proxy=self._proxy) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_futures_account(self) -> dict:
        assert self._fapi_session is not None
        params = self._signed_params()
        async with self._fapi_session.get(
            "/fapi/v2/account", params=params, headers=self._auth_headers(), proxy=self._proxy
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_spot_price(self, symbol: str) -> float:
        assert self._api_session is not None
        async with self._api_session.get(
            "/api/v3/ticker/price", params={"symbol": symbol}, proxy=self._proxy
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return float(data["price"])


class BinanceWebSocketClient:
    """单个 Binance WebSocket 连接（期权或交割合约），指数退避重连。"""

    PING_INTERVAL = 20
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60

    def __init__(self, url: str, on_message, on_reconnect=None, proxy: str | None = None) -> None:
        self._url = url
        self._on_message = on_message
        self._on_reconnect = on_reconnect
        self._proxy = proxy
        self._streams: list[str] = []

    def set_streams(self, streams: list[str]) -> None:
        self._streams = streams

    async def run(self) -> None:
        retry_delay = self.RECONNECT_BASE
        while True:
            try:
                await self._connect_and_run()
                retry_delay = self.RECONNECT_BASE
            except Exception as exc:
                logger.warning("[binance ws] Disconnected: %s — reconnecting in %ds", exc, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_MAX)
                if self._on_reconnect:
                    await self._on_reconnect()

    async def _connect_and_run(self) -> None:
        connector = ProxyConnector.from_url(self._proxy) if self._proxy else None
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.ws_connect(self._url) as ws:
                logger.info("[binance ws] Connected to %s", self._url)
                await self._subscribe(ws)
                ping_task = asyncio.create_task(self._heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # combined stream 包装格式：{"stream":..., "data":{...}}
                            if "data" in data:
                                await self._on_message(data["data"])
                            else:
                                await self._on_message(data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws) -> None:
        payload = {
            "method": "SUBSCRIBE",
            "params": self._streams,
            "id": 1,
        }
        await ws.send_str(json.dumps(payload))

    async def _heartbeat(self, ws) -> None:
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            await ws.send_str(json.dumps({"method": "LIST_SUBSCRIPTIONS", "id": 99}))


class BinanceRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        from pcp_arbitrage.instruments import build_triplets
        from pcp_arbitrage.market_data import MarketData
        from pcp_arbitrage.models import BookSnapshot, FeeRates, top_of_book_contracts
        from pcp_arbitrage.exchanges.okx import print_triplet_summary
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
        margin_type = ex.margin_type          # 固定 "usdt" for Binance
        symbols = symbols_for_exchange("binance", app.symbols)
        if not symbols:
            notify_monitoring_ready("binance")
            return

        async with BinanceRestClient(api_key=ex.api_key, secret=ex.secret_key, proxy=app.proxy) as rest:
            # 1. 费率
            logger.info("[binance] Fetching fee rates...")
            option_taker_rate = 0.0003
            option_maker_rate = 0.0002
            future_taker_rate = 0.0004
            future_maker_rate = 0.0002
            try:
                opt_acct = await rest.get_option_account()
                option_taker_rate = float(opt_acct["optionCommissionRate"]["taker"])
                option_maker_rate = float(opt_acct["optionCommissionRate"]["maker"])
                fut_acct = await rest.get_futures_account()
                future_taker_rate = float(fut_acct["takerCommissionRate"])
                future_maker_rate = float(fut_acct["makerCommissionRate"])
            except Exception as e:
                logger.warning("[binance] Fee fetch failed, using defaults: %s", e)
            fee_rates = FeeRates(
                option_taker_rate=option_taker_rate,
                option_maker_rate=option_maker_rate,
                future_taker_rate=future_taker_rate,
                future_maker_rate=future_maker_rate,
            )
            logger.info("[binance] Fee rates: option taker=%.4f maker=%.4f  future taker=%.4f maker=%.4f",
                        fee_rates.option_taker_rate, fee_rates.option_maker_rate,
                        fee_rates.future_taker_rate, fee_rates.future_maker_rate)

            # 2. 合约 + 价格
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            opt_info = await rest.get_option_exchange_info()
            raw_all_opts = opt_info.get("optionSymbols", [])

            fut_info = await rest.get_futures_exchange_info()
            raw_all_futs = [
                s for s in fut_info.get("symbols", [])
                if "_" in s.get("symbol", "")   # 交割合约（带到期日）的 symbol 格式为 BTCUSDT_YYMMDD
            ]

            for sym in symbols:
                raw_opts = [o for o in raw_all_opts if o.get("underlying", "").startswith(sym)]
                raw_futs = [f for f in raw_all_futs if f["symbol"].startswith(sym)]
                opts, futs = _normalize_instruments(raw_opts, raw_futs, margin_type)
                all_options.extend(opts)
                all_futures.extend(futs)

                spot_sym = f"{sym}USDT"
                atm_prices[sym] = await rest.get_spot_price(spot_sym)
                logger.info("[binance] %s spot price: %.2f", sym, atm_prices[sym])

            # 3. Build triplets
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
                exchange="binance",
            )
            logger.info("[binance] Built %d triplets", len(triplets))
            settle_type = "U本位(USDT)" if margin_type == "usdt" else "币本位(USD)"
            register_dashboard_runner_meta(
                "binance",
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
                "binance",
                triplets,
                all_options,
                all_futures,
                symbols,
                "USDT",
                pairing_log_dir=pairing_dir or None,
                atm_prices=atm_prices,
                atm_range=app.atm_range,
                now_ms=now_ms,
                min_days_to_expiry=app.min_days_to_expiry,
            )
            if not triplets:
                logger.error("[binance] No triplets — check config and Binance connectivity")
                notify_monitoring_ready("binance")
                return

            # 4. exp_ms_by_call
            exp_ms_by_call: dict[str, int] = {
                o["instId"]: int(o["expTime"]) for o in all_options
            }

            # 5. 构建 WS 流名称（从内部 instId 反推 Binance 原始格式）
            def _internal_to_binance_option(inst_id: str) -> str:
                """BTC-USDT-260627-70000-C → BTC-260627-70000-C"""
                parts = inst_id.split("-")   # [SYM, USDT, EXP, STK, TYPE]
                return f"{parts[0]}-{parts[2]}-{parts[3]}-{parts[4]}"

            def _internal_to_binance_future(inst_id: str) -> str:
                """BTC-USDT-260627 → btcusdt_260627（全小写，下划线）"""
                parts = inst_id.split("-")   # [SYM, USDT, EXP]
                return f"{parts[0].lower()}{parts[1].lower()}_{parts[2]}"

            opt_streams: list[str] = []
            fut_streams: list[str] = []
            for t in triplets:
                opt_streams.append(f"{_internal_to_binance_option(t.call_id)}@depth5")
                opt_streams.append(f"{_internal_to_binance_option(t.put_id)}@depth5")
                fut_streams.append(f"{_internal_to_binance_future(t.future_id)}@depth5")

            opt_streams = list(set(opt_streams))
            fut_streams = list(set(fut_streams))

            # index price streams: btcusdt@markPrice etc. (field "i" = index price)
            mark_price_streams: list[str] = [f"{sym.lower()}usdt@markPrice" for sym in symbols]
            # symbol name lookup: "BTCUSDT" → "BTC"
            mark_sym_lookup: dict[str, str] = {f"{sym.upper()}USDT": sym for sym in symbols}

            # 6. Binance 原始 symbol → 内部 instId 映射
            opt_raw_to_internal: dict[str, str] = {}
            for o in all_options:
                parts = o["instId"].split("-")
                raw = f"{parts[0]}-{parts[2]}-{parts[3]}-{parts[4]}"
                opt_raw_to_internal[raw] = o["instId"]

            fut_raw_to_internal: dict[str, str] = {}
            for f in all_futures:
                parts = f["instId"].split("-")
                raw_lower = f"{parts[0].lower()}{parts[1].lower()}_{parts[2]}"
                fut_raw_to_internal[raw_lower] = f["instId"]

            # 7. MarketData
            market = MarketData()

            async def on_reconnect() -> None:
                logger.warning("[binance ws] Reconnected — clearing order book cache")
                market.clear()

            async def on_message(msg: dict) -> None:
                raw_sym = msg.get("s", "")

                # markPrice stream: e="markPriceUpdate", field "i" = index price
                if msg.get("e") == "markPriceUpdate":
                    idx_px = msg.get("i")
                    if idx_px and raw_sym in mark_sym_lookup:
                        update_dashboard_index_price(mark_sym_lookup[raw_sym], float(idx_px))
                    return

                bids = msg.get("b", [])
                asks = msg.get("a", [])
                ts = int(msg.get("T", 0))
                if not bids or not asks:
                    return

                logger.debug("[binance tick] %s bid=%s ask=%s", raw_sym, bids[0][0], asks[0][0])

                inst_id = opt_raw_to_internal.get(raw_sym) or fut_raw_to_internal.get(raw_sym.lower())
                if not inst_id:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                    bid_sz=top_of_book_contracts(bids[0]),
                    ask_sz=top_of_book_contracts(asks[0]),
                )
                market.update(inst_id, snap)
                update_dashboard_leg_price("binance", inst_id, snap.bid, snap.ask)

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
                    spot_price = 1.0   # Binance USDT margin: no conversion needed
                    emit_triplet_if_books_ready(
                        "binance",
                        t,
                        books_snapshot,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        spot_price=spot_price,
                        stale_threshold_ms=app.stale_threshold_ms,
                        index_for_fee=atm_prices[t.symbol],
                        min_annualized_rate=app.min_annualized_rate,
                    )

            async def _startup_sweep_binance() -> None:
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
                        "binance",
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

            # 8. 两个 WS 并发运行（/ws 端点支持 SUBSCRIBE JSON 方法）
            OPT_WS_URL = "wss://fstream.binance.com/public/ws"
            FUT_WS_URL = "wss://fstream.binance.com/ws"

            opt_ws = BinanceWebSocketClient(OPT_WS_URL, on_message=on_message, on_reconnect=on_reconnect, proxy=app.proxy)
            opt_ws.set_streams(opt_streams)

            fut_ws = BinanceWebSocketClient(FUT_WS_URL, on_message=on_message, on_reconnect=on_reconnect, proxy=app.proxy)
            fut_ws.set_streams(fut_streams + mark_price_streams)

            logger.info("[binance] Subscribing opt=%d fut=%d mark=%d streams",
                        len(opt_streams), len(fut_streams), len(mark_price_streams))
            if dashboard_enabled():
                register_startup_market_sweep(_startup_sweep_binance)
            notify_monitoring_ready("binance")
            await asyncio.gather(opt_ws.run(), fut_ws.run())


def _normalize_instruments(
    raw_options: list[dict],
    raw_futures: list[dict],
    margin_type: str,
) -> tuple[list[dict], list[dict]]:
    """
    Normalize Binance raw API payloads to the unified instId format expected by instruments.py.

    Options: BTC-260627-70000-C (4-part) → BTC-USD-260627-70000-C (5-part)
    交割合约 USDT: BTCUSDT_260627 → BTC-USDT-260627
    交割合约 coin: BTCUSD_260627  → BTC-USD-260627
    """
    settle = "USDT" if margin_type == "usdt" else "USD"

    normalized_options = []
    for s in raw_options:
        sym = s["underlying"].replace("USDT", "").replace("USD", "")
        raw_id = s["symbol"]            # "BTC-260627-70000-C"
        parts = raw_id.split("-")       # ["BTC", "260627", "70000", "C"]
        norm_id = f"{sym}-{settle}-{parts[1]}-{parts[2]}-{parts[3]}"
        normalized_options.append({
            "instId": norm_id,
            "stk": s["strikePrice"],
            "optType": "C" if s["side"] == "CALL" else "P",
            "expTime": str(s["expiryDate"]),
        })

    normalized_futures = []
    for f in raw_futures:
        raw_id = f["symbol"]            # "BTCUSDT_260627"
        sym_part, exp = raw_id.split("_")
        sym = sym_part.replace("USDT", "").replace("USD", "")
        normalized_futures.append({
            "instId": f"{sym}-{settle}-{exp}",
        })

    return normalized_options, normalized_futures
