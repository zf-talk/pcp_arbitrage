# Binance + Deribit PCP 套利监听集成实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补全 BinanceRunner + 新建 DeribitRunner，接入现有多交易所并发架构，实现 Binance（USDT 结算期权+期货）和 Deribit（币本位）PCP 套利机会实时监听。

**Architecture:** 两个新 Runner 均遵循与 OKXRunner 相同模式：REST 初始化（费率 → 合约 → 价格 → triplets）→ WebSocket 实时推送 → on_message 回调触发 pcp_calculator + signal_printer。OKX 现有代码完全不动。直接调用官方 API（aiohttp），不引入 ccxt。

**Tech Stack:** Python 3.12, aiohttp, asyncio; Binance eapi/fapi/api; Deribit JSON-RPC v2 REST + WS

---

## 文件结构

| 文件 | 操作 | 职责 |
|------|------|------|
| `src/pcp_arbitrage/exchanges/deribit.py` | **新建** | DeribitRestClient + DeribitWebSocketClient + DeribitRunner + 归一化辅助函数 |
| `src/pcp_arbitrage/exchanges/binance.py` | **修改** | 补充 BinanceRestClient + BinanceWebSocketClient；完善 BinanceRunner.run() |
| `tests/test_deribit_normalize.py` | **新建** | Deribit 归一化纯函数单元测试 |
| `src/pcp_arbitrage/main.py` | **修改** | RUNNERS 字典增加 deribit 条目 |
| `config.yaml` | **修改** | 增加 deribit 块 |
| `tests/fixtures/config_test.yaml` | **修改** | 增加 deribit 块 |

**不动的文件：** `okx.py`, `okx_client.py`, `fee_fetcher.py`, `instruments.py`, `pcp_calculator.py`, `signal_printer.py`, `market_data.py`, `models.py`, `config.py`, `tests/test_binance_normalize.py`

---

## Task 1：Deribit 归一化函数 + DeribitRestClient 骨架 + 测试

**Files:**
- Create: `src/pcp_arbitrage/exchanges/deribit.py`
- Create: `tests/test_deribit_normalize.py`

### 步骤

- [ ] **Step 1.1：写失败测试（日期转换）**

```python
# tests/test_deribit_normalize.py
from pcp_arbitrage.exchanges.deribit import _deribit_date_to_yymmdd, _normalize_instruments, _internal_to_deribit


def test_date_jan():
    assert _deribit_date_to_yymmdd("01JAN26") == "260101"

def test_date_feb():
    assert _deribit_date_to_yymmdd("28FEB25") == "250228"

def test_date_mar():
    assert _deribit_date_to_yymmdd("28MAR25") == "250328"

def test_date_jun():
    assert _deribit_date_to_yymmdd("27JUN25") == "250627"

def test_date_sep():
    assert _deribit_date_to_yymmdd("26SEP25") == "250926"

def test_date_dec():
    assert _deribit_date_to_yymmdd("31DEC25") == "251231"
```

- [ ] **Step 1.2：运行确���失败**

```bash
pytest tests/test_deribit_normalize.py -v
```
预期：`ImportError` 或 `ModuleNotFoundError`（文件不存在）

- [ ] **Step 1.3：创建 `deribit.py` 实现归一化函数和反查辅助函数**

```python
# src/pcp_arbitrage/exchanges/deribit.py
import asyncio
import json
import logging
import time

import aiohttp

from pcp_arbitrage.config import AppConfig, ExchangeConfig

logger = logging.getLogger(__name__)

MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def _deribit_date_to_yymmdd(deribit_date: str) -> str:
    """'27JUN25' → '250627'"""
    day = deribit_date[:2]
    mon = deribit_date[2:5]
    year = deribit_date[5:]
    return f"{year}{MONTH_MAP[mon]}{day}"


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
        sym = parts[0]
        exp = _deribit_date_to_yymmdd(parts[1])
        stk = str(int(o["strike"]))          # number → string, drop decimals
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
            continue                         # skip PERPETUAL etc.
        sym = parts[0]
        exp = _deribit_date_to_yymmdd(parts[1])
        normalized_futures.append({
            "instId": f"{sym}-USD-{exp}",
        })

    return normalized_options, normalized_futures


# 月份反查表（模块级，避免每次调用重建）
_MONTH_REV = {v: k for k, v in MONTH_MAP.items()}


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
        return f"{sym}-{dd}{mon}{yy}-{stk}-{opt}"
    elif len(parts) == 3:
        # future: SYM-USD-YYMMDD
        sym, _, yymmdd = parts
        yy, mm, dd = yymmdd[:2], yymmdd[2:4], yymmdd[4:]
        mon = _MONTH_REV[mm]
        return f"{sym}-{dd}{mon}{yy}"
    return inst_id
```

- [ ] **Step 1.4：补充合约归一化测试，运行全部测试**

```python
# 在 tests/test_deribit_normalize.py 追加

RAW_OPTIONS = [
    {
        "instrument_name": "BTC-27JUN25-70000-C",
        "strike": 70000.0,
        "option_type": "call",
        "expiration_timestamp": 1751040000000,
    },
    {
        "instrument_name": "BTC-27JUN25-70000-P",
        "strike": 70000.0,
        "option_type": "put",
        "expiration_timestamp": 1751040000000,
    },
]

RAW_FUTURES = [
    {"instrument_name": "BTC-27JUN25"},
    {"instrument_name": "BTC-PERPETUAL"},   # 应被过滤掉
]


def test_normalize_option_instid():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [])
    assert opts[0]["instId"] == "BTC-USD-250627-70000-C"
    assert opts[1]["instId"] == "BTC-USD-250627-70000-P"


def test_normalize_option_fields():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [])
    c = opts[0]
    assert c["stk"] == "70000"
    assert c["optType"] == "C"
    assert c["expTime"] == "1751040000000"
    assert opts[1]["optType"] == "P"


def test_normalize_future_instid():
    _, futs = _normalize_instruments([], RAW_FUTURES)
    assert len(futs) == 1                   # PERPETUAL 被过滤
    assert futs[0]["instId"] == "BTC-USD-250627"


def test_internal_to_deribit_option():
    assert _internal_to_deribit("BTC-USD-250627-70000-C") == "BTC-27JUN25-70000-C"


def test_internal_to_deribit_future():
    assert _internal_to_deribit("BTC-USD-250627") == "BTC-27JUN25"
```

```bash
pytest tests/test_deribit_normalize.py -v
```
预期：全部 11 个测试 PASS

- [ ] **Step 1.5：在 `deribit.py` 添加 DeribitRestClient 骨架**

```python
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
```

- [ ] **Step 1.6：添加 DeribitRunner 骨架（仅占位，run() 留空）**

```python
class DeribitRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        raise NotImplementedError("DeribitRunner.run() not yet implemented")
```

- [ ] **Step 1.7：确认现有测试不受影响**

```bash
pytest tests/ -v
```
预期：test_binance_normalize.py 5 个 + test_deribit_normalize.py 11 个，全 PASS

- [ ] **Step 1.8：提交**

```bash
git add src/pcp_arbitrage/exchanges/deribit.py tests/test_deribit_normalize.py
git commit -m "feat: add Deribit normalize functions, RestClient, and Runner skeleton"
```

---

## Task 2：DeribitWebSocketClient + DeribitRunner.run() 完整实现

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/deribit.py`

### 步骤

- [ ] **Step 2.1：在 `deribit.py` 添加 DeribitWebSocketClient**

```python
class DeribitWebSocketClient:
    """Deribit JSON-RPC 2.0 WebSocket 客户端，单连接，指数退避重连。"""

    WS_URL = "wss://www.deribit.com/ws/api/v2"
    HEARTBEAT_INTERVAL = 5          # Deribit 要求 10s 内发一次消息
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60

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
                retry_delay = self.RECONNECT_BASE
            except Exception as exc:
                logger.warning("[deribit ws] Disconnected: %s — reconnecting in %ds", exc, retry_delay)
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
        payload = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "params": {"channels": self._channels},
            "id": 1,
        }
        await ws.send_str(json.dumps(payload))

    async def _heartbeat(self, ws) -> None:
        ping = json.dumps({"jsonrpc": "2.0", "method": "public/test", "id": 99})
        while True:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            await ws.send_str(ping)
```

- [ ] **Step 2.2：实现 DeribitRunner.run()（替换占位 raise）**

完整 run() 逻辑，模仿 okx.py 结构：

```python
    async def run(self) -> None:
        from pcp_arbitrage.instruments import build_triplets
        from pcp_arbitrage.market_data import MarketData
        from pcp_arbitrage.models import BookSnapshot, FeeRates
        from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse
        from pcp_arbitrage.signal_printer import print_signal

        ex = self._ex
        app = self._app
        margin_type = ex.margin_type          # "coin" for Deribit

        async with DeribitRestClient(api_key=ex.api_key, secret=ex.secret_key) as rest:
            # 1. 认证（私有接口需要 token）
            logger.info("[deribit] Authenticating...")
            await rest._authenticate()

            # 2. 费率
            logger.info("[deribit] Fetching fee rates...")
            all_fees: dict[str, float] = {"option": 0.0003, "future": 0.0005}
            try:
                for sym in app.symbols:
                    summary = await rest.get_account_summary(sym)
                    for fee in summary.get("fees", []):
                        if fee["instrument_type"] == "option":
                            all_fees["option"] = float(fee["taker_commission"])
                        elif fee["instrument_type"] == "future":
                            all_fees["future"] = float(fee["taker_commission"])
                    break   # 所有币种费率相同，取第一个即可
            except Exception as e:
                logger.warning("[deribit] Fee fetch failed, using defaults: %s", e)
            fee_rates = FeeRates(
                option_rate=all_fees["option"],
                future_rate=all_fees["future"],
            )
            logger.info("[deribit] Fee rates: option=%.4f future=%.4f",
                        fee_rates.option_rate, fee_rates.future_rate)

            # 3. 合约 + 价格
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            for sym in app.symbols:
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
            triplets = build_triplets(
                options=all_options,
                futures=all_futures,
                atm_prices=atm_prices,
                symbols=app.symbols,
                atm_range=app.atm_range,
                now_ms=int(time.time() * 1000),
                margin_type=margin_type,
            )
            logger.info("[deribit] Built %d triplets", len(triplets))
            if not triplets:
                logger.error("[deribit] No triplets — check config and Deribit connectivity")
                return

            # 5. exp_ms_by_call（内部 instId → expiry ms，供 WS 回调用）
            exp_ms_by_call: dict[str, int] = {
                o["instId"]: int(o["expTime"]) for o in all_options
            }

            # 6. 构建 WS 频道（使用模块级 _internal_to_deribit 反查 Deribit 原始格式）
            inst_ids: set[str] = set()
            for t in triplets:
                inst_ids.update([t.call_id, t.put_id, t.future_id])
            channels = [f"book.{_internal_to_deribit(i)}.5" for i in sorted(inst_ids)]

            # 7. MarketData + WS 回调
            market = MarketData()
            spot_price_cache = dict(atm_prices)   # coin: spot_price = atm_prices[sym]

            async def on_reconnect() -> None:
                logger.warning("[deribit ws] Reconnected — clearing order book cache")
                market.clear()

            async def on_message(msg: dict) -> None:
                if msg.get("method") != "subscription":
                    return
                params = msg.get("params", {})
                data = params.get("data", {})
                raw_name = data.get("instrument_name", "")
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if not bids or not asks:
                    return
                ts = int(data.get("timestamp", 0))

                # 将 Deribit 原始名转为内部 instId
                parts = raw_name.split("-")
                if len(parts) == 4:
                    # option
                    sym, ddate, stk, opt = parts
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USD-{exp}-{stk}-{opt}"
                elif len(parts) == 2:
                    sym, ddate = parts
                    if ddate == "PERPETUAL":
                        return              # 防止永续合约泄入 WS 频道
                    exp = _deribit_date_to_yymmdd(ddate)
                    inst_id = f"{sym}-USD-{exp}"
                else:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                )
                market.update(inst_id, snap)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    books_clean = {k: v for k, v in books_snapshot.items() if v is not None}
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = spot_price_cache[t.symbol]   # coin margin

                    for calc in (calculate_forward, calculate_reverse):
                        sig = calc(
                            triplet=t,
                            books=books_clean,
                            fee_rates=fee_rates,
                            lot_size=lot_size,
                            days_to_expiry=days_to_expiry,
                            spot_price=spot_price,
                            stale_threshold_ms=app.stale_threshold_ms,
                        )
                        if sig and sig.annualized_return >= app.min_annualized_rate:
                            print_signal(sig)

            # 8. 启动 WS
            logger.info("[deribit] Subscribing to %d channels", len(channels))
            ws_client = DeribitWebSocketClient(on_message=on_message, on_reconnect=on_reconnect)
            ws_client.set_channels(channels)
            await ws_client.run()
```

- [ ] **Step 2.3：��行测试确认无回归**

```bash
pytest tests/ -v
```
预期：全 PASS（新增 WS/REST 代码无法单元测试，不新增测试）

- [ ] **Step 2.4：提交**

```bash
git add src/pcp_arbitrage/exchanges/deribit.py
git commit -m "feat: implement DeribitWebSocketClient and DeribitRunner.run()"
```

---

## Task 3：BinanceRestClient + BinanceRunner.run() REST 初始化部分

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/binance.py`

### 步骤

- [ ] **Step 3.1：在 `binance.py` 顶部添加 imports 和签名辅助**

在文件顶部替换/补充 imports，添加签名函数：

```python
import asyncio
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import urlencode

import aiohttp

from pcp_arbitrage.config import AppConfig, ExchangeConfig

logger = logging.getLogger(__name__)


def _sign(secret: str, params: dict) -> str:
    """params 必须已包含 timestamp。对完整 query string 做 HMAC-SHA256。"""
    qs = urlencode(params)
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
```

- [ ] **Step 3.2：添加 BinanceRestClient**

```python
class BinanceRestClient:
    """Binance REST 客户端（eapi / fapi / api 三个 base URL）。"""

    BASE_EAPI = "https://eapi.binance.com"
    BASE_FAPI = "https://fapi.binance.com"
    BASE_API  = "https://api.binance.com"

    def __init__(self, api_key: str, secret: str) -> None:
        self._api_key = api_key
        self._secret = secret
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
        async with self._eapi_session.get("/eapi/v1/exchangeInfo") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_option_account(self) -> dict:
        assert self._eapi_session is not None
        params = self._signed_params()
        async with self._eapi_session.get(
            "/eapi/v1/account", params=params, headers=self._auth_headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_futures_exchange_info(self) -> dict:
        assert self._fapi_session is not None
        async with self._fapi_session.get("/fapi/v1/exchangeInfo") as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_futures_account(self) -> dict:
        assert self._fapi_session is not None
        params = self._signed_params()
        async with self._fapi_session.get(
            "/fapi/v2/account", params=params, headers=self._auth_headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_spot_price(self, symbol: str) -> float:
        assert self._api_session is not None
        async with self._api_session.get(
            "/api/v3/ticker/price", params={"symbol": symbol}
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return float(data["price"])
```

- [ ] **Step 3.3：实现 BinanceRunner.run() REST 初始化部分**

替换原来的 `raise NotImplementedError`，先只写 REST 部分，WS 循环留 `pass`：

```python
    async def run(self) -> None:
        from pcp_arbitrage.instruments import build_triplets
        from pcp_arbitrage.market_data import MarketData
        from pcp_arbitrage.models import BookSnapshot, FeeRates
        from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse
        from pcp_arbitrage.signal_printer import print_signal

        ex = self._ex
        app = self._app
        margin_type = ex.margin_type          # 固定 "usdt" for Binance

        async with BinanceRestClient(api_key=ex.api_key, secret=ex.secret_key) as rest:
            # 1. 费率
            logger.info("[binance] Fetching fee rates...")
            option_rate = 0.0003
            future_rate = 0.0004
            try:
                opt_acct = await rest.get_option_account()
                option_rate = float(opt_acct["optionCommissionRate"]["taker"])
                fut_acct = await rest.get_futures_account()
                future_rate = float(fut_acct["takerCommissionRate"])
            except Exception as e:
                logger.warning("[binance] Fee fetch failed, using defaults: %s", e)
            fee_rates = FeeRates(option_rate=option_rate, future_rate=future_rate)
            logger.info("[binance] Fee rates: option=%.4f future=%.4f",
                        fee_rates.option_rate, fee_rates.future_rate)

            # 2. 合约 + 价格
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            opt_info = await rest.get_option_exchange_info()
            raw_all_opts = opt_info.get("optionSymbols", [])

            fut_info = await rest.get_futures_exchange_info()
            raw_all_futs = [
                s for s in fut_info.get("symbols", [])
                if "_" in s.get("symbol", "")   # 有日期期货的 symbol 格式为 BTCUSDT_YYMMDD
            ]

            for sym in app.symbols:
                raw_opts = [o for o in raw_all_opts if o.get("underlying", "").startswith(sym)]
                raw_futs = [f for f in raw_all_futs if f["symbol"].startswith(sym)]
                opts, futs = _normalize_instruments(raw_opts, raw_futs, margin_type)
                all_options.extend(opts)
                all_futures.extend(futs)

                spot_sym = f"{sym}USDT"
                atm_prices[sym] = await rest.get_spot_price(spot_sym)
                logger.info("[binance] %s spot price: %.2f", sym, atm_prices[sym])

            # 3. Build triplets
            triplets = build_triplets(
                options=all_options,
                futures=all_futures,
                atm_prices=atm_prices,
                symbols=app.symbols,
                atm_range=app.atm_range,
                now_ms=int(time.time() * 1000),
                margin_type=margin_type,
            )
            logger.info("[binance] Built %d triplets", len(triplets))
            if not triplets:
                logger.error("[binance] No triplets — check config and Binance connectivity")
                return

            # 4. exp_ms_by_call（内部 instId → expiry ms）
            exp_ms_by_call: dict[str, int] = {
                o["instId"]: int(o["expTime"]) for o in all_options
            }

            # WS 部分在 Task 4 实现
            logger.info("[binance] REST init complete — WS not yet implemented")
```

- [ ] **Step 3.4：运行全部测试确认无回归**

```bash
pytest tests/ -v
```
预期：全 PASS（test_binance_normalize.py 5 个不受影响）

- [ ] **Step 3.5：提交**

```bash
git add src/pcp_arbitrage/exchanges/binance.py
git commit -m "feat: add BinanceRestClient and BinanceRunner REST init"
```

---

## Task 4：BinanceWebSocketClient + BinanceRunner.run() WS 循环

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/binance.py`

### 步骤

- [ ] **Step 4.1：在 `binance.py` 添加 BinanceWebSocketClient**

```python
class BinanceWebSocketClient:
    """单个 Binance WebSocket 连接（期权或期货），指数退避重连。"""

    PING_INTERVAL = 20
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60

    def __init__(self, url: str, on_message, on_reconnect=None) -> None:
        self._url = url
        self._on_message = on_message
        self._on_reconnect = on_reconnect
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
        async with aiohttp.ClientSession() as session:
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
```

- [ ] **Step 4.2：替换 BinanceRunner.run() 中的 WS 占位，完整实现**

在 Task 3 的 `run()` 末尾，将 `logger.info("[binance] REST init complete...")` 替换为完整 WS 启动逻辑：

```python
            # 5. 构建 WS 流名称
            # 期权：原始 symbol（来自 RAW_OPTIONS["symbol"]），格式 BTC-260627-70000-C
            # 需要从 all_options 记录反查原始 symbol（normalize 之前丢失了）
            # 更好方案：在 normalize 时保留 rawId 字段，这里直接从内部 instId 反推
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

            # 6. Binance 原始 symbol → 内部 instId 映射（供 on_message 用）
            opt_raw_to_internal: dict[str, str] = {}
            for o in all_options:
                # 内部 instId: BTC-USDT-260627-70000-C
                parts = o["instId"].split("-")
                raw = f"{parts[0]}-{parts[2]}-{parts[3]}-{parts[4]}"
                opt_raw_to_internal[raw] = o["instId"]

            fut_raw_to_internal: dict[str, str] = {}
            for f in all_futures:
                # 内部 instId: BTC-USDT-260627
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
                bids = msg.get("b", [])
                asks = msg.get("a", [])
                ts = int(msg.get("T", 0))
                if not bids or not asks:
                    return

                # 映射 raw symbol → 内部 instId
                inst_id = opt_raw_to_internal.get(raw_sym) or fut_raw_to_internal.get(raw_sym.lower())
                if not inst_id:
                    return

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                )
                market.update(inst_id, snap)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    books_clean = {k: v for k, v in books_snapshot.items() if v is not None}
                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = 1.0   # Binance USDT margin: no conversion needed

                    for calc in (calculate_forward, calculate_reverse):
                        sig = calc(
                            triplet=t,
                            books=books_clean,
                            fee_rates=fee_rates,
                            lot_size=lot_size,
                            days_to_expiry=days_to_expiry,
                            spot_price=spot_price,
                            stale_threshold_ms=app.stale_threshold_ms,
                        )
                        if sig and sig.annualized_return >= app.min_annualized_rate:
                            print_signal(sig)

            # 8. 两个 WS 并发运行
            # 注意：SUBSCRIBE JSON 方法需要连接到 /ws 端点（而非 /stream）
            OPT_WS_URL = "wss://nbstream.binance.com/eoptions/ws"
            FUT_WS_URL = "wss://fstream.binance.com/ws"

            opt_ws = BinanceWebSocketClient(OPT_WS_URL, on_message=on_message, on_reconnect=on_reconnect)
            opt_ws.set_streams(opt_streams)

            fut_ws = BinanceWebSocketClient(FUT_WS_URL, on_message=on_message, on_reconnect=on_reconnect)
            fut_ws.set_streams(fut_streams)

            logger.info("[binance] Subscribing opt=%d fut=%d streams", len(opt_streams), len(fut_streams))
            await asyncio.gather(opt_ws.run(), fut_ws.run())
```

- [ ] **Step 4.3：运行全部测试确认无回归**

```bash
pytest tests/ -v
```
预期：全 PASS

- [ ] **Step 4.4：提交**

```bash
git add src/pcp_arbitrage/exchanges/binance.py
git commit -m "feat: add BinanceWebSocketClient and complete BinanceRunner.run()"
```

---

## Task 5：更新 main.py + config.yaml + config_test.yaml + 全量验证

**Files:**
- Modify: `src/pcp_arbitrage/main.py`
- Modify: `config.yaml`
- Modify: `tests/fixtures/config_test.yaml`

### 步骤

- [ ] **Step 5.1：更新 main.py**

```python
# 在现有 import 后追加
from pcp_arbitrage.exchanges.deribit import DeribitRunner

# 更新 RUNNERS 字典
RUNNERS = {
    "okx": OKXRunner,
    "binance": BinanceRunner,
    "deribit": DeribitRunner,
}
```

- [ ] **Step 5.2：更新 config.yaml，追加 deribit 块**

在 `exchanges:` 下的 `binance:` 块之后添加：

```yaml
  deribit:
    enabled: false
    margin_type: coin        # Deribit 固定币本位
    api_key: ""              # client_id
    secret_key: ""           # client_secret
    passphrase: ""           # 不使用
```

- [ ] **Step 5.3：更新 tests/fixtures/config_test.yaml，追加 deribit 块**

```yaml
  deribit:
    enabled: false
    margin_type: coin
    api_key: "test_key_d"
    secret_key: "test_secret_d"
    passphrase: ""
```

- [ ] **Step 5.4：运行全量测试**

```bash
pytest tests/ -v
```
预期：全 PASS，包括：
- `test_binance_normalize.py` 5 个
- `test_deribit_normalize.py` 6 个
- 其他现有测试

- [ ] **Step 5.5：验证 main.py 可导入（smoke test）**

```bash
python -c "from pcp_arbitrage.main import main; print('OK')"
```
预期：打印 `OK` 无报错

- [ ] **Step 5.6：提交**

```bash
git add src/pcp_arbitrage/main.py config.yaml tests/fixtures/config_test.yaml
git commit -m "feat: register DeribitRunner, add deribit config blocks"
```

---

## 实现顺序总结

| Task | 主要内容 | 关键验证 |
|------|---------|---------|
| 1 | Deribit 归一化 + RestClient + Runner 骨架 | `test_deribit_normalize.py` 11 个 PASS |
| 2 | DeribitWebSocketClient + `run()` 完整 | 全量测试 PASS（WS 不做单元测试） |
| 3 | BinanceRestClient + REST 初始化 | 全量测试 PASS |
| 4 | BinanceWebSocketClient + WS 循环 | 全量测试 PASS |
| 5 | main.py + configs | 全量 PASS + smoke test |
