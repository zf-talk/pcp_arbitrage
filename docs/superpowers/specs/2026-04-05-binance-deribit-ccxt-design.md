# Binance + Deribit PCP 套利监听集成设计文档

**日期：** 2026-04-05
**版本：** v2.1（直接调官方 API，不引入 ccxt）
**范围：** 补全 BinanceRunner、新建 DeribitRunner，接入现有多交易所并发架构

---

## 1. 背景与目标

现有架构已完成：
- OKX：自定义 `OKXRestClient` + `OKXWebSocketClient`，完整实现，**完全不动**
- Binance：`BinanceRunner` stub（raise NotImplementedError）+ `_normalize_instruments(raw_options, raw_futures, margin_type)` 辅助函数
- Deribit：尚不存在

目标：
1. 补全 `BinanceRunner.run()`，新建 `BinanceRestClient` + `BinanceWebSocketClient`
2. 新建 `DeribitRunner` + `DeribitRestClient` + `DeribitWebSocketClient`
3. 在 `main.py` 注册 `deribit` → `DeribitRunner`
4. 更新 `config.yaml` / `tests/fixtures/config_test.yaml` 新增 `deribit:` 块

**原则：OKX 现有代码、测试、共享模块（instruments.py、pcp_calculator.py、signal_printer.py、market_data.py、models.py）全部不动。**

---

## 2. 目录结构变化

```
src/pcp_arbitrage/
├── exchanges/
│   ├── base.py          # 不变（ExchangeRunner Protocol）
│   ├── okx.py           # 不变
│   ├── binance.py       # 补全 run()；新增 BinanceRestClient + BinanceWebSocketClient
│   └── deribit.py       # 新建 DeribitRunner + DeribitRestClient + DeribitWebSocketClient
├── okx_client.py        # 不变（OKXRestClient + OKXWebSocketClient）
└── ...                  # 其他模块不变
```

客户端类与 Runner 放在同一文件，保持与 OKX 结构一致（`okx_client.py` 被 `exchanges/okx.py` 使用）。

---

## 3. config.yaml 变化

新增 `deribit:` 块，复用现有 `ExchangeConfig` 字段（无需改动 `config.py`）：

```yaml
exchanges:
  okx:
    enabled: true
    margin_type: coin
    api_key: "..."
    secret_key: "..."
    passphrase: "..."
    is_paper_trading: false

  binance:
    enabled: false
    margin_type: usdt        # Binance 欧式期权固定 USDT 结算（本次范围内只支持 usdt）
    api_key: "..."
    secret_key: "..."

  deribit:
    enabled: false
    margin_type: coin        # Deribit 固定币本位
    api_key: "..."           # client_id
    secret_key: "..."        # client_secret
    passphrase: ""           # 不使用，留空

arbitrage:
  min_annualized_rate: 0.010
  atm_range: 0.20
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000

contracts:
  lot_size:
    BTC: 0.01
    ETH: 0.1
```

**`ExchangeConfig` 和 `load_config` 不需要任何改动。**

---

## 4. main.py 变化（最小）

```python
from pcp_arbitrage.exchanges.deribit import DeribitRunner

RUNNERS = {
    "okx": OKXRunner,
    "binance": BinanceRunner,
    "deribit": DeribitRunner,
}
```

---

## 5. 通用运行流程（Binance 和 Deribit 相同模式，与 OKX 一致）

```
REST 初始化：
  1. fetch_fee_rates()        → FeeRates
  2. fetch_instruments()      → normalize → build_triplets()
  3. fetch_ticker()           → atm_prices
  4. 构建 exp_ms_by_call: dict[str, int]  # {call_instId → expiry_ms}，供 on_message 计算 days_to_expiry
  5. print_triplet_summary()

WS 循环（单连接，推送所有合约盘口）：
  6. 订阅所有 triplet instruments
  7. on_message:
     - 若 bids 或 asks 为空则跳过（防御空快照）
     - 归一化原始 instId → 内部格式
     - BookSnapshot → market.update(inst_id, snap)
     - pcp_calculator → signal_printer
```

---

## 6. Binance Runner

### 6.1 API 端点

| 用途 | 方法 | 端点 |
|------|------|------|
| 期权合约列表 | GET | `https://eapi.binance.com/eapi/v1/exchangeInfo` |
| 期权账户/费率（认证） | GET | `https://eapi.binance.com/eapi/v1/account` |
| 期货合约列表（U本位） | GET | `https://fapi.binance.com/fapi/v1/exchangeInfo` |
| 期货账户/费率（认证） | GET | `https://fapi.binance.com/fapi/v2/account` |
| 现货价格 | GET | `https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT` |
| WS 盘口（期权） | WS | `wss://nbstream.binance.com/eoptions/stream` |
| WS 盘口（期货 U本位） | WS | `wss://fstream.binance.com/stream` |

### 6.2 认证

Binance REST 签名：将所有请求参数（含 `timestamp=<ms>`）拼成查询字符串后做 HMAC-SHA256，签名追加为 `signature` 参数，Header 带 `X-MBX-APIKEY`。

```python
import hmac, hashlib, time
from urllib.parse import urlencode

def _sign(secret: str, params: dict) -> str:
    """params 必须已包含 timestamp 字段。对整个查询字符串做 HMAC-SHA256。"""
    qs = urlencode(params)
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

# 使用示例：
params = {"timestamp": int(time.time() * 1000)}
params["signature"] = _sign(secret, params)
# GET /eapi/v1/account?timestamp=...&signature=...
# Header: X-MBX-APIKEY: <api_key>
```

### 6.3 合约格式归一化

已有 `_normalize_instruments(raw_options, raw_futures, margin_type)` 函数（`tests/test_binance_normalize.py` 有 5 个测试覆盖），**签名保持不变**：

| 类型 | Binance 原始 | 归一化后（项目内部） |
|------|-------------|---------------------|
| 期权 | `BTC-260627-70000-C`（4-part） | `BTC-USDT-260627-70000-C`（5-part） |
| 期货 U本位 | `BTCUSDT_260627` | `BTC-USDT-260627` |

`stk` 字段来自 API 的 `strikePrice`，`expTime` 来自 `expiryDate`（**毫秒**时间戳字符串，与 `instruments.py` 的 `int(call_opt["expTime"])` 一致）。

### 6.4 费率获取

```python
# 期权费率：GET /eapi/v1/account（需签名）
# 返回 optionCommissionRate.taker
option_rate = float(account_info["optionCommissionRate"]["taker"])

# 期货费率：GET /fapi/v2/account（需签名）
# 返回 takerCommissionRate（字符串，如 "0.0004"）
future_rate = float(futures_account["takerCommissionRate"])
```

### 6.5 现货价格

```python
# GET /api/v3/ticker/price?symbol=BTCUSDT → {"symbol":"BTCUSDT","price":"83000.00"}
atm_prices["BTC"] = float(resp["price"])
```

**注意：** Binance 在本次范围内固定 `margin_type = "usdt"`，因此 `spot_price = 1.0`（期权价格已是 USDT，无需换算）。但现货价格仍需获取，供 `build_triplets` 的 `atm_prices` 参数过滤 ATM 附近行权价。

### 6.6 BinanceRestClient 结构

```python
class BinanceRestClient:
    """Binance REST 客户端，覆盖三个 base URL（eapi/fapi/api）。"""

    BASE_EAPI = "https://eapi.binance.com"   # 欧式期权
    BASE_FAPI = "https://fapi.binance.com"   # U本位期货
    BASE_API  = "https://api.binance.com"    # 现货

    def __init__(self, api_key: str, secret: str): ...
    async def __aenter__(self): ...   # 创建 aiohttp.ClientSession
    async def __aexit__(self, *_): ... # 关闭 session

    async def get_option_exchange_info(self) -> dict: ...         # public
    async def get_option_account(self) -> dict: ...               # 认证，含 optionCommissionRate
    async def get_futures_exchange_info(self) -> dict: ...        # public
    async def get_futures_account(self) -> dict: ...              # 认证，含 takerCommissionRate
    async def get_spot_price(self, symbol: str) -> float: ...     # public
```

所有方法返回已解析的 JSON 数据（不返回原始 `aiohttp.ClientResponse`）。

### 6.7 WebSocket 设计

Binance 期权和期货是**不同的 WS 端点**，需两个连接并发。

**订阅格式：**

```python
# 期权流（wss://nbstream.binance.com/eoptions/stream）
# 使用 Binance 原始期权 symbol（混合大小写，如 BTC-260627-70000-C）
{"method": "SUBSCRIBE", "params": ["BTC-260627-70000-C@depth5"], "id": 1}

# 期货流（wss://fstream.binance.com/stream）
# 使用全小写 symbol，下划线分隔（如 btcusdt_260627）
{"method": "SUBSCRIBE", "params": ["btcusdt_260627@depth5"], "id": 2}
```

**消息格式（期权深度流）：**

```json
{"e":"depth","s":"BTC-260627-70000-C","b":[["70000","10"]],"a":[["70100","5"]],"T":1700000000000}
```

字段映射：`b[0][0]` → `bid`（价格），`a[0][0]` → `ask`（价格），`T` → `ts`。

**on_message 处理要点：**
1. `b` 或 `a` 为空时直接 return（`@depth5` 可能推送空快照）
2. `s` 字段是 Binance 原始 symbol，需调用 `_normalize_instruments` 逻辑中的 symbol 映射转为内部 instId 后再做 `market.update()`
3. 期货流消息格式与期权流相同，字段名一致

`BinanceWebSocketClient` 与 `OKXWebSocketClient` 结构相同：心跳、指数退避重连、`on_message` / `on_reconnect` 回调。

`BinanceRunner.run()` 内部：

```python
await asyncio.gather(
    self._run_options_ws(triplets, market, ...),
    self._run_futures_ws(triplets, market, ...),
)
```

两个 WS 共享同一 `MarketData` 实例，`on_message` 逻辑相同。

---

## 7. Deribit Runner

### 7.1 API 端点

| 用途 | 方法 | 端点 |
|------|------|------|
| 期权合约列表 | GET | `https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option` |
| 期货合约列表 | GET | `https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=future` |
| 现货价格 | GET | `https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd` |
| 认证 | POST | `https://www.deribit.com/api/v2/public/auth` |
| 费率（认证） | GET | `https://www.deribit.com/api/v2/private/get_account_summary?currency=BTC&extended=true` |
| WS | WS | `wss://www.deribit.com/ws/api/v2` |

所有 Deribit REST 响应格式为 `{"jsonrpc":"2.0","result":<data>,...}`。`DeribitRestClient` 的所有方法**统一解包 `result` 字段**后返回，外部调用者不感知 JSON-RPC 封装。

### 7.2 认证与 Token 刷新

Deribit 使用 OAuth2 client_credentials，access_token **900 秒后过期**，需主动刷新。

```python
# POST /api/v2/public/auth（application/json body）
{
  "jsonrpc": "2.0",
  "method": "public/auth",
  "params": {
    "grant_type": "client_credentials",
    "client_id": api_key,
    "client_secret": secret_key
  }
}
# result: {"access_token": "...", "expires_in": 900, ...}
```

`DeribitRestClient` 内部维护 `_token` 和 `_token_expires_at`，每次调用私有接口前检查是否需要刷新：

```python
async def _refresh_token_if_needed(self) -> None:
    """若 token 将在 60 秒内过期则重新认证。"""
    if time.time() >= self._token_expires_at - 60:
        await self._authenticate()
```

`api_key` → `client_id`，`secret_key` → `client_secret`，`passphrase` 不使用。

### 7.3 合约格式归一化

| 类型 | Deribit 原始 | 归一化后（项目内部） |
|------|-------------|---------------------|
| 期权 | `BTC-27JUN25-70000-C` | `BTC-USD-250627-70000-C` |
| 期货 | `BTC-27JUN25` | `BTC-USD-250627` |

日期转换：`27JUN25` → `250627`，需月份映射表：

```python
MONTH_MAP = {
    "JAN":"01","FEB":"02","MAR":"03","APR":"04",
    "MAY":"05","JUN":"06","JUL":"07","AUG":"08",
    "SEP":"09","OCT":"10","NOV":"11","DEC":"12",
}

def _deribit_date_to_yymmdd(deribit_date: str) -> str:
    """'27JUN25' → '250627'"""
    day, mon, year = deribit_date[:2], deribit_date[2:5], deribit_date[5:]
    return f"{year}{MONTH_MAP[mon]}{day}"
```

归一化函数（可单元测试，settle 固定 `"USD"`）：

```python
def _normalize_instruments(
    raw_options: list[dict],
    raw_futures: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Deribit 原始 API result 列表 → 项目内部统一格式。
    settle 固定 'USD'（Deribit 币本位）。
    返回 (normalized_options, normalized_futures)。
    """
    ...
```

Deribit `get_instruments` 返回字段映射（`result` 已解包）：

| Deribit 字段 | 项目内部字段 | 备注 |
|-------------|-------------|------|
| `instrument_name` | → `instId`（归一化后） | |
| `strike` | `stk` | 数值，转为字符串 |
| `option_type` (`"call"`/`"put"`) | `optType` (`"C"`/`"P"`) | |
| `expiration_timestamp` | `expTime`（毫秒字符串） | Deribit 返回**毫秒**，与 `instruments.py` 一致 |

### 7.4 费率获取

`get_account_summary` 返回的 `result` 已由客户端解包，`fees` 字段为列表：

```python
# result["fees"] 示例：
# [{"instrument_type": "option", "taker_commission": 0.0003, ...},
#  {"instrument_type": "future", "taker_commission": 0.0005, ...}]
for fee in account_summary["fees"]:
    if fee["instrument_type"] == "option":
        option_rate = float(fee["taker_commission"])
    elif fee["instrument_type"] == "future":
        future_rate = float(fee["taker_commission"])
```

### 7.5 现货价格

```python
# GET /api/v2/public/get_index_price?index_name=btc_usd
# result（已解包）: {"index_price": 83000.0, ...}
atm_prices["BTC"] = float(index_price_result["index_price"])
```

`margin_type == "coin"` → `spot_price = atm_prices[sym]`（Deribit 期权以 BTC 计价，需乘以 spot 换算为 USDT）。

### 7.6 DeribitRestClient 结构

```python
class DeribitRestClient:
    BASE = "https://www.deribit.com"

    def __init__(self, api_key: str, secret: str): ...
    async def __aenter__(self): ...
    async def __aexit__(self, *_): ...

    async def _authenticate(self) -> None: ...              # 更新 _token 和 _token_expires_at
    async def _refresh_token_if_needed(self) -> None: ...   # 检查并按需刷新
    async def get_instruments(self, currency: str, kind: str) -> list[dict]: ...  # public，返回 result 列表
    async def get_index_price(self, index_name: str) -> dict: ...                 # public，返回 result dict
    async def get_account_summary(self, currency: str) -> dict: ...               # private，Bearer，返回 result dict
```

### 7.7 WebSocket 设计

Deribit WS 是 JSON-RPC 2.0，单连接订阅所有合约（与 OKX 模式相同）。`book.*` 频道为**公开频道，无需 WS 认证**。

**订阅格式（使用 Deribit 原始 instrument_name）：**

```json
{
  "jsonrpc": "2.0",
  "method": "public/subscribe",
  "params": {"channels": ["book.BTC-27JUN25-70000-C.5", "book.BTC-27JUN25.5"]},
  "id": 1
}
```

**推送消息格式：**

```json
{
  "method": "subscription",
  "params": {
    "channel": "book.BTC-27JUN25-70000-C.5",
    "data": {
      "instrument_name": "BTC-27JUN25-70000-C",
      "bids": [[70000.0, 10.0]],
      "asks": [[70100.0, 5.0]],
      "timestamp": 1700000000000
    }
  }
}
```

字段映射：`data.bids[0][0]` → `bid`，`data.asks[0][0]` → `ask`，`data.timestamp` → `ts`。

**on_message 处理要点：**
1. `bids` 或 `asks` 为空时直接 return
2. `data.instrument_name` 是 Deribit 原始格式，需调用 `_normalize_instruments` 中的同一日期转换逻辑转为内部 instId

**心跳：** Deribit WS 要求每 **10 秒**内至少发一次消息，否则服务端断开。使用 `public/test` 作为心跳（建议间隔 5 秒）：

```json
{"jsonrpc": "2.0", "method": "public/test", "id": 99}
```

`DeribitWebSocketClient` 结构与 `OKXWebSocketClient` 相同：重连、心跳、回调。

---

## 8. 测试策略

| 文件 | 测试内容 |
|------|---------||
| `tests/test_binance_normalize.py` | 已有 5 个测试保持不变（`_normalize_instruments(raw_options, raw_futures, margin_type)`） |
| `tests/test_deribit_normalize.py` | `_deribit_date_to_yymmdd()` 各月份转换；期权归一化 instId；期货归一化 instId；费率字段提取逻辑 |

网络层（REST 客户端、WS 连接）不做单元测试，与 OKX 保持一致。

---

## 9. 不在本次范围内

- Binance coin 本位期货（`margin_type=coin`）
- Deribit / Binance 账户余额打印
- 跨交易所套利信号对比
- OKX 任何改动

---

## 10. 实现顺序

1. **Task 1**：`_deribit_date_to_yymmdd` + `_normalize_instruments` + `DeribitRestClient` + `DeribitRunner` 骨架 + `tests/test_deribit_normalize.py`
2. **Task 2**：`DeribitWebSocketClient` + `DeribitRunner.run()` 完整实现
3. **Task 3**：`BinanceRestClient` + 补全 `BinanceRunner.run()` REST 初始化部分
4. **Task 4**：`BinanceWebSocketClient` + `BinanceRunner.run()` WS 循环
5. **Task 5**：更新 `main.py` + `config.yaml` + `config_test.yaml` + 全量测试
