# ccxt.pro 适配器层设计文档

**日期：** 2026-04-04
**版本：** v1.0
**范围：** 用 ccxt.pro 替换手写的 `okx_client.py` + `fee_fetcher.py`，引入 `ExchangeAdapter` 抽象层，支持未来多交易所扩展

---

## 1. 背景与目标

当前项目使用手写的 `okx_client.py` 封装 OKX REST 和 WebSocket，包含 HMAC 签名、心跳、指数退避重连等约 159 行底层代码。存在以下问题：

- 维护成本高（签名 bug 已出现一次）
- 强绑定 OKX，无法支持其他交易所

**目标：**
1. 用 `ccxt.pro` 替换底层网络层，消除手写签名和重连逻辑
2. 引入 `ExchangeAdapter` Protocol，上层逻辑与交易所解耦
3. 为将来支持 Binance、Deribit 等交易所预留扩展点
4. 启动时将合约列表写入 JSON 文件，便于调试

---

## 2. 目录结构变化

```
src/pcp_arbitrage/
├── adapters/
│   ├── __init__.py
│   ├── base.py          # ExchangeAdapter Protocol
│   └── okx.py           # OKX ccxt.pro 实现
├── config.py            # 新增 exchange.name 字段
├── main.py              # 只改适配器初始化 + JSON 写入
├── instruments.py       # 不变
├── market_data.py       # 不变
├── models.py            # 不变
├── pcp_calculator.py    # 不变
└── signal_printer.py    # 不变
```

**删除：**
- `okx_client.py`
- `fee_fetcher.py`

---

## 3. ExchangeAdapter Protocol（base.py）

```python
from typing import Protocol, AsyncGenerator
from pcp_arbitrage.models import FeeRates, Triplet, BookSnapshot
from pcp_arbitrage.config import AppConfig

class ExchangeAdapter(Protocol):
    async def fetch_fee_rates(self) -> FeeRates:
        """获取账户手续费等级，返回正值费率。"""
        ...

    async def fetch_instruments(
        self,
        symbols: list[str],
        atm_range: float,
        now_ms: int,
    ) -> tuple[list[Triplet], dict[str, int]]:
        """
        拉取期权+期货合约，构建并过滤 Triplet 列表。
        过滤规则：symbol 匹配、days_to_expiry >= 1、strike 在 ATM ± atm_range 内、
        存在对应 USDT 本位期货。
        返回 (triplets, exp_ms_by_call)，其中 exp_ms_by_call 是 call_id → expiry_ms 映射，
        供 main.py 计算 days_to_expiry 使用。
        """
        ...

    async def watch_order_books(
        self,
        triplets: list[Triplet],
    ) -> AsyncGenerator[tuple[str, BookSnapshot | None], None]:
        """
        持续推送 order book 更新（async generator 函数，调用方直接 async for 迭代，无需 await）。
        每次有任意一腿更新，yield (inst_id, BookSnapshot)。
        内部处理重连，重连时 yield ("__reconnect__", None) 通知调用方清空缓存。
        网络故障时，同一事件可能触发多个 "__reconnect__" sentinel（每个订阅协程各一个）；
        main.py 的 market.clear() 是幂等操作，多次调用无副作用。
        """
        ...

    async def close(self) -> None:
        """关闭 ccxt.pro 连接。"""
        ...
```

---

## 4. OKX 适配器（okx.py）

### 4.1 初始化

```python
import ccxt.pro as ccxtpro
from pcp_arbitrage.config import AppConfig

class OKXAdapter:
    def __init__(self, cfg: AppConfig) -> None:
        self._exchange = ccxtpro.okx({
            "apiKey": cfg.api_key,
            "secret": cfg.secret_key,
            "password": cfg.passphrase,
            "options": {"defaultType": "option"},
        })
        if cfg.is_paper_trading:
            self._exchange.set_sandbox_mode(True)
        self._cfg = cfg
```

### 4.2 fetch_fee_rates

调用 `exchange.fetch_trading_fee("BTC/USD:BTC")` 获取期权费率，`exchange.fetch_trading_fee("BTC/USDT:USDT")` 获取期货费率。

**重要：** OKX 通过 ccxt 返回的 taker 费率仍为负值（如 `-0.0002`），必须取 `abs()` 后再构建 `FeeRates`，否则会导致手续费计算错误（费用变为负数，净利润虚高）：

```python
option_rate = abs(float(option_fee_data.get("taker", -0.0003)))
future_rate = abs(float(future_fee_data.get("taker", -0.0005)))
return FeeRates(option_rate=option_rate, future_rate=future_rate)
```

### 4.3 fetch_instruments

1. 调用 `exchange.fetch_markets()` 获取所有市场
2. 过滤出 `type == "option"` 且 `base in symbols` 的期权
3. 过滤出 `type == "future"` 且 `settle == "USDT"` 的期货
4. 获取现货价格（`fetch_ticker("{SYM}/USDT")`）作为 ATM 基准
5. **字段映射：** `build_triplets()` 期望 OKX 原始 dict 格式（含 `instId`、`strike`、`optType`、`expTime`）。从 ccxt 市场对象取 `market["info"]`（原始 OKX payload）传入，无需额外转换：

```python
raw_options = [m["info"] for m in markets if m["type"] == "option" and m["base"] in symbols]
raw_futures = [m["info"] for m in markets if m["type"] == "future" and m.get("settle") == "USDT"
                                            and m["base"] in symbols]
# 构建 ATM 基准价格（顺序拉取各 symbol 现货价，通常只有 2 个 symbol，延迟可接受）
atm_prices = {
    sym: float((await self._exchange.fetch_ticker(f"{sym}/USDT"))["last"])
    for sym in symbols
}
# raw_options 的 info 字段包含：instId, strike, optType, expTime（OKX 原始格式）
# raw_futures 的 info 字段包含：instId（格式 BTC-USDT-250425）
triplets = build_triplets(raw_options, raw_futures, atm_prices, symbols, atm_range, now_ms)
```

6. **同时构建 `exp_ms_by_call` 映射**（`dict[str, int]`，call_id → expiry_ms），从 `raw_options` 中提取，返回给 main.py 用于计算 `days_to_expiry`：

```python
exp_ms_by_call = {o["instId"]: int(o["expTime"]) for o in raw_options if o["optType"] == "C"}
```

`fetch_instruments()` 返回值调整为 `tuple[list[Triplet], dict[str, int]]`（triplets + exp_ms_by_call），Protocol 同步更新。

**注意：** ccxt 市场对象的 `info` 字段包含的关键字段（OKX 原始）：
- `info["instId"]`：完整合约 ID，如 `BTC-USD-250425-95000-C`（期权）或 `BTC-USDT-250425`（期货）
- `info["strike"]`：行权价字符串，如 `"95000"`（仅期权）
- `info["optType"]`：`"C"` 或 `"P"`（仅期权）
- `info["expTime"]`：到期时间戳字符串（ms），如 `"1745596800000"`（仅期权）
- 期货的 `info["instId"]` 格式为 `{SYM}-USDT-{EXPIRY}`，供 `build_triplets()` 匹配使用

### 4.4 watch_order_books

**注意：** 此方法是 async generator 函数（含 `yield`），调用方直接 `async for` 迭代，不需要 `await`。
网络故障时所有订阅协程会同时抛出异常，每个协程各发出一个 `("__reconnect__", None)` sentinel；
`main.py` 对每个 sentinel 调用 `market.clear()`，该操作幂等，多次调用无副作用。

```python
import time  # 用于 ob["timestamp"] 为 None 时的回退值

async def watch_order_books(self, triplets):
    # 收集所有需要订阅的 inst_id
    inst_ids = {id for t in triplets for id in (t.call_id, t.put_id, t.future_id)}

    # 用 asyncio.Queue 汇聚多个 watch 协程的结果
    queue = asyncio.Queue()

    async def _watch_one(inst_id: str):
        while True:
            try:
                ob = await self._exchange.watch_order_book(inst_id, limit=5)
                snap = BookSnapshot(
                    bid=float(ob["bids"][0][0]),
                    ask=float(ob["asks"][0][0]),
                    ts=ob["timestamp"] if ob["timestamp"] is not None else int(time.time() * 1000),
                )
                await queue.put((inst_id, snap))
            except (ccxtpro.NetworkError, ccxtpro.ExchangeNotAvailable):
                await queue.put(("__reconnect__", None))
                await asyncio.sleep(1)

    tasks = [asyncio.create_task(_watch_one(id)) for id in inst_ids]
    try:
        while True:
            yield await queue.get()
    finally:
        for t in tasks: t.cancel()
```

### 4.5 错误处理

| 异常 | 处理方式 |
|------|----------|
| `ccxt.AuthenticationError` | 启动时立即抛出，进程退出，打印明确错误 |
| `ccxt.NetworkError` | watch 内部捕获，yield `__reconnect__` 信号，main.py 清空 MarketData |
| `ccxt.ExchangeNotAvailable` | 同上 |
| 合约无 USDT 期货 | `logger.warning` + 丢弃（instruments.py 现有逻辑） |

---

## 5. 合约列表 JSON 写入

启动时，`fetch_instruments()` 返回 Triplet 列表后，`main.py` 将其写入 `instruments_cache.json`（调试用，不影响运行）。

**格式：**
```json
{
  "generated_at": "2026-04-04T12:00:00.000Z",
  "exchange": "okx",
  "count": 42,
  "triplets": [
    {
      "symbol": "BTC",
      "expiry": "250425",
      "strike": 95000.0,
      "call_id": "BTC-USD-250425-95000-C",
      "put_id": "BTC-USD-250425-95000-P",
      "future_id": "BTC-USDT-250425"
    }
  ]
}
```

**写入位置：** 项目根目录 `instruments_cache.json`（加入 `.gitignore`）
**写入时机：** build_triplets 成功后、启动 WebSocket 之前
**写入失败：** 仅打印 warning，不中断启动流程

---

## 6. config.yaml 变更

新增 `exchange` 节：

```yaml
exchange:
  name: okx   # 将来改成 binance / deribit 即可
```

`main.py` 根据 `cfg.exchange_name` 选择对应适配器：

```python
ADAPTERS = {"okx": OKXAdapter}
adapter = ADAPTERS[cfg.exchange_name](cfg)
```

`AppConfig` 新增字段：`exchange_name: str`

`load_config()` 新增解析（在 `return AppConfig(...)` 调用中增加）：

```python
exchange_name=raw["exchange"]["name"],
```

---

## 7. main.py 变化摘要

```python
# 旧
async with OKXRestClient(...) as rest:
    fee_rates = await fetch_fee_rates(rest)
    # ... REST 拉取 ...
    ws_client = OKXWebSocketClient(...)
    await ws_client.run()

# 新
adapter = ADAPTERS[cfg.exchange_name](cfg)
try:
    fee_rates = await adapter.fetch_fee_rates()
    triplets, exp_ms_by_call = await adapter.fetch_instruments(
        symbols=cfg.symbols,
        atm_range=cfg.atm_range,
        now_ms=int(time.time() * 1000),
    )
    _write_instruments_json(triplets, cfg.exchange_name)  # 写 JSON
    async for inst_id, snap in adapter.watch_order_books(triplets):
        if inst_id == "__reconnect__":
            market.clear()
            continue
        market.update(inst_id, snap)
        # ... PCP 计算 ...
finally:
    await adapter.close()
```

---

## 8. 依赖变更

```toml
# 移除（不再需要）
# aiohttp（REST 和 WS 全交给 ccxt.pro）

# 新增
"ccxt[pro]>=4.0"
```

> 注：ccxt.pro 内部使用 aiohttp，但不需要项目直接依赖。

---

## 9. 测试策略

- `adapters/base.py`：无逻辑，Protocol 定义，无需测试
- `adapters/okx.py`：网络层，无单元测试（集成测试 = 烟雾测试）
- `instruments.py`：现有 8 个测试继续有效，输入格式保持 dict 列表，不变
- `main.py`：现有 38 个测试全部继续有效（适配器不在测试范围内）
- JSON 写入：1 个新测试验证格式正确性

---

## 10. 不在本次范围内

- 第二家交易所实现（Binance/Deribit）
- ccxt.pro 连接池优化（单个 ccxt 实例可处理所有订阅）
- 跨交易所套利信号对比
