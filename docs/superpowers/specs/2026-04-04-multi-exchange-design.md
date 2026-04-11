# 多交易所并发支持 + U本位/币本位可配置 设计文档

**日期：** 2026-04-04
**版本：** v1.0
**范围：** 新增 `margin_type` 配置项，重构为多交易所并发架构，新增 Binance 交易所支持

---

## 1. 背景与目标

当前代码强绑定 OKX，且期货结算方式（U本位/币本位）在代码中硬编码。目标：

1. 在配置文件中为每个交易所指定 `margin_type`（`coin` 或 `usdt`）
2. 支持配置多个交易所，`enabled: true` 的交易所并发运行，共享同一 asyncio 事件循环
3. 新增 Binance 欧式期权支持（USDT 结算和币本位均支持）
4. 现有 OKX 逻辑、测试、信号打印全部保持不变

---

## 2. 目录结构变化

```
src/pcp_arbitrage/
├── exchanges/
│   ├── __init__.py
│   ├── base.py          # ExchangeConfig dataclass + ExchangeRunner Protocol
│   ├── okx.py           # OKX 完整运行逻辑（从 main.py 迁移）
│   └── binance.py       # Binance 实现
├── config.py            # AppConfig 重构，新增 ExchangeConfig
├── main.py              # 只负责加载配置、并发启动 runners
├── instruments.py       # 新增 margin_type 参数，其余不变
├── market_data.py       # 不变
├── models.py            # 不变
├── pcp_calculator.py    # 不变（spot_price=1.0 处理 USDT 本位）
├── signal_printer.py    # 不变
├── okx_client.py        # 不变，由 exchanges/okx.py 使用
└── fee_fetcher.py       # 不变，由 exchanges/okx.py 使用
```

---

## 3. config.yaml 新结构

```yaml
exchanges:
  okx:
    enabled: true
    margin_type: coin        # coin = 币本位期货(BTC-USD-YYMMDD), usdt = U本位(BTC-USDT-YYMMDD)
    api_key: "..."
    secret_key: "..."
    passphrase: "..."
    is_paper_trading: false

  binance:
    enabled: false
    margin_type: usdt        # binance 欧式期权默认 USDT 结算
    api_key: "..."
    secret_key: "..."

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

---

## 4. config.py 重构

### 4.1 新增 ExchangeConfig

```python
@dataclass
class ExchangeConfig:
    name: str              # "okx" | "binance"
    enabled: bool
    margin_type: str       # "coin" | "usdt"
    api_key: str
    secret_key: str
    passphrase: str        # OKX 专用；Binance 用空字符串 "" 作为哨兵值
    is_paper_trading: bool # OKX 专用；Binance 忽略此字段（始终为 False）

@dataclass
class AppConfig:
    exchanges: dict[str, ExchangeConfig]
    symbols: list[str]
    min_annualized_rate: float
    atm_range: float
    stale_threshold_ms: int
    lot_size: dict[str, float]
```

### 4.2 load_config 变化

```python
def load_config(path) -> AppConfig:
    raw = yaml.safe_load(open(path))
    exchanges = {}
    for name, ex in raw["exchanges"].items():
        exchanges[name] = ExchangeConfig(
            name=name,
            enabled=ex.get("enabled", False),
            margin_type=ex.get("margin_type", "coin"),
            api_key=ex.get("api_key", ""),
            secret_key=ex.get("secret_key", ""),
            passphrase=ex.get("passphrase", ""),
            is_paper_trading=ex.get("is_paper_trading", False),
        )
    arb = raw["arbitrage"]
    return AppConfig(
        exchanges=exchanges,
        symbols=arb["symbols"],
        min_annualized_rate=arb["min_annualized_rate"],
        atm_range=arb["atm_range"],
        stale_threshold_ms=arb["stale_threshold_ms"],
        lot_size=raw["contracts"]["lot_size"],
    )
```

---

## 5. exchanges/base.py

```python
from typing import Protocol
from pcp_arbitrage.config import AppConfig, ExchangeConfig

class ExchangeRunner(Protocol):
    async def run(self) -> None:
        """启动完整的套利监听循环（REST 初始化 + WS 订阅），不返回。"""
        ...
```

---

## 6. instruments.py 变化

`build_triplets` 新增 `margin_type: str` 参数，控制期货匹配条件。

**重要约束：** `instruments.py` 只处理统一格式，不关心交易所差异：
- 期权 `instId` 必须是 5-part 破折号格式：`{SYM}-{SETTLE}-{YYMMDD}-{STRIKE}-{C/P}`，如 `BTC-USD-260627-70000-C`
- 期货 `instId` 必须是 3-part 破折号格式：`{SYM}-{MARGIN}-{YYMMDD}`，如 `BTC-USD-260627`
- 期权 strike 来自 dict 字段 `opt.get("strike") or opt["stk"]`（**不从 `instId` parts 解析**）

各交易所的 Runner 负责在调用 `build_triplets` 前将原始 API 数据归一化为上述格式。

```python
def build_triplets(
    options: list[dict],
    futures: list[dict],
    atm_prices: dict[str, float],
    symbols: list[str],
    atm_range: float,
    now_ms: int,
    margin_type: str = "coin",   # 新增
) -> list[Triplet]:
    margin_label = "USD" if margin_type == "coin" else "USDT"
    # 期货过滤：margin == margin_label
    for fut in futures:
        parts = fut["instId"].split("-")
        if len(parts) != 3:
            continue
        sym, margin, exp = parts
        if sym not in symbols or margin != margin_label:
            continue
        future_index.setdefault(sym, {})[exp] = fut["instId"]
    # ...
    # warning 日志动态化（保留完整原文）：
    logger.warning(
        "[instruments] No %s future found for %s expiry %s — dropping all triplets for this expiry",
        margin_label, sym, exp,
    )
```

---

## 7. exchanges/okx.py

将现有 `main.py` 中 OKX 相关逻辑完整迁移，封装为 `OKXRunner` 类：

```python
class OKXRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        async with OKXRestClient(...) as rest:
            # 打印余额
            # fetch fee rates
            # fetch instruments（传入 margin_type=self._ex.margin_type）
            # 打印 triplet 摘要
            # spot_price = atm_prices[sym] if margin_type == "coin" else 1.0
            # 启动 WebSocket 循环
```

**`spot_price` 规则：**
- `margin_type == "coin"`：`spot_price = atm_prices[t.symbol]`（期权价格是币，需 × spot）
- `margin_type == "usdt"`：`spot_price = 1.0`（期权价格已是 USDT，无需换算）

**拉期货的 `uly`：**
- `margin_type == "coin"`：`uly = f"{sym}-USD"`
- `margin_type == "usdt"`：`uly = f"{sym}-USDT"`

**`_print_triplet_summary` 中的期货识别：**

`margin_label`（`"USD"` 或 `"USDT"`）从 `ex_cfg.margin_type` 派生后传入，替换原来的两处硬编码：

```python
def _print_triplet_summary(triplets, all_options, all_futures, symbols, margin_label: str) -> None:
    # 期货识别：使用 margin_label 而非硬编码 "USD"
    if len(parts) == 3 and parts[1] == margin_label and parts[0] in symbols:
        fut_expiries[parts[0]].add(parts[2])
    # ...
    # 标题行也动态化：
    print(f"  {sym}  (期权到期日共 {len(all_exp)} 个, {margin_label} 期货 {len(fut_expiries[sym])} 个)")
```

**`fee_fetcher.py` 的 `uly` 问题：**

`fee_fetcher.py` 目前调用 `client.get_fee_rates("OPTION", uly="BTC-USD")`，在 OKX 中期权费率是账户等级决定的，与 uly 无关，`BTC-USD` 和 `BTC-USDT` 均返回相同结果。故 `fee_fetcher.py` 无需修改，`margin_type` 不影响费率查询。

---

## 8. exchanges/binance.py

### 8.1 合约格式

| 类型 | 格式 | 示例 |
|------|------|------|
| 期权 | `{SYM}-{YYMMDD}-{STRIKE}-{C/P}` | `BTC-260627-70000-C` |
| 期货（U本位） | `{SYM}USDT_{YYMMDD}` | `BTCUSDT_260627` |
| 期货（币本位） | `{SYM}USD_{YYMMDD}` | `BTCUSD_260627` |

### 8.2 REST 接口

- 期权合约列表：`GET /eapi/v1/exchangeInfo`（返回所有期权，按 `underlying` 过滤）
- 期权费率：`GET /eapi/v1/account`（返回 `optionCommissionRate`）
- 期货合约列表：U本位 `GET /fapi/v1/exchangeInfo`，币本位 `GET /dapi/v1/exchangeInfo`
- 期货费率：`GET /fapi/v2/account` 或 `GET /dapi/v1/account`
- 现货价格：`GET /api/v3/ticker/price?symbol=BTCUSDT`

### 8.3 字段映射与归一化

**`instruments.py` 要求的统一格式（见 Section 6）：**
- 期权 `instId` 必须是 5-part：`BTC-USD-260627-70000-C`
- 期货 `instId` 必须是 3-part：`BTC-USD-260627` 或 `BTC-USDT-260627`

Binance 原始格式与统一格式的差异：

| 类型 | Binance 原始 | 归一化后 |
|------|-------------|---------|
| 期权 | `BTC-260627-70000-C`（4-part） | `BTC-USD-260627-70000-C`（插入 settle 段）|
| 期货（U本位） | `BTCUSDT_260627`（`_` 分隔） | `BTC-USDT-260627`（拆分重组）|
| 期货（币本位） | `BTCUSD_260627`（`_` 分隔） | `BTC-USD-260627`（拆分重组）|

`BinanceRunner.fetch_instruments()` 归一化逻辑：

```python
# margin_type == "usdt" 时 settle = "USDT"，否则 "USD"
settle = "USDT" if self._ex.margin_type == "usdt" else "USD"

# 期权归一化
normalized_options = []
for s in raw_options:
    sym = s["underlying"].replace("USDT", "").replace("USD", "")  # "BTCUSDT" → "BTC", "ETHUSDT" → "ETH"
    raw_id = s["symbol"]               # "BTC-260627-70000-C"
    parts = raw_id.split("-")          # ["BTC", "260627", "70000", "C"]
    norm_id = f"{sym}-{settle}-{parts[1]}-{parts[2]}-{parts[3]}"  # sym 替换 parts[0]，语义更清晰
    normalized_options.append({
        "instId": norm_id,             # "BTC-USD-260627-70000-C"
        "stk": s["strikePrice"],       # 注意：strike 来自 dict 字段，instruments.py 不从 instId 解析 strike
        "optType": "C" if s["side"] == "CALL" else "P",
        "expTime": str(s["expiryDate"]),
    })

# 期货归一化（以 U本位为例：BTCUSDT_260627 → BTC-USDT-260627）
normalized_futures = []
for f in raw_futures:
    raw_id = f["symbol"]               # "BTCUSDT_260627"
    sym_part, exp = raw_id.split("_") # ["BTCUSDT", "260627"]
    sym = sym_part.replace("USDT", "").replace("USD", "")  # "BTC"
    normalized_futures.append({
        "instId": f"{sym}-{settle}-{exp}",  # "BTC-USDT-260627"
    })
```

**注意：** `BookSnapshot` 中存储的 `inst_id` 使用归一化后的格式，与 `Triplet` 字段一致。WebSocket 消息里原始的 Binance `instId` 需要同样归一化后再做 `market.update()`。

### 8.4 WebSocket

Binance 期权 WebSocket：`wss://nbstream.binance.com/eoptions/stream`

订阅深度：`{symbol}@depth5`，例如：`BTC-260627-70000-C@depth5`

消息格式：
```json
{
  "e": "depth",
  "s": "BTC-260627-70000-C",
  "b": [["70000", "10"]],
  "a": [["70100", "5"]],
  "T": 1700000000000
}
```

字段映射到 `BookSnapshot`（只存价格，无数量字段）：`b[0][0]` → `bid`（价格），`a[0][0]` → `ask`（价格），`T` → `ts`。数量字段 `b[0][1]`、`a[0][1]` 忽略。

期货 WebSocket（U本位）：`wss://fstream.binance.com/stream`，订阅 `btcusdt_260627@depth5`。

### 8.5 BinanceRunner 结构

```python
class BinanceRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None: ...

    async def run(self) -> None:
        # 1. fetch fee rates (REST)
        # 2. fetch instruments → normalize → build_triplets
        # 3. 打印 triplet 摘要
        # 4. 订阅 WebSocket（期权 + 期货分两个连接）
        # 5. on_message → BookSnapshot → pcp_calculator
```

---

## 9. main.py 精简后

```python
RUNNERS = {
    "okx": OKXRunner,
    "binance": BinanceRunner,
}

async def _run(cfg_path: str = "config.yaml") -> None:
    cfg = load_config(cfg_path)
    tasks = []
    for name, ex_cfg in cfg.exchanges.items():
        if not ex_cfg.enabled:
            continue
        runner_cls = RUNNERS.get(name)
        if runner_cls is None:
            logger.error("[main] Unknown exchange: %s", name)
            continue
        tasks.append(asyncio.create_task(runner_cls(ex_cfg, cfg).run()))
    if not tasks:
        logger.error("[main] No enabled exchanges found in config")
        return
    await asyncio.gather(*tasks)

def main() -> None:
    asyncio.run(_run())
```

---

## 10. 测试策略

| 模块 | 测试方式 |
|------|----------|
| `config.py` | 2 个新测试：验证 `ExchangeConfig` 解析、`margin_type` 默认值 |
| `instruments.py` | 现有 8 个测试加 `margin_type` 参数，新增 2 个测试验证 `usdt` 模式过滤 |
| `exchanges/okx.py` | 无单元测试（网络层，集成测试）|
| `exchanges/binance.py` | 1 个测试验证字段 normalize 逻辑 |
| `main.py` | 现有测试调整为新 `AppConfig` 结构 |

---

## 11. 不在本次范围内

- Binance 账户余额打印（暂不做，REST 接口格式差异大）
- Binance paper trading 模式
- 跨交易所套利信号对比
- ccxt.pro 适配器层（另有独立 spec）
