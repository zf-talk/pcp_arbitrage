# PCP 套利监听器

针对加密货币衍生品市场的实时 Put-Call Parity（PCP）套利信号监听工具，支持 OKX、Binance 和 Deribit。

[English](README.md)

## 文档

- **[docs/README.md](docs/README.md)** — 文档索引：**上手请读根目录本文件 + `docs/` 下主文档**；`docs/superpowers/` 为可选存档，不必进入即可掌握项目。

## 工作原理

看跌-看涨平价公式（欧式期权）：

```
C - P = F - K · e^(-rT)
```

当市场价格偏离平价关系时，存在无风险套利机会。本工具实时监控期权和交割合约订单簿，当年化收益超过配置阈值时打印信号。

**正向套利** — 买 call、卖 put、卖交割合约：

```
gross = put_bid + future_bid − call_ask − K
```

**反向套利** — 卖 call、买 put、买交割合约：

```
gross = call_bid + K − put_ask − future_ask
```

### 币本位 vs. U本位

两种结算方式的核心区别：期权报价单位不同，利润是否真正锁定为 USDT 也不同。

#### U本位（Binance）

期权和交割合约全部以 USDT 报价，无需换算（`spot_price = 1.0`）。

| 场景 | BTC 涨到 110,000 | BTC 跌到 90,000 |
|------|----------------|----------------|
| **正向**（买 C、卖 P、卖 F） | Call 行权抵消交割合约亏损，Put 作废保留权利金，净利润锁定为 USDT。 | Call 作废，Put 被行权，交割合约盈利对冲，净利润锁定为 USDT。 |
| **反向**（卖 C、买 P、买 F） | Call 被行权，交割合约盈利对冲，Put 作废，净利润锁定为 USDT。 | Put 行权抵消交割合约亏损，Call 作废保留权利金，净利润锁定为 USDT。 |

无论行情涨跌，利润完全以 USDT 锁定。✅

#### 币本位（OKX、Deribit）

期权以 BTC 报价（如 0.002 BTC），计算时按当前现货价换算为 USDT：

```
call_ask_usdt = call_ask_btc × spot
put_bid_usdt  = put_bid_btc  × spot
gross（USDT） = put_bid_usdt + future_bid − call_ask_usdt − K
```

`gross` 以 USDT 计算，但实际到期结算收到的是 BTC，因此存在残余现货敞口：

| 场景 | BTC 涨到 110,000 | BTC 跌到 90,000 |
|------|----------------|----------------|
| **正向**（买 C、卖 P、卖 F） | Call 以 BTC 行权，交割合约亏损以 USDT 计。收到的 BTC 价值更高 → 实际 USDT 收益超出预期。 | Put 以 BTC 被行权，交割合约盈利以 USDT 计。支付的 BTC 价值更低 → 实际 USDT 收益超出预期。 |
| **反向**（卖 C、买 P、买 F） | Call 被行权须交割 BTC，交割合约盈利以 USDT 计。支付的 BTC 价值更高 → 实际 USDT 收益低于预期。 | Put 以 BTC 行权，交割合约亏损以 USDT 计。收到的 BTC 价值更低 → 实际 USDT 收益低于预期。 |

信号在建仓时按当前现货价计算。若到期前现货价大幅波动，实际利润将偏离预期。若要真正锁定 USDT 利润，需在建仓时同步在现货市场对冲残余 BTC 敞口。

## 手续费（模型说明）

手续费在 `pcp_arbitrage/pcp_calculator.py` 中按**统一近似公式**计算，并非逐家交易所逐条规则复刻。`option_rate`、`future_rate` 在能拉到账户费率时来自各所 REST（拉取失败则用代码内默认值）。

### 单次成交、每条腿

**期权（Call 或 Put 各算一条）** — 两式取较小值（标的「名义 × 费率」 vs 权利金上限）：

```
fee_opt = min( lot_size × index_usdt × option_rate ,  option_price × lot_size × 0.125 )
```

- `option_price` 为 **USDT**（币本位期权：币价 × 现货）。
- `index_usdt` 为 **标的指数 / 现货 USDT 价**：币本位与计算器里 `spot_price` 一致；U 本位盘口若用 `spot_price=1` 换算，Runner 会额外传入 `index_for_fee`，避免「名义」一支几乎为 0。
- **Deribit（币本位期权，与 [费率说明](https://www.deribit.com/kb/fees) 一致）：** 单张合约手续费 = **`MIN( 标的侧一支 , 0.125 × 期权价格 ) × 张数`**（权利金侧上限为期权价的 **12.5%**；标的侧常见表述为名义的 **0.03%** 等，以账户费率为准）。**BTC 示例：** 成交价 **0.008 BTC**、数量 **3** → `MIN(0.0003, 0.125×0.008) × 3 = MIN(0.0003, 0.001) × 3 = 0.0009 BTC`（**0.0003** 为文档中与 0.03% 档位对应的示例量级）。本仓库公式 `fee_opt` 将同一 **MIN** 结构换到 **USDT**，再乘 `lot_size`。

**交割合约**

```
fee_fut = future_price × lot_size × future_rate
```

- `future_price` 为对应方向的盘口价（与 PCP 里用的 `bid` / `ask` 一致），单位为 **USDT / USD**（与毛利公式一致）。

### 开平双边（信号里打印的 `total_fee`）

模型假设三条腿各成交一次开仓、再一次平仓，因此总手续费为：

```
total_fee = 2 × ( fee_call + fee_put + fee_fut )
净利润    = gross − total_fee
```

终端里「进出场双边 × 3 腿」对应这里的 **×2**（开仓 + 平仓），三条腿的费用都计入。

### 数值示例（数量级）

设 `lot_size = 0.01` BTC，`index_usdt ≈ 66_900`，`option_rate = 0.0003`，`future_rate = 0.0005`，正向信号里 `future_bid = 68_617.5` USDT：

- **期权（单腿、单次）：** `fee_by_face = 0.01 × 66_900 × 0.0003 ≈ 0.20` USDT；与权利金 cap 取 `min` 后，每条期权腿常在 **约 0.2 USDT** 量级（**不是**接近 0）。
- **交割合约：** `fee_fut ≈ 68_617.5 × 0.01 × 0.0005 ≈ 0.343` USDT（单次）。
- **合计（示意）：** `total_fee ≈ 2 × (0.20 + 0.20 + 0.343) ≈ 1.49` USDT（随报价与 `min` 哪侧更紧而变）。

## 支持交易所

| 交易所   | 期权类型 | 期权结算 | 交割合约结算 | 标的 | 状态 |
|---------|---------|---------|---------|------|------|
| OKX     | 欧式期权 | 币本位（BTC / ETH） | 币本位（BTC / ETH） | BTC、ETH | ✅ 已实现 |
| Binance | 欧式期权 | USDT 结算 | USDT 结算 | BTC、ETH | ✅ 已实现 |
| Deribit（Inverse） | 欧式期权 | 币本位（BTC / ETH） | 币本位（BTC / ETH） | BTC、ETH | ✅ 已实现 |
| Deribit（Linear）  | 欧式期权 | USDC 结算 | USDC 结算 | BTC、ETH | ✅ 已实现 |

> **说明：** Deribit Inverse 和 Linear 在 `config.yaml` 中作为两个独立交易所配置（`deribit` 和 `deribit_linear`），共用同一套 API 密钥。

## 安装

**依赖：** Python 3.12+，[uv](https://github.com/astral-sh/uv)

```bash
# 安装依赖
uv sync

# 配置 API 密钥
cp config.yaml.example config.yaml   # 然后填入你的密钥
```

**`config.yaml` 结构：**

```yaml
exchanges:
  okx:
    enabled: true
    api_key: "..."
    secret_key: "..."
    passphrase: "..."        # 仅 OKX 使用

  binance:
    enabled: false
    api_key: "..."
    secret_key: "..."

  deribit:
    enabled: false
    api_key: "..."           # client_id
    secret_key: "..."        # client_secret

  deribit_linear:
    enabled: false
    api_key: "..."           # 与 deribit 相同的 client_id
    secret_key: "..."        # 与 deribit 相同的 client_secret

arbitrage:
  min_annualized_rate: 0.010  # 最小年化收益率，低于此不打印
  atm_range: 0.20             # 平值附近范围 ±20%
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000
```

### 标的（`symbols`）

`arbitrage.symbols` 为要监控的标的列表；各 Runner 只处理**该交易所支持**的交集：OKX、Deribit（币本位）为 **BTC、ETH**；**Binance** 另支持 **BNB、SOL、XRP、DOGE**；**Deribit Linear** 另支持 **AVAX、SOL、TRX、XRP**。

**合约数量（lot_size）** 默认在代码中 `DEFAULT_LOT_SIZES`（`config.py`）定义；仅在需要覆盖某标的时再在 YAML 写 `contracts.lot_size`。

默认 **`dashboard_quiet_exchanges: true`** 会把 `pcp_arbitrage.exchanges` 与 `okx_client` 的日志降到 WARNING，减轻仪表盘场景下的 **stderr** 刷屏；需要调试时可设为 `false`，或在 `logging.levels` 里单独指定级别（会覆盖该默认）。

## 运行

```bash
uv run python -m pcp_arbitrage.main
```

## 开发

```bash
# 运行测试
uv run pytest tests/ -v

# 代码检查
uv run ruff check src/ tests/
```

## 架构

```
main.py                     ← 多交易所调度器
exchanges/
  okx.py                    ← OKXRunner（REST 初始化 + WebSocket）
  binance.py                ← BinanceRunner（eapi + fapi + WS）
  deribit.py                ← DeribitRunner（Inverse，币本位）
                               DeribitLinearRunner（Linear，USDC 结算）
instruments.py              ← build_triplets() — 期权/交割合约配对
pcp_calculator.py           ← 正向/反向套利信号计算
signal_printer.py           ← classic 模式多行格式化
signal_output.py            ← classic / dashboard 路由
opportunity_dashboard.py    ← Rich Live + Table 动态表格
market_data.py              ← 内存订单簿存储
models.py                   ← BookSnapshot, FeeRates, Triplet
config.py                   ← AppConfig / ExchangeConfig 加载
exchange_symbols.py         ← 各所支持的标的
```

每个 Runner 遵循相同的生命周期：
1. **REST 初始化** — 获取手续费率、合约列表、现货价格，构建 triplets
2. **WebSocket** — 订阅所有 triplet 订单簿，每次 tick 计算 PCP 套利信号

## 后续计划

- [ ] **自动入场** — 检测到信号后自动下单
- [ ] **持续监控** — 实时追踪持仓状态和盈亏
- [ ] **止盈出场** — 达到目标收益后自动平仓
- [ ] **消息通知** — 信号触发或仓位变动时通过 Telegram / 邮件推送
- [ ] **网页展示** — 实时信号流、下单历史记录、持仓概览

## 参考文档

- [Deribit — Inverse Options（币本位期权）](https://support.deribit.com/hc/en-us/articles/31424939096093-Inverse-Options)
- [Deribit — Linear (USDC) Options（USDC 期权）](https://support.deribit.com/hc/en-us/articles/31424932728093-Linear-USDC-Options)
- [Deribit — Inverse Futures（币本位交割合约）](https://support.deribit.com/hc/en-us/articles/31424938981533-Inverse-Futures)
- [Deribit — Linear Futures（USDC 交割合约）](https://support.deribit.com/hc/en-us/articles/31424954805405-Linear-Futures)
