# PCP 套利监控系统设计文档

**日期：** 2026-04-04
**版本：** v1.1
**语言：** Python（uv 管理）
**范围：** BTC、ETH，仅监控输出，不下单

---

## 1. 背景与目标

基于 Put-Call Parity（PCP）平价关系，在 OKX 上监控 BTC/ETH 期权的套利机会。

**PCP 公式：**
- 正向套利（买 Call + 卖 Put + 卖 Future）：`put_bid - call_ask + future_bid - K`
- 反向套利（卖 Call + 买 Put + 买 Future）：`call_bid - put_ask + K - future_ask`

**关键约束：**
- 标的腿必须使用 **USDT 本位交割期货**（非现货、非币本位合约），避免币本位合约非线性 P&L 导致 Delta 风险
- Call 与 Put 须同一行权价 K、同一到期日 T
- Future 只需同一到期日 T（无行权价）
- 第一版仅控制台输出，不执行下单

---

## 2. 整体架构

### 2.1 目录结构

```
pcp_arbitrage/
├── Makefile
├── pyproject.toml
├── config.yaml
└── src/
    └── pcp_arbitrage/
        ├── main.py              # 入口：初始化 + 启动 asyncio 事件循环
        ├── config.py            # 加载 config.yaml，暴露全局配置对象
        ├── okx_client.py        # OKX REST + WebSocket 封装
        ├── market_data.py       # 内存 order book 状态管理
        ├── instruments.py       # 合约发现与过滤
        ├── fee_fetcher.py       # 启动时获取账户手续费等级
        ├── pcp_calculator.py    # PCP 套利计算（正向/反向）
        └── signal_printer.py    # 套利信号格式化输出到控制台
```

### 2.2 架构方案

**单进程事件驱动（asyncio）**，原因：
- PCP 套利核心竞争力是延迟，单进程 asyncio 延迟最低
- PCP 计算量极轻，不会阻塞事件循环
- 结构简单，易于调试；后期加 Redis/DB 只需在输出层插入

### 2.3 数据流

```
启动
 ├─ REST: 获取账户手续费等级 → FeeRates 对象存入内存
 ├─ REST: 拉取 BTC/ETH 期权合约列表（instType=OPTION）
 ├─ REST: 拉取 BTC/ETH USDT 本位交割期货列表（instType=FUTURES）
 ├─ REST: 获取当前 BTC/ETH 价格（用于 ATM ±20% 过滤）
 └─ 构建订阅列表：三元组 (call, put, future) per (symbol, expiry, strike)
     （若某到期日无对应 USDT 本位期货，该到期日所有三元组静默丢弃并打印警告）

运行
 └─ WebSocket 推送 books5
     └─ 更新内存 order book（含时间戳）
         └─ 触发 PCP 计算（含数据完整性检查）
             └─ 满足条件 → 控制台输出
```

---

## 3. 合约发现与过滤

### 3.1 合约三元组

每个套利机会对应一个三元组：

```
(Call[K, T], Put[K, T], Future[T])
```

- Call 和 Put：同行权价 K、同到期日 T
- Future：同到期日 T，USDT 本位交割合约
- **若某到期日在 OKX 无对应 USDT 本位交割期货**：该到期日的所有期权三元组静默丢弃，启动时打印一条警告日志

### 3.2 过滤规则

- **标的：** BTC、ETH
- **到期日：** 所有在市合约，**当日到期合约（days_to_expiry < 1）排除**（避免年化计算除零）
- **行权价：** ATM ±20% 范围内（ATM 基准为启动时现货价格）

---

## 4. 内存数据结构

```python
from dataclasses import dataclass

@dataclass
class BookSnapshot:
    bid: float
    ask: float
    ts: int   # 毫秒时间戳，来自 WebSocket 推送

@dataclass
class FeeRates:
    option_rate: float   # 期权费率，单位 USDT/币（如 0.0002）
    future_rate: float   # 期货费率，百分比（如 0.0005）

@dataclass
class Triplet:
    symbol: str          # "BTC" or "ETH"
    expiry: str          # "250425"
    strike: float        # 行权价，USDT
    call_id: str         # order_books 中的 key
    put_id: str
    future_id: str

# 全局内存状态（由 market_data.py 管理）
order_books: dict[str, BookSnapshot] = {}

# 全局费率（由 fee_fetcher.py 在启动时填充，传入 calculator）
fee_rates: FeeRates

# 合约三元组列表（由 instruments.py 在启动时构建）
triplets: list[Triplet] = []
```

**示例：**
```python
order_books = {
    "BTC-USD-250425-100000-C": BookSnapshot(bid=1250.0, ask=1300.0, ts=1743000000000),
    "BTC-USD-250425-100000-P": BookSnapshot(bid=850.0,  ask=900.0,  ts=1743000000000),
    "BTC-USD-250425":          BookSnapshot(bid=99500.0,ask=99600.0,ts=1743000000000),
}
```

---

## 5. WebSocket 订阅

- **频道：** `books5`（5 档盘口，低延迟）
- **触发：** 任意一腿更新 → 取三腿最新缓存 → 执行数据完整性检查 → 计算 PCP
- **连接管理：** 单连接最多 240 个频道，超出自动分多连接
- **心跳：** 每 20 秒发送 `ping`，超时未收到 `pong` 视为断线

### 5.1 断线重连策略

- 断线后使用**指数退避**重连（初始 1 秒，最大 60 秒）
- 重连成功后：
  1. 重新发送完整订阅列表
  2. 清空断线期间的所有 `order_books` 缓存（防止使用过期数据触发虚假套利信号）
  3. 打印重连成功日志

### 5.2 数据完整性检查

触发���算前，需满足以下条件，否则跳过：
1. 三腿均已存在于 `order_books`（防止 KeyError）
2. 三腿的 `ts` 均不超过 5 秒（防止使用过期快照）
3. 三腿的 `bid > 0` 且 `ask > 0`（防止空盘口）

---

## 6. 手续费计算

`fee_rates` 由 `fee_fetcher.py` 在启动时通过 REST API 获取，存为 `FeeRates` 对象，在整个生命周期内传入 `pcp_calculator.py`。

### 6.1 期权腿手续费（单腿，单次进/出场）

```
fee_by_face_value = lot_size × fee_rate_option
fee_cap           = option_price × lot_size × 0.125
option_fee        = min(fee_by_face_value, fee_cap)
```

- `lot_size`：BTC=0.01，ETH=0.1（合约面值，单位为币）
- `fee_rate_option`：账户期权费率（USDT/币），来自 `FeeRates.option_rate`
- `option_price`：**买入时取 ask，卖出时取 bid**（具体方向见下表）

| 套利方向 | Call 用价 | Put 用价 |
|---------|-----------|---------|
| 正向    | ask（买入）| bid（卖出）|
| 反向    | bid（卖出）| ask（买入）|

### 6.2 期货腿手续费（单腿，单次进/出场）

```
future_fee = future_price × lot_size × fee_rate_future
```

- `lot_size`：同期权，BTC=0.01，ETH=0.1（OKX USDT 本位期货合约面值）
- `future_price`：**正向卖出取 bid，反向买入取 ask**
- `fee_rate_future`：来自 `FeeRates.future_rate`

### 6.3 总手续费（进出场双边，共 6 次）

```
total_fee = 2 × (call_fee + put_fee + future_fee)
```

> 行权费暂不计入（第一版不持有到期）

---

## 7. PCP 套利计算

### 7.1 正向套利（买 Call + 卖 Put + 卖 Future）

```
gross_profit = put_bid - call_ask + future_bid - K
call_fee     = min(lot_size × option_rate, call_ask × lot_size × 0.125)
put_fee      = min(lot_size × option_rate, put_bid  × lot_size × 0.125)
future_fee   = future_bid × lot_size × future_rate
total_fee    = 2 × (call_fee + put_fee + future_fee)
net_profit   = gross_profit - total_fee
```

### 7.2 反向套利（卖 Call + 买 Put + 买 Future）

```
gross_profit = call_bid - put_ask + K - future_ask
call_fee     = min(lot_size × option_rate, call_bid  × lot_size × 0.125)
put_fee      = min(lot_size × option_rate, put_ask   × lot_size × 0.125)
future_fee   = future_ask × lot_size × future_rate
total_fee    = 2 × (call_fee + put_fee + future_fee)
net_profit   = gross_profit - total_fee
```

### 7.3 年化收益率

```
days_to_expiry = (expiry_timestamp - now_timestamp) / 86400  # 秒转天，可为小数
annualized_return = (net_profit / K) × (365 / days_to_expiry) × 100%
```

> 注：以 K 作为本金基准（v1 简化方案，后续优化为实际占用资金）
> `days_to_expiry < 1` 的合约在启动时已过滤，无除零风险

### 7.4 输出条件

- `net_profit > 0`
- `annualized_return > min_annualized_rate`（默认 10%，配置文件可调）

---

## 8. 配置文件（config.yaml）

```yaml
okx:
  api_key: ""
  secret_key: ""
  passphrase: ""
  is_paper_trading: false

arbitrage:
  min_annualized_rate: 0.10   # 年化收益率阈值
  atm_range: 0.20             # ATM ±20% 行权价过滤范围
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000    # order book 数据超过此毫秒数视为过期

contracts:
  lot_size:
    BTC: 0.01
    ETH: 0.1
```

---

## 9. 控制台输出格式

输出标注说明：`← 卖一` 表示使用订单簿卖方最优报价（ask），`← 买一` 表示使用订单簿买方最优报价（bid）。

**正向套利示例**（数值为自洽示例）：
```
[2026-04-04 12:00:01.234] [正向套利] BTC-250425-95000
  期权组合  : 买 Call + 卖 Put + 卖 Future
  call_ask  :  200.00 USDT  ← 卖一
  put_bid   : 5600.00 USDT  ← 买一
  future_bid: 89630.00 USDT ← 买一
  K         : 95000.00 USDT
  手续费    :    2.50 USDT  (进出场双边 × 3腿)
  净利润    :   27.50 USDT  (5600.00 - 200.00 + 89630.00 - 95000.00 - 2.50)
  年化收益  :    0.50%      (27.50 / 95000 × 365 / 21 × 100)
────────────────────────────────────────────────────
```

**反向套利示例**：
```
[2026-04-04 12:00:01.456] [反向套利] ETH-250425-2000
  期权组合  : 卖 Call + 买 Put + 买 Future
  call_bid  :  120.00 USDT  ← 买一
  put_ask   :   80.00 USDT  ← 卖一
  future_ask: 1958.00 USDT  ← 卖一
  K         : 2000.00 USDT
  手续费    :    0.80 USDT  (进出场双边 × 3腿)
  净利润    :   81.20 USDT  (120.00 - 80.00 + 2000.00 - 1958.00 - 0.80)
  年化收益  :   70.85%      (81.20 / 2000 × 365 / 21 × 100)
────────────────────────────────────────────────────
```

---

## 10. Makefile 指令

| 指令 | 说明 |
|------|------|
| `make install` | `uv sync` 安装依赖 |
| `make run` | 启动监控（实盘） |
| `make dev` | 启动监控（模拟盘） |
| `make lint` | `ruff check` |
| `make fmt` | `ruff format` |
| `make clean` | 清理 `__pycache__` 等临时文件 |

---

## 11. 启动流程

```
1. 加载 config.yaml
2. REST 初始化：
   a. 获取账户手续费等级 → 构建 FeeRates 对象
   b. 拉取所有 BTC/ETH 期权合约（instType=OPTION）
   c. 拉取所有 BTC/ETH USDT 本位交割期货（instType=FUTURES）
   d. 获取当前 BTC/ETH 现货价格（ATM 基准）
   e. 过滤：行权价在 ATM ±20% 范围内，days_to_expiry ≥ 1
   f. 匹配期权到期日与期货到期日，无匹配者打印警告并丢弃
   g. 构建三元组列表 (call, put, future)
3. 启动 WebSocket，订阅所有合约的 books5 频道
4. 事件循环运行：推送 → 更新内存 → 完整性检查 → 计算 → 输出
```

---

## 12. 后期扩展方向（不在 v1 范围）

- 数据库持久化套利信号记录
- Redis 缓存 order book（支持多进程/多策略）
- 自动下单执行（三腿市价单）
- 保证金动态计算，优化年化基准
- 行权费计入（持有到期场景）
