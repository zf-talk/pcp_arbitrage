# 下单与撤单流程

本文档与当前实现（`order_manager.py`、`signal_output.py`、`position_tracker.py`）对齐；若与代码冲突，以代码为准。

---

## 调用入口

```
submit_entry(triplet, signal, signal_id, cfg, sqlite_path)
  ├─ exchange == okx     → _submit_entry_inner()
  └─ exchange == deribit → _submit_entry_deribit_inner()

submit_exit(position, cfg, sqlite_path)
  ├─ exchange == okx     → _submit_exit_inner()
  └─ exchange == deribit → _submit_exit_deribit_inner()
```

**谁调用 `submit_entry`**

| 来源 | 行为 |
|------|------|
| **自动** | `signal_output.emit_opportunity_evaluation`：年化 ≥ `order_min_annualized_rate` 且本激活周期未通知过时，先发 Telegram / Web 推送，再 `asyncio.create_task(submit_entry(...))`。**仅当** 配置中存在该交易所（**键名大小写不敏感**，如 `OKX`/`okx`）且 `ExchangeConfig.arbitrage_enabled == true` 时才会真正下单；否则打日志并跳过自动下单。 |
| **Web 仪表盘** | 用户点击下单 → `web_dashboard` 直接调用 `submit_entry`（**不经过** `arbitrage_enabled` 门闸，由操作者负责）。 |

---

## 入场流程（OKX）

限价与数量来自 **`ArbitrageSignal` / 三腿价格**（信号计算时的盘口快照），**不在**提交前再次拉盘口（若需对齐最新盘口，见 `docs/TODO.md`）。

```
_submit_entry_inner()
│
├─ 1. 前置校验
│     ├─ 按 lot_size 向下取整 qty；qty <= 0 → 抛错，外层返回失败
│     ├─ _check_and_cap_qty()：按账户余额约束截断 qty
│     │     ├─ adj_eq ≤ total_eq × entry_reserve_pct → 返回 0（跳过开仓）
│     │     └─ 可用上限：min(total_eq × entry_max_trade_pct, adj_eq - reserve_floor)
│     └─ blocking_entry_status()：同一 (exchange, symbol, expiry, strike, direction)
│           若已有 status ∈ {open, opening, closing} → 跳过（含「已有持仓 / 正在开仓 / 正在平仓」）
│
├─ 2. 写 DB（positions）
│     └─ INSERT positions，status='opening'，返回 position_id
│
├─ 3. 确定三腿方向与限价（来自 signal）
│     正向：buy call / sell put / sell future
│     反向：sell call / buy put / buy future
│
├─ 4. 写 DB（orders × 3）
│     └─ INSERT orders，action='open'，order_type='limit'，status='pending'
│
├─ 5. 并发提交三腿限价单（asyncio.gather，超时 30s）
│     ├─ OKX 响应校验：code≠"0" 时抛出（含 sCode/sMsg）；请求/响应可打 INFO 日志
│     ├─ 超时 → _mark_partial_failed()：失败腿 orders.last_error，仓位 partial_failed，撤已挂单
│     └─ 任一腿报错 → 同上（per-order 原因写入 orders.last_error）
│
├─ 6. 存交易所 order_id 到 DB
│
├─ 7. 并发轮询三腿成交（每 2s，单腿最多约 30s）
│     └─ filled order dict 或 None（超时）
│
├─ 8. 写 DB 结果
│     ├─ 全部成交 → orders status='filled'（含 filled_px、fee 等）
│     │              positions status='open'（由 opening 更新）
│     └─ 部分/超时 → 已成交腿 filled，未成交腿 failed + orders.last_error
│                    positions status='partial_failed'
│
├─ 9. 撤未成交单（best-effort，新建 session）
│
└─ 10. Telegram 通知（成功 / 失败）
```

## 入场流程（Deribit）

与 OKX 结构相同，差异：

| 项目 | OKX | Deribit |
|------|-----|---------|
| HTTP | `aiohttp.ClientSession` | `DeribitRestClient`（OAuth） |
| 期货数量 | 与期权同「张」口径（由 signal 等决定） | 期货腿按 USD 合约面值换算 `amount` |
| 撤单 | `_cancel_order()` | `dclient.cancel_order()` |
| fee_type | 订单回包推断 maker/taker | 成交后 `get_trades_by_order`，liquidity M/T |

---

## 出场流程（OKX：`_submit_exit_inner`）

```
_submit_exit_inner()
│
├─ 1. 从 DB 读入场单 action='open'，构建 leg → order 映射（含 inst_id）
│
├─ 2. 校验入场单
│     ├─ 不足 3 条 → 打错误日志，return
│     └─ 任一脚 status≠'filled' → 未成交腿标 failed + orders.last_error，仓位 partial_failed，return
│
├─ 3. OKX 交易所持仓核对（/api/v5/account/positions）
│     └─ 三腿张数/方向须与 DB 开仓一致；否则仅对开仓 orders 写入 last_error，仓位 partial_failed，return（不提交平仓单）
│
├─ 4. 并发拉三腿盘口（/api/v5/market/books）
│     └─ 失败或无价 → 用该腿入场 limit_px 兜底
│
├─ 5. 出场方向与价格（被动价：挂在己方一侧，偏 maker）
│     正向入场（曾 buy call / sell put / sell fut）→ 平仓：
│           sell call @ ask，buy put @ bid，buy future @ bid
│     反向入场 → 平仓：
│           buy call @ bid，sell put @ ask，sell future @ ask
│
├─ 6. 写 DB：INSERT orders ×3，action='close'，status='pending'
│
├─ 7. 并发提交三腿限价（超时 30s）
│     └─ 超时/报错 → _update_exit_failed()：平仓单 failed + orders.last_error，仓位 partial_failed，Telegram，return
│
├─ 8. 存 exchange_order_id，轮询成交
│
├─ 9. 逐腿更新 orders；累计 realized_pnl（entry 用 filled_px，缺省用 limit_px）
│
├─ 10. 若未全成 → _escalating_exit_loop_okx()
│      ├─ 取消旧单，按盘口重挂；failed_legs 带 entry_px 供 PnL
│      ├─ 先 maker 侧追价；maker_chase_secs 超时后切 taker（总时长受 maker_chase_max_minutes 限制）
│      └─ 仍失败 → partial_failed，失败平仓单 orders.last_error
│
└─ 11. 全成 → positions closed + realized_pnl_usdt；否则 Telegram 失败通知
```

## 出场流程（Deribit：`_submit_exit_deribit_inner`）

- 从 DB 读开仓单、拉 `get_order_book`、下反向三腿、轮询、更新 DB、失败时 `_update_exit_failed` 等，**与 OKX 目标一致**。
- **差异**：**无** `_escalating_exit_loop_okx` 等价实现；未全成交时由 **`exit_monitor_loop` 再次 `submit_exit`** 等方式兜底（行为以代码为准）。

---

## 自动平仓触发（`position_tracker`）

- **`_check_positions`**：对 `status='open'` 的仓位，在满足 `exit_days_before_expiry` / `exit_target_profit_pct` 等条件时（注：当前 `_calc_pnl_pct` 未接真实浮动盈亏，**止盈比例项实际常为 0**，主要依赖**临近到期**等条件），将仓位标为 **`closing`** 并 `asyncio.create_task(submit_exit(...))`。
- **`exit_monitor_loop`**：扫描 **`partial_failed`**  
  - 若入场单全未成交 → 可将仓位标为 **`failed`**，并给开仓单写 `last_error`（见代码）。  
  - **OKX**：先 **`okx_exit_allowed_by_exchange`**（与交易所三腿持仓一致）再 `submit_exit`；不一致则**跳过**，避免无仓反复平仓。  
  - 跳过已在 `_exit_active` 中的仓位（与内联升级循环互斥）。

---

## 错误信息落库（摘要）

| 场景 | 主要载体 |
|------|----------|
| 入场/平仓单失败、超时、交易所拒单 | **`orders.last_error`**（必要时配合 status=filled/failed） |
| 仓位级 `positions.last_error` | 仍可能存在于旧数据或少数路径；**仪表盘主表以订单子表展示为准** |

---

## 撤单时机

| 触发条件 | 撤单范围 |
|----------|----------|
| 入场提交超时（30s） | 已拿到 order_id 的腿 |
| 任一脚提交报错 | 其余已提交腿 best-effort 撤单 |
| 轮询未全成 | 未成交腿对应挂单 |

撤单失败仅打 warning，不阻塞 DB 状态写入。

---

## DB 状态流转

### positions.status（与 `blocking_entry_status` / 面板一致）

```
opening（已建仓位记录，等待三腿成交）
   → open（三腿均 filled）
   → closing（已触发平仓流程）
   → closed（平仓三腿均成交）

opening / open → partial_failed（入场或平仓未完全成功，可重试或人工处理）
partial_failed → failed（如守护逻辑认定入场全未成交、无需再平）  [见代码分支]
```

### orders.status

```
pending → filled
        → failed（并常配合 orders.last_error）
```

---

## 仓位数量控制（`_check_and_cap_qty`）

开仓前按账户权益约束 qty：

```
reserve_floor = total_eq × entry_reserve_pct
trade 上限预算 = min(total_eq × entry_max_trade_pct, adj_eq - reserve_floor)
max_qty ≈ 预算 / index_price_usdt
qty = floor 到 lot_size
```

---

## 相关文档

- 架构总览：`architecture-and-flow.md`
- 待办与已知差异：`TODO.md`
