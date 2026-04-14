# 下单与撤单流程

## 入口

```
submit_entry(triplet, signal, signal_id, cfg, sqlite_path)
  ├─ exchange == okx   → _submit_entry_inner()
  └─ exchange == deribit → _submit_entry_deribit_inner()

submit_exit(position, cfg, sqlite_path)
  ├─ exchange == okx     → _submit_exit_inner()
  └─ exchange == deribit → _submit_exit_deribit_inner()
```

---

## 入场流程（OKX）

```
_submit_entry_inner()
│
├─ 1. 前置校验
│     ├─ 按 lot_size 向下取整 qty；qty <= 0 → 直接返回失败
│     ├─ _check_and_cap_qty()：按账户余额约束截断 qty
│     │     ├─ paper 模式 → 跳过
│     │     ├─ adj_eq ≤ total_eq × entry_reserve_pct → 返回 0（跳过开仓）
│     │     └─ 上限 = min(total_eq × entry_max_trade_pct, adj_eq - reserve_floor)
│     └─ DB 查重：同一 (exchange, symbol, expiry, strike, direction) 已有 open 仓位 → 跳过
│
├─ 2. 写 DB（positions）
│     └─ INSERT positions，status='open'，返回 position_id
│
├─ 3. 确定三腿方向
│     正向：buy call / sell put / sell future
│     反向：sell call / buy put / buy future
│
├─ 4. 写 DB（orders × 3）
│     └─ INSERT orders，action='open'，order_type='limit'，status='pending'
│
├─ 5. 并发提交三腿限价单（asyncio.gather，超时 30s）
│     ├─ 超时 → _mark_partial_failed()，撤所有已提交单，raise
│     └─ 任一腿报错 → 同上
│
├─ 6. 存交易所 order_id 到 DB
│
├─ 7. 并发轮询三腿成交（每 2s 一次，最多 30s）
│     └─ 返回 filled order dict 或 None（超时）
│
├─ 8. 写 DB 结果
│     ├─ 全部成交 → orders status='filled'（含 filled_px、fee_type）
│     │              positions status 维持 'open'
│     └─ 部分/超时 → 已成交腿 status='filled'，未成交腿 status='failed'
│                    positions status='partial_failed'
│
├─ 9. 撤未成交单（best-effort，新建 session）
│
└─ 10. 发 Telegram 通知
```

## 入场流程（Deribit）

与 OKX 流程相同，差异点：

| 项目 | OKX | Deribit |
|------|-----|---------|
| HTTP 客户端 | `aiohttp.ClientSession` | `DeribitRestClient`（含 OAuth） |
| 期货数量单位 | BTC/ETH | USD 合约面值（BTC=10, ETH=1） |
| 撤单 | `_cancel_order()` | `dclient.cancel_order()` |
| fee_type 来源 | 订单回包 `rebate`/`fee` 字段 | 成交后调 `get_user_trades_by_order`，读 `liquidity: M/T` |

---

## 出场流程（OKX / Deribit）

两套实现结构相同，差异点与入场一致（客户端、撤单方式、fee_type 来源）。

### OKX（`_submit_exit_inner`）

```
_submit_exit_inner()
│
├─ 1. 从 DB 读入场单（action='open'），构建 leg → order 映射
│
├─ 2. 并发拉三腿盘口（/api/v5/market/books，取 top-of-book）
│     └─ 失败时降级：用入场的 limit_px 作为出场价
│
├─ 3. 确定出场价格与方向（入场的反向）
│     正向入场 → sell call at bid / buy put at ask / buy future at ask
│     反向入场 → buy call at ask / sell put at bid / sell future at bid
│
├─ 4. 写 DB（orders × 3）
│     └─ INSERT orders，action='close'，order_type='limit'，status='pending'
│
├─ 5. 并发提交三腿限价单（超时 30s）
│     ├─ 超时 → _update_exit_failed()，发 Telegram 失败通知，return
│     └─ 任一腿报错 → 同上
│
├─ 6. 存交易所 order_id 到 DB
│
├─ 7. 并发轮询三腿成交（每 2s，最多 30s）
│
├─ 8. 全部成交 → 计算 realized_pnl
│     对每腿：exit_px vs entry filled_px（无则用 limit_px）
│     sell 腿：pnl += (exit_px - entry_px) * qty
│     buy  腿：pnl += (entry_px - exit_px) * qty
│
├─ 9. 写 DB 结果
│     ├─ 全部成交 → orders status='filled'（含 fee_type）
│     │              positions status='closed'，记 realized_pnl_usdt
│     └─ 部分/超时 → orders/positions status='partial_failed'
│                    发 Telegram 失败通知
│
├─ 10. 升级重试循环（若有腿 partial_failed）
│      _escalating_exit_loop_okx()
│      ├─ 取消旧单，刷新盘口价格
│      ├─ elapsed < exit_taker_escalate_secs → maker 追单（poll 等待 exit_maker_chase_secs）
│      ├─ elapsed ≥ exit_taker_escalate_secs → taker 强平（无限等待，硬上限 10 分钟）
│      ├─ 每轮失败后从 DB 重建待重试腿列表
│      └─ 全部成交 → positions status='closed'
│
└─ 11. 发 Telegram 通知（成功/失败）
```

### Deribit（`_submit_exit_deribit_inner`）

流程与 OKX 出场相同，差异：
- 用 `DeribitRestClient.get_order_book()` 拉盘口（`best_bid_price` / `best_ask_price`）
- 期货出场数量同样换算为 USD 合约面值
- 成交后调 `get_trades_by_order` 取 `fee_type`（maker/taker）
- 撤单用 `dclient.cancel_order()`，重新开一个 `DeribitRestClient` context

---

## 撤单时机

撤单不是独立触发的，只在以下情况作为入场/出场流程的**清理动作**发生：

| 触发条件 | 撤单范围 |
|----------|----------|
| 提交阶段超时（30s） | 撤所有已拿到 order_id 的单 |
| 任一腿提交报错 | 撤其余已提交的单 |
| 轮询阶段超时（30s × 3腿并发） | 撤所有未成交单（已标 failed 的腿） |

撤单均为 best-effort：失败只 warning log，不影响主流程状态写入。

---

## DB 状态流转

### positions.status
```
open（建仓后）→ closing（触发平仓中）
             → closed（出场全成交）
             → partial_failed（任一腿未成交，等待升级重试或守护协程接管）
```

### orders.status
```
pending（INSERT 时）→ filled（成交）
                   → failed（超时/报错/未成交）
```

---

## 平仓守护协程（exit_monitor_loop）

`position_tracker.exit_monitor_loop()` 在 `main.py` 中作为独立任务启动，每 `exit_monitor_interval_secs`（默认 10s）扫描一次：

```
exit_monitor_loop()
├─ 查 DB：status='partial_failed' 的仓位
├─ 跳过已在 _exit_active 中的仓位（内联升级循环正在处理）
└─ 对其余仓位：
      _exit_active.add(pid)
      asyncio.create_task(_run_exit)
          submit_exit(pos, cfg, sqlite_path)
          finally: _exit_active.discard(pid)
```

用途：兜底 app 重启后遗留的 `partial_failed` 仓位，或内联循环因异常中断后的恢复。

---

## 仓位数量控制（_check_and_cap_qty）

开仓前按账户可用保证金约束 qty：

```
可用预算 = min(total_eq × entry_max_trade_pct,
               adj_eq - total_eq × entry_reserve_pct)
max_qty  = 可用预算 / index_price_usdt
qty      = min(原始盘口深度qty, floor(max_qty / lot_size) × lot_size)
```

- `entry_max_trade_pct`（默认 20%）：单笔最多占用账户总权益的比例
- `entry_reserve_pct`（默认 10%）：账户底线，永远不动用
- paper 模式自动跳过检查
