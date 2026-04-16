# TODO

> 状态更新时间：2026-04-15。`[x]` 表示已在主分支落地；`[ ]` 仍为待办。

## 高优先级

### 开仓优化

- [ ] **开仓前刷新价格**：`_submit_entry_inner` 仍使用信号里的价格；平仓路径会在提交前拉盘口。应在开仓提交前对三腿各拉一次盘口，用最新价限价单。

- [ ] **开仓加入重试机制**：开仓失败仍整体 `partial_failed`，无类似 OKX 平仓的 maker→taker 升级循环。

- [ ] **`partially_filled` 开仓腿处理**：部分成交腿仍按失败处理；需记录成交量并追单/撤补。

### Deribit 平仓升级循环

- [ ] **`_submit_exit_deribit_inner` 完整升级循环**：尚无 OKX 式 escalating loop；`exit_monitor_loop` 仍会重试，但非 maker→taker 分级。

---

## 中优先级

### 风控增强

- [ ] **多笔持仓总保证金上限**：`_check_and_cap_qty` 仍按单笔 `entry_max_trade_pct` 约束，未汇总所有 `open` 仓位名义占用。

- [x] **`arbitrage_enabled` 开关硬校验**：`signal_output.emit_opportunity_evaluation` 在 `submit_entry` 前检查对应交易所 `ExchangeConfig.arbitrage_enabled`；未开启则打日志并跳过自动下单（Telegram/仪表盘通知仍可能发出）。

### 开仓利润计算精度

- [ ] **费率口径统一**：信号侧与成交后实际 maker/taker 仍可能不一致；需在成交回填 `fee_type` 后区分展示「预估 / 实际」。

---

## 低优先级

### 测试补全

- [ ] **预存失败测试修复**：若干历史失败用例需单独排查（以当前 `pytest` 结果为准）。

- [ ] **升级循环测试补充**（maker / taker / partially_filled 等场景）。

- [ ] **开仓余额约束集成测试**。

### 代码质量

- [x] **`import time` 位置**：`order_manager.py` 顶部统一 `import time`，`_escalating_exit_loop_okx` / `_submit_exit_inner` 等不再局部导入。

- [x] **`entry_px` 补全**：`_submit_exit_inner` 构建进入 OKX 升级循环的 `failed_legs` 时，从 DB 开仓单写入 `entry_px`；`_submit_and_poll_exit_legs_okx` 对 `leg_map` 增加 `entry_px` 回退，供升级路径 PnL 使用。

- [x] **`datetime.utcnow()` 弃用**：`db._utc_iso` / `_utc_now_iso` 与 `position_tracker` 改用 `datetime.now(timezone.utc)` 风格时间戳。

---

## 说明

- 文档中曾列项若与代码不一致，以本文件勾选与仓库实现为准。
- 大项（开仓刷新盘口、Deribit 升级循环、总保证金、测试补全）仍建议按业务优先级排期。
