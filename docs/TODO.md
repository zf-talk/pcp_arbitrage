# TODO

## 高优先级

### 开仓优化

- [ ] **开仓前刷新价格**：`_submit_entry_inner` 目前使用信号计算时的盘口快照价格，与平仓不同（平仓会在提交前重新查盘口）。信号触发到下单之间可能过了 100ms+，高波动时挂单会立刻成交（taker 行为）或偏离。应在提交前重新拉一次盘口，用最新价下单。

- [ ] **开仓加入重试机制**：开仓目前无重试——任意一腿提交失败或轮询超时，整个机会作废（标 `partial_failed`）。参考平仓的升级循环，至少加一次 maker 追单重试，减少因网络抖动丢失高质量机会。

- [ ] **`partially_filled` 开仓腿处理**：部分成交的开仓腿目前被认为是失败，信息丢失。应记录已成交量，对未成交量做精细处理（追单补齐或撤销已成交腿）。

### Deribit 平仓升级循环

- [ ] **`_submit_exit_deribit_inner` 完整升级循环**：当前 Deribit 平仓失败只做 `_exit_active` 保护和 DB 标记，没有 maker→taker 升级重试。`exit_monitor_loop` 会自动接管，但 Deribit 的升级循环应有专属实现（复用 `_escalating_exit_loop_okx` 的逻辑但换 Deribit 客户端）。

---

## 中优先级

### 风控增强

- [ ] **多笔持仓总保证金上限**：`_check_and_cap_qty` 只管单笔上限（20%），没有检查当前所有 `open` 仓位加起来已用了多少比例。若同时有多笔仓位，总敞口可能超预期。需要在开仓前统计现有持仓名义价值，从可用预算中扣除。

- [ ] **`arbitrage_enabled` 开关硬校验**：`signal_output.py` 触发下单时应显式检查 `exchange_cfg.arbitrage_enabled`，当前架构文档已标注这一点，但代码层面是否已完整执行需确认。

### 开仓利润计算精度

- [ ] **费率口径统一**：`pcp_calculator.py` 计算信号利润时使用 taker 费率，但限价单实际可能以 maker 费率成交，导致预估利润偏保守。可在开仓成交后用实际 `fee_type` 重新计算记录到 DB，并在 PnL 展示中区分"预估"和"实际"。

---

## 低优先级

### 测试补全

- [ ] **预存失败测试修复**：`tests/test_submit_exit.py`、`tests/test_signal_output_notifications.py`、`tests/test_pcp_calculator.py`、`tests/test_signal_printer.py`、`tests/test_opportunity_dashboard.py` 共约 31 个失败测试（与本期改动无关，属历史遗留），需逐一排查修复。

- [ ] **升级循环测试补充**：
  - maker 第一轮成功，验证不进 taker 路径
  - taker 也失败，验证 `(False, 0.0)` 返回
  - taker 模式下 `partially_filled` 继续轮询，验证不提前退出

- [ ] **开仓余额约束集成测试**：paper 模式开仓验证不受余额限制；真实模式在模拟低余额时 qty 被截断。

### 代码质量

- [ ] **`import time` 位置**：`_submit_exit_inner` 和 `_escalating_exit_loop_okx` 中有局部 `import time`，应移至文件顶部统一导入。

- [ ] **`entry_px` 补全**：`_escalating_exit_loop_okx` 构建 `failed_legs` 时 `entry_px` 硬编码为 `0.0`，应从 DB 的入场成交价取值（影响 PnL 显示精度，不影响下单逻辑）。

- [ ] **`datetime.datetime.utcnow()` 弃用警告**：`db.py` 和 `position_tracker.py` 中使用了已弃用的 `utcnow()`，应替换为 `datetime.now(datetime.UTC)`。
