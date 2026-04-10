# 三交易所保证金规则调研（PCP 三腿）

## 1. 目标
说明 Binance / OKX / Deribit 在期权 + 期货/交割合约场景下的保证金口径，评估“固定公式”与“真实口径（IM_before/IM_after）”的可行性。

## 2. 结论摘要
- 三家均不存在可统一套用的单一固定公式。
- 最准确口径是“账户风险引擎输出”：`ΔIM = IM_after - IM_before`。
- 若无 what-if 接口，则用账户 IM/MM + 分档参数做估算并标注 `estimated`。

## 3. Binance

### 3.1 官方文档
- Option Margin Account Information (`/eapi/v1/marginAccount`)
  - <https://developers.binance.com/docs/derivatives/options-trading/account>
- Option Position Information (`/eapi/v1/position`)
  - <https://developers.binance.com/docs/derivatives/options-trading/trade/Option-Position-Information>
- Notional and Leverage Brackets (`/fapi/v1/leverageBracket`)
  - <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Notional-and-Leverage-Brackets>

### 3.2 可落地口径
- 已有仓位：读账户 `initialMargin` / `maintMargin`。
- 新增三腿：优先 what-if（若无统一接口则分产品估算）。
- 合约腿按 notional bracket 的 `maintMarginRatio` 做分档估算。

### 3.3 注意事项
- PM/普通模式、逐仓/全仓口径差异明显。
- 开发者文档偏接口说明，组合公式公开不完整。

## 4. OKX

### 4.1 官方文档
- Portfolio margin mode: Risk Unit Merge
  - <https://www.okx.com/en-us/help/portfolio-margin-mode-cross-margin-trading-risk-unit-merge>
- Multi-currency vs Portfolio margin mode
  - <https://www.okx.com/help/vi-multi-currency-margin-mode-vs-portfolio-margin-mode>
- Position Builder（文档中提及）
  - <https://www.okx.com/en-us/trade-position-builder>
- OKX API v5
  - <https://www.okx.com/docs-v5/en>

### 4.2 可落地口径
- 推荐直接用 Position Builder/API 做 before/after 试算。
- 取 `ΔIMR` 或 `ΔMMR` 作为机会保证金占用。

### 4.3 注意事项
- 风险模型包含 MR1~MR9（spot shock、vega、basis、extreme move 等）。
- 同 underlying 风险单元合并，跨 underlying 不对冲。

## 5. Deribit

### 5.1 官方文档
- API: private/get_account_summaries
  - <https://docs.deribit.com/api-reference/account-management/private-get_account_summaries>
- Portfolio Margin（Support）
  - <https://support.deribit.com/hc/en-us/articles/25944756247837-Portfolio-Margin>
- Margin types and usage（Support）
  - <https://support.deribit.com/hc/en-us/articles/25944811317149-Margin-types-and-usage>
- Deribit Portfolio Margin Model (PDF)
  - <https://statics.deribit.com/files/DeribitPortfolioMarginModel.pdf>

### 5.2 可落地口径
- 读取 `initial_margin` / `maintenance_margin` / `projected_*` 字段。
- PM 账户优先用账户级风险字段，不建议本地复刻完整 PME。

### 5.3 注意事项
- SM 与 PM 结果差异大；账号模式是前置条件。
- 组合/对冲收益必须由交易所风险引擎体现。

## 6. 推荐实施策略（当前项目）

### 6.1 计算优先级
1. `required_margin_exact = IM_after - IM_before`（what-if）
2. 账户级差值近似（若无 what-if）
3. 分档/静态估算（fallback）

### 6.2 年化口径
- 当前：`ann = net / K * 365 / dte`
- 建议：`ann = net / required_margin * 365 / dte`
- 增加标记：`margin_source = exact | estimated | fallback`

### 6.3 风险控制建议
- 缓存 before 值（1~2s）避免频繁拉接口。
- 为估算结果显示置信级别。
- 记录 `estimate_error = |exact-estimated|/exact` 做持续校准。

## 7. 备注
- 官方文档会动态更新，以上链接建议定期校验。
