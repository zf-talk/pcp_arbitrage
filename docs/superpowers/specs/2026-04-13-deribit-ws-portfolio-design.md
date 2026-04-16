# Deribit WebSocket Portfolio Subscription Design

**Date:** 2026-04-13
**Problem:** `account_fetcher` 每 5 分钟新建 `DeribitRestClient` 并调用 `public/auth` REST 端点获取余额，触发 429 限流。
**Solution:** 通过已有的 Deribit WebSocket 连接订阅 `user.portfolio.{currency}` 私有频道，彻底消除 REST 认证调用。

---

## 变更范围

4 个文件：`deribit.py`、`web_dashboard.py`、`main.py`、`account_fetcher.py`

---

## 1. `web_dashboard.py`

新增函数 `update_single_account_balance(name: str, bal: dict) -> None`，合并单个交易所余额到 `_account_balances_cache`，而不是替换整个字典：

```python
def update_single_account_balance(name: str, bal: dict) -> None:
    _account_balances_cache[name] = bal
```

现有 `update_account_balances(balances)` 保留，供 OKX/Binance 的批量更新使用。

---

## 2. `DeribitWebSocketClient`（`exchanges/deribit.py`）

### 构造函数新增可选参数

```python
def __init__(
    self,
    on_message,
    on_reconnect=None,
    api_key: str | None = None,
    secret: str | None = None,
    private_channels: list[str] | None = None,
) -> None:
```

`private_channels` 示例：`["user.portfolio.BTC"]`、`["user.portfolio.USDC"]`

### `_connect_and_run` 变更

```
连接 WS
  └─ 如果 api_key 存在:
       send public/auth (id=AUTH_REQ_ID=9000)
       等待该 id 的响应消息（手动 recv 循环）
       若 error → raise RuntimeError，让外层重连逻辑处理
  └─ _subscribe(ws)  ← 公有频道，不变
  └─ 如果 private_channels:
       send private/subscribe (id=9001)
  启动心跳 + 消息循环
```

WS 认证格式：
```json
{
  "jsonrpc": "2.0",
  "method": "public/auth",
  "params": {"grant_type": "client_credentials", "client_id": "...", "client_secret": "..."},
  "id": 9000
}
```

Private subscribe 格式：
```json
{
  "jsonrpc": "2.0",
  "method": "private/subscribe",
  "params": {"channels": ["user.portfolio.BTC"]},
  "id": 9001
}
```

### `on_message` 路由

`user.portfolio.*` 消息由 `_on_message` 统一处理（与行情消息同一回调）。Caller 在回调内按 `channel.startswith("user.portfolio.")` 分支处理。

---

## 3. Runner 中定义 Portfolio 回调

### `DeribitRunner`（BTC 计价）

在 `on_message` 内新增分支：

```python
if channel.startswith("user.portfolio."):
    _handle_deribit_portfolio(data, currency="BTC", exchange_name="deribit")
    return
```

`_handle_deribit_portfolio` 逻辑：
- 读 `data.get("equity")`、`available_funds`、`initial_margin`、`maintenance_margin`
- BTC/USD 价格从 runner 闭包中的 `spot_price_cache`（dict，key 为 "BTC"）读取，该缓存由已有的 `deribit_price_index.btc_usd` WS 消息实时更新
- 折算为 USD，构造标准 balance dict
- 调用 `update_single_account_balance("deribit", bal)`

若 `spot_price_cache` 中无 BTC 价格（启动初期 index channel 尚未推送），跳过本次更新（等下次 portfolio 推送时再处理）。

**注意**：`_INDEX_PRICE_CACHE` 由 REST 调用填充，不可用于此处；应使用 `spot_price_cache`（runner 闭包变量，由 WS index price 消息维护）。

### `DeribitLinearRunner`（USDC 计价）

同上，`currency="USDC"`，`price=1.0`，`exchange_name="deribit_linear"`。

### WS Client 构建时传入凭据

```python
ws_client = DeribitWebSocketClient(
    on_message=on_message,
    on_reconnect=on_reconnect,
    api_key=ex.api_key or None,
    secret=ex.secret_key or None,
    private_channels=["user.portfolio.BTC"] if ex.api_key else None,
)
```

无 API Key 时行为与现在完全相同（向后兼容）。

---

## 4. `main.py`

`_balance_fetch_loop` 中跳过 Deribit 交易所（WS 已覆盖）：

```python
balances = await account_fetcher.fetch_all_balances(
    cfg, skip_exchanges={"deribit", "deribit_linear"}
)
```

---

## 5. `account_fetcher.py`

`fetch_all_balances` 增加 `skip_exchanges: set[str] | None = None` 参数：

```python
for name, ex_cfg in cfg.exchanges.items():
    if skip_exchanges and name in skip_exchanges:
        continue
    ...
```

---

## 数据流对比

| | 现在 | 改后 |
|---|---|---|
| 余额来源 | REST `public/auth` + `get_account_summary` | WS `user.portfolio` 推送 |
| 更新频率 | 每 5 分钟 | 实时（Deribit 每次账户变动推送） |
| auth 调用 | 每 5 分钟 1 次 REST POST | 无（WS 连接建立时一次 WS 帧） |
| BTC→USD 价格 | 每次 REST `get_index_price` | 复用已有 `_INDEX_PRICE_CACHE` |

---

## 错误处理

- WS auth 失败 → `raise RuntimeError` → 外层 `run()` 退避重连逻辑处理，行为一致
- `user.portfolio` 消息解析失败 → log warning，跳过，不影响行情处理
- WS 断线重连后，重新发 auth + private/subscribe（已由 `_connect_and_run` 结构保证）

---

## 不变部分

- OKX、Binance 余额仍由 `_balance_fetch_loop` 的 REST 轮询处理
- `account_fetcher._get_deribit_balance` 保留（可供独立调试用）
- `_INDEX_PRICE_CACHE` 结构不变
