# Deribit WebSocket Portfolio Subscription Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过 Deribit WebSocket 的 `user.portfolio.{currency}` 私有频道实时获取账户余额，消除 `account_fetcher` 每 5 分钟新建 REST 会话并调用 `public/auth` 而导致的 429 限流。

**Architecture:** 扩展现有 `DeribitWebSocketClient` 支持可选的 WS 认证（`public/auth` over WS）和私有频道订阅（`private/subscribe`）。两个 Runner 在 `on_message` 中路由 `user.portfolio.*` 消息，转换为标准 balance dict 并调用 `update_single_account_balance()`。OKX/Binance 仍走 REST 轮询，Deribit 从 `_balance_fetch_loop` 中剔除。

**Tech Stack:** Python asyncio, aiohttp WebSocket, sqlite3, Deribit JSON-RPC 2.0

---

## File Map

| 文件 | 变更类型 | 内容 |
|------|---------|------|
| `src/pcp_arbitrage/web_dashboard.py` | Modify | 新增 `update_single_account_balance()` |
| `src/pcp_arbitrage/account_fetcher.py` | Modify | `fetch_all_balances` 增加 `skip_exchanges` 参数 |
| `src/pcp_arbitrage/main.py` | Modify | `_balance_fetch_loop` 跳过 Deribit |
| `src/pcp_arbitrage/exchanges/deribit.py` | Modify | WS client 扩展 + 两个 Runner 的 portfolio 处理 |
| `tests/test_web_dashboard_notifications.py` | Modify | 增加 `update_single_account_balance` 测试 |
| `tests/test_deribit_portfolio.py` | Create | 新建：portfolio 消息解析单元测试 |

---

## Task 1: `web_dashboard.py` — 新增 `update_single_account_balance`

**Files:**
- Modify: `src/pcp_arbitrage/web_dashboard.py` (在 `update_account_balances` 附近，约第 192 行)
- Test: `tests/test_web_dashboard_notifications.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_web_dashboard_notifications.py` 末尾追加：

```python
def test_update_single_account_balance_merges() -> None:
    """update_single_account_balance should merge one exchange without wiping others."""
    from pcp_arbitrage import web_dashboard
    web_dashboard._account_balances_cache.clear()
    web_dashboard.update_account_balances({"okx": {"total_eq_usdt": 1000.0}})

    web_dashboard.update_single_account_balance("deribit", {"total_eq_usdt": 500.0})

    assert web_dashboard._account_balances_cache["okx"]["total_eq_usdt"] == 1000.0
    assert web_dashboard._account_balances_cache["deribit"]["total_eq_usdt"] == 500.0
```

- [ ] **Step 2: 跑测试，确认失败**

```bash
cd /Users/fei/Codespace/python/pcp_arbitrage
python -m pytest tests/test_web_dashboard_notifications.py::test_update_single_account_balance_merges -v
```

期望：`AttributeError: module 'pcp_arbitrage.web_dashboard' has no attribute 'update_single_account_balance'`

- [ ] **Step 3: 实现**

在 `src/pcp_arbitrage/web_dashboard.py` 的 `update_account_balances` 函数之后插入：

```python
def update_single_account_balance(name: str, bal: dict) -> None:
    """Merge one exchange's balance into cache without replacing other exchanges."""
    _account_balances_cache[name] = bal
```

- [ ] **Step 4: 跑测试，确认通过**

```bash
python -m pytest tests/test_web_dashboard_notifications.py::test_update_single_account_balance_merges -v
```

期望：PASS

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/web_dashboard.py tests/test_web_dashboard_notifications.py
git commit -m "feat: add update_single_account_balance for per-exchange balance update"
```

---

## Task 2: `account_fetcher.py` + `main.py` — 剔除 Deribit REST 认证

**Files:**
- Modify: `src/pcp_arbitrage/account_fetcher.py` (`fetch_all_balances` 约第 263 行)
- Modify: `src/pcp_arbitrage/main.py` (`_balance_fetch_loop` 约第 121 行)

- [ ] **Step 1: 修改 `account_fetcher.fetch_all_balances`**

将 `fetch_all_balances` 签名和循环改为：

```python
async def fetch_all_balances(
    cfg: "AppConfig", skip_exchanges: set[str] | None = None
) -> dict[str, dict]:
    """Fetch balances for all enabled exchanges with API keys. Returns {exchange_name: balance_info}."""
    results: dict[str, dict] = {}
    tasks = []
    names = []
    for name, ex_cfg in cfg.exchanges.items():
        if skip_exchanges and name in skip_exchanges:
            continue
        if ex_cfg.enabled and ex_cfg.api_key:
            tasks.append(get_exchange_balance(ex_cfg, cfg))
            names.append(name)
    if tasks:
        balances = await asyncio.gather(*tasks, return_exceptions=True)
        for name, bal in zip(names, balances):
            if isinstance(bal, Exception):
                logger.warning("[account_fetcher] Error fetching %s balance: %s", name, bal)
            elif bal:
                results[name] = bal
    return results
```

- [ ] **Step 2: 修改 `main.py` 的 `_balance_fetch_loop`**

将：
```python
balances = await account_fetcher.fetch_all_balances(cfg)
```
改为：
```python
balances = await account_fetcher.fetch_all_balances(
    cfg, skip_exchanges={"deribit", "deribit_linear"}
)
```

- [ ] **Step 3: 跑现有测试，确认不破坏**

```bash
python -m pytest tests/ -v -x --ignore=tests/test_order_manager.py
```

期望：全部 PASS（无 `fetch_all_balances` 相关错误）

- [ ] **Step 4: Commit**

```bash
git add src/pcp_arbitrage/account_fetcher.py src/pcp_arbitrage/main.py
git commit -m "feat: skip deribit from REST balance loop (will use WS portfolio)"
```

---

## Task 3: 创建 portfolio 消息解析的测试文件

**Files:**
- Create: `tests/test_deribit_portfolio.py`

这个测试文件会在 Task 4 中用到，先建好框架：

- [ ] **Step 1: 创建测试文件**

```python
"""Unit tests for Deribit WebSocket user.portfolio message handling."""
from __future__ import annotations


def _parse_portfolio_btc(data: dict, btc_usd: float) -> dict | None:
    """将 user.portfolio.BTC 消息 data 转换为标准 balance dict，供测试使用。

    这是 DeribitRunner.on_message 中 portfolio 分支逻辑的抽取。
    实现时直接在 on_message 中编写等效逻辑（不提取为独立函数，因为需要访问闭包变量）。
    此函数仅用于测试驱动设计。
    """
    def _sf(x: object) -> float:
        try:
            return float(x) if x is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    if btc_usd <= 0:
        return None
    eq_coin = _sf(data.get("equity"))
    avail_coin = _sf(data.get("available_funds"))
    im_coin = _sf(data.get("initial_margin"))
    mm_coin = _sf(data.get("maintenance_margin"))
    total_eq = eq_coin * btc_usd
    adj_eq = avail_coin * btc_usd
    imr = im_coin * btc_usd
    mmr = mm_coin * btc_usd
    denom = total_eq if total_eq > 0 else (adj_eq if adj_eq > 0 else 1.0)
    return {
        "total_eq_usdt": total_eq,
        "adj_eq_usdt": adj_eq,
        "imr_usdt": imr,
        "mmr_usdt": mmr,
        "im_pct": (imr / denom * 100) if denom > 0 else 0.0,
        "mm_pct": (mmr / denom * 100) if denom > 0 else 0.0,
        "currency_details": {},
    }


def test_parse_portfolio_btc_converts_to_usd() -> None:
    data = {
        "equity": 1.0,
        "available_funds": 0.8,
        "initial_margin": 0.1,
        "maintenance_margin": 0.05,
    }
    result = _parse_portfolio_btc(data, btc_usd=80000.0)
    assert result is not None
    assert result["total_eq_usdt"] == 80000.0
    assert result["adj_eq_usdt"] == 64000.0
    assert result["imr_usdt"] == 8000.0
    assert result["mmr_usdt"] == 4000.0
    assert abs(result["im_pct"] - 10.0) < 0.01
    assert abs(result["mm_pct"] - 5.0) < 0.01
    assert result["currency_details"] == {}


def test_parse_portfolio_btc_no_price_returns_none() -> None:
    data = {"equity": 1.0, "available_funds": 0.8}
    assert _parse_portfolio_btc(data, btc_usd=0.0) is None


def test_parse_portfolio_usdc_price_1() -> None:
    """USDC portfolio: price = 1.0, values pass through unchanged."""
    data = {
        "equity": 5000.0,
        "available_funds": 4000.0,
        "initial_margin": 500.0,
        "maintenance_margin": 250.0,
    }
    result = _parse_portfolio_btc(data, btc_usd=1.0)
    assert result is not None
    assert result["total_eq_usdt"] == 5000.0
    assert result["adj_eq_usdt"] == 4000.0
```

- [ ] **Step 2: 跑测试，确认通过**

```bash
python -m pytest tests/test_deribit_portfolio.py -v
```

期望：3 tests PASS（这些测试不依赖真实 WS，只测纯逻辑）

- [ ] **Step 3: Commit**

```bash
git add tests/test_deribit_portfolio.py
git commit -m "test: add deribit portfolio message parsing unit tests"
```

---

## Task 4: 扩展 `DeribitWebSocketClient` 支持 WS 认证和私有频道

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/deribit.py`
  - `DeribitWebSocketClient.__init__` (第 402 行)
  - `DeribitWebSocketClient._connect_and_run` (第 426 行)
  - 新增 `_ws_authenticate` 和 `_subscribe_private` 方法

- [ ] **Step 1: 在文件顶部常量区添加 WS 请求 ID 常量**

在 `deribit.py` 顶部（`MONTH_MAP` 之前）添加：

```python
_WS_AUTH_REQ_ID = 9000    # WS public/auth 请求 ID
_WS_PRIV_SUB_ID = 9001    # WS private/subscribe 请求 ID
```

- [ ] **Step 2: 修改 `DeribitWebSocketClient.__init__`**

将：
```python
def __init__(self, on_message, on_reconnect=None) -> None:
    self._on_message = on_message
    self._on_reconnect = on_reconnect
    self._channels: list[str] = []
```

改为：
```python
def __init__(
    self,
    on_message,
    on_reconnect=None,
    api_key: str | None = None,
    secret: str | None = None,
    private_channels: list[str] | None = None,
) -> None:
    self._on_message = on_message
    self._on_reconnect = on_reconnect
    self._channels: list[str] = []
    self._api_key = api_key
    self._secret = secret
    self._private_channels: list[str] = private_channels or []
```

- [ ] **Step 3: 在 `_heartbeat` 之后新增 `_ws_authenticate` 方法**

```python
async def _ws_authenticate(self, ws) -> None:
    """Send public/auth over WS and wait for the response before continuing."""
    payload = {
        "jsonrpc": "2.0",
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": self._api_key,
            "client_secret": self._secret,
        },
        "id": _WS_AUTH_REQ_ID,
    }
    await ws.send_str(json.dumps(payload))
    async for raw in ws:
        if raw.type == aiohttp.WSMsgType.TEXT:
            msg = json.loads(raw.data)
            if msg.get("id") == _WS_AUTH_REQ_ID:
                if "error" in msg:
                    raise RuntimeError(f"[deribit ws] WS auth failed: {msg['error']}")
                logger.info("[deribit ws] Authenticated via WebSocket")
                return
        elif raw.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            raise ConnectionError("[deribit ws] Connection closed during WS auth")
```

- [ ] **Step 4: 新增 `_subscribe_private` 方法**

```python
async def _subscribe_private(self, ws) -> None:
    """Subscribe to private channels (requires prior WS auth)."""
    payload = {
        "jsonrpc": "2.0",
        "method": "private/subscribe",
        "params": {"channels": self._private_channels},
        "id": _WS_PRIV_SUB_ID,
    }
    await ws.send_str(json.dumps(payload))
    logger.info("[deribit ws] Sent private/subscribe for %d channels", len(self._private_channels))
```

- [ ] **Step 5: 修改 `_connect_and_run` 以调用新方法**

将：
```python
async def _connect_and_run(self) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(self.WS_URL) as ws:
            logger.info("[deribit ws] Connected")
            await self._subscribe(ws)
            ping_task = asyncio.create_task(self._heartbeat(ws))
```

改为：
```python
async def _connect_and_run(self) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(self.WS_URL) as ws:
            logger.info("[deribit ws] Connected")
            if self._api_key and self._secret:
                await self._ws_authenticate(ws)
            await self._subscribe(ws)
            if self._private_channels:
                await self._subscribe_private(ws)
            ping_task = asyncio.create_task(self._heartbeat(ws))
```

- [ ] **Step 6: 跑现有测试，确认不破坏**

```bash
python -m pytest tests/test_deribit_normalize.py tests/test_deribit_portfolio.py -v
```

期望：全部 PASS

- [ ] **Step 7: Commit**

```bash
git add src/pcp_arbitrage/exchanges/deribit.py
git commit -m "feat: extend DeribitWebSocketClient with WS auth and private channel subscribe"
```

---

## Task 5: `DeribitRunner` — index price 缓存更新 + portfolio 消息处理

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/deribit.py`
  - `DeribitRunner.run()` 内的 `on_message`（约第 1013 行）
  - `DeribitRunner.run()` 内的 WS client 构建（约第 1133 行）

- [ ] **Step 1: 在 `DeribitRunner.on_message` 的 index price 分支中更新 `spot_price_cache`**

当前代码（约第 1021-1028 行）：
```python
if channel.startswith("deribit_price_index."):
    idx_px = data.get("price")
    if idx_px is not None:
        suffix = channel[len("deribit_price_index."):]
        sym = suffix.split("_")[0].upper()
        update_dashboard_index_price(sym, float(idx_px))
    return
```

改为：
```python
if channel.startswith("deribit_price_index."):
    idx_px = data.get("price")
    if idx_px is not None:
        suffix = channel[len("deribit_price_index."):]
        sym = suffix.split("_")[0].upper()
        spot_price_cache[sym] = float(idx_px)   # ← 新增：更新本地价格缓存
        update_dashboard_index_price(sym, float(idx_px))
    return
```

- [ ] **Step 2: 在 `DeribitRunner.on_message` 中新增 portfolio 分支**

在上一步的 `return` 之后（即在 `raw_name = data.get(...)` 之前）插入：

```python
# user.portfolio.BTC → 更新账户余额 dashboard
if channel.startswith("user.portfolio."):
    btc_usd = spot_price_cache.get("BTC", 0.0)
    if btc_usd <= 0:
        return  # index price 尚未到达，等下次推送
    def _sf(x: object) -> float:
        try:
            return float(x) if x is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
    eq_coin = _sf(data.get("equity"))
    avail_coin = _sf(data.get("available_funds"))
    im_coin = _sf(data.get("initial_margin"))
    mm_coin = _sf(data.get("maintenance_margin"))
    total_eq = eq_coin * btc_usd
    adj_eq = avail_coin * btc_usd
    imr = im_coin * btc_usd
    mmr = mm_coin * btc_usd
    denom = total_eq if total_eq > 0 else (adj_eq if adj_eq > 0 else 1.0)
    from pcp_arbitrage import web_dashboard as _wd
    _wd.update_single_account_balance("deribit", {
        "total_eq_usdt": total_eq,
        "adj_eq_usdt": adj_eq,
        "imr_usdt": imr,
        "mmr_usdt": mmr,
        "im_pct": (imr / denom * 100) if denom > 0 else 0.0,
        "mm_pct": (mmr / denom * 100) if denom > 0 else 0.0,
        "currency_details": {},
    })
    return
```

- [ ] **Step 3: 修改 `DeribitRunner` 中构建 WS client 的代码**

当前（约第 1133 行）：
```python
ws_client = DeribitWebSocketClient(on_message=on_message, on_reconnect=on_reconnect)
```

改为：
```python
_portfolio_currency = "BTC"
ws_client = DeribitWebSocketClient(
    on_message=on_message,
    on_reconnect=on_reconnect,
    api_key=ex.api_key or None,
    secret=ex.secret_key or None,
    private_channels=[f"user.portfolio.{_portfolio_currency}"] if ex.api_key else None,
)
```

- [ ] **Step 4: 跑测试**

```bash
python -m pytest tests/ -v -x --ignore=tests/test_order_manager.py
```

期望：全部 PASS

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/exchanges/deribit.py
git commit -m "feat: DeribitRunner subscribes user.portfolio.BTC via WS for real-time balance"
```

---

## Task 6: `DeribitLinearRunner` — portfolio 消息处理（USDC）

**Files:**
- Modify: `src/pcp_arbitrage/exchanges/deribit.py`
  - `DeribitLinearRunner.run()` 内的 `on_message`（约第 736 行）
  - `DeribitLinearRunner.run()` 内的 WS client 构建（约第 845 行）

- [ ] **Step 1: 在 `DeribitLinearRunner.on_message` 中新增 portfolio 分支**

在 `deribit_price_index.*` 的 `return` 之后（`raw_name = data.get(...)` 之前）插入：

```python
# user.portfolio.USDC → 更新账户余额 dashboard（USDC ≈ USD，price = 1.0）
if channel.startswith("user.portfolio."):
    def _sf(x: object) -> float:
        try:
            return float(x) if x is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
    eq = _sf(data.get("equity"))
    avail = _sf(data.get("available_funds"))
    imr = _sf(data.get("initial_margin"))
    mmr = _sf(data.get("maintenance_margin"))
    denom = eq if eq > 0 else (avail if avail > 0 else 1.0)
    from pcp_arbitrage import web_dashboard as _wd
    _wd.update_single_account_balance("deribit_linear", {
        "total_eq_usdt": eq,
        "adj_eq_usdt": avail,
        "imr_usdt": imr,
        "mmr_usdt": mmr,
        "im_pct": (imr / denom * 100) if denom > 0 else 0.0,
        "mm_pct": (mmr / denom * 100) if denom > 0 else 0.0,
        "currency_details": {},
    })
    return
```

- [ ] **Step 2: 修改 `DeribitLinearRunner` 中构建 WS client 的代码**

当前（约第 845 行）：
```python
ws_client = DeribitWebSocketClient(on_message=on_message, on_reconnect=on_reconnect)
```

改为：
```python
_portfolio_currency = "USDC"
ws_client = DeribitWebSocketClient(
    on_message=on_message,
    on_reconnect=on_reconnect,
    api_key=ex.api_key or None,
    secret=ex.secret_key or None,
    private_channels=[f"user.portfolio.{_portfolio_currency}"] if ex.api_key else None,
)
```

- [ ] **Step 3: 跑全部测试**

```bash
python -m pytest tests/ -v --ignore=tests/test_order_manager.py
```

期望：全部 PASS

- [ ] **Step 4: Commit**

```bash
git add src/pcp_arbitrage/exchanges/deribit.py
git commit -m "feat: DeribitLinearRunner subscribes user.portfolio.USDC via WS for real-time balance"
```

---

## Task 7: 集成验证

- [ ] **Step 1: 确认日志中无 `[account_fetcher] Failed to fetch Deribit balance` 的 429 错误**

启动程序后观察 2-3 分钟日志，应不再出现：
```
[account_fetcher] Failed to fetch Deribit balance: 429, ...url='.../public/auth'
```

- [ ] **Step 2: 确认 WS 认证和订阅成功**

日志应出现：
```
[deribit ws] Authenticated via WebSocket
[deribit ws] Sent private/subscribe for 1 channels
```

- [ ] **Step 3: 确认 web dashboard 余额能显示**

Web dashboard 的账户余额部分应在几秒内（第一次 portfolio 推送到达后）显示 Deribit 余额，而不是等 5 分钟。

- [ ] **Step 4: 最终 commit**

```bash
git add .
git commit -m "feat: deribit balance via WS portfolio subscription, eliminates REST auth 429"
```
