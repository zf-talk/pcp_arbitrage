# Exit Escalating Retry + Entry Position Sizing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 两个独立功能：(1) 平仓升级重试——maker→追单→taker 自动升级，后台守护兜底；(2) 开仓按账户余额百分比控制下单量（最多 20%，预留 10%）。

**Architecture:**
- 平仓：在 `_submit_exit_inner` / `_submit_exit_deribit_inner` 内嵌升级循环替代当前单次重试；`position_tracker.py` 增加 `exit_monitor_loop` 守护协程，扫描 `partial_failed` 仓位并接管。用模块级 `_exit_active: set[int]` 防止双重操作。
- 开仓：在 `_submit_entry_inner` / `_submit_entry_deribit_inner` 提交前调用 `account_fetcher.get_exchange_balance()`，按名义价值（qty × index_price）计算上限，截断 qty。

**Tech Stack:** Python asyncio, aiohttp, SQLite (`_ensure_column` migration), existing `account_fetcher.py`

---

## 文件改动总览

| 文件 | 改动类型 | 内容 |
|------|---------|------|
| `src/pcp_arbitrage/config.py` | 修改 | 新增 5 个配置字段 |
| `src/pcp_arbitrage/db.py` | 修改 | `_ensure_column` 新增 3 列到 positions |
| `src/pcp_arbitrage/order_manager.py` | 修改 | 升级平仓循环；开仓加余额检查 |
| `src/pcp_arbitrage/position_tracker.py` | 修改 | 新增 `exit_monitor_loop` 守护协程 |
| `tests/test_order_manager.py` | 修改 | 新增相关测试 |
| `config.yaml` (示例) | 修改 | 补充新配置项注释 |

---

## Task 1: 新增配置字段

**Files:**
- Modify: `src/pcp_arbitrage/config.py`

- [ ] **Step 1: 在 `AppConfig` dataclass 末尾追加 5 个字段**

```python
# 平仓升级重试
exit_maker_chase_secs: int = 60        # maker 追单间隔（秒），超时后刷新价格再挂
exit_taker_escalate_secs: int = 300    # 总超时（秒）后切换 taker 强平
exit_monitor_interval_secs: int = 10   # 后台守护扫描间隔（秒）
# 开仓仓位控制
entry_max_trade_pct: float = 0.20      # 单笔最多占用账户总权益的比例
entry_reserve_pct: float = 0.10        # 账户预留比例（不可动用）
```

- [ ] **Step 2: 在 `load_config` 中读取这 5 个字段**

在 `load_config` 函数返回 `AppConfig(...)` 的地方追加（其他字段已有示例可参考）：

```python
exit_maker_chase_secs=int(raw.get("exit_maker_chase_secs", 60)),
exit_taker_escalate_secs=int(raw.get("exit_taker_escalate_secs", 300)),
exit_monitor_interval_secs=int(raw.get("exit_monitor_interval_secs", 10)),
entry_max_trade_pct=float(raw.get("entry_max_trade_pct", 0.20)),
entry_reserve_pct=float(raw.get("entry_reserve_pct", 0.10)),
```

- [ ] **Step 3: 验证加载不报错**

```bash
cd /Users/fei/Codespace/python/pcp_arbitrage
python -c "from pcp_arbitrage.config import load_config; c = load_config('config.yaml'); print(c.exit_maker_chase_secs, c.entry_max_trade_pct)"
```

Expected: `60 0.2`

- [ ] **Step 4: Commit**

```bash
git add src/pcp_arbitrage/config.py
git commit -m "feat: add exit retry and entry sizing config fields"
```

---

## Task 2: DB Schema — positions 表新增 3 列

**Files:**
- Modify: `src/pcp_arbitrage/db.py`

- [ ] **Step 1: 写失败测试**

在 `tests/test_order_manager.py`（或新建 `tests/test_db.py`）添加：

```python
import sqlite3, pytest
from pcp_arbitrage import db as _db

def test_positions_exit_columns():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    _db.init_db(con)
    cols = {row[1] for row in con.execute("PRAGMA table_info(positions)")}
    assert "exit_attempt_count" in cols
    assert "exit_started_at" in cols
    assert "exit_last_attempt_at" in cols
```

- [ ] **Step 2: 运行确认失败**

```bash
pytest tests/test_db.py::test_positions_exit_columns -v
```

Expected: FAIL — `AssertionError: 'exit_attempt_count' not in cols`

- [ ] **Step 3: 在 `db.py` 的 `init_db` 函数末尾追加 `_ensure_column` 调用**

找到 `init_db` 函数中已有的 `_ensure_column` 调用区域，仿照格式追加：

```python
_ensure_column(con, "positions", "exit_attempt_count",  "INTEGER NOT NULL DEFAULT 0")
_ensure_column(con, "positions", "exit_started_at",     "TEXT")
_ensure_column(con, "positions", "exit_last_attempt_at","TEXT")
```

- [ ] **Step 4: 运行确认通过**

```bash
pytest tests/test_db.py::test_positions_exit_columns -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/db.py tests/test_db.py
git commit -m "feat: add exit_attempt_count/exit_started_at/exit_last_attempt_at to positions"
```

---

## Task 3: 平仓升级循环（内联）

**Files:**
- Modify: `src/pcp_arbitrage/order_manager.py`

### 背景

当前 `_submit_exit_inner`（OKX，约 L1199）结尾有一段 `if not all_filled:` 做单次 2 秒重试，改为完整升级循环。`_submit_exit_deribit_inner` 逻辑类似，同步修改。

### 设计：升级循环函数

在 `order_manager.py` 内新增一个内部辅助函数 `_escalating_exit_loop_okx`，供两处调用：

```python
async def _escalating_exit_loop_okx(
    *,
    session: aiohttp.ClientSession,
    failed_legs: list[dict],   # 每个元素含 leg/inst_id/side/entry_px/oid_db
    qty: float,
    position_id: int,
    signal_id: int | None,
    api_key: str,
    secret: str,
    passphrase: str,
    is_paper: bool,
    sqlite_path: str,
    exit_started_at: float,    # time.monotonic() 首次发起平仓的时刻
    cfg: "AppConfig",
) -> tuple[bool, float]:
    """
    对 failed_legs 循环升级重试，直到全部成交或 taker 强平后仍失败。
    返回 (all_filled, incremental_pnl)。
    """
    import time
    maker_chase_secs = cfg.exit_maker_chase_secs        # 默认 60
    taker_escalate_secs = cfg.exit_taker_escalate_secs  # 默认 300

    remaining = list(failed_legs)
    total_pnl = 0.0

    while remaining:
        elapsed = time.monotonic() - exit_started_at
        use_taker = elapsed >= taker_escalate_secs

        # 取消交易所上仍挂着的旧单（best effort）
        for leg in remaining:
            if leg.get("exch_ord_id"):
                await _cancel_order(session, inst_id=leg["inst_id"],
                                    ord_id=leg["exch_ord_id"],
                                    api_key=api_key, secret=secret,
                                    passphrase=passphrase, is_paper=is_paper)

        # 刷新价格
        book_tasks = [
            _fetch_order_book_top(session, leg["inst_id"],
                                  api_key=api_key, secret=secret,
                                  passphrase=passphrase, is_paper=is_paper)
            for leg in remaining
        ]
        book_results = await asyncio.gather(*book_tasks, return_exceptions=True)

        retry_legs = []
        for i, leg in enumerate(remaining):
            br = book_results[i]
            if isinstance(br, Exception) or br is None:
                bid_px, ask_px = leg["last_px"], leg["last_px"]
            else:
                bid_px, ask_px = br  # (bid, ask)

            if use_taker:
                # taker: 买用 ask，卖用 bid（主动穿越）
                px = ask_px if leg["side"] == "buy" else bid_px
            else:
                # maker: 买用 bid，卖用 ask（被动挂单）
                px = bid_px if leg["side"] == "buy" else ask_px

            retry_legs.append({**leg, "px": px})

        # 更新 DB 记录 exit_last_attempt_at + attempt_count
        with sqlite3.connect(sqlite_path) as conn:
            now_iso = _utcnow_iso()
            conn.execute(
                "UPDATE positions SET exit_last_attempt_at=?, "
                "exit_attempt_count = exit_attempt_count + 1 "
                "WHERE id=?",
                (now_iso, position_id),
            )

        # 提交 + 轮询
        poll_timeout = None if use_taker else maker_chase_secs
        filled, pnl = await _submit_and_poll_exit_legs_okx(
            legs=retry_legs,
            qty=qty,
            position_id=position_id,
            signal_id=signal_id,
            leg_map={l["leg"]: l for l in retry_legs},
            api_key=api_key, secret=secret, passphrase=passphrase,
            is_paper=is_paper, sqlite_path=sqlite_path,
            poll_timeout=poll_timeout,  # None = 无限等待（taker 模式）
        )
        total_pnl += pnl

        if filled:
            remaining = []
        elif use_taker:
            # taker 仍失败（极少见，交易所错误），放弃
            logger.error("exit_escalating [pos=%d]: taker round failed, giving up", position_id)
            break
        else:
            # maker 超时，从 DB 查哪些腿仍 failed，重建 remaining
            # （_submit_and_poll_exit_legs_okx 只返回 all_filled bool，不返回逐腿结果）
            with sqlite3.connect(sqlite_path) as _conn:
                _conn.row_factory = sqlite3.Row
                failed_rows = _conn.execute(
                    "SELECT leg, inst_id, side FROM orders "
                    "WHERE position_id=? AND action='close' AND status='failed'",
                    (position_id,),
                ).fetchall()
            leg_index = {l["leg"]: l for l in retry_legs}
            remaining = [
                {**leg_index[r["leg"]], "exch_ord_id": None}
                for r in failed_rows
                if r["leg"] in leg_index
            ]

    return len(remaining) == 0, total_pnl
```

**注意**：`_submit_and_poll_exit_legs_okx` 需增加可选 `poll_timeout` 参数（目前硬编码 30s），传 `None` 时改用 `while True` 轮询。

- [ ] **Step 1: 写测试（mock 场景：maker 失败→taker 成功）**

```python
# tests/test_order_manager.py
import asyncio, unittest.mock as mock, pytest

@pytest.mark.asyncio
async def test_escalating_exit_loop_taker_fallback(tmp_path):
    """maker 超时后，升级到 taker 成功平仓"""
    import time
    from pcp_arbitrage import order_manager as om
    from pcp_arbitrage.config import AppConfig

    cfg = mock.MagicMock()
    cfg.exit_maker_chase_secs = 1      # 1 秒 maker 等待（测试用短时间）
    cfg.exit_taker_escalate_secs = 2   # 2 秒后切 taker

    call_count = 0

    async def fake_submit_and_poll(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return False, 0.0  # maker 失败
        return True, 10.0      # taker 成功

    with mock.patch.object(om, "_submit_and_poll_exit_legs_okx", side_effect=fake_submit_and_poll), \
         mock.patch.object(om, "_cancel_order", return_value=None), \
         mock.patch.object(om, "_fetch_order_book_top", return_value=(100.0, 101.0)):
        # started_at 设为 2 秒前，触发 taker
        started_at = time.monotonic() - 2.5
        filled, pnl = await om._escalating_exit_loop_okx(
            session=mock.AsyncMock(),
            failed_legs=[{"leg": "call", "inst_id": "BTC-USD-C", "side": "sell",
                          "entry_px": 90.0, "oid_db": 1, "last_px": 100.0}],
            qty=0.01, position_id=1, signal_id=None,
            api_key="k", secret="s", passphrase="p", is_paper=True,
            sqlite_path=str(tmp_path / "test.db"),
            exit_started_at=started_at, cfg=cfg,
        )
    assert filled is True
    assert pnl == 10.0
    assert call_count == 2  # 第1次 maker 失败，第2次 taker 成功
```

- [ ] **Step 2: 运行确认失败**

```bash
pytest tests/test_order_manager.py::test_escalating_exit_loop_taker_fallback -v
```

Expected: FAIL — `AttributeError: module has no attribute '_escalating_exit_loop_okx'`

- [ ] **Step 3: 实现 `_escalating_exit_loop_okx`**

按上方设计代码在 `order_manager.py` 中实现（放在 `_submit_and_poll_exit_legs_okx` 之后）。

同时修改 `_submit_and_poll_exit_legs_okx` 的 `poll_timeout` 参数：
- 函数签名加 `poll_timeout: float | None = _POLL_TIMEOUT_SEC`
- `_poll_order_fill` 调用时传入 `poll_timeout`
- `_poll_order_fill` 内部：`poll_timeout is None` 时去掉 deadline 检查（改为 `while True`）

- [ ] **Step 4: 修改 `_submit_exit_inner` 末尾的重试逻辑**

找到当前 `if not all_filled:` 的单次重试块，替换为：

```python
# 在三腿并行提交前记录起始时刻（放到 _submit_exit_inner 开头，本次仅展示关键改动）
# _exit_submit_start = time.monotonic()   ← 在函数开头添加这一行

if not all_filled:
    import time as _time
    now_iso = _utcnow_iso()
    with sqlite3.connect(sqlite_path) as _conn:
        _conn.execute(
            "UPDATE positions SET "
            "exit_started_at = COALESCE(exit_started_at, ?), "
            "exit_attempt_count = exit_attempt_count + 1, "
            "exit_last_attempt_at = ? "
            "WHERE id=?",
            (now_iso, now_iso, position_id),
        )
    _exit_active.add(position_id)
    try:
        failed_legs = [
            {
                "leg": exit_legs[i]["leg"],
                "inst_id": exit_legs[i]["inst_id"],
                "side": exit_legs[i]["side"],
                "entry_px": entry_fills[i]["entry_px"] if entry_fills else 0.0,
                "oid_db": exit_order_ids_db[i],
                "last_px": exit_legs[i]["px"],
                "exch_ord_id": exch_order_ids[i],
            }
            for i in range(3) if filled_data[i] is None
        ]
        loop_filled, loop_pnl = await _escalating_exit_loop_okx(
            session=session,
            failed_legs=failed_legs,
            qty=qty,
            position_id=position_id,
            signal_id=signal_id,
            api_key=api_key, secret=secret, passphrase=passphrase,
            is_paper=is_paper, sqlite_path=sqlite_path,
            # ⚠️ 用函数开头记录的时刻，让超时从第一次提交开始算
            exit_started_at=_exit_submit_start,
            cfg=cfg,
        )
        if loop_filled:
            all_filled = True
            realized_pnl += loop_pnl
    finally:
        _exit_active.discard(position_id)
```

**实现要点**：在 `_submit_exit_inner` 函数体**最开头**（查盘口之前）加一行：
```python
_exit_submit_start = time.monotonic()
```
这样 `taker_escalate_secs` 从第一次提交开始计时，而不是从循环开始计时。

- [ ] **Step 5: 在模块顶部添加内存锁**

```python
# 模块级，防止内联循环与守护协程双重操作同一仓位
_exit_active: set[int] = set()
```

- [ ] **Step 6: 对 Deribit 做同等修改**

`_submit_exit_deribit_inner` 中找到类似的失败处理，同样替换为 `_escalating_exit_loop_okx` 的 Deribit 版本（可复用同一函数，传入 Deribit session 即可，或单独实现 `_escalating_exit_loop_deribit`）。

- [ ] **Step 7: 运行测试**

```bash
pytest tests/test_order_manager.py -v -k "exit"
```

Expected: 全部 PASS

- [ ] **Step 8: Commit**

```bash
git add src/pcp_arbitrage/order_manager.py tests/test_order_manager.py
git commit -m "feat: escalating exit retry loop (maker→reprice→taker)"
```

---

## Task 4: 后台守护协程 exit_monitor_loop

**Files:**
- Modify: `src/pcp_arbitrage/position_tracker.py`

### 设计

```python
async def exit_monitor_loop(cfg: "AppConfig") -> None:
    """
    后台守护：每 exit_monitor_interval_secs 秒扫描一次 partial_failed 仓位，
    对不在 _exit_active 集合中的仓位重新发起升级平仓流程。
    """
    from pcp_arbitrage import order_manager as _om
    interval = cfg.exit_monitor_interval_secs  # 默认 10s
    while True:
        await asyncio.sleep(interval)
        try:
            with sqlite3.connect(cfg.sqlite_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM positions WHERE status='partial_failed'"
                ).fetchall()
            for row in rows:
                pid = row["id"]
                if pid in _om._exit_active:
                    continue  # 已有内联循环在处理

                logger.info("[exit_monitor] 接管 partial_failed 仓位 %d", pid)
                _om._exit_active.add(pid)

                async def _run_exit(pos_dict: dict, _pid: int = pid) -> None:
                    try:
                        await _om.submit_exit(pos_dict, cfg, cfg.sqlite_path)
                    finally:
                        # 无论成功/失败/异常，都清除锁，让下一轮扫描可以重新接管
                        _om._exit_active.discard(_pid)

                asyncio.create_task(
                    _run_exit(dict(row)),
                    name=f"exit_monitor_{pid}",
                )
        except Exception as exc:
            logger.warning("[exit_monitor] 扫描异常: %s", exc)
```

- [ ] **Step 1: 写测试**

```python
# tests/test_position_tracker.py（已存在或新建）
import asyncio, pytest, sqlite3, unittest.mock as mock
from pcp_arbitrage import position_tracker as pt, order_manager as om

@pytest.mark.asyncio
async def test_exit_monitor_picks_up_partial_failed(tmp_path, monkeypatch):
    """守护协程应接管 partial_failed 仓位并调用 submit_exit"""
    db_path = str(tmp_path / "test.db")
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY, status TEXT, exchange TEXT,
            exit_attempt_count INTEGER DEFAULT 0,
            exit_started_at TEXT, exit_last_attempt_at TEXT
        )
    """)
    con.execute("INSERT INTO positions (id, status, exchange) VALUES (1, 'partial_failed', 'okx')")
    con.commit()
    con.close()

    submitted = []
    async def fake_submit_exit(pos, cfg, path):
        submitted.append(pos["id"])

    cfg = mock.MagicMock()
    cfg.sqlite_path = db_path
    cfg.exit_monitor_interval_secs = 0.05  # 50ms

    monkeypatch.setattr(om, "submit_exit", fake_submit_exit)

    task = asyncio.create_task(pt.exit_monitor_loop(cfg))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert 1 in submitted

@pytest.mark.asyncio
async def test_exit_monitor_no_duplicate(tmp_path, monkeypatch):
    """同一 partial_failed 仓位已在 _exit_active 中，守护协程不重复提交"""
    db_path = str(tmp_path / "test.db")
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE positions (
            id INTEGER PRIMARY KEY, status TEXT, exchange TEXT,
            exit_attempt_count INTEGER DEFAULT 0,
            exit_started_at TEXT, exit_last_attempt_at TEXT
        )
    """)
    con.execute("INSERT INTO positions (id, status, exchange) VALUES (1, 'partial_failed', 'okx')")
    con.commit()
    con.close()

    submitted = []
    async def fake_submit_exit(pos, cfg, path):
        submitted.append(pos["id"])

    cfg = mock.MagicMock()
    cfg.sqlite_path = db_path
    cfg.exit_monitor_interval_secs = 0.05

    monkeypatch.setattr(om, "submit_exit", fake_submit_exit)
    om._exit_active.add(1)  # 模拟内联循环已在处理

    task = asyncio.create_task(pt.exit_monitor_loop(cfg))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        om._exit_active.discard(1)

    assert submitted == []  # 不应重复提交
```

- [ ] **Step 2: 运行确认失败**

```bash
pytest tests/test_position_tracker.py::test_exit_monitor_picks_up_partial_failed -v
```

Expected: FAIL — `AttributeError: module 'pcp_arbitrage.position_tracker' has no attribute 'exit_monitor_loop'`

- [ ] **Step 3: 实现 `exit_monitor_loop`**

按上方设计在 `position_tracker.py` 中实现。

- [ ] **Step 4: 在 `main.py` 中启动守护协程**

找到 `main.py` 中启动 `run_position_tracker_loop` 的地方（`asyncio.create_task`），在附近添加：

```python
asyncio.create_task(
    position_tracker.exit_monitor_loop(cfg),
    name="exit_monitor",
)
```

- [ ] **Step 5: 运行测试**

```bash
pytest tests/test_position_tracker.py -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pcp_arbitrage/position_tracker.py src/pcp_arbitrage/main.py tests/test_position_tracker.py
git commit -m "feat: exit_monitor_loop daemon picks up partial_failed positions"
```

---

## Task 5: 开仓按账户余额控制数量

**Files:**
- Modify: `src/pcp_arbitrage/order_manager.py`
- Modify: `src/pcp_arbitrage/account_fetcher.py`（如需添加同步余额查询辅助）

### 设计

在 `_submit_entry_inner`（OKX）和 `_submit_entry_deribit_inner`（Deribit）中，计算出 `qty` 后、提交订单前，插入余额校验：

```python
async def _check_and_cap_qty(
    qty: float,
    symbol: str,
    index_price_usdt: float,
    exchange_cfg: "ExchangeConfig",
    app_cfg: "AppConfig",
    lot_size: float,
    is_paper: bool = False,
) -> float:
    """
    根据账户余额约束截断 qty。
    - paper 模式下跳过检查（返回原始 qty）
    - 名义价值 = qty × index_price_usdt
    - 上限 = total_eq_usdt × entry_max_trade_pct
    - 预留 = total_eq_usdt × entry_reserve_pct（可用权益必须超过此值）
    返回调整后的 qty（向下取整到 lot_size）。如果余额不足则返回 0.0。
    """
    if is_paper:
        # paper trading 无真实余额，不做约束
        return qty

    from pcp_arbitrage import account_fetcher as _af

    bal = await _af.get_exchange_balance(exchange_cfg, app_cfg)
    if bal is None:
        logger.warning("[entry_sizing] 无法获取账户余额，跳过余额约束")
        return qty  # 无法获取时不限制，保持原逻辑

    total_eq = bal.get("total_eq_usdt", 0.0)
    adj_eq   = bal.get("adj_eq_usdt", 0.0)

    reserve_floor  = total_eq * app_cfg.entry_reserve_pct     # e.g. 10%
    trade_budget   = total_eq * app_cfg.entry_max_trade_pct   # e.g. 20%

    if adj_eq <= reserve_floor:
        logger.warning(
            "[entry_sizing] 可用权益 %.2f USDT ≤ 预留底线 %.2f USDT，跳过开仓",
            adj_eq, reserve_floor,
        )
        return 0.0

    usable = min(trade_budget, adj_eq - reserve_floor)
    max_qty = usable / index_price_usdt if index_price_usdt > 0 else 0.0

    if max_qty < qty:
        logger.info(
            "[entry_sizing] qty %.4f → %.4f（受余额约束: budget=%.2f USDT, index=%.2f）",
            qty, max_qty, usable, index_price_usdt,
        )

    capped = math.floor(max_qty / lot_size) * lot_size if lot_size > 0 else max_qty
    return min(qty, capped)
```

调用位置（OKX，约 L408 附近，lot_size 截断之后）：

```python
qty = await _check_and_cap_qty(
    qty=qty,
    symbol=triplet.symbol,
    index_price_usdt=signal.index_for_fee_usdt,  # 已有字段
    exchange_cfg=cfg.exchanges[exchange_name],
    app_cfg=cfg,
    lot_size=lot_size,
)
if qty <= 0:
    raise RuntimeError(f"[{tag}] 账户余额不足，跳过开仓")
```

Deribit 同理，`index_price_usdt` 用 `signal.index_for_fee_usdt`（已有）。

- [ ] **Step 1: 写测试**

```python
# tests/test_order_manager.py
@pytest.mark.asyncio
async def test_check_and_cap_qty_balance_limit():
    """当仓位名义价值超过 20%，qty 应被截断"""
    from pcp_arbitrage import order_manager as om, account_fetcher as af
    import unittest.mock as mock

    fake_balance = {
        "total_eq_usdt": 10_000.0,
        "adj_eq_usdt":   8_000.0,
    }
    cfg = mock.MagicMock()
    cfg.entry_max_trade_pct = 0.20   # 20% of 10000 = 2000 USDT budget
    cfg.entry_reserve_pct   = 0.10   # 10% of 10000 = 1000 USDT reserve

    with mock.patch.object(af, "get_exchange_balance", return_value=fake_balance):
        # index=50000, lot_size=0.01
        # max_qty = min(2000, 8000-1000) / 50000 = 2000/50000 = 0.04
        result = await om._check_and_cap_qty(
            qty=0.1,          # 原始深度
            symbol="BTC",
            index_price_usdt=50_000.0,
            exchange_cfg=mock.MagicMock(),
            app_cfg=cfg,
            lot_size=0.01,
        )
    assert result == pytest.approx(0.04, abs=1e-9)

@pytest.mark.asyncio
async def test_check_and_cap_qty_reserve_insufficient():
    """可用余额低于预留底线，应返回 0"""
    from pcp_arbitrage import order_manager as om, account_fetcher as af
    import unittest.mock as mock

    fake_balance = {"total_eq_usdt": 10_000.0, "adj_eq_usdt": 500.0}  # 低于 1000 预留
    cfg = mock.MagicMock()
    cfg.entry_max_trade_pct = 0.20
    cfg.entry_reserve_pct   = 0.10

    with mock.patch.object(af, "get_exchange_balance", return_value=fake_balance):
        result = await om._check_and_cap_qty(
            qty=0.1, symbol="BTC", index_price_usdt=50_000.0,
            exchange_cfg=mock.MagicMock(), app_cfg=cfg, lot_size=0.01,
        )
    assert result == 0.0
```

- [ ] **Step 2: 运行确认失败**

```bash
pytest tests/test_order_manager.py::test_check_and_cap_qty_balance_limit tests/test_order_manager.py::test_check_and_cap_qty_reserve_insufficient -v
```

Expected: FAIL

- [ ] **Step 3: 实现 `_check_and_cap_qty`**

按上方设计在 `order_manager.py` 中实现（放在 `submit_entry` 之前）。

- [ ] **Step 4: 在 `_submit_entry_inner` 和 `_submit_entry_deribit_inner` 中调用**

按设计注释的位置插入调用和 `qty <= 0` 检查。

- [ ] **Step 5: 运行所有测试**

```bash
pytest tests/test_order_manager.py -v
```

Expected: 全部 PASS

- [ ] **Step 6: Commit**

```bash
git add src/pcp_arbitrage/order_manager.py tests/test_order_manager.py
git commit -m "feat: cap entry qty by account balance (20% max, 10% reserve)"
```

---

## Task 6: 端到端验证

- [ ] **Step 1: 启动服务（paper trading 模式），确认无启动错误**

```bash
python -m pcp_arbitrage.main --config config.yaml 2>&1 | head -50
```

Expected: 无 `AttributeError` / `KeyError`，看到 `[exit_monitor]` 日志行启动。

- [ ] **Step 2: 手动触发一个 partial_failed 仓位，验证守护协程接管**

在 SQLite 中插入一条 `partial_failed` 记录，观察日志出现 `[exit_monitor] 接管` 字样，并在 10 秒内看到重试尝试。

- [ ] **Step 3: 确认余额约束日志**

开仓被触发时，检查日志是否有 `[entry_sizing]` 相关输出（无论截断与否）。

- [ ] **Step 4: 运行完整测试套件**

```bash
pytest tests/ -v --tb=short
```

Expected: 全部 PASS

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "test: end-to-end verification for exit retry and entry sizing"
```
