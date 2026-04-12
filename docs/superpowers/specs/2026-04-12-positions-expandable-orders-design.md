# 持仓页面展开订单 — 设计规格

**日期：** 2026-04-12
**状态：** 已批准

---

## 概述

在 Web Dashboard 的"持仓"标签页中，展示所有状态的仓位（包括 failed/closed），并支持点击行展开查看该仓位的订单明细。

---

## 数据层（`db.py`）

### 新增函数（不替换现有函数）

> 注意：`db.py` 中已有 `get_position_orders(conn, position_id, action="open")` 供 `order_manager` 使用，以下两个函数为**新增**，不修改现有函数。

**`list_all_positions(path, limit=100) -> list[dict]`**
- 查询 `positions` 表所有状态的记录，自行开关 SQLite 连接
- 排序：open/pending/opening/closing 优先（使用 CASE 表达式），然后 `opened_at DESC`：
  ```sql
  ORDER BY
    CASE WHEN status IN ('open','pending','opening','closing') THEN 0 ELSE 1 END ASC,
    opened_at DESC
  ```
- 返回字段：id, exchange, symbol, expiry, strike, direction, status,
  realized_pnl_usdt, opened_at, closed_at, current_mark_usdt, last_updated

**`list_position_orders(path, position_id) -> list[dict]`**
- 查询 `orders` 表中 `position_id=?` 的所有记录（不限 action），自行开关 SQLite 连接
- 排序：`submitted_at ASC`
- 返回字段：id, leg, action, order_type, side, qty, limit_px, filled_px, status,
  exchange_order_id, fee_type, actual_fee_usdt, submitted_at, filled_at

---

## 后端（`web_dashboard.py`）

### WebSocket payload 变更
- `_build_payload` 中的 `positions` 字段改为调用 `list_all_positions(sqlite_path, limit=100)`（直接读 DB），替代仅依赖 `_open_positions_cache`
- `_open_positions_cache` 继续由 `position_tracker` 维护，供其内部逻辑使用，不影响 web payload
- payload 中每个 position 对象包含 `realized_pnl_usdt` 和 `closed_at` 字段（可为 null）

### 新增 REST endpoint
```
GET /api/position/<id>/orders
```
- 路径参数：`id`（position id，整数）
- 错误处理：
  - sqlite_path 未配置 → 503
  - id 非整数 → 400
  - position 不存在但 id 合法 → 返回 `{"orders": []}` (空列表，不返回 404；空列表与"有仓位但无订单"语义一致)
- 成功：返回 `{"orders": [...]}`

---

## 前端（`dashboard.html`）

### 持仓表格列（共 10 列）
| # | 列 | 说明 |
|---|---|---|
| 1 | ▶/▼ | 展开指示，最左列 |
| 2 | ID | 仓位 ID |
| 3 | 交易所 | exchange |
| 4 | 币种 | symbol |
| 5 | 到期日 | expiry |
| 6 | 行权价 | strike |
| 7 | 方向 | direction |
| 8 | 标记价 | current_mark_usdt |
| 9 | 开仓时间 | opened_at |
| 10 | 状态 | status，带颜色 |

> 备注：`realized_pnl_usdt` 字段在展开的订单子行标题处显示（仓位级别 PnL），不单独占一列。

> **colspan 更新**：持仓面板内所有静态 empty-state `<td colspan>` 需从 9 改为 10。

**状态颜色：**
- `open` → 绿（`--green`）
- `closing` / `pending` / `opening` → 黄（`--yellow`）
- `failed` / `partial_failed` → 红（`--red`）
- `closed` → 灰（`--text-muted`）

### 过滤控件
- 持仓面板右上角添加"只看持仓中"复选框（默认不勾选）
- 勾选后：只显示 status in (open, pending, opening, closing) 的仓位

### 点击展开逻辑
1. 点击仓位行 → 切换该仓位 ID 的展开/折叠状态
2. 展开状态记录在 `_expandedPositionIds`（Set），折叠时从 Set 中移除
3. 首次展开：发 `GET /api/position/<id>/orders`，请求期间在子行显示 loading 文字
4. **请求成功**：结果缓存到 `_positionOrdersCache[id]`（object），子行显示订单表格
5. **请求失败**：子行显示错误提示（"加载失败，点击重试"），**不写入缓存**，下次展开会重新请求
6. 结果已缓存时直接展开，不重复请求

### WebSocket 重渲染时保持展开状态
- `renderPositions()` 在每次 WebSocket 广播时调用（约 2 秒一次）
- 渲染流程：
  1. 生成仓位行 HTML
  2. 对 `_expandedPositionIds` 中的每个 id，若有缓存订单数据则在对应仓位行后插入 detail 子行
  3. 整体写入 `tbody.innerHTML`（或逐行 DOM 操作，推荐整体写入再补插子行）

### 订单子行结构
```html
<tr class="order-detail-row" data-pos-id="<id>">
  <td colspan="10">
    <!-- 仓位 realized_pnl_usdt（如有） -->
    <!-- 订单子表格 -->
  </td>
</tr>
```

### 订单子表格列
| 腿位 | 动作 | 类型 | 方向 | 数量 | 限价 | 成交价 | 状态 | 交易所单号 | 提交时间 | 成交时间 |
|---|---|---|---|---|---|---|---|---|---|---|
| leg | action | order_type | side | qty | limit_px | filled_px | status | exchange_order_id | submitted_at | filled_at |

---

## 变更文件清单

1. `src/pcp_arbitrage/db.py` — 新增 `list_all_positions`, `list_position_orders`
2. `src/pcp_arbitrage/web_dashboard.py` — 更新 `_build_payload`，注册 `/api/position/<id>/orders` endpoint
3. `src/pcp_arbitrage/templates/dashboard.html` — 更新 `renderPositions()`（保持展开状态、展开逻辑、订单子表格），更新 empty-state colspan

---

## 不在范围内

- 订单的实时刷新（展开后不自动更新，需重新点击刷新）
- 仓位分页（limit=100 足够当前规模）
- 订单的增删改操作
