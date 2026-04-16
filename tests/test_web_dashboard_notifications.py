"""Tests for web dashboard notification queue mechanism."""

from __future__ import annotations

import asyncio

import pytest

from pcp_arbitrage import web_dashboard


def test_push_notification_enqueues_item() -> None:
    """push_notification() should put the dict into _notification_queue."""
    # Drain the queue first in case previous tests left items
    while not web_dashboard._notification_queue.empty():
        try:
            web_dashboard._notification_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

    notification = {
        "exchange": "Deribit",
        "label": "BTC-28MAR25-100000",
        "direction": "正向",
        "ann_pct": 12.5,
        "net_profit": 45.67,
    }
    web_dashboard.push_notification(notification)

    assert not web_dashboard._notification_queue.empty()
    item = web_dashboard._notification_queue.get_nowait()
    assert item == notification


def test_push_notification_multiple_items() -> None:
    """push_notification() can enqueue multiple notifications in order."""
    while not web_dashboard._notification_queue.empty():
        try:
            web_dashboard._notification_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

    n1 = {"exchange": "OKX", "label": "ETH-28MAR25-3000", "direction": "反向", "ann_pct": 8.0, "net_profit": 12.3}
    n2 = {"exchange": "Deribit", "label": "BTC-28JUN25-90000", "direction": "正向", "ann_pct": 15.0, "net_profit": 99.9}

    web_dashboard.push_notification(n1)
    web_dashboard.push_notification(n2)

    assert web_dashboard._notification_queue.qsize() == 2
    assert web_dashboard._notification_queue.get_nowait() == n1
    assert web_dashboard._notification_queue.get_nowait() == n2


@pytest.mark.asyncio
async def test_notification_push_loop_adds_type() -> None:
    """_notification_push_loop broadcasts with type='notification' prepended."""
    # Drain queue
    while not web_dashboard._notification_queue.empty():
        try:
            web_dashboard._notification_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

    broadcast_calls: list[str] = []

    async def fake_broadcast(msg: str) -> None:
        broadcast_calls.append(msg)

    notif = {
        "exchange": "Deribit",
        "label": "BTC-28MAR25-100000",
        "direction": "正向",
        "ann_pct": 20.0,
        "net_profit": 50.0,
    }
    web_dashboard._notification_queue.put_nowait(notif)

    # Manually run one iteration of the loop body
    item = await web_dashboard._notification_queue.get()
    import json
    broadcasted = json.dumps({"type": "notification", **item}, ensure_ascii=False)
    await fake_broadcast(broadcasted)

    assert len(broadcast_calls) == 1
    import json as _json
    parsed = _json.loads(broadcast_calls[0])
    assert parsed["type"] == "notification"
    assert parsed["exchange"] == "Deribit"
    assert parsed["label"] == "BTC-28MAR25-100000"
    assert parsed["direction"] == "正向"
    assert parsed["ann_pct"] == 20.0
    assert parsed["net_profit"] == 50.0


def test_update_single_account_balance_merges() -> None:
    """update_single_account_balance should merge one exchange without wiping others."""
    from pcp_arbitrage import web_dashboard
    web_dashboard._account_balances_cache.clear()
    web_dashboard.update_account_balances({"okx": {"total_eq_usdt": 1000.0}})

    web_dashboard.update_single_account_balance("deribit", {"total_eq_usdt": 500.0})

    assert web_dashboard._account_balances_cache["okx"]["total_eq_usdt"] == 1000.0
    assert web_dashboard._account_balances_cache["deribit"]["total_eq_usdt"] == 500.0


def test_build_payload_fallback_sessions_when_rows_empty(tmp_path) -> None:
    """内存 _rows 为空但 SQLite 中有已结束会话时，机会监控仍能显示行（与「历史有机会、监控空白」一致）。"""
    from pcp_arbitrage.db import close_opportunity_session, init_db, insert_opportunity_session
    from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard

    path = str(tmp_path / "o.db")
    init_db(path)
    sid = insert_opportunity_session(
        path,
        exchange="okx",
        contract="BTC-260417-74500",
        direction="反向",
        started_utc="2026-04-15T10:00:00.000Z",
    )
    close_opportunity_session(
        path,
        sid,
        ended_utc="2026-04-15T10:05:00.000Z",
        duration_sec=300.0,
        gross_usdt=10.0,
        fee_usdt=1.0,
        net_usdt=9.0,
        tradeable=2.0,
        ann_pct=3.0,
        ann_pct_max=3.5,
        days_to_exp=2.0,
    )
    dash = OpportunityDashboard(sqlite_path=path)
    assert not dash._rows
    payload = web_dashboard._build_payload(dash, full=True)
    assert len(payload["rows"]) >= 1
    row = payload["rows"][0]
    assert row["exchange"] == "okx"
    assert row["label"] == "BTC-260417-74500"
    assert row["direction"] == "反向"


def test_fetch_linked_opportunity_overlays_current_when_session_row_has_no_ann(tmp_path) -> None:
    """未 close 的 session 无 ann_pct；应从 opportunity_current（正向/反向）补全。"""
    import sqlite3

    from pcp_arbitrage.db import init_db, insert_opportunity_session

    path = str(tmp_path / "sess.db")
    init_db(path)
    sid = insert_opportunity_session(
        path,
        exchange="okx",
        contract="BTC-T-80000",
        direction="反向",
        started_utc="2026-01-01T00:00:00Z",
    )
    con = sqlite3.connect(path)
    con.execute(
        "INSERT INTO opportunity_current "
        "(exchange, contract, direction, updated_at, gross_usdt, fee_usdt, net_usdt, "
        " tradeable, ann_pct, ann_pct_max, days_to_exp, active) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,1)",
        (
            "okx",
            "BTC-T-80000",
            "反向",
            "2026-01-01T00:00:00Z",
            10.0,
            1.0,
            9.0,
            0.5,
            12.34,
            15.0,
            5.0,
        ),
    )
    con.commit()
    con.close()

    got = web_dashboard._fetch_linked_opportunity_session(
        path,
        sid,
        exchange="okx",
        contract="BTC-T-80000",
        direction_en="reverse",
    )
    assert got["ann_pct"] == pytest.approx(12.34)
    assert got["net_usdt"] == pytest.approx(9.0)
    assert got["tradeable"] == pytest.approx(0.5)


def test_build_payload_includes_recent_closed_positions_and_orders(tmp_path) -> None:
    """已 closed 的仓位应出现在 positions 中并带上 open+close 订单（此前仅合并 open 与 failed/partial_failed）。"""
    import sqlite3

    from pcp_arbitrage.db import init_db
    from pcp_arbitrage.opportunity_dashboard import OpportunityDashboard

    path = str(tmp_path / "closed_pos.db")
    init_db(path)
    con = sqlite3.connect(path)
    con.execute(
        "INSERT INTO positions (signal_id, exchange, symbol, expiry, strike, direction, status, "
        "realized_pnl_usdt, opened_at, closed_at, call_inst_id, put_inst_id, future_inst_id) "
        "VALUES (NULL, 'OKX', 'BTC', '260417', 74500, 'forward', 'closed', 1.0, "
        "'2026-04-01T00:00:00Z', '2026-04-15T00:00:00Z', 'BTC-USD-260417-74500-C', "
        "'BTC-USD-260417-74500-P', 'BTC-USD-260417')",
    )
    pid = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
    con.execute(
        "INSERT INTO orders (position_id, inst_id, leg, action, order_type, requested_order_type, "
        "side, qty, limit_px, status, submitted_at, filled_px) "
        "VALUES (?, 'BTC-USD-260417', 'future', 'open', 'limit', 'limit', 'sell', 1, 74000, "
        "'filled', '2026-04-01T00:01:00Z', 74000)",
        (pid,),
    )
    con.execute(
        "INSERT INTO orders (position_id, inst_id, leg, action, order_type, requested_order_type, "
        "side, qty, limit_px, status, submitted_at, filled_px) "
        "VALUES (?, 'BTC-USD-260417', 'future', 'close', 'limit', 'limit', 'buy', 1, 74005, "
        "'filled', '2026-04-15T00:02:00Z', 74005)",
        (pid,),
    )
    con.commit()
    con.close()

    web_dashboard.update_positions_cache([])
    dash = OpportunityDashboard(sqlite_path=path)
    payload = web_dashboard._build_payload(dash, full=True)
    pos_list = payload.get("positions") or []
    match = next((p for p in pos_list if p["id"] == pid), None)
    assert match is not None
    assert match["status"] == "closed"
    actions = {o["action"] for o in match.get("orders", [])}
    assert actions == {"open", "close"}
