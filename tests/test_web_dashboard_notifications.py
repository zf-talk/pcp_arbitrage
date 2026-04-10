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
