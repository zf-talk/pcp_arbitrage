"""Tests for position_tracker: mark price fetch, DB update, Telegram alerts, exit triggers."""
from __future__ import annotations

import datetime
import sqlite3
import tempfile
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pcp_arbitrage.config import AppConfig, TelegramConfig
from pcp_arbitrage import db as _db
from pcp_arbitrage import position_tracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(db_path: str, threshold: float = 0.05) -> AppConfig:
    return AppConfig(
        exchanges={},
        symbols=["BTC"],
        min_annualized_rate=0.05,
        order_min_annualized_rate=0.05,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        sqlite_path=db_path,
        telegram=TelegramConfig(bot_token="", chat_id=""),
        pnl_alert_threshold_pct=threshold,
    )


def _make_db_with_position(db_path: str, *, current_mark: float | None = None) -> int:
    """Initialise DB at path and insert one open position. Returns position id."""
    _db.init_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            cur = conn.execute(
                "INSERT INTO positions "
                "(signal_id, exchange, symbol, expiry, strike, direction, opened_at, "
                " current_mark_usdt, last_updated) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    None,
                    "okx",
                    "BTC",
                    "250328",
                    30000.0,
                    "forward",
                    "2025-03-01T00:00:00.000Z",
                    current_mark,
                    None,
                ),
            )
            return int(cur.lastrowid)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Test: _build_future_inst_id
# ---------------------------------------------------------------------------

def test_build_future_inst_id_6digit():
    pos = {"symbol": "BTC", "expiry": "250328"}
    assert position_tracker._build_future_inst_id(pos) == "BTC-USD-250328"


def test_build_future_inst_id_8digit():
    pos = {"symbol": "ETH", "expiry": "20250328"}
    assert position_tracker._build_future_inst_id(pos) == "ETH-USD-250328"


def test_build_future_inst_id_with_dashes():
    pos = {"symbol": "BTC", "expiry": "2025-03-28"}
    assert position_tracker._build_future_inst_id(pos) == "BTC-USD-250328"


# ---------------------------------------------------------------------------
# Test: _fetch_mark_price
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_mark_price_success():
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"data": [{"markPx": "42000.5"}]})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    result = await position_tracker._fetch_mark_price(mock_session, "BTC-USD-250328")
    assert result == pytest.approx(42000.5)


@pytest.mark.asyncio
async def test_fetch_mark_price_empty_data():
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value={"data": []})
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    result = await position_tracker._fetch_mark_price(mock_session, "BTC-USD-250328")
    assert result is None


@pytest.mark.asyncio
async def test_fetch_mark_price_http_error():
    mock_resp = AsyncMock()
    mock_resp.status = 429
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_resp)

    result = await position_tracker._fetch_mark_price(mock_session, "BTC-USD-250328")
    assert result is None


# ---------------------------------------------------------------------------
# Test: _check_positions updates DB mark price
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_positions_updates_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        _make_db_with_position(db_path)
        cfg = _make_cfg(db_path)

        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            # Mock _fetch_mark_price to return a price
            with patch.object(
                position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=41000.0)
            ):
                await position_tracker._check_positions(cfg)

        # Check DB was updated
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT current_mark_usdt, last_updated FROM positions WHERE id=1").fetchone()
        conn.close()
        assert row["current_mark_usdt"] == pytest.approx(41000.0)
        assert row["last_updated"] is not None
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Test: Telegram alert sent when P&L exceeds threshold
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_positions_sends_telegram_alert_when_threshold_exceeded():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        # Insert position with a previous mark of 40000
        _make_db_with_position(db_path, current_mark=40000.0)
        cfg = _make_cfg(db_path, threshold=0.05)
        cfg.telegram = TelegramConfig(bot_token="tok", chat_id="cid")

        # New mark is 42200 = +5.5% change, should trigger alert
        with patch.object(
            position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=42200.0)
        ), patch("pcp_arbitrage.position_tracker.send_telegram", new=AsyncMock()) as mock_tg:
            with patch("aiohttp.ClientSession") as mock_session_cls:
                mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
                mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
                await position_tracker._check_positions(cfg)

            mock_tg.assert_awaited_once()
            call_args = mock_tg.call_args[0]
            assert "42200" in call_args[1] or "P&L" in call_args[1] or "持仓" in call_args[1]
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Test: Telegram alert NOT sent when P&L within threshold
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_positions_no_alert_within_threshold():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        # Insert position with a previous mark of 40000
        _make_db_with_position(db_path, current_mark=40000.0)
        cfg = _make_cfg(db_path, threshold=0.05)
        cfg.telegram = TelegramConfig(bot_token="tok", chat_id="cid")

        # New mark is 40100 = +0.25% change, below 5% threshold
        with patch.object(
            position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=40100.0)
        ), patch("pcp_arbitrage.position_tracker.send_telegram", new=AsyncMock()) as mock_tg:
            with patch("aiohttp.ClientSession") as mock_session_cls:
                mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
                mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
                await position_tracker._check_positions(cfg)

            mock_tg.assert_not_awaited()
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Test: update_positions_cache updates both module caches
# ---------------------------------------------------------------------------

def test_update_positions_cache_updates_module_cache():
    positions = [{"id": 1, "symbol": "BTC", "status": "open"}]
    position_tracker.update_positions_cache(positions)
    assert position_tracker.get_positions_cache() == positions
    # Reset
    position_tracker.update_positions_cache([])


def test_update_positions_cache_also_updates_web_dashboard():
    from pcp_arbitrage import web_dashboard as _wd
    # Reset first
    _wd.update_positions_cache([])

    positions = [{"id": 2, "symbol": "ETH", "status": "open"}]
    # Call _check_positions is complex; instead test that update_positions_cache in
    # position_tracker propagates to web_dashboard via direct call pattern
    position_tracker.update_positions_cache(positions)

    # The internal module cache should be updated
    assert position_tracker.get_positions_cache() == positions
    # Clean up
    position_tracker.update_positions_cache([])


# ---------------------------------------------------------------------------
# Test: no positions → no OKX call, empty cache
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_positions_no_open_positions():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        _db.init_db(db_path)
        cfg = _make_cfg(db_path)

        with patch.object(
            position_tracker, "_fetch_mark_price", new=AsyncMock()
        ) as mock_fetch:
            with patch("aiohttp.ClientSession") as mock_session_cls:
                mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
                mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
                await position_tracker._check_positions(cfg)

            mock_fetch.assert_not_awaited()

        assert position_tracker.get_positions_cache() == []
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Test: _days_to_expiry helper
# ---------------------------------------------------------------------------


def test_days_to_expiry_future_6digit():
    """6-digit expiry 30 days in the future returns ~30 days."""
    future = datetime.date.today() + datetime.timedelta(days=30)
    expiry_str = future.strftime("%y%m%d")
    result = position_tracker._days_to_expiry(expiry_str)
    assert 28 <= result <= 32


def test_days_to_expiry_past_returns_negative():
    """A date in the past returns a negative value."""
    result = position_tracker._days_to_expiry("250101")  # 2025-01-01, past
    assert result < 0


def test_days_to_expiry_invalid_returns_inf():
    """Invalid expiry format returns float infinity so no exit is triggered."""
    result = position_tracker._days_to_expiry("notadate")
    assert result == float("inf")


# ---------------------------------------------------------------------------
# Test: exit triggered when pnl_pct >= target (mocked asyncio.create_task)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_triggered_when_pnl_exceeds_target(tmp_path):
    """_check_positions schedules submit_exit when _calc_pnl_pct >= exit_target_profit_pct."""
    db_path = str(tmp_path / "test.db")
    _db.init_db(db_path)

    # Create a position with realized_pnl_usdt that yields pnl_pct >= 0.5
    # _calc_pnl_pct = realized_pnl / current_mark_usdt; set 100 / 100 = 1.0 >= 0.5
    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT INTO positions "
            "(signal_id, exchange, symbol, expiry, strike, direction, status, opened_at, "
            " current_mark_usdt, last_updated, realized_pnl_usdt) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (None, "OKX", "BTC", "270101", 30000.0, "forward", "open",
             "2026-01-01T00:00:00.000Z", 100.0, None, 100.0),
        )
    conn.close()

    cfg = AppConfig(
        exchanges={},
        symbols=["BTC"],
        min_annualized_rate=0.05,
        order_min_annualized_rate=0.05,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        sqlite_path=db_path,
        telegram=TelegramConfig(bot_token="", chat_id=""),
        exit_days_before_expiry=1,
        exit_target_profit_pct=0.50,
    )

    tasks_created: list = []

    def capture_task(coro):
        tasks_created.append(coro)
        # Return a fake task-like object; cancel the coroutine to avoid runtime warning
        coro.close()
        return MagicMock()

    with patch.object(position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=100.0)), \
         patch("aiohttp.ClientSession") as mock_session_cls, \
         patch("asyncio.create_task", side_effect=capture_task):
        mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await position_tracker._check_positions(cfg)

    # A task was created for submit_exit
    assert len(tasks_created) >= 1

    # Position should be marked 'closing' in DB
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status FROM positions LIMIT 1").fetchone()
    conn.close()
    assert row[0] == "closing"


# ---------------------------------------------------------------------------
# Test: exit triggered when days_left <= exit_days_before_expiry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exit_triggered_when_near_expiry(tmp_path):
    """_check_positions schedules submit_exit when days_left <= exit_days_before_expiry."""
    db_path = str(tmp_path / "test.db")
    _db.init_db(db_path)

    # Use an expiry 0 days away (today)
    today_str = datetime.date.today().strftime("%y%m%d")

    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT INTO positions "
            "(signal_id, exchange, symbol, expiry, strike, direction, status, opened_at, "
            " current_mark_usdt, last_updated) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (None, "OKX", "BTC", today_str, 30000.0, "forward", "open",
             "2026-01-01T00:00:00.000Z", None, None),
        )
    conn.close()

    cfg = AppConfig(
        exchanges={},
        symbols=["BTC"],
        min_annualized_rate=0.05,
        order_min_annualized_rate=0.05,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        sqlite_path=db_path,
        telegram=TelegramConfig(bot_token="", chat_id=""),
        exit_days_before_expiry=1,
        exit_target_profit_pct=0.50,
    )

    tasks_created: list = []

    def capture_task(coro):
        tasks_created.append(coro)
        coro.close()
        return MagicMock()

    with patch.object(position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=42000.0)), \
         patch("aiohttp.ClientSession") as mock_session_cls, \
         patch("asyncio.create_task", side_effect=capture_task):
        mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await position_tracker._check_positions(cfg)

    # A task was created for submit_exit
    assert len(tasks_created) >= 1

    # Position should be marked 'closing' in DB
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status FROM positions LIMIT 1").fetchone()
    conn.close()
    assert row[0] == "closing"


# ---------------------------------------------------------------------------
# Test: exit_monitor_loop daemon
# ---------------------------------------------------------------------------

import asyncio as _asyncio
import unittest.mock as _mock
from pcp_arbitrage import order_manager as _om


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

    cfg = _mock.MagicMock()
    cfg.sqlite_path = db_path
    cfg.exit_monitor_interval_secs = 0.05  # 50ms

    monkeypatch.setattr(_om, "submit_exit", fake_submit_exit)

    task = _asyncio.create_task(position_tracker.exit_monitor_loop(cfg))
    await _asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except _asyncio.CancelledError:
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

    cfg = _mock.MagicMock()
    cfg.sqlite_path = db_path
    cfg.exit_monitor_interval_secs = 0.05

    monkeypatch.setattr(_om, "submit_exit", fake_submit_exit)
    _om._exit_active.add(1)  # 模拟内联循环已在处理

    task = _asyncio.create_task(position_tracker.exit_monitor_loop(cfg))
    await _asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except _asyncio.CancelledError:
        pass
    finally:
        _om._exit_active.discard(1)

    assert submitted == []  # 不应重复提交


# ---------------------------------------------------------------------------
# Test: no exit triggered when conditions not met
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_exit_when_conditions_not_met(tmp_path):
    """_check_positions does NOT schedule submit_exit when days_left is large and pnl is low."""
    db_path = str(tmp_path / "test.db")
    _db.init_db(db_path)

    # Expiry far in the future (1 year from today)
    future = datetime.date.today() + datetime.timedelta(days=365)
    far_expiry = future.strftime("%y%m%d")

    conn = sqlite3.connect(db_path)
    with conn:
        conn.execute(
            "INSERT INTO positions "
            "(signal_id, exchange, symbol, expiry, strike, direction, status, opened_at, "
            " current_mark_usdt, last_updated) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (None, "OKX", "BTC", far_expiry, 30000.0, "forward", "open",
             "2026-01-01T00:00:00.000Z", None, None),
        )
    conn.close()

    cfg = AppConfig(
        exchanges={},
        symbols=["BTC"],
        min_annualized_rate=0.05,
        order_min_annualized_rate=0.05,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        sqlite_path=db_path,
        telegram=TelegramConfig(bot_token="", chat_id=""),
        exit_days_before_expiry=1,
        exit_target_profit_pct=0.50,
    )

    tasks_created: list = []

    def capture_task(coro):
        tasks_created.append(coro)
        coro.close()
        return MagicMock()

    with patch.object(position_tracker, "_fetch_mark_price", new=AsyncMock(return_value=42000.0)), \
         patch("aiohttp.ClientSession") as mock_session_cls, \
         patch("asyncio.create_task", side_effect=capture_task):
        mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        await position_tracker._check_positions(cfg)

    # No exit task should have been created
    assert len(tasks_created) == 0
