"""Tests for order_manager and related DB helpers."""
from __future__ import annotations

import sqlite3
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pcp_arbitrage import db as _db
from pcp_arbitrage.config import AppConfig, ExchangeConfig, TelegramConfig
from pcp_arbitrage.models import Triplet
from pcp_arbitrage.pcp_calculator import ArbitrageSignal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """Return an in-memory SQLite connection with the schema initialised."""
    _db.init_db(":memory:")
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            expiry TEXT NOT NULL,
            strike REAL NOT NULL,
            direction TEXT NOT NULL,
            target_state TEXT NOT NULL DEFAULT 'open',
            status TEXT NOT NULL DEFAULT 'open',
            realized_pnl_usdt REAL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            current_mark_usdt REAL,
            last_updated TEXT,
            call_inst_id TEXT,
            put_inst_id TEXT,
            future_inst_id TEXT,
            last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            position_id INTEGER NOT NULL REFERENCES positions(id),
            inst_id TEXT,
            leg TEXT NOT NULL,
            action TEXT NOT NULL,
            order_type TEXT NOT NULL DEFAULT 'limit',
            requested_order_type TEXT NOT NULL DEFAULT 'limit',
            side TEXT NOT NULL,
            exchange_order_id TEXT,
            limit_px REAL NOT NULL,
            filled_px REAL,
            filled_qty REAL,
            qty REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            fee_type TEXT,
            actual_fee REAL,
            actual_fee_usdt REAL,
            fee_ccy TEXT,
            submitted_at TEXT NOT NULL,
            filled_at TEXT,
            last_error TEXT
        );
    """)
    conn.commit()
    return conn


def _make_triplet(exchange: str = "okx") -> Triplet:
    return Triplet(
        exchange=exchange,
        symbol="BTC",
        expiry="250425",
        strike=80000.0,
        call_id="BTC-USD-250425-80000-C",
        put_id="BTC-USD-250425-80000-P",
        future_id="BTC-USD-250425",
    )


def _make_signal(triplet: Triplet, direction: str = "forward") -> ArbitrageSignal:
    return ArbitrageSignal(
        direction=direction,
        triplet=triplet,
        call_price=100.0,
        put_price=90.0,
        future_price=80000.0,
        call_price_coin=0.00125,
        put_price_coin=0.001125,
        spot_price=80000.0,
        gross_profit=55.0,
        total_fee=5.0,
        net_profit=50.0,
        annualized_return=0.10,
        days_to_expiry=30.0,
        index_for_fee_usdt=80000.0,
        tradeable_qty=0.01,
        depth_contracts=1.0,
    )


def _make_cfg() -> AppConfig:
    return AppConfig(
        exchanges={
            "OKX": ExchangeConfig(
                name="OKX",
                enabled=True,
                margin_type="coin",
                api_key="test_key",
                secret_key="test_secret",
                passphrase="test_pass",
            )
        },
        symbols=["BTC"],
        min_annualized_rate=0.01,
        order_min_annualized_rate=0.05,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        telegram=TelegramConfig(bot_token="", chat_id=""),
        sqlite_path=":memory:",
    )


# ---------------------------------------------------------------------------
# DB helper tests (in-memory SQLite)
# ---------------------------------------------------------------------------

class TestDbHelpers:
    def test_create_position_returns_id(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        assert isinstance(pid, int)
        assert pid > 0

    def test_create_order_returns_id(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        oid = _db.create_order(
            conn,
            position_id=pid,
            leg="call",
            action="open",
            side="buy",
            limit_px=100.0,
            qty=2.0,
        )
        assert isinstance(oid, int)
        assert oid > 0

    def test_update_order_status(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        oid = _db.create_order(
            conn,
            position_id=pid,
            leg="call",
            action="open",
            side="buy",
            limit_px=100.0,
            qty=2.0,
        )
        conn.commit()
        _db.update_order_status(conn, oid, "filled", filled_px=99.5, filled_at="2026-04-09T00:00:00Z")
        conn.commit()
        row = conn.execute("SELECT status, filled_px FROM orders WHERE id=?", (oid,)).fetchone()
        assert row[0] == "filled"
        assert row[1] == 99.5

    def test_update_position_status(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=42,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="reverse",
        )
        conn.commit()
        _db.update_position_status(
            conn, pid, "partial_failed", last_error="test error 原因",
        )
        conn.commit()
        row = conn.execute(
            "SELECT status, last_error FROM positions WHERE id=?",
            (pid,),
        ).fetchone()
        assert row[0] == "partial_failed"
        assert row[1] == "test error 原因"
        _db.update_position_status(conn, pid, "open")
        conn.commit()
        row2 = conn.execute(
            "SELECT status, last_error FROM positions WHERE id=?",
            (pid,),
        ).fetchone()
        assert row2[0] == "open"
        assert row2[1] is None

    def test_has_open_position_false_initially(self):
        conn = _make_db()
        result = _db.has_open_position(conn, "OKX", "BTC", "250425", 80000.0, "forward")
        assert result is False

    def test_has_open_position_false_while_opening(self):
        conn = _make_db()
        _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        conn.commit()
        assert _db.has_open_position(conn, "OKX", "BTC", "250425", 80000.0, "forward") is False
        assert (
            _db.blocking_entry_status(conn, "OKX", "BTC", "250425", 80000.0, "forward")
            == "opening"
        )

    def test_has_open_position_true_after_marked_open(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        conn.commit()
        _db.update_position_status(conn, pid, "open")
        conn.commit()
        assert _db.has_open_position(conn, "OKX", "BTC", "250425", 80000.0, "forward") is True
        assert (
            _db.blocking_entry_status(conn, "OKX", "BTC", "250425", 80000.0, "forward")
            == "open"
        )

    def test_has_open_position_different_direction_not_blocked(self):
        conn = _make_db()
        _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        conn.commit()
        # reverse direction should NOT be blocked
        result = _db.has_open_position(conn, "OKX", "BTC", "250425", 80000.0, "reverse")
        assert result is False

    def test_has_open_position_closed_not_blocking(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        conn.commit()
        _db.update_position_status(conn, pid, "partial_failed")
        conn.commit()
        result = _db.has_open_position(conn, "OKX", "BTC", "250425", 80000.0, "forward")
        assert result is False
        assert (
            _db.blocking_entry_status(conn, "OKX", "BTC", "250425", 80000.0, "forward") is None
        )

    def test_get_open_positions_empty(self):
        conn = _make_db()
        assert _db.get_open_positions(conn) == []

    def test_get_open_positions_returns_open(self):
        conn = _make_db()
        pid = _db.create_position(
            conn,
            signal_id=None,
            exchange="OKX",
            symbol="BTC",
            expiry="250425",
            strike=80000.0,
            direction="forward",
        )
        conn.commit()
        assert _db.get_open_positions(conn) == []
        _db.update_position_status(conn, pid, "open")
        conn.commit()
        rows = _db.get_open_positions(conn)
        assert len(rows) == 1
        assert rows[0]["exchange"] == "OKX"
        assert rows[0]["status"] == "open"


# ---------------------------------------------------------------------------
# order_manager tests (mocked aiohttp)
# ---------------------------------------------------------------------------

def _make_order_response(ord_id: str) -> dict:
    return {"code": "0", "data": [{"ordId": ord_id, "sCode": "0"}]}


def _make_query_response(ord_id: str, state: str = "filled", avg_px: str = "100.5") -> dict:
    return {"code": "0", "data": [{"ordId": ord_id, "state": state, "avgPx": avg_px, "px": avg_px}]}


def test_okx_contracts_from_qty_btc_option_missing_ctmult():
    """缺 ctMult 时不得默认 1.0，否则 0.01 BTC 名义会被算成 0.01 张（应为 1 张）。"""
    from pcp_arbitrage import order_manager as om

    inst = {
        "instType": "OPTION",
        "instId": "BTC-USD-260417-74500-C",
        "ctValCcy": "BTC",
        "ctVal": "1",
    }
    contracts = om._okx_contracts_from_qty_btc(inst, 0.01, future_mark_usd=None)
    assert abs(contracts - 1.0) < 1e-9


def test_okx_contracts_from_qty_btc_option_ctmult_one_implies_wrong_coin_per():
    """接口若返回 ctMult=1（与 OKX BTC 期权 0.01 BTC/张 不符），按 0.01 BTC/张 修正。"""
    from pcp_arbitrage import order_manager as om

    inst = {
        "instType": "OPTION",
        "instId": "BTC-USD-260417-74500-C",
        "ctValCcy": "BTC",
        "ctVal": "1",
        "ctMult": "1",
    }
    contracts = om._okx_contracts_from_qty_btc(inst, 0.01, future_mark_usd=None)
    assert abs(contracts - 1.0) < 1e-9


def _build_mock_session(
    post_order_ids: list[str] | None = None,
    query_states: list[str] | None = None,
    raise_on_post: bool = False,
) -> MagicMock:
    """Build a mock aiohttp.ClientSession context manager."""
    post_order_ids = post_order_ids or ["oid_call", "oid_put", "oid_future"]
    query_states = query_states or ["filled", "filled", "filled"]

    # POST /api/v5/trade/order — cycles through post_order_ids
    post_call_count = [0]

    def _make_post_cm(ord_id: str | None, raise_exc: bool = False):
        cm = MagicMock()
        if raise_exc:
            cm.__aenter__ = AsyncMock(side_effect=RuntimeError("submit failed"))
        else:
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(return_value=_make_order_response(ord_id or ""))
            cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    def _make_get_cm(state: str, ord_id: str):
        cm = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = AsyncMock(return_value=_make_query_response(ord_id, state))
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    session = MagicMock()

    def _post_side_effect(*args, **kwargs):
        idx = post_call_count[0] % len(post_order_ids)
        post_call_count[0] += 1
        return _make_post_cm(post_order_ids[idx], raise_on_post and idx == 0)

    def _get_side_effect(*args, **kwargs):
        path = str(args[0]) if args else ""
        if "max-size" in path:
            cm = MagicMock()
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json = AsyncMock(
                return_value={
                    "code": "0",
                    "msg": "",
                    "data": [
                        {
                            "instId": kwargs.get("params", {}).get("instId", ""),
                            "maxBuy": "9999",
                            "maxSell": "9999",
                        }
                    ],
                }
            )
            cm.__aenter__ = AsyncMock(return_value=resp)
            cm.__aexit__ = AsyncMock(return_value=False)
            return cm
        # Figure out which leg this is by ordId param
        params = kwargs.get("params", {})
        ord_id = params.get("ordId", "")
        try:
            idx = post_order_ids.index(ord_id)
            state = query_states[idx] if idx < len(query_states) else "filled"
        except ValueError:
            state = "filled"
        return _make_get_cm(state, ord_id)

    session.post = MagicMock(side_effect=_post_side_effect)
    session.get = MagicMock(side_effect=_get_side_effect)

    # session as async context manager
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session)
    session_cm.__aexit__ = AsyncMock(return_value=False)
    return session_cm


async def _mock_okx_get_instrument_for_tests(_sess, inst_id: str):
    """与生产 OKX BTC-USD 期权 / 反向交割期货字段一致，用于下单张数换算测试。"""
    if inst_id.endswith("-C") or inst_id.endswith("-P"):
        return {
            "instId": inst_id,
            "instType": "OPTION",
            "ctVal": "1",
            "ctMult": "0.01",
            "ctValCcy": "BTC",
            "lotSz": "1",
            "minSz": "1",
        }
    return {
        "instId": inst_id,
        "instType": "FUTURES",
        "ctType": "inverse",
        "ctVal": "100",
        "ctValCcy": "USD",
        "lotSz": "0.1",
        "minSz": "0.1",
    }


class TestOrderManagerSubmitEntry:
    """Integration-style tests with mocked aiohttp and a real in-memory DB."""

    @pytest.fixture(autouse=True)
    def _mock_okx_preflight_max_size(self):
        """避免测试请求真实 max-size；生产由 _okx_preflight_max_size_legs 校验。"""
        with patch(
            "pcp_arbitrage.order_manager._okx_preflight_max_size_legs",
            new=AsyncMock(return_value=None),
        ), patch(
            "pcp_arbitrage.order_manager._okx_backfill_open_entry_orders_detail",
            new=AsyncMock(return_value=None),
        ):
            yield

    @pytest.mark.asyncio
    async def test_has_open_position_guard_prevents_double_entry(self, tmp_path):
        """submit_entry returns early without calling OKX if open position exists."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet()
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        # Pre-create an open position
        conn = sqlite3.connect(db_path)
        _db.create_position(
            conn,
            signal_id=None,
            exchange="okx",
            symbol=triplet.symbol,
            expiry=triplet.expiry,
            strike=triplet.strike,
            direction=signal.direction,
        )
        conn.commit()
        conn.close()

        from pcp_arbitrage import order_manager as om
        with patch("aiohttp.ClientSession") as mock_session_cls:
            await om.submit_entry(triplet, signal, None, cfg, db_path)
            # OKX session should never have been entered (no orders placed)
            mock_session_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_submit_entry_calls_okx_post_for_3_legs(self, tmp_path):
        """submit_entry places 3 POST requests to /api/v5/trade/order."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet()
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        session_mock = _build_mock_session()
        underlying_session = None

        async def _get_session(*args, **kwargs):
            nonlocal underlying_session
            s = await session_mock.__aenter__()
            underlying_session = s
            return s

        from pcp_arbitrage import order_manager as om

        with patch.object(om, "_okx_get_instrument", _mock_okx_get_instrument_for_tests), \
             patch("aiohttp.ClientSession", return_value=session_mock):
            await om.submit_entry(triplet, signal, None, cfg, db_path)

        session = await session_mock.__aenter__()
        assert session.post.call_count == 3

    @pytest.mark.asyncio
    async def test_all_filled_sets_position_status_open(self, tmp_path):
        """When all 3 orders fill, position status remains 'open' in DB."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet()
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        from pcp_arbitrage import order_manager as om

        async def mock_place(session, *, inst_id, td_mode, side, ord_type, px, sz,
                              api_key, secret, passphrase, **kwargs):
            return f"ord_{inst_id}"

        async def mock_poll(session, *, inst_id, ord_id, api_key, secret, passphrase,
                             poll_interval=2.0, poll_timeout=30.0, **kwargs):
            return {"ordId": ord_id, "state": "filled", "avgPx": "100.0", "px": "100.0"}

        with patch.object(om, "_okx_get_instrument", _mock_okx_get_instrument_for_tests), \
             patch.object(om, "_place_order", mock_place), \
             patch.object(om, "_poll_order_fill", mock_poll), \
             patch("aiohttp.ClientSession") as mock_cls:
            mock_session_cm = MagicMock()
            mock_session_inner = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_inner)
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_session_cm

            await om.submit_entry(triplet, signal, None, cfg, db_path)

        # Check position in DB
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT status FROM positions LIMIT 1").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "open"

    @pytest.mark.asyncio
    async def test_partial_fill_sets_partial_failed(self, tmp_path):
        """When some orders don't fill, position status is set to 'partial_failed'."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet()
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        from pcp_arbitrage import order_manager as om

        async def mock_place(session, *, inst_id, td_mode, side, ord_type, px, sz,
                              api_key, secret, passphrase, **kwargs):
            return f"ord_{inst_id}"

        call_count = [0]

        async def mock_poll(session, *, inst_id, ord_id, api_key, secret, passphrase,
                             poll_interval=2.0, poll_timeout=30.0, **kwargs):
            call_count[0] += 1
            # First leg fills, others time out
            if call_count[0] == 1:
                return {"ordId": ord_id, "state": "filled", "avgPx": "100.0", "px": "100.0"}
            return None  # timeout

        async def mock_cancel(session, *, inst_id, ord_id, api_key, secret, passphrase, **kwargs):
            pass

        with patch.object(om, "_okx_get_instrument", _mock_okx_get_instrument_for_tests), \
             patch.object(om, "_place_order", mock_place), \
             patch.object(om, "_poll_order_fill", mock_poll), \
             patch.object(om, "_cancel_order", mock_cancel), \
             patch.object(om, "_fetch_order_book_top", new=AsyncMock(return_value=(100.0, 101.0))), \
             patch("aiohttp.ClientSession") as mock_cls:
            mock_session_cm = MagicMock()
            mock_session_inner = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_inner)
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_session_cm

            await om.submit_entry(triplet, signal, None, cfg, db_path)

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT status FROM positions LIMIT 1").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "partial_failed"

    @pytest.mark.asyncio
    async def test_no_okx_config_returns_early(self, tmp_path):
        """submit_entry returns without error if OKX exchange not in config."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet()
        signal = _make_signal(triplet)
        cfg = AppConfig(
            exchanges={},  # No OKX
            symbols=["BTC"],
            min_annualized_rate=0.01,
            order_min_annualized_rate=0.05,
            atm_range=0.1,
            min_days_to_expiry=1.0,
            stale_threshold_ms=5000,
            lot_size={"BTC": 0.01},
            telegram=TelegramConfig(bot_token="", chat_id=""),
            sqlite_path=db_path,
        )

        from pcp_arbitrage import order_manager as om
        with patch("aiohttp.ClientSession") as mock_cls:
            await om.submit_entry(triplet, signal, None, cfg, db_path)
            mock_cls.assert_not_called()


@pytest.mark.asyncio
async def test_okx_preflight_max_size_raises_when_below_sell_cap():
    """计划张数超过 OKX max-size 返回的卖侧上限时拒绝开仓。"""
    from pcp_arbitrage import order_manager as om

    legs = [
        {"leg": "call", "inst_id": "BTC-USD-250425-80000-C", "side": "buy", "px": 0.01},
        {"leg": "put", "inst_id": "BTC-USD-250425-80000-P", "side": "sell", "px": 0.01},
        {"leg": "future", "inst_id": "BTC-USD-250425", "side": "sell", "px": 80000.0},
    ]
    session = MagicMock()

    async def fake_row(_session, *, inst_id: str, **kwargs):
        if "80000-P" in inst_id:
            return {"maxBuy": "99", "maxSell": "0.05"}
        return {"maxBuy": "9999", "maxSell": "9999"}

    with patch.object(om, "_okx_get_max_size_row", side_effect=fake_row):
        with pytest.raises(RuntimeError, match="超过账户当前可卖上限"):
            await om._okx_preflight_max_size_legs(
                "t", session, legs, 1.0, "k", "s", "p"
            )


# ---------------------------------------------------------------------------
# Tests for new behaviour introduced with exchange routing
# ---------------------------------------------------------------------------

class TestExchangeRouting:
    """Tests for same-exchange-only routing and lot-size rounding."""

    @pytest.fixture(autouse=True)
    def _mock_okx_preflight_max_size(self):
        """与 TestOrderManagerSubmitEntry 相同，避免未 mock 的 session 误跑 max-size。"""
        with patch(
            "pcp_arbitrage.order_manager._okx_preflight_max_size_legs",
            new=AsyncMock(return_value=None),
        ), patch(
            "pcp_arbitrage.order_manager._okx_backfill_open_entry_orders_detail",
            new=AsyncMock(return_value=None),
        ):
            yield

    @pytest.mark.asyncio
    async def test_unsupported_exchange_skips_silently(self, tmp_path):
        """Exchanges not in _SUPPORTED_EXEC_EXCHANGES return without error and without DB writes."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet(exchange="binance")
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        from pcp_arbitrage import order_manager as om
        ok, msg = await om.submit_entry(triplet, signal, None, cfg, db_path)

        assert ok is False
        assert "暂不支持" in msg

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        conn.close()
        assert count == 0

    @pytest.mark.asyncio
    async def test_deribit_routes_to_deribit_inner(self, tmp_path):
        """triplet.exchange='deribit' calls _submit_entry_deribit_inner, not OKX."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet(exchange="deribit")
        signal = _make_signal(triplet)
        cfg = AppConfig(
            exchanges={
                "deribit": ExchangeConfig(
                    name="deribit",
                    enabled=True,
                    margin_type="coin",
                    api_key="deribit_key",
                    secret_key="deribit_secret",
                    passphrase="",
                )
            },
            symbols=["BTC"],
            min_annualized_rate=0.01,
            order_min_annualized_rate=0.05,
            atm_range=0.1,
            min_days_to_expiry=1.0,
            stale_threshold_ms=5000,
            lot_size={"BTC": 0.1},
            telegram=TelegramConfig(bot_token="", chat_id=""),
            sqlite_path=db_path,
        )

        from pcp_arbitrage import order_manager as om

        deribit_inner_called = []

        async def mock_deribit_inner(triplet, signal, signal_id, cfg, sqlite_path):
            deribit_inner_called.append(True)
            return "下单成功，仓位 ID: 1，数量: 0.1"

        with patch.object(om, "_submit_entry_deribit_inner", mock_deribit_inner), \
             patch("aiohttp.ClientSession") as mock_okx_session:
            await om.submit_entry(triplet, signal, None, cfg, db_path)

        assert deribit_inner_called, "Deribit inner was not called"
        mock_okx_session.assert_not_called()

    @pytest.mark.asyncio
    async def test_okx_does_not_route_to_deribit(self, tmp_path):
        """triplet.exchange='okx' never calls _submit_entry_deribit_inner."""
        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet(exchange="okx")
        signal = _make_signal(triplet)
        cfg = _make_cfg()

        from pcp_arbitrage import order_manager as om

        deribit_called = []

        async def mock_deribit_inner(*args, **kwargs):
            deribit_called.append(True)
            return "ok"

        async def mock_place(*args, **kwargs):
            return "oid_x"

        async def mock_poll(*args, **kwargs):
            return {"ordId": "oid_x", "state": "filled", "avgPx": "100.0", "px": "100.0"}

        with patch.object(om, "_submit_entry_deribit_inner", mock_deribit_inner), \
             patch.object(om, "_okx_get_instrument", _mock_okx_get_instrument_for_tests), \
             patch.object(om, "_place_order", mock_place), \
             patch.object(om, "_poll_order_fill", mock_poll), \
             patch("aiohttp.ClientSession") as mock_cls:
            mock_session_cm = MagicMock()
            mock_session_cm.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_session_cm.__aexit__ = AsyncMock(return_value=False)
            mock_cls.return_value = mock_session_cm

            await om.submit_entry(triplet, signal, None, cfg, db_path)

        assert not deribit_called

    def test_lot_size_floor_rounding(self):
        """tradeable_qty is floored to nearest lot_size multiple before submission."""
        import math
        lot_size = 0.1

        # 0.35 → 0.3
        qty = math.floor(0.35 / lot_size) * lot_size
        assert abs(qty - 0.3) < 1e-9

        # 0.19 → 0.1
        qty = math.floor(0.19 / lot_size) * lot_size
        assert abs(qty - 0.1) < 1e-9

        # exactly 0.2 → 0.2 (no change)
        qty = math.floor(0.2 / lot_size) * lot_size
        assert abs(qty - 0.2) < 1e-9

        # 0.09 → 0.0 (would be caught by the qty <= 0 guard)
        qty = math.floor(0.09 / lot_size) * lot_size
        assert qty == 0.0

    @pytest.mark.asyncio
    async def test_zero_qty_after_rounding_raises(self, tmp_path):
        """submit_entry fails cleanly when tradeable_qty is zero."""
        from dataclasses import replace as dc_replace

        db_path = str(tmp_path / "test.db")
        _db.init_db(db_path)

        triplet = _make_triplet(exchange="okx")
        signal = _make_signal(triplet)
        signal = dc_replace(signal, tradeable_qty=0.0)
        cfg = _make_cfg()

        from pcp_arbitrage import order_manager as om
        with patch("aiohttp.ClientSession"):
            ok, msg = await om.submit_entry(triplet, signal, None, cfg, db_path)

        assert ok is False
        assert "零" in msg

        # No position should have been created
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        conn.close()
        assert count == 0

    def test_deribit_futures_amount_calculation(self):
        """Deribit futures amount = round(qty * future_price / contract_size) * contract_size."""
        from pcp_arbitrage.pcp_calculator import DERIBIT_INVERSE_FUT_USD_FACE

        # BTC: contract_size = 10 USD
        contract_size = DERIBIT_INVERSE_FUT_USD_FACE["BTC"]
        assert contract_size == 10.0

        qty = 0.1          # BTC
        future_price = 80000.0  # USD
        fut_amount = round(qty * future_price / contract_size) * contract_size
        # 0.1 BTC × 80000 USD = 8000 USD total; rounded to nearest 10 USD = 8000 USD
        assert fut_amount == 8000.0

        qty = 0.3
        fut_amount = round(qty * future_price / contract_size) * contract_size
        assert fut_amount == 24000.0


@pytest.mark.asyncio
async def test_escalating_exit_loop_taker_fallback(tmp_path):
    """maker 超时后，升级到 taker 成功平仓"""
    import time
    from pcp_arbitrage import order_manager as om

    cfg = mock.MagicMock()
    cfg.maker_chase_secs = 1
    cfg.maker_chase_max_minutes = 3

    call_count = 0

    async def fake_submit_and_poll(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return False, 0.0
        return True, 10.0

    with mock.patch.object(om, "_submit_and_poll_exit_legs_okx", side_effect=fake_submit_and_poll), \
         mock.patch.object(om, "_cancel_order", return_value=None), \
         mock.patch.object(om, "_fetch_order_book_top", return_value=(100.0, 101.0)):
        started_at = time.monotonic() - 2.5
        # Need to init DB for the function to query
        import sqlite3
        con = sqlite3.connect(str(tmp_path / "test.db"))
        con.execute(
            "CREATE TABLE positions ("
            "id INTEGER PRIMARY KEY, "
            "target_state TEXT NOT NULL DEFAULT 'open', "
            "exit_attempt_count INTEGER DEFAULT 0, "
            "exit_last_attempt_at TEXT)"
        )
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, position_id INTEGER, action TEXT, status TEXT, "
            "leg TEXT, inst_id TEXT, side TEXT, "
            "filled_px REAL, filled_qty REAL, fee_type TEXT, actual_fee REAL, fee_ccy TEXT, filled_at TEXT, "
            "order_type TEXT, last_error TEXT)"
        )
        con.execute(
            "INSERT INTO orders (id, position_id, action, status, leg, inst_id, side, last_error) "
            "VALUES (1, 1, 'close', 'pending', 'call', 'BTC-USD-C', 'sell', NULL)"
        )
        con.commit()
        con.close()

        filled, pnl = await om._escalating_exit_loop_okx(
            session=mock.AsyncMock(),
            failed_legs=[{
                "leg": "call",
                "inst_id": "BTC-USD-C",
                "side": "sell",
                "qty": 1.0,
                "entry_px": 90.0,
                "oid_db": 1,
                "last_px": 100.0,
                "exch_ord_id": None,
            }],
            position_id=1,
            signal_id=None,
            api_key="k",
            secret="s",
            passphrase="p",
            sqlite_path=str(tmp_path / "test.db"),
            exit_started_at=started_at,
            cfg=cfg,
        )
    assert filled is True
    assert pnl == 10.0
    assert call_count == 2
    con = sqlite3.connect(str(tmp_path / "test.db"))
    row = con.execute("SELECT status, last_error FROM orders WHERE id=1").fetchone()
    con.close()
    assert row is not None
    assert row[0] == "canceled"
    assert "策略撤单" in str(row[1] or "")


# ---------------------------------------------------------------------------
# Tests for account balance capping (Task 5)
# ---------------------------------------------------------------------------

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


@pytest.mark.asyncio
async def test_reconcile_close_orders_status_updates_failed_to_filled(tmp_path):
    from pcp_arbitrage import order_manager as om

    db_path = str(tmp_path / "reconcile_close.db")
    _db.init_db(db_path)
    cfg = _make_cfg()
    cfg.sqlite_path = db_path

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            pid = _db.create_position(
                conn,
                signal_id=None,
                exchange="OKX",
                symbol="BTC",
                expiry="250425",
                strike=80000.0,
                direction="forward",
            )
            oid = _db.create_order(
                conn,
                signal_id=None,
                position_id=pid,
                inst_id="BTC-USD-250425-80000-C",
                leg="call",
                action="close",
                side="sell",
                limit_px=0.01,
                qty=1.0,
                exchange_order_id="oid_close_1",
            )
            _db.update_order_status(conn, oid, "failed")
    finally:
        conn.close()

    snap = {
        "ordId": "oid_close_1",
        "state": "filled",
        "avgPx": "0.0123",
        "px": "0.0123",
        "fillSz": "1",
        "ordType": "limit",
        "fee": "-0.00001",
        "feeCcy": "BTC",
        "execType": "M",
    }
    with patch.object(om, "_okx_get_order_snapshot", AsyncMock(return_value=snap)):
        stats = await om.reconcile_close_orders_status(
            position_id=pid,
            cfg=cfg,
            sqlite_path=db_path,
        )
    assert stats["filled"] == 1
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status, filled_px, fee_type FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "filled"
    assert row[1] == pytest.approx(0.0123)
    assert row[2] == "taker"


@pytest.mark.asyncio
async def test_reconcile_close_orders_status_updates_failed_to_pending_on_live(tmp_path):
    from pcp_arbitrage import order_manager as om

    db_path = str(tmp_path / "reconcile_close_live.db")
    _db.init_db(db_path)
    cfg = _make_cfg()
    cfg.sqlite_path = db_path

    conn = sqlite3.connect(db_path)
    try:
        with conn:
            pid = _db.create_position(
                conn,
                signal_id=None,
                exchange="OKX",
                symbol="BTC",
                expiry="250425",
                strike=80000.0,
                direction="forward",
            )
            oid = _db.create_order(
                conn,
                signal_id=None,
                position_id=pid,
                inst_id="BTC-USD-250425-80000-P",
                leg="put",
                action="close",
                side="buy",
                limit_px=0.02,
                qty=1.0,
                exchange_order_id="oid_close_2",
            )
            _db.update_order_status(conn, oid, "failed")
    finally:
        conn.close()

    snap = {"ordId": "oid_close_2", "state": "live", "px": "0.02", "ordType": "limit"}
    with patch.object(om, "_okx_get_order_snapshot", AsyncMock(return_value=snap)):
        stats = await om.reconcile_close_orders_status(
            position_id=pid,
            cfg=cfg,
            sqlite_path=db_path,
        )
    assert stats["pending"] == 1
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status, last_error FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "pending"
    assert "等待成交中" in (row[1] or "")
