"""Tests for dual-threshold notification logic in signal_output.py."""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

from pcp_arbitrage.config import AppConfig, TelegramConfig
from pcp_arbitrage.exchange_symbols import format_strike_display
from pcp_arbitrage.models import Triplet
from pcp_arbitrage.pcp_calculator import ArbitrageSignal
import pcp_arbitrage.signal_output as signal_output


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_triplet(symbol: str = "BTC", expiry: str = "250425", strike: float = 80000.0) -> Triplet:
    return Triplet(
        symbol=symbol,
        expiry=expiry,
        strike=strike,
        call_id=f"{symbol}-{expiry}-{int(strike)}-C",
        put_id=f"{symbol}-{expiry}-{int(strike)}-P",
        future_id=f"{symbol}-{expiry}",
    )


def _make_signal(
    triplet: Triplet,
    annualized_return: float = 0.10,
    net_profit: float = 50.0,
    tradeable_qty: float = 5.0,
    direction: str = "forward",
) -> ArbitrageSignal:
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
        net_profit=net_profit,
        annualized_return=annualized_return,
        days_to_expiry=30.0,
        tradeable_qty=tradeable_qty,
    )


def _make_cfg(
    order_min_annualized_rate: float = 0.05,
    bot_token: str = "123:ABC",
    chat_id: str = "-100999",
) -> AppConfig:
    return AppConfig(
        exchanges={},
        symbols=["BTC"],
        min_annualized_rate=0.01,
        order_min_annualized_rate=order_min_annualized_rate,
        atm_range=0.1,
        min_days_to_expiry=1.0,
        stale_threshold_ms=5000,
        lot_size={"BTC": 0.01},
        telegram=TelegramConfig(bot_token=bot_token, chat_id=chat_id),
    )


def _label(triplet: Triplet) -> str:
    return f"{triplet.symbol}-{triplet.expiry}-{format_strike_display(triplet.symbol, triplet.strike)}"


def _configure(cfg: AppConfig) -> None:
    """Configure signal_output in classic mode with given cfg."""
    signal_output._cfg = cfg
    signal_output._mode = "classic"
    signal_output._dash = None
    signal_output._notified_keys = {}


def _make_create_task_mock() -> MagicMock:
    """Return a mock for asyncio.create_task that closes passed coroutines to avoid RuntimeWarning."""
    def _close_coro(coro: object) -> None:
        if hasattr(coro, "close"):
            coro.close()  # type: ignore[union-attr]
    return MagicMock(side_effect=_close_coro)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_order_min_annualized_rate_default():
    """order_min_annualized_rate defaults to 0.05 when not provided in config."""
    cfg = _make_cfg()
    assert cfg.order_min_annualized_rate == 0.05


async def test_notification_fires_when_above_order_threshold():
    """create_task is called when ann_rate >= order_min_annualized_rate and key not yet notified."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)  # 10% > 5% threshold
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )

    # Each threshold crossing fires 2 tasks: send_telegram + submit_entry
    assert mock_create_task.call_count == 2
    # First call should be a coroutine (send_telegram)
    first_args, _ = mock_create_task.call_args_list[0]
    assert inspect.iscoroutine(first_args[0]), "expected a coroutine passed to create_task"
    first_args[0].close()  # prevent "coroutine was never awaited" warning


async def test_notification_not_fired_again_for_same_key():
    """create_task is NOT called a second time for same (exchange, label, direction) key."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        # First call: should notify (fires 2 tasks)
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )
        # Second call: same key, already notified — no additional tasks
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )

    # Only 2 tasks total (from first call only)
    assert mock_create_task.call_count == 2


async def test_notification_fires_again_after_inactive_cycle():
    """After key goes inactive (popped from _notified_keys), next activation fires again."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        # First activation: notifies (2 tasks)
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )
        # Goes inactive: sig=None, key should be popped
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", None, min_annualized_rate=0.01
        )
        # Second activation: should notify again (2 more tasks)
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )

    # 2 activations × 2 tasks each = 4 total
    assert mock_create_task.call_count == 4


async def test_notification_not_fired_below_order_threshold():
    """create_task is NOT called when ann_rate < order_min_annualized_rate."""
    triplet = _make_triplet()
    # ann_rate 3% < 5% threshold
    sig = _make_signal(triplet, annualized_return=0.03)
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )

    mock_create_task.assert_not_called()


async def test_inactive_pops_notified_key():
    """When signal goes inactive, the key is removed from _notified_keys."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    label = _label(triplet)
    notif_key = ("OKX", label, "forward")

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        # Activate and notify
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )
        assert signal_output._notified_keys.get(notif_key) is True

        # Now go inactive
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", None, min_annualized_rate=0.01
        )
        assert notif_key not in signal_output._notified_keys


async def test_notification_not_fired_when_cfg_is_none():
    """No create_task call when _cfg is None (unconfigured)."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)

    # Reset to unconfigured state
    signal_output._cfg = None
    signal_output._dash = None
    signal_output._mode = "classic"
    signal_output._notified_keys = {}

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )

    mock_create_task.assert_not_called()


async def test_notification_keys_are_per_direction():
    """Different directions for same triplet get independent dedup state."""
    triplet = _make_triplet()
    sig_fwd = _make_signal(triplet, annualized_return=0.10, direction="forward")
    sig_rev = _make_signal(triplet, annualized_return=0.10, direction="reverse")
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig_fwd, min_annualized_rate=0.01
        )
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "reverse", sig_rev, min_annualized_rate=0.01
        )

    # Each direction fires independently (2 tasks each = 4 total)
    assert mock_create_task.call_count == 4


async def test_notification_keys_are_per_exchange():
    """Same triplet+direction on different exchanges get independent dedup state."""
    triplet = _make_triplet()
    sig = _make_signal(triplet, annualized_return=0.10)
    cfg = _make_cfg(order_min_annualized_rate=0.05)
    _configure(cfg)

    mock_create_task = _make_create_task_mock()
    with patch("asyncio.create_task", mock_create_task), \
         patch.object(signal_output, "_trace_evaluation"):
        signal_output.emit_opportunity_evaluation(
            "OKX", triplet, "forward", sig, min_annualized_rate=0.01
        )
        signal_output.emit_opportunity_evaluation(
            "Binance", triplet, "forward", sig, min_annualized_rate=0.01
        )

    # Each exchange fires independently (2 tasks each = 4 total)
    assert mock_create_task.call_count == 4


async def test_configure_signal_output_resets_notified_keys():
    """configure_signal_output() clears _notified_keys."""
    # Pre-populate the dict
    signal_output._notified_keys[("OKX", "BTC-250425-80000", "forward")] = True

    cfg = _make_cfg()
    # Patch out TTY check and dashboard initialisation
    with patch("sys.stdout") as mock_stdout:
        mock_stdout.isatty.return_value = False
        signal_output.configure_signal_output(cfg)

    assert signal_output._notified_keys == {}
