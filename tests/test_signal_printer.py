from pcp_arbitrage.signal_printer import format_signal
from pcp_arbitrage.pcp_calculator import ArbitrageSignal
from pcp_arbitrage.models import Triplet


def _make_signal(direction: str) -> ArbitrageSignal:
    triplet = Triplet(
        symbol="BTC",
        expiry="250425",
        strike=95000.0,
        call_id="BTC-USD-250425-95000-C",
        put_id="BTC-USD-250425-95000-P",
        future_id="BTC-USD-250425",
    )
    return ArbitrageSignal(
        direction=direction,
        triplet=triplet,
        call_price=200.0,
        put_price=5600.0,
        future_price=89630.0,
        call_price_coin=0.002,
        put_price_coin=0.056,
        spot_price=100000.0,
        gross_profit=30.0,
        total_fee=2.5,
        net_profit=27.5,
        annualized_return=0.005,
        days_to_expiry=21.0,
    )


def test_forward_signal_contains_direction():
    sig = _make_signal("forward")
    output = format_signal(sig)
    assert "正向套利" in output


def test_reverse_signal_contains_direction():
    sig = _make_signal("reverse")
    output = format_signal(sig)
    assert "反向套利" in output


def test_signal_contains_strike():
    sig = _make_signal("forward")
    output = format_signal(sig)
    assert "95000" in output


def test_signal_contains_net_profit():
    sig = _make_signal("forward")
    output = format_signal(sig)
    assert "27.50" in output


def test_signal_contains_annualized_return():
    sig = _make_signal("forward")
    output = format_signal(sig)
    # 0.005 × 100 = 0.50%
    assert "0.50%" in output


def test_signal_contains_separator():
    sig = _make_signal("forward")
    output = format_signal(sig)
    assert "────" in output


def test_forward_labels_call_ask():
    sig = _make_signal("forward")
    output = format_signal(sig)
    assert "卖一" in output  # call_ask → "← 卖一"


def test_reverse_labels_call_bid():
    sig = _make_signal("reverse")
    output = format_signal(sig)
    assert "买一" in output  # call_bid → "← 买一"
