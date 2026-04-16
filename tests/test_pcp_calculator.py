import pytest
from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse
from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet
import time

NOW_MS = int(time.time() * 1000)


@pytest.fixture
def fee_rates():
    return FeeRates(
        option_taker_rate=0.0002,
        option_maker_rate=0.0001,
        future_taker_rate=0.0005,
        future_maker_rate=0.0002,
    )


@pytest.fixture
def triplet():
    return Triplet(
        exchange="okx",
        symbol="BTC",
        expiry="250425",
        strike=95000.0,
        call_id="BTC-USD-250425-95000-C",
        put_id="BTC-USD-250425-95000-P",
        future_id="BTC-USD-250425",
    )


@pytest.fixture
def books_forward():
    """Forward: put_bid - call_ask + future_bid - K should be positive."""
    return {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=200.0, ask=200.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=5600.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425": BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }


@pytest.fixture
def books_reverse():
    """Reverse: call_bid - put_ask + K - future_ask should be positive."""
    return {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=120.0, ask=120.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=80.0, ask=80.0, ts=NOW_MS),
        "BTC-USD-250425": BookSnapshot(bid=1958.0, ask=1958.0, ts=NOW_MS),
    }


# ── Forward ────────────────────────────────────────────────────────────────


def test_forward_gross_profit(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is not None
    # gross = 5600 - 200 + 89630 - 95000 = 30
    assert abs(sig.gross_profit - 30.0) < 0.01


def test_forward_net_profit_less_than_gross(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is not None
    assert sig.net_profit < sig.gross_profit


def test_forward_tradeable_qty_is_min_of_three_legs(triplet, fee_rates):
    """正向：各腿张数换算为 BTC 名义后取 min（与余额约束 qty×index 一致）。"""
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(
            bid=200.0, ask=200.0, ts=NOW_MS, bid_sz=999.0, ask_sz=10.0
        ),
        "BTC-USD-250425-95000-P": BookSnapshot(
            bid=5600.0, ask=5600.0, ts=NOW_MS, bid_sz=5.0, ask_sz=1.0
        ),
        "BTC-USD-250425": BookSnapshot(
            bid=89630.0, ask=89640.0, ts=NOW_MS, bid_sz=100.0, ask_sz=2.0
        ),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is not None
    # call 10×0.01=0.1, put 5×0.01=0.05, fut 100×0.01=1.0（无 inverse 面值时用 lot_size）→ min=0.05
    assert sig.tradeable_qty == pytest.approx(0.05)
    assert sig.depth_contracts == pytest.approx(min(10.0, 5.0, 100.0))


def test_reverse_tradeable_qty_is_min_of_three_legs(triplet, fee_rates):
    """反向：各腿换算 BTC 名义后取 min。"""
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(
            bid=120.0, ask=120.0, ts=NOW_MS, bid_sz=3.0, ask_sz=9.0
        ),
        "BTC-USD-250425-95000-P": BookSnapshot(
            bid=80.0, ask=80.0, ts=NOW_MS, bid_sz=11.0, ask_sz=40.0
        ),
        "BTC-USD-250425": BookSnapshot(
            bid=1958.0, ask=1958.0, ts=NOW_MS, bid_sz=2.0, ask_sz=7.0
        ),
    }
    sig = calculate_reverse(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is not None
    # 3×0.01, 40×0.01, 7×0.01 → min=0.03
    assert sig.tradeable_qty == pytest.approx(0.03)
    assert sig.depth_contracts == pytest.approx(min(3.0, 40.0, 7.0))


def test_tradeable_uses_inverse_future_usd_face_when_set(triplet, fee_rates):
    """币本位反向期货深度用 USD 面值/指数 换算 BTC 名义。"""
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(
            bid=200.0, ask=200.0, ts=NOW_MS, ask_sz=10.0
        ),
        "BTC-USD-250425-95000-P": BookSnapshot(
            bid=5600.0, ask=5600.0, ts=NOW_MS, bid_sz=5.0
        ),
        "BTC-USD-250425": BookSnapshot(
            bid=89630.0, ask=89640.0, ts=NOW_MS, bid_sz=100000.0
        ),
    }
    spot = 100_000.0
    face = 100.0
    sig = calculate_forward(
        triplet,
        books,
        fee_rates,
        lot_size=0.01,
        days_to_expiry=21.0,
        spot_price=spot,
        future_inverse_usd_face=face,
    )
    assert sig is not None
    c = 10.0 * 0.01
    p = 5.0 * 0.01
    f = 100_000.0 * (face / spot)  # 0.1
    assert sig.tradeable_qty == pytest.approx(min(c, p, f))
    assert sig.tradeable_qty == pytest.approx(0.05)


def test_forward_returns_none_when_not_profitable(triplet, fee_rates):
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=200.0, ask=5000.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=50.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425": BookSnapshot(bid=89000.0, ask=89010.0, ts=NOW_MS),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is None or sig.net_profit <= 0


# ── Fee cap ────────────────────────────────────────────────────────────────


def test_option_fee_cap_applied(fee_rates):
    """When fee_cap < fee_by_face (index path), cap applies."""
    from pcp_arbitrage.pcp_calculator import _option_fee

    # fee_by_face = 0.01 × 1.0 × 0.0002 = 0.000002
    # fee_cap = 0.01 × 0.01 × 0.125 = 0.0000125  → face wins (smaller of the two)
    fee = _option_fee(option_price=0.01, lot_size=0.01, option_rate=0.0002, index_usdt=1.0)
    assert abs(fee - 0.000002) < 1e-9


def test_option_fee_cap_clamps_high_rate():
    """When fee_by_face > cap, cap is returned."""
    from pcp_arbitrage.pcp_calculator import _option_fee

    # fee_by_face = 100 × 1.0 × 1.0 = 100; fee_cap = 0.001 × 100 × 0.07 → cap wins
    fee = _option_fee(option_price=0.001, lot_size=100.0, option_rate=1.0, index_usdt=1.0)
    assert fee == pytest.approx(0.001 * 100.0 * 0.07)


def test_option_fee_uses_underlying_index():
    """Coin-margined style: face fee = lot × index × rate (USDT)."""
    from pcp_arbitrage.pcp_calculator import _option_fee

    fee = _option_fee(
        option_price=20_000.0,
        lot_size=0.01,
        option_rate=0.0003,
        index_usdt=67_000.0,
    )
    # face = 0.01 × 67000 × 0.0003 = 0.201; cap = 20000 × 0.01 × 0.125 = 2500 → min = 0.201
    assert fee == pytest.approx(0.201)


# ── Annualized return ──────────────────────────────────────────────────────


def test_annualized_return_formula(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is not None
    expected = (sig.net_profit / triplet.strike) * (365 / 21.0)
    assert abs(sig.annualized_return - expected) < 1e-9


# ── Stale data ────────────────────────────────────────────────────────────


def test_stale_books_returns_none(triplet, fee_rates):
    old_ts = NOW_MS - 10_000  # 10 seconds ago
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=200.0, ask=200.0, ts=old_ts),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=5600.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425": BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }
    sig = calculate_forward(
        triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0, stale_threshold_ms=5000
    )
    assert sig is None


def test_zero_bid_ask_returns_none(triplet, fee_rates):
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=0.0, ask=200.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=5600.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425": BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0, spot_price=1.0)
    assert sig is None
