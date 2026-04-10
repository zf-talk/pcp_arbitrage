from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet


def test_book_snapshot_fields():
    snap = BookSnapshot(bid=100.0, ask=101.0, ts=1700000000000)
    assert snap.bid == 100.0
    assert snap.ask == 101.0
    assert snap.ts == 1700000000000


def test_fee_rates_fields():
    fees = FeeRates(
        option_taker_rate=0.0002,
        option_maker_rate=0.0001,
        future_taker_rate=0.0005,
        future_maker_rate=0.0002,
    )
    assert fees.option_taker_rate == 0.0002
    assert fees.option_maker_rate == 0.0001
    assert fees.future_taker_rate == 0.0005
    assert fees.future_maker_rate == 0.0002


def test_triplet_fields():
    t = Triplet(
        symbol="BTC",
        expiry="250425",
        strike=95000.0,
        call_id="BTC-USD-250425-95000-C",
        put_id="BTC-USD-250425-95000-P",
        future_id="BTC-USD-250425",
    )
    assert t.symbol == "BTC"
    assert t.expiry == "250425"
    assert t.strike == 95000.0
    assert t.call_id == "BTC-USD-250425-95000-C"
    assert t.put_id == "BTC-USD-250425-95000-P"
    assert t.future_id == "BTC-USD-250425"
