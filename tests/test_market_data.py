from pcp_arbitrage.market_data import MarketData
from pcp_arbitrage.models import BookSnapshot


def test_update_and_get():
    store = MarketData()
    snap = BookSnapshot(bid=100.0, ask=101.0, ts=1700000000000)
    store.update("BTC-USD-250425-95000-C", snap)
    result = store.get("BTC-USD-250425-95000-C")
    assert result == snap


def test_get_missing_returns_none():
    store = MarketData()
    assert store.get("nonexistent") is None


def test_clear_removes_all():
    store = MarketData()
    store.update("A", BookSnapshot(bid=1.0, ask=2.0, ts=1000))
    store.update("B", BookSnapshot(bid=3.0, ask=4.0, ts=2000))
    store.clear()
    assert store.get("A") is None
    assert store.get("B") is None


def test_has_all_true():
    store = MarketData()
    store.update("X", BookSnapshot(bid=1.0, ask=2.0, ts=1000))
    store.update("Y", BookSnapshot(bid=3.0, ask=4.0, ts=2000))
    assert store.has_all(["X", "Y"]) is True


def test_has_all_false():
    store = MarketData()
    store.update("X", BookSnapshot(bid=1.0, ask=2.0, ts=1000))
    assert store.has_all(["X", "Y"]) is False
