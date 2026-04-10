"""exchange_symbols: per-venue filtering."""

from pcp_arbitrage.exchange_symbols import EXCHANGE_SYMBOLS, symbols_for_exchange


def test_symbols_for_exchange_filters_and_preserves_order():
    req = ["ETH", "DOGE", "BTC", "AVAX"]
    assert symbols_for_exchange("okx", req) == ["ETH", "BTC"]
    assert symbols_for_exchange("binance", req) == ["ETH", "DOGE", "BTC"]
    assert symbols_for_exchange("deribit", req) == ["ETH", "BTC"]
    assert symbols_for_exchange("deribit_linear", req) == ["ETH", "BTC", "AVAX"]


def test_exchange_symbols_keys_cover_runners():
    assert "okx" in EXCHANGE_SYMBOLS
    assert "binance" in EXCHANGE_SYMBOLS
    assert "deribit" in EXCHANGE_SYMBOLS
    assert "deribit_linear" in EXCHANGE_SYMBOLS
