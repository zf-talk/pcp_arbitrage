from pcp_arbitrage.exchanges.binance import _normalize_instruments


RAW_OPTIONS = [
    {
        "symbol": "BTC-260627-70000-C",
        "underlying": "BTCUSDT",
        "strikePrice": "70000",
        "side": "CALL",
        "expiryDate": 1751040000000,
    },
    {
        "symbol": "BTC-260627-70000-P",
        "underlying": "BTCUSDT",
        "strikePrice": "70000",
        "side": "PUT",
        "expiryDate": 1751040000000,
    },
]

RAW_USDT_FUTURES = [
    {"symbol": "BTCUSDT_260627"},
]

RAW_COIN_FUTURES = [
    {"symbol": "BTCUSD_260627"},
]


def test_normalize_usdt_option_instid():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [], "usdt")
    assert opts[0]["instId"] == "BTC-USDT-260627-70000-C"
    assert opts[1]["instId"] == "BTC-USDT-260627-70000-P"


def test_normalize_coin_option_instid():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [], "coin")
    assert opts[0]["instId"] == "BTC-USD-260627-70000-C"


def test_normalize_usdt_future_instid():
    _, futs = _normalize_instruments([], RAW_USDT_FUTURES, "usdt")
    assert futs[0]["instId"] == "BTC-USDT-260627"


def test_normalize_coin_future_instid():
    _, futs = _normalize_instruments([], RAW_COIN_FUTURES, "coin")
    assert futs[0]["instId"] == "BTC-USD-260627"


def test_normalize_option_fields():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [], "usdt")
    c = opts[0]
    assert c["stk"] == "70000"
    assert c["optType"] == "C"
    assert c["expTime"] == "1751040000000"
    p = opts[1]
    assert p["optType"] == "P"
