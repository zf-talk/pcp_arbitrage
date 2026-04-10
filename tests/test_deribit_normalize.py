from pcp_arbitrage.exchanges.deribit import (
    _deribit_book_channel,
    _deribit_date_to_yymmdd,
    _is_deribit_expiry_token,
    _normalize_instruments,
    _normalize_instruments_linear,
    _internal_to_deribit,
    _internal_to_deribit_linear,
)


def test_date_jan():
    assert _deribit_date_to_yymmdd("01JAN26") == "260101"

def test_date_feb():
    assert _deribit_date_to_yymmdd("28FEB25") == "250228"

def test_date_mar():
    assert _deribit_date_to_yymmdd("28MAR25") == "250328"

def test_date_jun():
    assert _deribit_date_to_yymmdd("27JUN25") == "250627"

def test_date_sep():
    assert _deribit_date_to_yymmdd("26SEP25") == "250926"

def test_date_dec():
    assert _deribit_date_to_yymmdd("31DEC25") == "251231"


def test_date_single_digit_day_dmmmyy():
    """Deribit uses 6 chars (DMMMYY) for days 1–9, e.g. 6APR26."""
    assert _deribit_date_to_yymmdd("6APR26") == "260406"
    assert _deribit_date_to_yymmdd("9APR26") == "260409"
    assert _is_deribit_expiry_token("6APR26") is True


def test_expiry_token_pr2_rejected():
    assert _is_deribit_expiry_token("PR2") is False
    assert _is_deribit_expiry_token("27JUN25") is True


RAW_OPTIONS = [
    {
        "instrument_name": "BTC-27JUN25-70000-C",
        "strike": 70000.0,
        "option_type": "call",
        "expiration_timestamp": 1751040000000,
    },
    {
        "instrument_name": "BTC-27JUN25-70000-P",
        "strike": 70000.0,
        "option_type": "put",
        "expiration_timestamp": 1751040000000,
    },
]

RAW_FUTURES = [
    {"instrument_name": "BTC-27JUN25"},
    {"instrument_name": "BTC-PERPETUAL"},   # 应被过滤掉
]


def test_normalize_option_instid():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [])
    assert opts[0]["instId"] == "BTC-USD-250627-70000-C"
    assert opts[1]["instId"] == "BTC-USD-250627-70000-P"


def test_normalize_option_single_digit_expiry_segment():
    raw = [
        {
            "instrument_name": "BTC-6APR26-70000-C",
            "strike": 70000.0,
            "option_type": "call",
            "expiration_timestamp": 1,
        },
    ]
    opts, _ = _normalize_instruments(raw, [])
    assert len(opts) == 1
    assert opts[0]["instId"] == "BTC-USD-260406-70000-C"


def test_normalize_skips_pr2_style_option():
    """Deribit may list non–DDMMMYY option segments (e.g. PR2); ignore them."""
    raw = RAW_OPTIONS + [
        {
            "instrument_name": "BTC-PR2-70000-C",
            "strike": 70000.0,
            "option_type": "call",
            "expiration_timestamp": 1751040000000,
        },
    ]
    opts, _ = _normalize_instruments(raw, [])
    assert len(opts) == 2


def test_normalize_option_fields():
    opts, _ = _normalize_instruments(RAW_OPTIONS, [])
    c = opts[0]
    assert c["stk"] == "70000"
    assert c["optType"] == "C"
    assert c["expTime"] == "1751040000000"
    assert opts[1]["optType"] == "P"


def test_normalize_future_instid():
    _, futs = _normalize_instruments([], RAW_FUTURES)
    assert len(futs) == 1                   # PERPETUAL 被过滤
    assert futs[0]["instId"] == "BTC-USD-250627"


def test_deribit_book_channel():
    assert _deribit_book_channel("BTC-27JUN25-70000-C") == "book.BTC-27JUN25-70000-C.none.1.100ms"


def test_internal_to_deribit_option():
    assert _internal_to_deribit("BTC-USD-250627-70000-C") == "BTC-27JUN25-70000-C"


def test_internal_to_deribit_option_single_digit_day():
    assert _internal_to_deribit("BTC-USD-260406-70000-C") == "BTC-6APR26-70000-C"


def test_internal_to_deribit_future():
    assert _internal_to_deribit("BTC-USD-250627") == "BTC-27JUN25"


def test_internal_to_deribit_future_single_digit_day():
    assert _internal_to_deribit("BTC-USD-260406") == "BTC-6APR26"


# --- Linear (USDC) ---

RAW_LINEAR_OPTIONS = [
    {
        "instrument_name": "BTC_USDC-27JUN25-70000-C",
        "strike": 70000.0,
        "option_type": "call",
        "expiration_timestamp": 1751040000000,
    },
    {
        "instrument_name": "BTC_USDC-27JUN25-70000-P",
        "strike": 70000.0,
        "option_type": "put",
        "expiration_timestamp": 1751040000000,
    },
]

RAW_LINEAR_FUTURES = [
    {"instrument_name": "BTC_USDC-27JUN25"},
    {"instrument_name": "BTC_USDC-PERPETUAL"},   # 应被过滤掉
]


def test_linear_normalize_option_instid():
    opts, _ = _normalize_instruments_linear(RAW_LINEAR_OPTIONS, [])
    assert opts[0]["instId"] == "BTC-USDC-250627-70000-C"
    assert opts[1]["instId"] == "BTC-USDC-250627-70000-P"


def test_linear_normalize_option_single_digit_expiry():
    raw = [
        {
            "instrument_name": "BTC_USDC-6APR26-70000-C",
            "strike": 70000.0,
            "option_type": "call",
            "expiration_timestamp": 1,
        },
    ]
    opts, _ = _normalize_instruments_linear(raw, [])
    assert opts[0]["instId"] == "BTC-USDC-260406-70000-C"


def test_linear_normalize_skips_pr2_style_option():
    raw = RAW_LINEAR_OPTIONS + [
        {
            "instrument_name": "BTC_USDC-PR2-70000-C",
            "strike": 70000.0,
            "option_type": "call",
            "expiration_timestamp": 1751040000000,
        },
    ]
    opts, _ = _normalize_instruments_linear(raw, [])
    assert len(opts) == 2


def test_linear_normalize_option_fields():
    opts, _ = _normalize_instruments_linear(RAW_LINEAR_OPTIONS, [])
    c = opts[0]
    assert c["stk"] == "70000"
    assert c["optType"] == "C"
    assert c["expTime"] == "1751040000000"
    assert opts[1]["optType"] == "P"


def test_linear_strike_fallback_when_api_strike_zero():
    """REST may return strike=0; instrument_name still has the strike."""
    raw = [
        {
            "instrument_name": "BTC_USDC-6APR26-95000-C",
            "strike": 0.0,
            "option_type": "call",
            "expiration_timestamp": 1,
        },
        {
            "instrument_name": "BTC_USDC-6APR26-95000-P",
            "strike": 0.0,
            "option_type": "put",
            "expiration_timestamp": 1,
        },
    ]
    opts, _ = _normalize_instruments_linear(raw, [])
    assert opts[0]["stk"] == "95000"
    assert opts[0]["instId"] == "BTC-USDC-260406-95000-C"
    assert opts[1]["stk"] == "95000"


def test_linear_fractional_strike_preserved():
    """USDC linear alts use fractional strikes (~1.32); must not int-truncate to 1."""
    raw = [
        {
            "instrument_name": "XRP_USDC-10APR26-1.35-C",
            "strike": 1.35,
            "option_type": "call",
            "expiration_timestamp": 1,
        },
        {
            "instrument_name": "XRP_USDC-10APR26-1.35-P",
            "strike": 1.35,
            "option_type": "put",
            "expiration_timestamp": 1,
        },
    ]
    opts, _ = _normalize_instruments_linear(raw, [])
    assert opts[0]["stk"] == "1.35"
    assert opts[0]["instId"] == "XRP-USDC-260410-1.35-C"


def test_linear_normalize_future_instid():
    _, futs = _normalize_instruments_linear([], RAW_LINEAR_FUTURES)
    assert len(futs) == 1                   # PERPETUAL 被过滤
    assert futs[0]["instId"] == "BTC-USDC-250627"


def test_internal_to_deribit_linear_option():
    assert _internal_to_deribit_linear("BTC-USDC-250627-70000-C") == "BTC_USDC-27JUN25-70000-C"


def test_internal_to_deribit_linear_option_single_digit_day():
    assert (
        _internal_to_deribit_linear("BTC-USDC-260406-70000-C")
        == "BTC_USDC-6APR26-70000-C"
    )


def test_internal_to_deribit_linear_future():
    assert _internal_to_deribit_linear("BTC-USDC-250627") == "BTC_USDC-27JUN25"


def test_internal_to_deribit_linear_future_single_digit_day():
    assert _internal_to_deribit_linear("BTC-USDC-260406") == "BTC_USDC-6APR26"
