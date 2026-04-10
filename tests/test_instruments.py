from pcp_arbitrage.instruments import build_triplets, describe_unmatched_expiry, parse_expiry_from_option_id
from pcp_arbitrage.models import Triplet


# ── parse_expiry_from_option_id ────────────────────────────────────────────


def test_parse_expiry_btc():
    assert parse_expiry_from_option_id("BTC-USD-250425-95000-C") == "250425"


def test_parse_expiry_eth():
    assert parse_expiry_from_option_id("ETH-USD-250425-2000-P") == "250425"


# ── build_triplets ────────────────────────────────────────────────────────

# Minimal fake API payloads (only fields consumed by instruments.py)
OPTIONS = [
    # expiry 250425, BTC, strikes 90000 & 95000
    {
        "instId": "BTC-USD-250425-90000-C",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "90000",
        "optType": "C",
        "expTime": "1745596800000",
    },
    {
        "instId": "BTC-USD-250425-90000-P",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "90000",
        "optType": "P",
        "expTime": "1745596800000",
    },
    {
        "instId": "BTC-USD-250425-95000-C",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "95000",
        "optType": "C",
        "expTime": "1745596800000",
    },
    {
        "instId": "BTC-USD-250425-95000-P",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "95000",
        "optType": "P",
        "expTime": "1745596800000",
    },
    # expiry with no matching future → should be dropped
    {
        "instId": "BTC-USD-250530-95000-C",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "95000",
        "optType": "C",
        "expTime": "1748649600000",
    },
    {
        "instId": "BTC-USD-250530-95000-P",
        "instType": "OPTION",
        "uly": "BTC-USD",
        "ctVal": "0.01",
        "strike": "95000",
        "optType": "P",
        "expTime": "1748649600000",
    },
]

FUTURES = [
    # Only has a future for 250425
    {
        "instId": "BTC-USD-250425",
        "instType": "FUTURES",
        "uly": "BTC-USD",
        "expTime": "1745596800000",
    },
]

ATM_PRICES = {"BTC": 92000.0, "ETH": 2000.0}


def test_build_triplets_returns_list_of_triplets():
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,  # well before expiry
        margin_type="coin",
    )
    assert all(isinstance(t, Triplet) for t in triplets)


def test_build_triplets_excludes_no_future_expiry():
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="coin",
    )
    # 250530 has no future → excluded; 250425 has future → included
    expiries = {t.expiry for t in triplets}
    assert "250530" not in expiries
    assert "250425" in expiries


def test_build_triplets_atm_filter():
    # ATM = 92000; range 20% → [73600, 110400]
    # 90000 and 95000 both in range
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="coin",
    )
    strikes = {t.strike for t in triplets}
    assert 90000.0 in strikes
    assert 95000.0 in strikes


def test_build_triplets_excludes_out_of_atm_range():
    options_with_far_strike = OPTIONS + [
        {
            "instId": "BTC-USD-250425-200000-C",
            "uly": "BTC-USD",
            "strike": "200000",
            "optType": "C",
            "expTime": "1745596800000",
            "ctVal": "0.01",
        },
        {
            "instId": "BTC-USD-250425-200000-P",
            "uly": "BTC-USD",
            "strike": "200000",
            "optType": "P",
            "expTime": "1745596800000",
            "ctVal": "0.01",
        },
    ]
    triplets = build_triplets(
        options=options_with_far_strike,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="coin",
    )
    strikes = {t.strike for t in triplets}
    assert 200000.0 not in strikes


def test_build_triplets_excludes_expired():
    # now_ms is after expiry (1745596800000) → should be filtered
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1745596800001 + 86400_000,  # 1+ day after expiry
        margin_type="coin",
    )
    assert triplets == []


def test_build_triplets_respects_min_days_to_expiry():
    """min_days_to_expiry=1 drops T<1d; min_days_to_expiry=0 keeps positive T."""
    now_ms = 1743000000000
    exp_ms = now_ms + 12 * 3600 * 1000
    exp_s = str(exp_ms)
    opts_near = [
        {
            "instId": "BTC-USD-250425-90000-C",
            "instType": "OPTION",
            "uly": "BTC-USD",
            "ctVal": "0.01",
            "strike": "90000",
            "optType": "C",
            "expTime": exp_s,
        },
        {
            "instId": "BTC-USD-250425-90000-P",
            "instType": "OPTION",
            "uly": "BTC-USD",
            "ctVal": "0.01",
            "strike": "90000",
            "optType": "P",
            "expTime": exp_s,
        },
        {
            "instId": "BTC-USD-250425-95000-C",
            "instType": "OPTION",
            "uly": "BTC-USD",
            "ctVal": "0.01",
            "strike": "95000",
            "optType": "C",
            "expTime": exp_s,
        },
        {
            "instId": "BTC-USD-250425-95000-P",
            "instType": "OPTION",
            "uly": "BTC-USD",
            "ctVal": "0.01",
            "strike": "95000",
            "optType": "P",
            "expTime": exp_s,
        },
    ]
    futs_near = [
        {
            "instId": "BTC-USD-250425",
            "instType": "FUTURES",
            "uly": "BTC-USD",
            "expTime": exp_s,
        },
    ]
    common = dict(
        options=opts_near,
        futures=futs_near,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=now_ms,
        margin_type="coin",
    )
    assert build_triplets(**common, min_days_to_expiry=1.0) == []
    triplets = build_triplets(**common, min_days_to_expiry=0.0)
    strikes = {t.strike for t in triplets}
    assert strikes == {90000.0, 95000.0}


def test_build_triplets_triplet_fields(triplet_from_build=None):
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="coin",
    )
    t95 = next(t for t in triplets if t.strike == 95000.0)
    assert t95.call_id == "BTC-USD-250425-95000-C"
    assert t95.put_id == "BTC-USD-250425-95000-P"
    assert t95.future_id == "BTC-USD-250425"
    assert t95.symbol == "BTC"


# ── margin_type="usdt" mode ────────────────────────────────────────────────

USDT_FUTURES = [
    {
        "instId": "BTC-USDT-250425",
        "instType": "FUTURES",
        "uly": "BTC-USDT",
        "expTime": "1745596800000",
    },
]

USDT_OPTIONS = [
    {
        "instId": "BTC-USDT-250425-90000-C",
        "instType": "OPTION",
        "uly": "BTC-USDT",
        "ctVal": "0.01",
        "strike": "90000",
        "optType": "C",
        "expTime": "1745596800000",
    },
    {
        "instId": "BTC-USDT-250425-90000-P",
        "instType": "OPTION",
        "uly": "BTC-USDT",
        "ctVal": "0.01",
        "strike": "90000",
        "optType": "P",
        "expTime": "1745596800000",
    },
]


def test_build_triplets_usdt_mode_matches_usdt_future():
    """margin_type='usdt' should match BTC-USDT-* futures and ignore BTC-USD-* ones."""
    triplets = build_triplets(
        options=USDT_OPTIONS,
        futures=USDT_FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="usdt",
    )
    assert len(triplets) == 1
    assert triplets[0].future_id == "BTC-USDT-250425"


def test_build_triplets_usdt_mode_ignores_coin_future():
    """margin_type='usdt' should NOT match BTC-USD-* (coin-margined) futures."""
    triplets = build_triplets(
        options=USDT_OPTIONS,
        futures=FUTURES,   # FUTURES has BTC-USD-250425 (coin-margined)
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
        margin_type="usdt",
    )
    assert triplets == []


# ── describe_unmatched_expiry ───────────────────────────────────────────────


def test_describe_unmatched_expiry_lt_one_day():
    now_ms = 1743000000000
    exp_ms = now_ms + 12 * 3600 * 1000
    opts = [
        {
            "instId": "BTC-USD-250425-90000-C",
            "strike": "90000",
            "optType": "C",
            "expTime": str(exp_ms),
        },
        {
            "instId": "BTC-USD-250425-90000-P",
            "strike": "90000",
            "optType": "P",
            "expTime": str(exp_ms),
        },
    ]
    s = describe_unmatched_expiry(
        "BTC", "250425", opts, "USD", {"BTC": 92000.0}, 0.2, now_ms
    )
    assert "阈值 1.0" in s


def test_describe_unmatched_expiry_outside_atm_band():
    exp_ms = 1745596800000
    opts = [
        {
            "instId": "BTC-USD-250425-200000-C",
            "strike": "200000",
            "optType": "C",
            "expTime": str(exp_ms),
        },
        {
            "instId": "BTC-USD-250425-200000-P",
            "strike": "200000",
            "optType": "P",
            "expTime": str(exp_ms),
        },
    ]
    s = describe_unmatched_expiry(
        "BTC", "250425", opts, "USD", {"BTC": 92000.0}, 0.2, 1743000000000
    )
    assert "平值带宽内无行权价" in s
    assert "无交集" in s


def test_describe_unmatched_expiry_zero_strikes():
    """Strikes at 0 should get a clear message, not [0,0] band confusion."""
    exp_ms = 1745596800000
    opts = [
        {
            "instId": "FOO-USDC-250425-0-C",
            "strike": "0",
            "optType": "C",
            "expTime": str(exp_ms),
        },
        {
            "instId": "FOO-USDC-250425-0-P",
            "strike": "0",
            "optType": "P",
            "expTime": str(exp_ms),
        },
    ]
    s = describe_unmatched_expiry(
        "FOO", "250425", opts, "USDC", {"FOO": 100.0}, 0.2, 1743000000000
    )
    assert "行权价为 0" in s or "异常" in s


def test_describe_unmatched_expiry_no_call_put_pair():
    opts = [
        {
            "instId": "BTC-USD-250425-90000-C",
            "strike": "90000",
            "optType": "C",
            "expTime": "1745596800000",
        },
    ]
    s = describe_unmatched_expiry(
        "BTC", "250425", opts, "USD", {"BTC": 92000.0}, 0.2, 1743000000000
    )
    assert "无同价" in s
