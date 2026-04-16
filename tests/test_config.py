from pathlib import Path
from pcp_arbitrage.config import DEFAULT_LOT_SIZES, load_config, AppConfig, ExchangeConfig

FIXTURE = Path(__file__).parent / "fixtures" / "config_test.yaml"


def test_load_config_returns_app_config():
    cfg = load_config(FIXTURE)
    assert isinstance(cfg, AppConfig)


def test_config_symbols():
    cfg = load_config(FIXTURE)
    assert "BTC" in cfg.symbols
    assert "ETH" in cfg.symbols


def test_config_lot_sizes_use_builtin_defaults():
    cfg = load_config(FIXTURE)
    assert cfg.lot_size["BTC"] == DEFAULT_LOT_SIZES["BTC"]
    assert cfg.lot_size["ETH"] == DEFAULT_LOT_SIZES["ETH"]
    assert cfg.lot_size["SOL"] == DEFAULT_LOT_SIZES["SOL"]


def test_config_min_annualized_rate():
    cfg = load_config(FIXTURE)
    assert cfg.min_annualized_rate == 0.10


def test_config_atm_range():
    cfg = load_config(FIXTURE)
    assert cfg.atm_range == 0.20


def test_config_min_days_to_expiry():
    cfg = load_config(FIXTURE)
    assert cfg.min_days_to_expiry == 1.0


def test_config_stale_threshold_ms():
    cfg = load_config(FIXTURE)
    assert cfg.stale_threshold_ms == 5000


def test_config_maker_chase_defaults():
    cfg = load_config(FIXTURE)
    assert cfg.maker_chase_secs == 60
    assert cfg.maker_chase_max_minutes == 3


def test_config_tick_interval_sec_default():
    cfg = load_config(FIXTURE)
    assert cfg.tick_interval_sec == 0.0


def test_config_dashboard_quiet_exchanges_default():
    cfg = load_config(FIXTURE)
    assert cfg.dashboard_quiet_exchanges is True


def test_config_tracing_defaults():
    cfg = load_config(FIXTURE)
    assert cfg.pairing_log_dir == "data/pairings"
    assert cfg.opportunity_csv_enabled is True
    assert cfg.opportunity_csv_path == "data/opportunities.csv"
    assert cfg.opportunity_csv_interval_sec == 5.0


def test_exchange_config_parsed():
    cfg = load_config(FIXTURE)
    assert "okx" in cfg.exchanges
    okx = cfg.exchanges["okx"]
    assert isinstance(okx, ExchangeConfig)
    assert okx.name == "okx"
    assert okx.enabled is True
    assert okx.margin_type == "coin"
    assert okx.api_key == "test_key"
    assert okx.passphrase == "test_pass"


def test_lot_size_yaml_override_merges_with_defaults():
    import pathlib
    import tempfile

    raw_yaml = """
exchanges:
  okx:
    enabled: true
    api_key: "k"
    secret_key: "s"
    passphrase: "p"
arbitrage:
  min_annualized_rate: 0.01
  atm_range: 0.20
  symbols: [BTC, ETH]
  stale_threshold_ms: 5000
contracts:
  lot_size:
    BTC: 0.02
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(raw_yaml)
        tmp = pathlib.Path(f.name)
    try:
        cfg = load_config(tmp)
        assert cfg.lot_size["BTC"] == 0.02
        assert cfg.lot_size["ETH"] == DEFAULT_LOT_SIZES["ETH"]
    finally:
        tmp.unlink()


def test_exchange_config_margin_type_fixed_by_exchange_name():
    """margin_type is fixed by exchange name, not YAML value."""
    raw_yaml = """
exchanges:
  okx:
    enabled: true
    api_key: "k"
    secret_key: "s"
    passphrase: "p"
arbitrage:
  min_annualized_rate: 0.01
  atm_range: 0.20
  symbols: [BTC]
  stale_threshold_ms: 5000
"""
    import pathlib
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(raw_yaml)
        tmp = pathlib.Path(f.name)
    try:
        cfg = load_config(tmp)
        assert cfg.exchanges["okx"].margin_type == "coin"
        assert cfg.min_days_to_expiry == 1.0
        assert cfg.lot_size["BTC"] == DEFAULT_LOT_SIZES["BTC"]
    finally:
        tmp.unlink()


def test_binance_exchange_config_parsed():
    cfg = load_config(FIXTURE)
    assert "binance" in cfg.exchanges
    binance = cfg.exchanges["binance"]
    assert binance.enabled is False
    assert binance.margin_type == "usdt"
    assert binance.passphrase == ""   # sentinel: not in YAML, defaults to ""


def test_exchange_config_margin_type_ignores_yaml_value():
    raw_yaml = """
exchanges:
  binance:
    enabled: true
    margin_type: coin
    api_key: "k"
    secret_key: "s"
arbitrage:
  min_annualized_rate: 0.01
  atm_range: 0.20
  symbols: [BTC]
  stale_threshold_ms: 5000
"""
    import pathlib
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(raw_yaml)
        tmp = pathlib.Path(f.name)
    try:
        cfg = load_config(tmp)
        assert cfg.exchanges["binance"].margin_type == "usdt"
    finally:
        tmp.unlink()
