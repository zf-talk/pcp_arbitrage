# PCP Arbitrage Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a real-time PCP (Put-Call Parity) arbitrage monitor for BTC/ETH options on OKX that detects and prints arbitrage opportunities to the console without placing any orders.

**Architecture:** Single-process asyncio event loop that fetches instrument lists and fee rates at startup via REST, then subscribes to OKX `books5` WebSocket channels for all matched (call, put, future) triplets. Every incoming book update triggers a PCP calculation; profitable signals above the configured annualized threshold are printed to stdout.

**Tech Stack:** Python 3.12+, uv (dependency manager), aiohttp (HTTP + WebSocket), PyYAML (config), ruff (lint/format), pytest + pytest-asyncio (tests)

---

## File Map

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Project metadata, dependencies, tool config |
| `Makefile` | Developer shortcuts (install, run, dev, lint, fmt, clean) |
| `config.yaml` | User-facing runtime configuration |
| `src/pcp_arbitrage/__init__.py` | Package marker |
| `src/pcp_arbitrage/config.py` | Load `config.yaml` → typed `AppConfig` dataclass |
| `src/pcp_arbitrage/models.py` | Shared dataclasses: `BookSnapshot`, `FeeRates`, `Triplet` |
| `src/pcp_arbitrage/okx_client.py` | OKX REST calls + WebSocket connection management |
| `src/pcp_arbitrage/instruments.py` | Fetch & filter options/futures; build `Triplet` list |
| `src/pcp_arbitrage/fee_fetcher.py` | Fetch account fee tier → `FeeRates` |
| `src/pcp_arbitrage/market_data.py` | In-memory `order_books` store; update on WS push |
| `src/pcp_arbitrage/pcp_calculator.py` | PCP math: gross profit, fee deduction, annualized return |
| `src/pcp_arbitrage/signal_printer.py` | Format + print arbitrage signals |
| `src/pcp_arbitrage/main.py` | Entry point: wire everything together, start event loop |
| `tests/conftest.py` | Shared pytest fixtures |
| `tests/test_config.py` | Config loading tests |
| `tests/test_models.py` | Dataclass sanity tests |
| `tests/test_instruments.py` | Filter & triplet-build logic tests |
| `tests/test_pcp_calculator.py` | PCP math tests (forward & reverse, fee caps) |
| `tests/test_signal_printer.py` | Output format tests |
| `tests/test_market_data.py` | Order-book state update tests |

---

## Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `Makefile`
- Create: `config.yaml`
- Create: `src/pcp_arbitrage/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "pcp-arbitrage"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "aiohttp>=3.9",
    "pyyaml>=6.0",
]

[project.scripts]
pcp-arbitrage = "pcp_arbitrage.main:main"

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "ruff>=0.4",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"

[tool.ruff]
line-length = 100
```

- [ ] **Step 2: Create `Makefile`**

```makefile
.PHONY: install run dev lint fmt clean

install:
	uv sync

run:
	uv run pcp-arbitrage

dev:
	PAPER_TRADING=true uv run pcp-arbitrage

lint:
	uv run ruff check src tests

fmt:
	uv run ruff format src tests

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
```

- [ ] **Step 3: Create `config.yaml`**

```yaml
okx:
  api_key: ""
  secret_key: ""
  passphrase: ""
  is_paper_trading: false

arbitrage:
  min_annualized_rate: 0.10
  atm_range: 0.20
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000

contracts:
  lot_size:
    BTC: 0.01
    ETH: 0.1
```

- [ ] **Step 4: Create package markers**

```bash
mkdir -p src/pcp_arbitrage tests
touch src/pcp_arbitrage/__init__.py tests/__init__.py
```

- [ ] **Step 5: Create `tests/conftest.py`**

```python
import pytest

@pytest.fixture
def btc_lot_size() -> float:
    return 0.01

@pytest.fixture
def eth_lot_size() -> float:
    return 0.1
```

- [ ] **Step 6: Install dependencies**

```bash
make install
```

Expected: uv resolves and installs all packages without error.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml Makefile config.yaml src/ tests/
git commit -m "chore: project scaffold with uv, aiohttp, pytest"
```

---

## Task 2: Shared Models

**Files:**
- Create: `src/pcp_arbitrage/models.py`
- Create: `tests/test_models.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_models.py
from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet

def test_book_snapshot_fields():
    snap = BookSnapshot(bid=100.0, ask=101.0, ts=1700000000000)
    assert snap.bid == 100.0
    assert snap.ask == 101.0
    assert snap.ts == 1700000000000

def test_fee_rates_fields():
    fees = FeeRates(option_rate=0.0002, future_rate=0.0005)
    assert fees.option_rate == 0.0002
    assert fees.future_rate == 0.0005

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
    assert t.strike == 95000.0
    assert t.future_id == "BTC-USD-250425"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_models.py -v
```

Expected: `ImportError` — module not found.

- [ ] **Step 3: Create `src/pcp_arbitrage/models.py`**

```python
from dataclasses import dataclass


@dataclass
class BookSnapshot:
    bid: float
    ask: float
    ts: int  # millisecond timestamp from WebSocket push


@dataclass
class FeeRates:
    option_rate: float   # USDT per coin (e.g. 0.0002)
    future_rate: float   # fraction of notional (e.g. 0.0005)


@dataclass
class Triplet:
    symbol: str       # "BTC" or "ETH"
    expiry: str       # "250425"
    strike: float     # strike price in USDT
    call_id: str      # key in order_books
    put_id: str
    future_id: str
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_models.py -v
```

Expected: 3 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/models.py tests/test_models.py
git commit -m "feat: shared dataclasses (BookSnapshot, FeeRates, Triplet)"
```

---

## Task 3: Config Loader

**Files:**
- Create: `src/pcp_arbitrage/config.py`
- Create: `tests/test_config.py`
- Create: `tests/fixtures/config_test.yaml`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_config.py
import pytest
from pathlib import Path
from pcp_arbitrage.config import load_config, AppConfig

FIXTURE = Path(__file__).parent / "fixtures" / "config_test.yaml"

def test_load_config_returns_app_config():
    cfg = load_config(FIXTURE)
    assert isinstance(cfg, AppConfig)

def test_config_symbols():
    cfg = load_config(FIXTURE)
    assert "BTC" in cfg.symbols
    assert "ETH" in cfg.symbols

def test_config_lot_sizes():
    cfg = load_config(FIXTURE)
    assert cfg.lot_size["BTC"] == 0.01
    assert cfg.lot_size["ETH"] == 0.1

def test_config_min_annualized_rate():
    cfg = load_config(FIXTURE)
    assert cfg.min_annualized_rate == 0.10

def test_config_atm_range():
    cfg = load_config(FIXTURE)
    assert cfg.atm_range == 0.20

def test_config_stale_threshold_ms():
    cfg = load_config(FIXTURE)
    assert cfg.stale_threshold_ms == 5000
```

- [ ] **Step 2: Create test fixture**

```bash
mkdir -p tests/fixtures
```

```yaml
# tests/fixtures/config_test.yaml
okx:
  api_key: "test_key"
  secret_key: "test_secret"
  passphrase: "test_pass"
  is_paper_trading: false

arbitrage:
  min_annualized_rate: 0.10
  atm_range: 0.20
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000

contracts:
  lot_size:
    BTC: 0.01
    ETH: 0.1
```

- [ ] **Step 3: Run test to verify it fails**

```bash
uv run pytest tests/test_config.py -v
```

Expected: `ImportError` — module not found.

- [ ] **Step 4: Create `src/pcp_arbitrage/config.py`**

```python
from dataclasses import dataclass
from pathlib import Path
import yaml


@dataclass
class AppConfig:
    api_key: str
    secret_key: str
    passphrase: str
    is_paper_trading: bool
    symbols: list[str]
    min_annualized_rate: float
    atm_range: float
    stale_threshold_ms: int
    lot_size: dict[str, float]


def load_config(path: Path | str = "config.yaml") -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    okx = raw["okx"]
    arb = raw["arbitrage"]
    return AppConfig(
        api_key=okx["api_key"],
        secret_key=okx["secret_key"],
        passphrase=okx["passphrase"],
        is_paper_trading=okx.get("is_paper_trading", False),
        symbols=arb["symbols"],
        min_annualized_rate=arb["min_annualized_rate"],
        atm_range=arb["atm_range"],
        stale_threshold_ms=arb["stale_threshold_ms"],
        lot_size=raw["contracts"]["lot_size"],
    )
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_config.py -v
```

Expected: 6 PASSED.

- [ ] **Step 6: Commit**

```bash
git add src/pcp_arbitrage/config.py tests/test_config.py tests/fixtures/config_test.yaml
git commit -m "feat: config loader with AppConfig dataclass"
```

---

## Task 4: Market Data Store

**Files:**
- Create: `src/pcp_arbitrage/market_data.py`
- Create: `tests/test_market_data.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_market_data.py
import pytest
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_market_data.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `src/pcp_arbitrage/market_data.py`**

```python
from pcp_arbitrage.models import BookSnapshot


class MarketData:
    """Thread-safe-enough in-memory order book store (single asyncio thread)."""

    def __init__(self) -> None:
        self._books: dict[str, BookSnapshot] = {}

    def update(self, inst_id: str, snap: BookSnapshot) -> None:
        self._books[inst_id] = snap

    def get(self, inst_id: str) -> BookSnapshot | None:
        return self._books.get(inst_id)

    def clear(self) -> None:
        self._books.clear()

    def has_all(self, inst_ids: list[str]) -> bool:
        return all(inst_id in self._books for inst_id in inst_ids)
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_market_data.py -v
```

Expected: 5 PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/market_data.py tests/test_market_data.py
git commit -m "feat: MarketData in-memory order book store"
```

---

## Task 5: PCP Calculator

**Files:**
- Create: `src/pcp_arbitrage/pcp_calculator.py`
- Create: `tests/test_pcp_calculator.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_pcp_calculator.py
import pytest
from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse, ArbitrageSignal
from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet
import time

NOW_MS = int(time.time() * 1000)

@pytest.fixture
def fee_rates():
    return FeeRates(option_rate=0.0002, future_rate=0.0005)

@pytest.fixture
def triplet():
    return Triplet(
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
        "BTC-USD-250425":         BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }

@pytest.fixture
def books_reverse():
    """Reverse: call_bid - put_ask + K - future_ask should be positive."""
    return {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=120.0, ask=120.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=80.0, ask=80.0, ts=NOW_MS),
        "BTC-USD-250425":         BookSnapshot(bid=1958.0, ask=1958.0, ts=NOW_MS),
    }

# ── Forward ────────────────────────────────────────────────────────────────

def test_forward_gross_profit(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0)
    assert sig is not None
    # gross = 5600 - 200 + 89630 - 95000 = 30
    assert abs(sig.gross_profit - 30.0) < 0.01

def test_forward_net_profit_less_than_gross(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0)
    assert sig is not None
    assert sig.net_profit < sig.gross_profit

def test_forward_returns_none_when_not_profitable(triplet, fee_rates):
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=200.0, ask=5000.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=50.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425":         BookSnapshot(bid=89000.0, ask=89010.0, ts=NOW_MS),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0)
    assert sig is None or sig.net_profit <= 0

# ── Fee cap ────────────────────────────────────────────────────────────────

def test_option_fee_cap_applied(fee_rates):
    """When option_price × 0.125 < lot_size × option_rate, cap applies."""
    from pcp_arbitrage.pcp_calculator import _option_fee
    # fee_by_face = 0.01 × 0.0002 = 0.000002
    # fee_cap = 0.01 × 0.01 × 0.125 = 0.0000125  → face value wins
    fee = _option_fee(option_price=0.01, lot_size=0.01, option_rate=0.0002)
    assert abs(fee - 0.000002) < 1e-9

def test_option_fee_cap_clamps_high_rate():
    """When fee_by_face > cap, cap is returned."""
    from pcp_arbitrage.pcp_calculator import _option_fee
    # fee_by_face = 0.01 × 1.0 = 0.01
    # fee_cap = 100.0 × 0.01 × 0.125 = 0.125  → face value (0.01) < cap (0.125)
    # Actually let's make face > cap:
    # fee_by_face = 100 × 1.0 = 100; fee_cap = 0.001 × 100 × 0.125 = 0.0125 → cap wins
    fee = _option_fee(option_price=0.001, lot_size=100.0, option_rate=1.0)
    assert fee == pytest.approx(0.001 * 100.0 * 0.125)

# ── Annualized return ──────────────────────────────────────────────────────

def test_annualized_return_formula(triplet, books_forward, fee_rates):
    sig = calculate_forward(triplet, books_forward, fee_rates, lot_size=0.01, days_to_expiry=21.0)
    assert sig is not None
    expected = (sig.net_profit / triplet.strike) * (365 / 21.0)
    assert abs(sig.annualized_return - expected) < 1e-9

# ── Stale data ────────────────────────────────────────────────────────────

def test_stale_books_returns_none(triplet, fee_rates):
    old_ts = NOW_MS - 10_000  # 10 seconds ago
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=200.0, ask=200.0, ts=old_ts),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=5600.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425":         BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0,
                            stale_threshold_ms=5000)
    assert sig is None

def test_zero_bid_ask_returns_none(triplet, fee_rates):
    books = {
        "BTC-USD-250425-95000-C": BookSnapshot(bid=0.0, ask=200.0, ts=NOW_MS),
        "BTC-USD-250425-95000-P": BookSnapshot(bid=5600.0, ask=5600.0, ts=NOW_MS),
        "BTC-USD-250425":         BookSnapshot(bid=89630.0, ask=89640.0, ts=NOW_MS),
    }
    sig = calculate_forward(triplet, books, fee_rates, lot_size=0.01, days_to_expiry=21.0)
    assert sig is None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pcp_calculator.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `src/pcp_arbitrage/pcp_calculator.py`**

```python
from dataclasses import dataclass
import time

from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet


@dataclass
class ArbitrageSignal:
    direction: str        # "forward" or "reverse"
    triplet: Triplet
    call_price: float     # price used (ask for forward buy, bid for reverse sell)
    put_price: float
    future_price: float
    gross_profit: float
    total_fee: float
    net_profit: float
    annualized_return: float  # e.g. 0.15 = 15%
    days_to_expiry: float


def _option_fee(option_price: float, lot_size: float, option_rate: float) -> float:
    fee_by_face = lot_size * option_rate
    fee_cap = option_price * lot_size * 0.125
    return min(fee_by_face, fee_cap)


def _future_fee(future_price: float, lot_size: float, future_rate: float) -> float:
    return future_price * lot_size * future_rate


def _integrity_check(
    inst_ids: list[str],
    books: dict[str, BookSnapshot],
    stale_threshold_ms: int,
) -> bool:
    now_ms = int(time.time() * 1000)
    for inst_id in inst_ids:
        snap = books.get(inst_id)
        if snap is None:
            return False
        if (now_ms - snap.ts) > stale_threshold_ms:
            return False
        if snap.bid <= 0 or snap.ask <= 0:
            return False
    return True


def calculate_forward(
    triplet: Triplet,
    books: dict[str, BookSnapshot],
    fee_rates: FeeRates,
    lot_size: float,
    days_to_expiry: float,
    stale_threshold_ms: int = 5000,
) -> ArbitrageSignal | None:
    """Forward: buy Call + sell Put + sell Future."""
    inst_ids = [triplet.call_id, triplet.put_id, triplet.future_id]
    if not _integrity_check(inst_ids, books, stale_threshold_ms):
        return None

    call_snap = books[triplet.call_id]
    put_snap = books[triplet.put_id]
    fut_snap = books[triplet.future_id]

    call_ask = call_snap.ask
    put_bid = put_snap.bid
    future_bid = fut_snap.bid
    K = triplet.strike

    gross = put_bid - call_ask + future_bid - K

    call_fee = _option_fee(call_ask, lot_size, fee_rates.option_rate)
    put_fee = _option_fee(put_bid, lot_size, fee_rates.option_rate)
    fut_fee = _future_fee(future_bid, lot_size, fee_rates.future_rate)
    total_fee = 2 * (call_fee + put_fee + fut_fee)

    net = gross - total_fee
    if net <= 0:
        return None

    ann = (net / K) * (365 / days_to_expiry)

    return ArbitrageSignal(
        direction="forward",
        triplet=triplet,
        call_price=call_ask,
        put_price=put_bid,
        future_price=future_bid,
        gross_profit=gross,
        total_fee=total_fee,
        net_profit=net,
        annualized_return=ann,
        days_to_expiry=days_to_expiry,
    )


def calculate_reverse(
    triplet: Triplet,
    books: dict[str, BookSnapshot],
    fee_rates: FeeRates,
    lot_size: float,
    days_to_expiry: float,
    stale_threshold_ms: int = 5000,
) -> ArbitrageSignal | None:
    """Reverse: sell Call + buy Put + buy Future."""
    inst_ids = [triplet.call_id, triplet.put_id, triplet.future_id]
    if not _integrity_check(inst_ids, books, stale_threshold_ms):
        return None

    call_snap = books[triplet.call_id]
    put_snap = books[triplet.put_id]
    fut_snap = books[triplet.future_id]

    call_bid = call_snap.bid
    put_ask = put_snap.ask
    future_ask = fut_snap.ask
    K = triplet.strike

    gross = call_bid - put_ask + K - future_ask

    call_fee = _option_fee(call_bid, lot_size, fee_rates.option_rate)
    put_fee = _option_fee(put_ask, lot_size, fee_rates.option_rate)
    fut_fee = _future_fee(future_ask, lot_size, fee_rates.future_rate)
    total_fee = 2 * (call_fee + put_fee + fut_fee)

    net = gross - total_fee
    if net <= 0:
        return None

    ann = (net / K) * (365 / days_to_expiry)

    return ArbitrageSignal(
        direction="reverse",
        triplet=triplet,
        call_price=call_bid,
        put_price=put_ask,
        future_price=future_ask,
        gross_profit=gross,
        total_fee=total_fee,
        net_profit=net,
        annualized_return=ann,
        days_to_expiry=days_to_expiry,
    )
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pcp_calculator.py -v
```

Expected: all PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/pcp_calculator.py tests/test_pcp_calculator.py
git commit -m "feat: PCP calculator with fee caps and integrity checks"
```

---

## Task 6: Signal Printer

**Files:**
- Create: `src/pcp_arbitrage/signal_printer.py`
- Create: `tests/test_signal_printer.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_signal_printer.py
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
    assert "卖一" in output   # call_ask → "← 卖一"

def test_reverse_labels_call_bid():
    sig = _make_signal("reverse")
    output = format_signal(sig)
    assert "买一" in output   # call_bid → "← 买一"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_signal_printer.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `src/pcp_arbitrage/signal_printer.py`**

```python
from datetime import datetime
from pcp_arbitrage.pcp_calculator import ArbitrageSignal

SEP = "─" * 52


def format_signal(sig: ArbitrageSignal) -> str:
    t = sig.triplet
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    direction_cn = "正向套利" if sig.direction == "forward" else "反向套利"

    if sig.direction == "forward":
        combo = "买 Call + 卖 Put + 卖 Future"
        call_label = "call_ask"
        call_note = "← 卖一"
        put_label = "put_bid "
        put_note = "← 买一"
        future_label = "future_bid"
        future_note = "← 买一"
    else:
        combo = "卖 Call + 买 Put + 买 Future"
        call_label = "call_bid"
        call_note = "← 买一"
        put_label = "put_ask "
        put_note = "← 卖一"
        future_label = "future_ask"
        future_note = "← 卖一"

    ann_pct = sig.annualized_return * 100
    net_str = (
        f"({sig.put_price:.2f} - {sig.call_price:.2f} + {sig.future_price:.2f} - "
        f"{t.strike:.2f} - {sig.total_fee:.2f})"
        if sig.direction == "forward"
        else (
            f"({sig.call_price:.2f} - {sig.put_price:.2f} + {t.strike:.2f} - "
            f"{sig.future_price:.2f} - {sig.total_fee:.2f})"
        )
    )

    lines = [
        f"[{now}] [{direction_cn}] {t.symbol}-{t.expiry}-{int(t.strike)}",
        f"  期权组合  : {combo}",
        f"  {call_label}  : {sig.call_price:>10.2f} USDT  {call_note}",
        f"  {put_label}   : {sig.put_price:>10.2f} USDT  {put_note}",
        f"  {future_label}: {sig.future_price:>10.2f} USDT {future_note}",
        f"  K         : {t.strike:>10.2f} USDT",
        f"  手续费    : {sig.total_fee:>10.2f} USDT  (进出场双边 × 3腿)",
        f"  净利润    : {sig.net_profit:>10.2f} USDT  {net_str}",
        f"  年化收益  : {ann_pct:>9.2f}%      ({sig.net_profit:.2f} / {int(t.strike)} × 365 / {sig.days_to_expiry:.0f} × 100)",
        SEP,
    ]
    return "\n".join(lines)


def print_signal(sig: ArbitrageSignal) -> None:
    print(format_signal(sig))
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_signal_printer.py -v
```

Expected: all PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/signal_printer.py tests/test_signal_printer.py
git commit -m "feat: signal formatter with Chinese labels and separator"
```

---

## Task 7: Instrument Discovery

**Files:**
- Create: `src/pcp_arbitrage/instruments.py`
- Create: `tests/test_instruments.py`

- [ ] **Step 1: Write failing tests**

The `build_triplets` function takes pre-fetched raw API data so it can be unit-tested without network calls.

```python
# tests/test_instruments.py
import pytest
from pcp_arbitrage.instruments import build_triplets, parse_expiry_from_option_id
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
    {"instId": "BTC-USD-250425-90000-C", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "90000", "optType": "C", "expTime": "1745596800000"},
    {"instId": "BTC-USD-250425-90000-P", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "90000", "optType": "P", "expTime": "1745596800000"},
    {"instId": "BTC-USD-250425-95000-C", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "95000", "optType": "C", "expTime": "1745596800000"},
    {"instId": "BTC-USD-250425-95000-P", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "95000", "optType": "P", "expTime": "1745596800000"},
    # expiry with no matching future → should be dropped
    {"instId": "BTC-USD-250530-95000-C", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "95000", "optType": "C", "expTime": "1748649600000"},
    {"instId": "BTC-USD-250530-95000-P", "instType": "OPTION", "uly": "BTC-USD", "ctVal": "0.01",
     "strike": "95000", "optType": "P", "expTime": "1748649600000"},
]

FUTURES = [
    # Only has a future for 250425
    {"instId": "BTC-USDT-250425", "instType": "FUTURES", "uly": "BTC-USDT",
     "expTime": "1745596800000"},
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
    )
    strikes = {t.strike for t in triplets}
    assert 90000.0 in strikes
    assert 95000.0 in strikes


def test_build_triplets_excludes_out_of_atm_range():
    options_with_far_strike = OPTIONS + [
        {"instId": "BTC-USD-250425-200000-C", "uly": "BTC-USD",
         "strike": "200000", "optType": "C", "expTime": "1745596800000", "ctVal": "0.01"},
        {"instId": "BTC-USD-250425-200000-P", "uly": "BTC-USD",
         "strike": "200000", "optType": "P", "expTime": "1745596800000", "ctVal": "0.01"},
    ]
    triplets = build_triplets(
        options=options_with_far_strike,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
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
    )
    assert triplets == []


def test_build_triplets_triplet_fields(triplet_from_build=None):
    triplets = build_triplets(
        options=OPTIONS,
        futures=FUTURES,
        atm_prices=ATM_PRICES,
        symbols=["BTC"],
        atm_range=0.20,
        now_ms=1743000000000,
    )
    t95 = next(t for t in triplets if t.strike == 95000.0)
    assert t95.call_id == "BTC-USD-250425-95000-C"
    assert t95.put_id == "BTC-USD-250425-95000-P"
    assert t95.future_id == "BTC-USDT-250425"
    assert t95.symbol == "BTC"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_instruments.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `src/pcp_arbitrage/instruments.py`**

```python
import logging
from pcp_arbitrage.models import Triplet

logger = logging.getLogger(__name__)


def parse_expiry_from_option_id(inst_id: str) -> str:
    """'BTC-USD-250425-95000-C' → '250425'"""
    return inst_id.split("-")[2]


def _parse_expiry_from_future_id(inst_id: str) -> str:
    """'BTC-USDT-250425' → '250425'"""
    return inst_id.split("-")[2]


def build_triplets(
    options: list[dict],
    futures: list[dict],
    atm_prices: dict[str, float],
    symbols: list[str],
    atm_range: float,
    now_ms: int,
) -> list[Triplet]:
    """
    Build (call, put, future) triplets from raw OKX API payloads.

    Filtering rules:
    - symbol in `symbols`
    - expiry has a matching USDT-margined future (warns and drops otherwise)
    - days_to_expiry >= 1
    - strike within ATM ± atm_range
    """
    # Index futures by symbol → expiry → inst_id
    future_index: dict[str, dict[str, str]] = {}
    for fut in futures:
        parts = fut["instId"].split("-")  # e.g. BTC-USDT-250425
        if len(parts) != 3:
            continue
        sym, margin, exp = parts
        if sym not in symbols or margin != "USDT":
            continue
        future_index.setdefault(sym, {})[exp] = fut["instId"]

    # Group options by (symbol, expiry, strike)
    call_map: dict[tuple[str, str, float], dict] = {}
    put_map: dict[tuple[str, str, float], dict] = {}

    for opt in options:
        inst_id = opt["instId"]
        parts = inst_id.split("-")
        if len(parts) != 5:
            continue
        sym = parts[0]
        if sym not in symbols:
            continue
        exp = parts[2]
        strike = float(opt["strike"])
        opt_type = opt["optType"]
        key = (sym, exp, strike)
        if opt_type == "C":
            call_map[key] = opt
        elif opt_type == "P":
            put_map[key] = opt

    triplets: list[Triplet] = []
    warned_expiries: set[str] = set()

    for key, call_opt in call_map.items():
        sym, exp, strike = key
        if key not in put_map:
            continue  # no matching put

        # Check days_to_expiry
        exp_ms = int(call_opt["expTime"])
        days_to_expiry = (exp_ms - now_ms) / 86_400_000
        if days_to_expiry < 1:
            continue

        # Check ATM filter
        atm = atm_prices.get(sym)
        if atm is None:
            continue
        lo, hi = atm * (1 - atm_range), atm * (1 + atm_range)
        if not (lo <= strike <= hi):
            continue

        # Check matching future
        fut_id = future_index.get(sym, {}).get(exp)
        if fut_id is None:
            if exp not in warned_expiries:
                logger.warning(
                    "[instruments] No USDT future found for %s expiry %s — dropping all triplets for this expiry",
                    sym, exp,
                )
                warned_expiries.add(exp)
            continue

        put_opt = put_map[key]
        triplets.append(
            Triplet(
                symbol=sym,
                expiry=exp,
                strike=strike,
                call_id=call_opt["instId"],
                put_id=put_opt["instId"],
                future_id=fut_id,
            )
        )

    return triplets
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_instruments.py -v
```

Expected: all PASSED.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/instruments.py tests/test_instruments.py
git commit -m "feat: instrument discovery and triplet builder"
```

---

## Task 8: OKX Client (REST)

**Files:**
- Create: `src/pcp_arbitrage/okx_client.py`

> **Note:** OKX client wraps live HTTP/WS. We test it at the integration level (no mocking). Unit tests for REST helper parsing belong in instrument/fee_fetcher tests. The WebSocket path is integration-tested manually.

- [ ] **Step 1: Create `src/pcp_arbitrage/okx_client.py`**

```python
import hmac
import base64
import hashlib
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

import aiohttp

logger = logging.getLogger(__name__)

BASE_REST = "https://www.okx.com"
BASE_REST_PAPER = "https://www.okx.com"   # same host, paper flag in header
WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
WS_PUBLIC_PAPER = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
WS_PRIVATE = "wss://ws.okx.com:8443/ws/v5/private"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sign(secret: str, ts: str, method: str, path: str, body: str = "") -> str:
    msg = ts + method.upper() + path + body
    mac = hmac.new(secret.encode(), msg.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()


class OKXRestClient:
    def __init__(self, api_key: str, secret: str, passphrase: str, is_paper: bool = False):
        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase
        self._is_paper = is_paper
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(base_url=BASE_REST)
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict:
        ts = _timestamp()
        sig = _sign(self._secret, ts, method, path, body)
        headers = {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }
        if self._is_paper:
            headers["x-simulated-trading"] = "1"
        return headers

    async def _get(self, path: str, params: dict | None = None, auth: bool = False) -> dict:
        assert self._session is not None
        headers = self._auth_headers("GET", path) if auth else {}
        async with self._session.get(path, params=params, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_instruments(self, inst_type: str, uly: str | None = None) -> list[dict]:
        """Fetch instrument list. inst_type: 'OPTION' or 'FUTURES'."""
        params: dict = {"instType": inst_type}
        if uly:
            params["uly"] = uly
        data = await self._get("/api/v5/public/instruments", params=params)
        return data.get("data", [])

    async def get_ticker(self, inst_id: str) -> dict:
        data = await self._get("/api/v5/market/ticker", params={"instId": inst_id})
        return data.get("data", [{}])[0]

    async def get_account_config(self) -> dict:
        """Returns account-level config including fee tier."""
        data = await self._get("/api/v5/account/config", auth=True)
        return data.get("data", [{}])[0]

    async def get_fee_rates(self, inst_type: str, inst_id: str = "") -> dict:
        """Fetch trading fee rates for given instType."""
        params: dict = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        data = await self._get("/api/v5/account/trade-fee", params=params, auth=True)
        return data.get("data", [{}])[0]


class OKXWebSocketClient:
    """Manages a single OKX WebSocket connection with heartbeat and reconnect."""

    MAX_CHANNELS_PER_CONN = 240
    PING_INTERVAL = 20
    RECONNECT_BASE = 1
    RECONNECT_MAX = 60

    def __init__(self, url: str, on_message, on_reconnect=None):
        self._url = url
        self._on_message = on_message
        self._on_reconnect = on_reconnect
        self._subscriptions: list[dict] = []
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None

    def add_subscriptions(self, channels: list[dict]) -> None:
        self._subscriptions.extend(channels)

    async def run(self) -> None:
        retry_delay = self.RECONNECT_BASE
        while True:
            try:
                await self._connect_and_run()
                retry_delay = self.RECONNECT_BASE  # reset on clean exit
            except Exception as exc:
                logger.warning("[ws] Disconnected: %s — reconnecting in %ds", exc, retry_delay)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_MAX)
                if self._on_reconnect:
                    await self._on_reconnect()

    async def _connect_and_run(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self._url) as ws:
                self._ws = ws
                logger.info("[ws] Connected to %s", self._url)
                await self._subscribe(ws)
                ping_task = asyncio.create_task(self._heartbeat(ws))
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if msg.data == "pong":
                                continue
                            await self._on_message(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                finally:
                    ping_task.cancel()

    async def _subscribe(self, ws) -> None:
        # chunk into groups of MAX_CHANNELS_PER_CONN if needed
        for i in range(0, len(self._subscriptions), self.MAX_CHANNELS_PER_CONN):
            chunk = self._subscriptions[i : i + self.MAX_CHANNELS_PER_CONN]
            await ws.send_str(json.dumps({"op": "subscribe", "args": chunk}))

    async def _heartbeat(self, ws) -> None:
        while True:
            await asyncio.sleep(self.PING_INTERVAL)
            await ws.send_str("ping")
```

- [ ] **Step 2: Verify import works**

```bash
uv run python -c "from pcp_arbitrage.okx_client import OKXRestClient, OKXWebSocketClient; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/pcp_arbitrage/okx_client.py
git commit -m "feat: OKX REST + WebSocket client with reconnect"
```

---

## Task 9: Fee Fetcher

**Files:**
- Create: `src/pcp_arbitrage/fee_fetcher.py`

- [ ] **Step 1: Create `src/pcp_arbitrage/fee_fetcher.py`**

```python
import logging
from pcp_arbitrage.models import FeeRates
from pcp_arbitrage.okx_client import OKXRestClient

logger = logging.getLogger(__name__)


async def fetch_fee_rates(client: OKXRestClient) -> FeeRates:
    """
    Fetch option and future fee rates from OKX.

    OKX returns taker rates as negative strings (e.g. "-0.0002").
    We take abs() so callers work with positive rates.

    Note: for OPTION instType the OKX endpoint requires a uly (underlying) to
    return meaningful per-contract rates. We use BTC-USD as the canonical uly;
    OKX fee tiers are account-level and the same across underlyings.
    """
    option_data = await client.get_fee_rates("OPTION", inst_id="BTC-USD-250425-95000-C")
    future_data = await client.get_fee_rates("FUTURES")

    # taker rate fields: "taker" for options, "taker" for futures
    option_rate = abs(float(option_data.get("taker", "0.0002")))
    future_rate = abs(float(future_data.get("taker", "0.0005")))

    logger.info("[fee_fetcher] option_rate=%.6f future_rate=%.6f", option_rate, future_rate)
    return FeeRates(option_rate=option_rate, future_rate=future_rate)
```

- [ ] **Step 2: Verify import**

```bash
uv run python -c "from pcp_arbitrage.fee_fetcher import fetch_fee_rates; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/pcp_arbitrage/fee_fetcher.py
git commit -m "feat: fee fetcher with taker rate parsing"
```

---

## Task 10: Main Entry Point

**Files:**
- Create: `src/pcp_arbitrage/main.py`

- [ ] **Step 1: Create `src/pcp_arbitrage/main.py`**

```python
import asyncio
import logging
import os
import time
from pathlib import Path

from pcp_arbitrage.config import load_config
from pcp_arbitrage.fee_fetcher import fetch_fee_rates
from pcp_arbitrage.instruments import build_triplets
from pcp_arbitrage.market_data import MarketData
from pcp_arbitrage.models import Triplet, FeeRates
from pcp_arbitrage.okx_client import OKXRestClient, OKXWebSocketClient
from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse
from pcp_arbitrage.signal_printer import print_signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _build_ws_args(triplets: list[Triplet]) -> list[dict]:
    inst_ids: set[str] = set()
    for t in triplets:
        inst_ids.update([t.call_id, t.put_id, t.future_id])
    return [{"channel": "books5", "instId": inst_id} for inst_id in sorted(inst_ids)]


async def _run(cfg_path: str = "config.yaml") -> None:
    cfg = load_config(cfg_path)

    # Override paper trading via env var (used by `make dev`)
    if os.environ.get("PAPER_TRADING", "").lower() == "true":
        cfg = cfg.__class__(
            **{**cfg.__dict__, "is_paper_trading": True}
        )

    async with OKXRestClient(
        api_key=cfg.api_key,
        secret=cfg.secret_key,
        passphrase=cfg.passphrase,
        is_paper=cfg.is_paper_trading,
    ) as rest:

        # 1. Fetch fee rates
        logger.info("[main] Fetching fee rates...")
        fee_rates: FeeRates = await fetch_fee_rates(rest)

        # 2. Fetch instruments
        logger.info("[main] Fetching instruments for %s...", cfg.symbols)
        all_options: list[dict] = []
        all_futures: list[dict] = []
        atm_prices: dict[str, float] = {}

        for sym in cfg.symbols:
            uly_option = f"{sym}-USD"
            uly_future = f"{sym}-USDT"
            opts = await rest.get_instruments("OPTION", uly=uly_option)
            futs = await rest.get_instruments("FUTURES", uly=uly_future)
            all_options.extend(opts)
            all_futures.extend(futs)

            # ATM price from spot-ish index (use BTC-USDT spot ticker)
            ticker = await rest.get_ticker(f"{sym}-USDT")
            atm_prices[sym] = float(ticker.get("last", 0))
            logger.info("[main] %s ATM price: %.2f", sym, atm_prices[sym])

        # 3. Build triplets
        triplets = build_triplets(
            options=all_options,
            futures=all_futures,
            atm_prices=atm_prices,
            symbols=cfg.symbols,
            atm_range=cfg.atm_range,
            now_ms=int(time.time() * 1000),
        )
        logger.info("[main] Built %d triplets", len(triplets))
        if not triplets:
            logger.error("[main] No triplets found — check config and OKX connectivity")
            return

        # 4. Set up market data store
        market = MarketData()

        # 5. Pre-build expiry lookup (once at startup, avoids per-message rebuild)
        exp_ms_by_call: dict[str, int] = {
            o["instId"]: int(o["expTime"]) for o in all_options
        }

        # 6. WS reconnect callback — clear stale cache
        async def on_reconnect() -> None:
            logger.warning("[main] WebSocket reconnected — clearing order book cache")
            market.clear()

        # 7. Handle incoming WS messages
        async def on_message(msg: dict) -> None:
            if msg.get("event"):  # subscribe/error acks
                if msg.get("event") == "error":
                    logger.error("[ws] Error: %s", msg)
                return

            arg = msg.get("arg", {})
            inst_id = arg.get("instId", "")
            data = msg.get("data", [])
            if not data:
                return

            book = data[0]
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            ts = int(book.get("ts", 0))

            if not bids or not asks:
                return

            from pcp_arbitrage.models import BookSnapshot
            snap = BookSnapshot(
                bid=float(bids[0][0]),
                ask=float(asks[0][0]),
                ts=ts,
            )
            market.update(inst_id, snap)

            # Trigger PCP calculation for all triplets that share this inst_id
            now_ms = int(time.time() * 1000)
            for t in triplets:
                if inst_id not in (t.call_id, t.put_id, t.future_id):
                    continue
                books_snapshot = {
                    t.call_id: market.get(t.call_id),
                    t.put_id: market.get(t.put_id),
                    t.future_id: market.get(t.future_id),
                }
                # Drop None entries — integrity check will handle it
                books_clean = {k: v for k, v in books_snapshot.items() if v is not None}

                lot_size = cfg.lot_size[t.symbol]
                # exp_ms_by_call is pre-built at startup (outside on_message) for efficiency
                exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)

                for calc in (calculate_forward, calculate_reverse):
                    sig = calc(
                        triplet=t,
                        books=books_clean,
                        fee_rates=fee_rates,
                        lot_size=lot_size,
                        days_to_expiry=days_to_expiry,
                        stale_threshold_ms=cfg.stale_threshold_ms,
                    )
                    if sig and sig.annualized_return >= cfg.min_annualized_rate:
                        print_signal(sig)

        # 8. Start WebSocket
        ws_args = _build_ws_args(triplets)
        logger.info("[main] Subscribing to %d channels", len(ws_args))
        ws_url = (
            "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
            if cfg.is_paper_trading
            else "wss://ws.okx.com:8443/ws/v5/public"
        )
        ws_client = OKXWebSocketClient(ws_url, on_message=on_message, on_reconnect=on_reconnect)
        ws_client.add_subscriptions(ws_args)
        await ws_client.run()


def main() -> None:
    asyncio.run(_run())
```

- [ ] **Step 2: Verify import**

```bash
uv run python -c "from pcp_arbitrage.main import main; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Run linter**

```bash
make lint
```

Fix any reported issues.

- [ ] **Step 4: Run all tests**

```bash
uv run pytest -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/pcp_arbitrage/main.py
git commit -m "feat: main entry point wiring config, WS, PCP calculator"
```

---

## Task 11: Full Test Suite Pass + Lint

**Files:** (no new files)

- [ ] **Step 1: Run full test suite**

```bash
uv run pytest -v --tb=short
```

Expected: all PASSED, no errors.

- [ ] **Step 2: Run linter**

```bash
make lint
```

Expected: no errors.

- [ ] **Step 3: Run formatter check**

```bash
uv run ruff format --check src tests
```

If formatting issues: `make fmt` then re-run.

- [ ] **Step 4: Commit any lint fixes**

```bash
git add -u
git commit -m "chore: lint and format fixes"
```

---

## Task 12: Smoke Test (Live API Optional)

> **Requires valid OKX credentials in `config.yaml` or paper trading mode.**

- [ ] **Step 1: Set paper trading mode**

Edit `config.yaml`:
```yaml
okx:
  is_paper_trading: true
```

Or use `make dev` which sets `PAPER_TRADING=true`.

- [ ] **Step 2: Run in dev mode**

```bash
make dev
```

Expected output (after a few seconds):
```
INFO main: Fetching fee rates...
INFO main: Fetching instruments for ['BTC', 'ETH']...
INFO main: BTC ATM price: 92000.00
INFO main: ETH ATM price: 2000.00
INFO main: Built N triplets
INFO main: Subscribing to M channels
```

If `net_profit > 0` and `annualized_return >= 0.10`, signals appear in the formatted output.

- [ ] **Step 3: Verify reconnect logic**

Kill the network connection momentarily. Confirm log shows:
```
WARNING ws: Disconnected: ... — reconnecting in 1s
INFO ws: Connected to wss://...
WARNING main: WebSocket reconnected — clearing order book cache
```

- [ ] **Step 4: Final commit**

```bash
git add -u
git commit -m "chore: verify smoke test pass"
```
