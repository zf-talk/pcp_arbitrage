# Multi-Exchange + margin_type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor config/main into a multi-exchange concurrent architecture with per-exchange `margin_type`, and add a Binance runner stub.

**Architecture:** Each exchange is an `ExchangeRunner` class in `src/pcp_arbitrage/exchanges/`; `main.py` loads config and `asyncio.gather()`s all enabled runners. `config.py` gains `ExchangeConfig` (per-exchange credentials + margin_type) and the top-level `AppConfig` loses the OKX-specific credential fields. `instruments.py` gains a `margin_type` param; all other shared modules (`pcp_calculator.py`, `signal_printer.py`, `market_data.py`, `models.py`) are untouched.

**Tech Stack:** Python 3.13, asyncio, aiohttp, pyyaml, pytest

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/pcp_arbitrage/config.py` | Add `ExchangeConfig`, restructure `AppConfig`, rewrite `load_config` |
| Modify | `src/pcp_arbitrage/instruments.py` | Add `margin_type` param to `build_triplets` |
| Modify | `src/pcp_arbitrage/main.py` | Slim to: load config → gather enabled runners |
| Create | `src/pcp_arbitrage/exchanges/__init__.py` | Empty package marker |
| Create | `src/pcp_arbitrage/exchanges/base.py` | `ExchangeRunner` Protocol |
| Create | `src/pcp_arbitrage/exchanges/okx.py` | `OKXRunner` — all current `main.py` logic |
| Create | `src/pcp_arbitrage/exchanges/binance.py` | `BinanceRunner` — stub (raises NotImplementedError) |
| Modify | `tests/fixtures/config_test.yaml` | Update to new `exchanges:` YAML structure |
| Modify | `tests/test_config.py` | Update to `ExchangeConfig`/new `AppConfig` shape |
| Modify | `tests/test_instruments.py` | Add `margin_type` param to all `build_triplets` calls; add 2 USDT-mode tests |
| Create | `tests/test_binance_normalize.py` | 1 test: Binance raw → normalized instId |

---

## Task 1: Restructure `config.py` — `ExchangeConfig` + `AppConfig`

**Files:**
- Modify: `src/pcp_arbitrage/config.py`
- Modify: `tests/fixtures/config_test.yaml`
- Modify: `tests/test_config.py`

### Background for implementer

The current `config.py` has a flat `AppConfig` with OKX credentials at the top level. We need:

```python
@dataclass
class ExchangeConfig:
    name: str              # "okx" | "binance"
    enabled: bool
    margin_type: str       # "coin" | "usdt"
    api_key: str
    secret_key: str
    passphrase: str        # OKX-only; Binance passes ""
    is_paper_trading: bool # OKX-only; Binance ignores (always False)

@dataclass
class AppConfig:
    exchanges: dict[str, ExchangeConfig]
    symbols: list[str]
    min_annualized_rate: float
    atm_range: float
    stale_threshold_ms: int
    lot_size: dict[str, float]
```

New `load_config`:

```python
def load_config(path: Path | str = "config.yaml") -> AppConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    exchanges = {}
    for name, ex in raw["exchanges"].items():
        exchanges[name] = ExchangeConfig(
            name=name,
            enabled=ex.get("enabled", False),
            margin_type=ex.get("margin_type", "coin"),
            api_key=ex.get("api_key", ""),
            secret_key=ex.get("secret_key", ""),
            passphrase=ex.get("passphrase", ""),
            is_paper_trading=ex.get("is_paper_trading", False),
        )
    arb = raw["arbitrage"]
    return AppConfig(
        exchanges=exchanges,
        symbols=arb["symbols"],
        min_annualized_rate=arb["min_annualized_rate"],
        atm_range=arb["atm_range"],
        stale_threshold_ms=arb["stale_threshold_ms"],
        lot_size=raw["contracts"]["lot_size"],
    )
```

New fixture YAML at `tests/fixtures/config_test.yaml`:

```yaml
exchanges:
  okx:
    enabled: true
    margin_type: coin
    api_key: "test_key"
    secret_key: "test_secret"
    passphrase: "test_pass"
    is_paper_trading: false
  binance:
    enabled: false
    margin_type: usdt
    api_key: "test_key_b"
    secret_key: "test_secret_b"

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

New `tests/test_config.py` — keep old assertions where shape still matches; add new ones:

```python
from pathlib import Path
from pcp_arbitrage.config import load_config, AppConfig, ExchangeConfig

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
    assert okx.is_paper_trading is False


def test_exchange_config_margin_type_default():
    """margin_type defaults to 'coin' when omitted from YAML."""
    import yaml, io
    from pcp_arbitrage.config import load_config
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
contracts:
  lot_size:
    BTC: 0.01
"""
    import tempfile, pathlib
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(raw_yaml)
        tmp = pathlib.Path(f.name)
    cfg = load_config(tmp)
    assert cfg.exchanges["okx"].margin_type == "coin"
    tmp.unlink()


def test_binance_exchange_config_parsed():
    cfg = load_config(FIXTURE)
    assert "binance" in cfg.exchanges
    binance = cfg.exchanges["binance"]
    assert binance.enabled is False
    assert binance.margin_type == "usdt"
    assert binance.passphrase == ""   # sentinel: not in YAML, defaults to ""
    assert binance.is_paper_trading is False
```

### Steps

- [ ] **Step 1: Update `tests/fixtures/config_test.yaml`** — replace with the new `exchanges:` YAML structure shown above.

- [ ] **Step 2: Write failing tests** in `tests/test_config.py` — replace the entire file with the new version shown above.

- [ ] **Step 3: Run tests to confirm they fail**

  ```bash
  uv run pytest tests/test_config.py -v
  ```

  Expected: multiple FAIL (ImportError on `ExchangeConfig`, or wrong shape).

- [ ] **Step 4: Update `src/pcp_arbitrage/config.py`** — replace with the new implementation:

  ```python
  from dataclasses import dataclass
  from pathlib import Path
  import yaml


  @dataclass
  class ExchangeConfig:
      name: str
      enabled: bool
      margin_type: str       # "coin" | "usdt"
      api_key: str
      secret_key: str
      passphrase: str        # OKX-only; "" for Binance
      is_paper_trading: bool # OKX-only; False for Binance


  @dataclass
  class AppConfig:
      exchanges: dict[str, ExchangeConfig]
      symbols: list[str]
      min_annualized_rate: float
      atm_range: float
      stale_threshold_ms: int
      lot_size: dict[str, float]


  def load_config(path: Path | str = "config.yaml") -> AppConfig:
      with open(path) as f:
          raw = yaml.safe_load(f)
      exchanges = {}
      for name, ex in raw["exchanges"].items():
          exchanges[name] = ExchangeConfig(
              name=name,
              enabled=ex.get("enabled", False),
              margin_type=ex.get("margin_type", "coin"),
              api_key=ex.get("api_key", ""),
              secret_key=ex.get("secret_key", ""),
              passphrase=ex.get("passphrase", ""),
              is_paper_trading=ex.get("is_paper_trading", False),
          )
      arb = raw["arbitrage"]
      return AppConfig(
          exchanges=exchanges,
          symbols=arb["symbols"],
          min_annualized_rate=arb["min_annualized_rate"],
          atm_range=arb["atm_range"],
          stale_threshold_ms=arb["stale_threshold_ms"],
          lot_size=raw["contracts"]["lot_size"],
      )
  ```

- [ ] **Step 5: Run config tests**

  ```bash
  uv run pytest tests/test_config.py -v
  ```

  Expected: all PASS.

- [ ] **Step 6: Run full suite to catch regressions**

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: config tests PASS; other tests may FAIL due to `main.py` still using old `AppConfig` shape — that's OK, they'll be fixed in Task 4. If non-config tests that were passing before now fail, investigate.

- [ ] **Step 7: Commit**

  ```bash
  git add src/pcp_arbitrage/config.py tests/fixtures/config_test.yaml tests/test_config.py
  git commit -m "feat: add ExchangeConfig, restructure AppConfig and load_config"
  ```

---

## Task 2: Add `margin_type` param to `instruments.py`

**Files:**
- Modify: `src/pcp_arbitrage/instruments.py`
- Modify: `tests/test_instruments.py`

### Background for implementer

`build_triplets` currently hard-codes `margin != "USD"` to filter coin-margined futures. We add a `margin_type: str = "coin"` parameter that derives `margin_label` and uses it everywhere the string `"USD"` or `"USDT"` appears in the filtering/warning logic.

The existing 8 `build_triplets` tests use coin-mode data (`BTC-USD-250425` futures, options with `BTC-USD-*` instId). We need to:
1. Add `margin_type="coin"` explicitly to every existing call (no behavior change).
2. Add 2 new tests that verify `margin_type="usdt"` filters correctly.

### Changes to `instruments.py`

Change the function signature and the two places that reference `"USD"`:

```python
def build_triplets(
    options: list[dict],
    futures: list[dict],
    atm_prices: dict[str, float],
    symbols: list[str],
    atm_range: float,
    now_ms: int,
    margin_type: str = "coin",   # NEW
) -> list[Triplet]:
    """
    Build (call, put, future) triplets from raw API payloads (exchange-normalized format).

    Filtering rules:
    - symbol in `symbols`
    - expiry has a matching future with the correct margin label (warns and drops otherwise)
    - days_to_expiry >= 1
    - strike within ATM ± atm_range

    margin_type: "coin" → futures must have margin "USD" (e.g. BTC-USD-250425)
                 "usdt" → futures must have margin "USDT" (e.g. BTC-USDT-250425)
    """
    margin_label = "USD" if margin_type == "coin" else "USDT"   # NEW

    # Index futures by symbol → expiry → inst_id
    future_index: dict[str, dict[str, str]] = {}
    for fut in futures:
        parts = fut["instId"].split("-")  # e.g. BTC-USD-250425
        if len(parts) != 3:
            continue
        sym, margin, exp = parts
        if sym not in symbols or margin != margin_label:   # CHANGED: was "USD"
            continue
        future_index.setdefault(sym, {})[exp] = fut["instId"]

    # ... (rest of function unchanged) ...

    # In the warning log, change the hardcoded string:
    logger.warning(
        "[instruments] No %s future found for %s expiry %s — dropping all triplets for this expiry",
        margin_label, sym, exp,   # CHANGED: was "USDT"
    )
```

Full updated `instruments.py`:

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
    margin_type: str = "coin",
) -> list[Triplet]:
    """
    Build (call, put, future) triplets from raw API payloads (exchange-normalized format).

    margin_type: "coin" → futures margin label "USD" (BTC-USD-YYMMDD)
                 "usdt" → futures margin label "USDT" (BTC-USDT-YYMMDD)
    """
    margin_label = "USD" if margin_type == "coin" else "USDT"

    future_index: dict[str, dict[str, str]] = {}
    for fut in futures:
        parts = fut["instId"].split("-")
        if len(parts) != 3:
            continue
        sym, margin, exp = parts
        if sym not in symbols or margin != margin_label:
            continue
        future_index.setdefault(sym, {})[exp] = fut["instId"]

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
        strike = float(opt.get("strike") or opt["stk"])
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
            continue

        exp_ms = int(call_opt["expTime"])
        days_to_expiry = (exp_ms - now_ms) / 86_400_000
        if days_to_expiry < 1:
            continue

        atm = atm_prices.get(sym)
        if atm is None:
            continue
        lo, hi = atm * (1 - atm_range), atm * (1 + atm_range)
        if not (lo <= strike <= hi):
            continue

        fut_id = future_index.get(sym, {}).get(exp)
        if fut_id is None:
            if exp not in warned_expiries:
                logger.warning(
                    "[instruments] No %s future found for %s expiry %s — dropping all triplets for this expiry",
                    margin_label, sym, exp,
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

### New tests for `tests/test_instruments.py`

Add `margin_type="coin"` to all existing `build_triplets` calls (no behavior change), then add these two tests at the end of the file:

```python
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
```

### Steps

- [ ] **Step 1: Add the 2 new USDT tests to `tests/test_instruments.py`**

  Append the `USDT_FUTURES`, `USDT_OPTIONS` constants and the two new test functions (`test_build_triplets_usdt_mode_matches_usdt_future`, `test_build_triplets_usdt_mode_ignores_coin_future`) shown above to the end of `tests/test_instruments.py`. Do NOT modify the existing calls yet.

- [ ] **Step 2: Run tests to verify new ones fail and existing ones still pass**

  ```bash
  uv run pytest tests/test_instruments.py -v
  ```

  Expected: 8 existing tests PASS, 2 new USDT tests FAIL with `TypeError: build_triplets() got an unexpected keyword argument 'margin_type'`.

- [ ] **Step 3: Update `src/pcp_arbitrage/instruments.py`** with the full new version shown above.

- [ ] **Step 4: Add `margin_type="coin"` to all existing `build_triplets` calls in `tests/test_instruments.py`**

  There are 6 existing test functions that call `build_triplets`. Add `margin_type="coin"` as a keyword argument to each call. This makes the coin-mode behavior explicit.

- [ ] **Step 5: Run instruments tests**

  ```bash
  uv run pytest tests/test_instruments.py -v
  ```

  Expected: all 10 PASS.

- [ ] **Step 6: Run full suite**

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: all passing tests from before still pass.

- [ ] **Step 7: Commit**

  ```bash
  git add src/pcp_arbitrage/instruments.py tests/test_instruments.py
  git commit -m "feat: add margin_type param to build_triplets"
  ```

---

## Task 3: Create `exchanges/` package — base Protocol + OKXRunner + BinanceRunner stub

**Files:**
- Create: `src/pcp_arbitrage/exchanges/__init__.py`
- Create: `src/pcp_arbitrage/exchanges/base.py`
- Create: `src/pcp_arbitrage/exchanges/okx.py`
- Create: `src/pcp_arbitrage/exchanges/binance.py`

### Background for implementer

We're moving all of the current `main.py` business logic into `OKXRunner`. The new `main.py` (Task 4) will be ~20 lines. `BinanceRunner` is a stub that raises `NotImplementedError` — the API integration comes later.

**`src/pcp_arbitrage/exchanges/__init__.py`** — empty:
```python
```

**`src/pcp_arbitrage/exchanges/base.py`:**
```python
from typing import Protocol


class ExchangeRunner(Protocol):
    async def run(self) -> None:
        """Start the full arbitrage listening loop (REST init + WS subscribe). Does not return."""
        ...
```

**`src/pcp_arbitrage/exchanges/okx.py`** — copy the full logic from current `main.py`, adapted to a class.

Key differences from current `main.py`:
- Uses `self._ex` (ExchangeConfig) and `self._app` (AppConfig) instead of flat `cfg`
- `margin_type` comes from `self._ex.margin_type`; `margin_label = "USD" if margin_type == "coin" else "USDT"`
- `uly = f"{sym}-{margin_label}"` for fetching instruments (coin → `BTC-USD`, usdt → `BTC-USDT`)
- `spot_price = atm_prices[t.symbol] if margin_type == "coin" else 1.0`
- `PAPER_TRADING` env-var override applies to `self._ex.is_paper_trading`
- `_print_triplet_summary` takes `margin_label: str` as a new last arg
- `build_triplets` called with `margin_type=margin_type`

Full `src/pcp_arbitrage/exchanges/okx.py`:

```python
import asyncio
import logging
import os
import time
from collections import defaultdict

from pcp_arbitrage.config import AppConfig, ExchangeConfig
from pcp_arbitrage.fee_fetcher import fetch_fee_rates
from pcp_arbitrage.instruments import build_triplets
from pcp_arbitrage.market_data import MarketData
from pcp_arbitrage.models import BookSnapshot, FeeRates
from pcp_arbitrage.okx_client import OKXRestClient, OKXWebSocketClient
from pcp_arbitrage.pcp_calculator import calculate_forward, calculate_reverse
from pcp_arbitrage.signal_printer import print_signal

logger = logging.getLogger(__name__)


def _print_triplet_summary(
    triplets: list,
    all_options: list[dict],
    all_futures: list[dict],
    symbols: list[str],
    margin_label: str,
) -> None:
    fut_expiries: dict[str, set[str]] = defaultdict(set)
    for f in all_futures:
        parts = f["instId"].split("-")
        if len(parts) == 3 and parts[1] == margin_label and parts[0] in symbols:
            fut_expiries[parts[0]].add(parts[2])

    opt_expiries: dict[str, set[str]] = defaultdict(set)
    for o in all_options:
        parts = o["instId"].split("-")
        if len(parts) == 5 and parts[0] in symbols:
            opt_expiries[parts[0]].add(parts[2])

    matched: dict[str, set[str]] = defaultdict(set)
    matched_strikes: dict[tuple[str, str], list[int]] = defaultdict(list)
    for t in triplets:
        matched[t.symbol].add(t.expiry)
        matched_strikes[(t.symbol, t.expiry)].append(int(t.strike))

    print("\n合约匹配摘要")
    print("─" * 60)
    for sym in symbols:
        all_exp = sorted(opt_expiries[sym])
        print(f"\n  {sym}  (期权到期日共 {len(all_exp)} 个, {margin_label} 期货 {len(fut_expiries[sym])} 个)")
        print(f"  {'到期日':<10} {'状态':<8} {'匹配的行权价'}")
        print(f"  {'─'*8}  {'─'*6}  {'─'*30}")
        for exp in all_exp:
            if exp in matched[sym]:
                strikes = sorted(matched_strikes[(sym, exp)])
                strike_str = "  ".join(str(k) for k in strikes)
                print(f"  {exp:<10} {'✓ 匹配':<8} {strike_str}")
            elif exp in fut_expiries[sym]:
                print(f"  {exp:<10} {'- ATM外':<8}")
            else:
                print(f"  {exp:<10} {'✗ 无期货':<8}")
    print("\n" + "─" * 60)


def _build_ws_args(triplets: list) -> list[dict]:
    inst_ids: set[str] = set()
    for t in triplets:
        inst_ids.update([t.call_id, t.put_id, t.future_id])
    return [{"channel": "books5", "instId": inst_id} for inst_id in sorted(inst_ids)]


class OKXRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        ex = self._ex
        app = self._app

        # Apply PAPER_TRADING env-var override
        if os.environ.get("PAPER_TRADING", "").lower() == "true":
            import dataclasses
            ex = dataclasses.replace(ex, is_paper_trading=True)

        margin_type = ex.margin_type
        margin_label = "USD" if margin_type == "coin" else "USDT"

        async with OKXRestClient(
            api_key=ex.api_key,
            secret=ex.secret_key,
            passphrase=ex.passphrase,
            is_paper=ex.is_paper_trading,
        ) as rest:
            # 1. Fetch fee rates
            logger.info("[okx] Fetching fee rates...")
            fee_rates: FeeRates = await fetch_fee_rates(rest)

            # 2. Print account balance
            balances = await rest.get_balance()
            if balances:
                bal_str = "  ".join(
                    f"{b['ccy']} {float(b['availBal']):.6g} (≈${float(b['eqUsd']):.2f})"
                    for b in balances
                )
                logger.info("[okx] Account balance: %s", bal_str)
            else:
                logger.info("[okx] Account balance: (empty)")

            # 3. Fetch instruments
            logger.info("[okx] Fetching instruments for %s...", app.symbols)
            all_options: list[dict] = []
            all_futures: list[dict] = []
            atm_prices: dict[str, float] = {}

            for sym in app.symbols:
                uly = f"{sym}-{margin_label}"
                opts = await rest.get_instruments("OPTION", uly=uly)
                futs = await rest.get_instruments("FUTURES", uly=uly)
                all_options.extend(opts)
                all_futures.extend(futs)

                ticker = await rest.get_ticker(f"{sym}-USDT")
                atm_prices[sym] = float(ticker.get("last", 0))
                logger.info("[okx] %s ATM price: %.2f", sym, atm_prices[sym])

            # 4. Build triplets
            triplets = build_triplets(
                options=all_options,
                futures=all_futures,
                atm_prices=atm_prices,
                symbols=app.symbols,
                atm_range=app.atm_range,
                now_ms=int(time.time() * 1000),
                margin_type=margin_type,
            )
            logger.info("[okx] Built %d triplets", len(triplets))
            if not triplets:
                logger.error("[okx] No triplets found — check config and OKX connectivity")
                return

            _print_triplet_summary(triplets, all_options, all_futures, app.symbols, margin_label)

            # 5. Market data store
            market = MarketData()
            exp_ms_by_call: dict[str, int] = {o["instId"]: int(o["expTime"]) for o in all_options}

            async def on_reconnect() -> None:
                logger.warning("[okx] WebSocket reconnected — clearing order book cache")
                market.clear()

            async def on_message(msg: dict) -> None:
                if msg.get("event"):
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

                snap = BookSnapshot(
                    bid=float(bids[0][0]),
                    ask=float(asks[0][0]),
                    ts=ts,
                )
                market.update(inst_id, snap)

                now_ms = int(time.time() * 1000)
                for t in triplets:
                    if inst_id not in (t.call_id, t.put_id, t.future_id):
                        continue
                    books_snapshot = {
                        t.call_id: market.get(t.call_id),
                        t.put_id: market.get(t.put_id),
                        t.future_id: market.get(t.future_id),
                    }
                    books_clean = {k: v for k, v in books_snapshot.items() if v is not None}

                    lot_size = app.lot_size[t.symbol]
                    exp_ms = exp_ms_by_call.get(t.call_id, now_ms + 86_400_000)
                    days_to_expiry = max((exp_ms - now_ms) / 86_400_000, 0.001)
                    spot_price = atm_prices[t.symbol] if margin_type == "coin" else 1.0

                    for calc in (calculate_forward, calculate_reverse):
                        sig = calc(
                            triplet=t,
                            books=books_clean,
                            fee_rates=fee_rates,
                            lot_size=lot_size,
                            days_to_expiry=days_to_expiry,
                            spot_price=spot_price,
                            stale_threshold_ms=app.stale_threshold_ms,
                        )
                        if sig and sig.annualized_return >= app.min_annualized_rate:
                            print_signal(sig)

            # 6. Start WebSocket
            ws_args = _build_ws_args(triplets)
            logger.info("[okx] Subscribing to %d channels", len(ws_args))
            ws_url = (
                "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
                if ex.is_paper_trading
                else "wss://ws.okx.com:8443/ws/v5/public"
            )
            ws_client = OKXWebSocketClient(ws_url, on_message=on_message, on_reconnect=on_reconnect)
            ws_client.add_subscriptions(ws_args)
            await ws_client.run()
```

**`src/pcp_arbitrage/exchanges/binance.py`** — stub:

```python
import logging
from pcp_arbitrage.config import AppConfig, ExchangeConfig

logger = logging.getLogger(__name__)


class BinanceRunner:
    def __init__(self, ex_cfg: ExchangeConfig, app_cfg: AppConfig) -> None:
        self._ex = ex_cfg
        self._app = app_cfg

    async def run(self) -> None:
        raise NotImplementedError("BinanceRunner is not yet implemented")
```

### Test for Binance normalization (`tests/test_binance_normalize.py`)

Even though `BinanceRunner.run()` is a stub, the normalization logic described in the spec (Section 8.3) can be tested standalone. Write a small helper function `_normalize_instruments` inside `binance.py` (not part of the class, just a module-level helper) so it can be imported and tested:

Add to the bottom of `src/pcp_arbitrage/exchanges/binance.py`:

```python
def _normalize_instruments(
    raw_options: list[dict],
    raw_futures: list[dict],
    margin_type: str,
) -> tuple[list[dict], list[dict]]:
    """
    Normalize Binance raw API payloads to the unified instId format expected by instruments.py.

    Options: BTC-260627-70000-C (4-part) → BTC-USD-260627-70000-C (5-part)
    Futures USDT: BTCUSDT_260627 → BTC-USDT-260627
    Futures coin: BTCUSD_260627  → BTC-USD-260627
    """
    settle = "USDT" if margin_type == "usdt" else "USD"

    normalized_options = []
    for s in raw_options:
        sym = s["underlying"].replace("USDT", "").replace("USD", "")
        raw_id = s["symbol"]            # "BTC-260627-70000-C"
        parts = raw_id.split("-")       # ["BTC", "260627", "70000", "C"]
        norm_id = f"{sym}-{settle}-{parts[1]}-{parts[2]}-{parts[3]}"
        normalized_options.append({
            "instId": norm_id,
            "stk": s["strikePrice"],
            "optType": "C" if s["side"] == "CALL" else "P",
            "expTime": str(s["expiryDate"]),
        })

    normalized_futures = []
    for f in raw_futures:
        raw_id = f["symbol"]            # "BTCUSDT_260627"
        sym_part, exp = raw_id.split("_")
        sym = sym_part.replace("USDT", "").replace("USD", "")
        normalized_futures.append({
            "instId": f"{sym}-{settle}-{exp}",
        })

    return normalized_options, normalized_futures
```

Write `tests/test_binance_normalize.py`:

```python
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
```

### Steps

- [ ] **Step 1: Write failing test** — create `tests/test_binance_normalize.py` with the content above.

- [ ] **Step 2: Run to confirm failure**

  ```bash
  uv run pytest tests/test_binance_normalize.py -v
  ```

  Expected: ImportError — `exchanges/binance.py` doesn't exist yet.

- [ ] **Step 3: Create package directory and `__init__.py`**

  ```bash
  mkdir -p src/pcp_arbitrage/exchanges
  touch src/pcp_arbitrage/exchanges/__init__.py
  ```

- [ ] **Step 4: Create `src/pcp_arbitrage/exchanges/base.py`** with the content above.

- [ ] **Step 5: Create `src/pcp_arbitrage/exchanges/okx.py`** with the full `OKXRunner` content above.

- [ ] **Step 6: Create `src/pcp_arbitrage/exchanges/binance.py`** with `BinanceRunner` stub + `_normalize_instruments` helper.

- [ ] **Step 7: Run normalize tests**

  ```bash
  uv run pytest tests/test_binance_normalize.py -v
  ```

  Expected: all 5 PASS.

- [ ] **Step 8: Run full suite**

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: all tests pass. (OKX runner has no unit tests — it's network-layer code.)

- [ ] **Step 9: Commit**

  ```bash
  git add src/pcp_arbitrage/exchanges/ tests/test_binance_normalize.py
  git commit -m "feat: add ExchangeRunner protocol, OKXRunner, BinanceRunner stub + normalize helper"
  ```

---

## Task 4: Slim down `main.py` + update live `config.yaml`

**Files:**
- Modify: `src/pcp_arbitrage/main.py`
- Modify: `config.yaml` (live config, not test fixture)

### Background for implementer

Current `main.py` (~220 lines) holds all the OKX logic. After Task 3, that logic lives in `OKXRunner`. New `main.py` is ~30 lines: load config, dispatch enabled runners via `asyncio.gather`.

The live `config.yaml` also needs to move to the new `exchanges:` structure.

**New `src/pcp_arbitrage/main.py`:**

```python
import asyncio
import logging

from pcp_arbitrage.config import load_config
from pcp_arbitrage.exchanges.okx import OKXRunner
from pcp_arbitrage.exchanges.binance import BinanceRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RUNNERS = {
    "okx": OKXRunner,
    "binance": BinanceRunner,
}


async def _run(cfg_path: str = "config.yaml") -> None:
    cfg = load_config(cfg_path)
    tasks = []
    for name, ex_cfg in cfg.exchanges.items():
        if not ex_cfg.enabled:
            continue
        runner_cls = RUNNERS.get(name)
        if runner_cls is None:
            logger.error("[main] Unknown exchange: %s", name)
            continue
        tasks.append(asyncio.create_task(runner_cls(ex_cfg, cfg).run()))
    if not tasks:
        logger.error("[main] No enabled exchanges found in config")
        return
    await asyncio.gather(*tasks)


def main() -> None:
    asyncio.run(_run())
```

**Updated `config.yaml`** (replace current content; preserve existing API credentials):

```yaml
exchanges:
  okx:
    enabled: true
    margin_type: coin
    api_key: "<copy from current config.yaml>"
    secret_key: "<copy from current config.yaml>"
    passphrase: "<copy from current config.yaml>"
    is_paper_trading: false

  binance:
    enabled: false
    margin_type: usdt
    api_key: ""
    secret_key: ""

arbitrage:
  min_annualized_rate: 0.010
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

### Steps

- [ ] **Step 1: Replace `src/pcp_arbitrage/main.py`** with the new 30-line version above.

- [ ] **Step 2: Update `config.yaml`** with the new `exchanges:` structure (keeping existing OKX credentials).

- [ ] **Step 3: Run full test suite**

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: all tests pass. (No tests directly test `main.py`'s internal structure — the config tests cover loading, the runner tests cover behavior.)

- [ ] **Step 4: Smoke-check imports**

  ```bash
  uv run python -c "from pcp_arbitrage.main import main; print('OK')"
  ```

  Expected: `OK` with no import errors.

- [ ] **Step 5: Commit**

  ```bash
  git add src/pcp_arbitrage/main.py config.yaml
  git commit -m "feat: slim main.py to multi-exchange dispatcher, update config.yaml"
  ```

---

## Final Verification

- [ ] Run full test suite one last time:

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: all tests pass (38 original + ~12 new = ~50 total).

- [ ] Confirm `make run` still starts without errors (it will try to connect to OKX — that's expected):

  ```bash
  make run
  ```

  Expected: logs show `[okx] Fetching fee rates...` (same as before). `binance` runner not started (disabled in config).
