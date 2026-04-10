# PCP Arbitrage Monitor

A real-time Put-Call Parity (PCP) arbitrage signal monitor for crypto derivatives markets. Supports OKX, Binance, and Deribit.

[中文文档](README.zh.md)

## How It Works

Put-Call Parity states that for European options:

```
C - P = F - K · e^(-rT)
```

When the market price deviates from parity, a risk-free arbitrage opportunity exists. This tool monitors option and futures order books in real time and prints signals when annualized returns exceed a configurable threshold.

**Forward arbitrage** — buy call, sell put, sell future:

```
gross = put_bid + future_bid − call_ask − K
```

**Reverse arbitrage** — sell call, buy put, buy future:

```
gross = call_bid + K − put_ask − future_ask
```

### Coin-margined vs. USDT-margined

The two settlement types differ in how option prices are denominated and whether profit is truly currency-locked.

#### USDT-margined (Binance)

Options and futures are all quoted in USDT. No conversion needed (`spot_price = 1.0`).

| Scenario | BTC rises to 110,000 | BTC falls to 90,000 |
|----------|---------------------|---------------------|
| **Forward** (buy C, sell P, sell F) | Call expires ITM, covers future loss. Put expires worthless, you keep premium. Net = locked USDT profit. | Call expires worthless. Put assigned, but future gain offsets. Net = locked USDT profit. |
| **Reverse** (sell C, buy P, buy F) | Call assigned, but future gain offsets. Put expires worthless. Net = locked USDT profit. | Put expires ITM, covers future loss. Call expires worthless, you keep premium. Net = locked USDT profit. |

Profit is fully locked in USDT regardless of price movement. ✅

#### Coin-margined (OKX, Deribit)

Options are quoted in BTC (e.g., 0.002 BTC). The calculator converts to USDT for comparison:

```
call_ask_usdt = call_ask_btc × spot
put_bid_usdt  = put_bid_btc  × spot
gross (USDT)  = put_bid_usdt + future_bid − call_ask_usdt − K
```

The `gross` is computed in USDT — but settlement is received in BTC. This creates residual spot exposure:

| Scenario | BTC rises to 110,000 | BTC falls to 90,000 |
|----------|---------------------|---------------------|
| **Forward** (buy C, sell P, sell F) | Call ITM in BTC; future loss in USDT. BTC received is worth more → extra USDT gain. | Put ITM paid in BTC; future gain in USDT. BTC paid is worth less → extra USDT gain. |
| **Reverse** (sell C, buy P, buy F) | Call assigned in BTC; future gain in USDT. BTC paid is worth more → extra USDT loss. | Put ITM in BTC; future loss in USDT. BTC received is worth less → extra USDT loss. |

The signal is computed at entry using the current spot price. If spot moves significantly before expiry, actual profit will deviate. To fully lock the USDT profit, hedge the residual BTC exposure in the spot market at entry.

## Trading fees (model)

Fees are **not** exchange-specific line-by-line; they use a small **approximation** in `pcp_calculator.py`. Taker rates (`option_rate`, `future_rate`) are taken from the exchange when the Runner fetches account fee tiers (with hard-coded fallbacks if the call fails).

### Per leg (one fill)

**Options (call or put)** — take the **minimum** of two terms (same structure as many venues: % of underlying notional vs. premium cap):

```
fee_opt = min( lot_size × index_usdt × option_rate ,  option_price × lot_size × 0.125 )
```

- `option_price` is in **USDT** (coin options: coin quote × spot).
- `index_usdt` is the **underlying index / spot in USDT** used for the notional leg: for coin-margined runners it is the same as the spot used to convert option quotes; for USDT-/USDC-quoted books (`spot_price = 1` in the calculator) the Runner passes the real index via `index_for_fee` so the first term is not accidentally near zero.
- **Deribit (inverse options):** per [fees](https://www.deribit.com/kb/fees), fee per contract = **`MIN` (underlying leg, 12.5% × option price) × contracts** (premium side capped at **12.5%** of option price; underlying side often described as **0.03%** of notional — use your live tier). **BTC example:** price **0.008 BTC**, size **3** → `MIN(0.0003, 0.125×0.008)×3 = 0.0009 BTC` (**0.0003** is the illustrative figure matching the **0.03%** tier in Deribit’s examples). The code’s `fee_opt` applies the same **MIN** structure in **USDT**, scaled by `lot_size`.

**Futures**

```
fee_fut = future_price × lot_size × future_rate
```

- `future_price` is the relevant side of the book (`bid` or `ask`) in **USDT / USD** as used in the PCP formulas.

### Round-trip (what the signal prints)

Opening **and** closing the three legs is modeled as paying each leg’s fee twice:

```
total_fee = 2 × ( fee_call + fee_put + fee_fut )
net_profit = gross_profit − total_fee
```

The console line *“进出场双边 × 3腿”* refers to this **×2** (entry + exit) on all three instruments.

### Worked example (order of magnitude)

Assume `lot_size = 0.01` BTC, `index_usdt ≈ 66_900`, `option_rate = 0.0003`, `future_rate = 0.0005`, and a forward signal with `future_bid = 68_617.5` USDT.

- **Options (per leg, one fill):** `fee_by_face = 0.01 × 66_900 × 0.0003 ≈ 0.20` USDT. The **premium cap** `option_price × lot_size × 0.125` is often larger; `min(...)` then keeps **~0.2 USDT** per option leg (not ~0).
- **Future:** `fee_fut ≈ 68_617.5 × 0.01 × 0.0005 ≈ 0.343` USDT per fill.
- **Total:** `total_fee ≈ 2 × (0.20 + 0.20 + 0.343) ≈ 1.49` USDT (illustrative — depends on quotes and which side of `min` binds).

## Exchanges

| Exchange | Option Type | Option Settlement | Future Settlement | Symbols | Status |
|----------|------------|------------------|------------------|---------|--------|
| OKX      | European   | Coin (BTC / ETH) | Coin (BTC / ETH) | BTC, ETH | ✅ Implemented |
| Binance  | European   | USDT             | USDT             | BTC, ETH | ✅ Implemented |
| Deribit (Inverse) | European | Coin (BTC / ETH) | Coin (BTC / ETH) | BTC, ETH | ✅ Implemented |
| Deribit (Linear)  | European | USDC             | USDC             | BTC, ETH | ✅ Implemented |

> **Note:** Deribit Inverse and Linear are configured as separate exchanges (`deribit` and `deribit_linear`) in `config.yaml`. They share the same API credentials.

## Setup

**Requirements:** Python 3.12+, [uv](https://github.com/astral-sh/uv)

```bash
# Install dependencies
uv sync

# Configure API keys
cp config.yaml.example config.yaml   # then fill in your keys
```

**`config.yaml` structure:**

```yaml
exchanges:
  okx:
    enabled: true
    margin_type: coin        # coin = BTC/ETH settled
    api_key: "..."
    secret_key: "..."
    passphrase: "..."        # OKX only

  binance:
    enabled: false
    margin_type: usdt
    api_key: "..."
    secret_key: "..."

  deribit:
    enabled: false
    margin_type: coin
    api_key: "..."           # client_id
    secret_key: "..."        # client_secret

  deribit_linear:
    enabled: false
    margin_type: usdc
    api_key: "..."           # same client_id as deribit
    secret_key: "..."        # same client_secret as deribit

arbitrage:
  min_annualized_rate: 0.010  # 1% minimum to print signal
  atm_range: 0.20             # ±20% around spot price
  symbols:
    - BTC
    - ETH
  stale_threshold_ms: 5000
  signal_ui: classic           # classic = multi-line print; dashboard = top-like table (TTY only)
  signal_dashboard_max_rows: 30
```

### Underlyings (`symbols`)

`arbitrage.symbols` lists base assets to monitor. Each runner **intersects** this list with what that venue supports: **OKX** and **Deribit** (coin-margined) — **BTC, ETH**; **Binance** — also **BNB, SOL, XRP, DOGE**; **Deribit Linear** — also **AVAX, SOL, TRX, XRP**.

**Lot sizes** for fee/signal math use built-in defaults (`DEFAULT_LOT_SIZES` in `config.py`). Override per symbol only if needed: `contracts: { lot_size: { BTC: 0.02 } }`.

### Signal UI (`signal_ui`)

- **`classic` (default)** — print full multi-line detail whenever annualized return is above the threshold.
- **`dashboard`** — refresh a `top`-like table on **interactive TTY** stdout. A row turns **`inactive` only after a new tick recomputes that triplet+direction and the result is below threshold** (or missing). It is **not** time-based: if an opportunity stays valid but no leg updates for a while, the row may remain **active** until any leg moves and triggers a recompute.

The dashboard uses **[Rich](https://github.com/Textualize/rich)** `Live` with a `Table` (`screen=True` uses the alternate buffer). Redraws stay off normal scrollback; Rich restores the terminal on exit. With **`dashboard_quiet_exchanges: true`** (default), `pcp_arbitrage.exchanges` and `okx_client` log at **WARNING** to cut stderr noise; set it to `false` or override per logger under `logging.levels` for debugging.

## Run

```bash
uv run python -m pcp_arbitrage.main
```

## Development

```bash
# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check src/ tests/
```

## Architecture

```
main.py                     ← multi-exchange dispatcher
exchanges/
  okx.py                    ← OKXRunner (REST init + WebSocket)
  binance.py                ← BinanceRunner (eapi + fapi + WS)
  deribit.py                ← DeribitRunner (Inverse, coin-settled)
                               DeribitLinearRunner (Linear, USDC-settled)
instruments.py              ← build_triplets() — option/future pairing
pcp_calculator.py           ← calculate_forward/reverse signals
signal_printer.py           ← classic multi-line formatting
signal_output.py            ← classic vs dashboard routing
opportunity_dashboard.py    ← Rich Live + Table dashboard
market_data.py              ← in-memory order book store
models.py                   ← BookSnapshot, FeeRates, Triplet
config.py                   ← AppConfig / ExchangeConfig loaders
exchange_symbols.py         ← per-venue supported underlyings
```

Each Runner follows the same lifecycle:
1. **REST init** — fetch fee rates, instruments, spot price, build triplets
2. **WebSocket** — subscribe to all triplet order books, compute PCP on each tick

## Roadmap

- [ ] **Auto-entry** — place orders automatically when a signal is detected
- [ ] **Position monitoring** — track open positions and P&L in real time
- [ ] **Take-profit exit** — close positions automatically when target return is reached
- [ ] **Notifications** — alert via Telegram / email when signals appear or positions close
- [ ] **Web dashboard** — live signal feed, order history, position overview

## References

- [Deribit — Inverse Options](https://support.deribit.com/hc/en-us/articles/31424939096093-Inverse-Options)
- [Deribit — Linear (USDC) Options](https://support.deribit.com/hc/en-us/articles/31424932728093-Linear-USDC-Options)
- [Deribit — Inverse Futures](https://support.deribit.com/hc/en-us/articles/31424938981533-Inverse-Futures)
- [Deribit — Linear Futures](https://support.deribit.com/hc/en-us/articles/31424954805405-Linear-Futures)
