"""Per-exchange supported underlying symbols (base asset codes)."""

from __future__ import annotations

import logging
import math

logger = logging.getLogger(__name__)

# 低价 USDC 线性标的：配对摘要 / 标签中行权价与报价统一保留 3 位小数
STRIKE_THREE_DECIMAL_SYMBOLS: frozenset[str] = frozenset({"XRP", "AVAX", "TRX"})


def format_strike_display(sym: str, strike: float) -> str:
    """行权价展示：XRP/AVAX/TRX 固定三位小数；大额整数标的用整数。"""
    if sym in STRIKE_THREE_DECIMAL_SYMBOLS:
        return f"{strike:.3f}"
    if math.isnan(strike) or math.isinf(strike):
        return "无效"
    if abs(strike - round(strike)) < 1e-9 * max(1.0, abs(strike)):
        return str(int(round(strike)))
    s = f"{strike:.10f}".rstrip("0").rstrip(".")
    return s if s else "0"


def format_quote_display(sym: str, x: float) -> str:
    """现指/带宽等价格展示：与 format_strike_display 规则一致（三标的三位小数）。"""
    if sym in STRIKE_THREE_DECIMAL_SYMBOLS:
        if math.isnan(x) or math.isinf(x):
            return "无效"
        return f"{x:.3f}"
    if math.isnan(x) or math.isinf(x):
        return "无效"
    ax = abs(x)
    if ax == 0:
        return "0"
    if ax >= 1000:
        return f"{x:.0f}"
    if ax >= 1:
        return f"{x:.2f}"
    return f"{x:.6g}"

# 各所实际可订阅/可配对的标的；配置里的 symbols 会按此取交集（顺序保留）。
EXCHANGE_SYMBOLS: dict[str, frozenset[str]] = {
    "okx": frozenset({"BTC", "ETH"}),
    "binance": frozenset({"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"}),
    "deribit": frozenset({"BTC", "ETH"}),
    "deribit_linear": frozenset({"BTC", "ETH", "AVAX", "SOL", "TRX", "XRP"}),
}


def symbols_for_exchange(exchange: str, requested: list[str]) -> list[str]:
    """Return requested symbols that are supported on this exchange; log skips."""
    allowed = EXCHANGE_SYMBOLS.get(exchange)
    if allowed is None:
        return list(requested)
    out = [s for s in requested if s in allowed]
    skipped = [s for s in requested if s not in allowed]
    if skipped:
        logger.info(
            "[%s] Skipping unsupported symbols (not listed on this venue): %s",
            exchange,
            ", ".join(skipped),
        )
    if not out:
        logger.warning(
            "[%s] No symbols left after filtering — check arbitrage.symbols vs %s",
            exchange,
            sorted(allowed),
        )
    return out
