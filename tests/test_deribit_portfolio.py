"""Unit tests for Deribit WebSocket user.portfolio message handling."""
from __future__ import annotations


def _parse_portfolio_btc(data: dict, btc_usd: float) -> dict | None:
    """将 user.portfolio.BTC 消息 data 转换为标准 balance dict，供测试使用。

    这是 DeribitRunner.on_message 中 portfolio 分支逻辑的抽取。
    实现时直接在 on_message 中编写等效逻辑（不提取为独立函数，因为需要访问闭包变量）。
    此函数仅用于测试驱动设计。
    """
    def _sf(x: object) -> float:
        try:
            return float(x) if x is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    if btc_usd <= 0:
        return None
    eq_coin = _sf(data.get("equity"))
    avail_coin = _sf(data.get("available_funds"))
    im_coin = _sf(data.get("initial_margin"))
    mm_coin = _sf(data.get("maintenance_margin"))
    total_eq = eq_coin * btc_usd
    adj_eq = avail_coin * btc_usd
    imr = im_coin * btc_usd
    mmr = mm_coin * btc_usd
    denom = total_eq if total_eq > 0 else (adj_eq if adj_eq > 0 else 1.0)
    return {
        "total_eq_usdt": total_eq,
        "adj_eq_usdt": adj_eq,
        "imr_usdt": imr,
        "mmr_usdt": mmr,
        "im_pct": (imr / denom * 100) if denom > 0 else 0.0,
        "mm_pct": (mmr / denom * 100) if denom > 0 else 0.0,
        "currency_details": {},
    }


def test_parse_portfolio_btc_converts_to_usd() -> None:
    data = {
        "equity": 1.0,
        "available_funds": 0.8,
        "initial_margin": 0.1,
        "maintenance_margin": 0.05,
    }
    result = _parse_portfolio_btc(data, btc_usd=80000.0)
    assert result is not None
    assert result["total_eq_usdt"] == 80000.0
    assert result["adj_eq_usdt"] == 64000.0
    assert result["imr_usdt"] == 8000.0
    assert result["mmr_usdt"] == 4000.0
    assert abs(result["im_pct"] - 10.0) < 0.01
    assert abs(result["mm_pct"] - 5.0) < 0.01
    assert result["currency_details"] == {}


def test_parse_portfolio_btc_no_price_returns_none() -> None:
    data = {"equity": 1.0, "available_funds": 0.8}
    assert _parse_portfolio_btc(data, btc_usd=0.0) is None


def test_parse_portfolio_usdc_price_1() -> None:
    """USDC portfolio: price = 1.0, values pass through unchanged."""
    data = {
        "equity": 5000.0,
        "available_funds": 4000.0,
        "initial_margin": 500.0,
        "maintenance_margin": 250.0,
    }
    result = _parse_portfolio_btc(data, btc_usd=1.0)
    assert result is not None
    assert result["total_eq_usdt"] == 5000.0
    assert result["adj_eq_usdt"] == 4000.0
