"""
Fetch account balance and margin info from exchanges.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pcp_arbitrage.config import AppConfig, ExchangeConfig

logger = logging.getLogger(__name__)


def _sf(x: object, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _okx_details_sum_eq_usd(details: list[dict]) -> float:
    return sum(_sf(d.get("eqUsd")) for d in details)


def _okx_details_sum_avail_usd(details: list[dict]) -> float:
    """各币种可用权益按 eqUsd/eq 比例折成 USD（多币种保证金）。"""
    s = 0.0
    for d in details:
        eq_usd = _sf(d.get("eqUsd"))
        eq = _sf(d.get("eq"))
        av = _sf(d.get("availEq"))
        if av == 0.0:
            av = _sf(d.get("availBal"))
        if eq > 1e-12 and eq_usd > 0:
            s += (av / eq) * eq_usd
        elif eq_usd > 0 and av > 0:
            s += eq_usd
    return s


async def get_exchange_balance(
    exchange_cfg: "ExchangeConfig", app_cfg: "AppConfig | None" = None
) -> dict | None:
    """按交易所拉取余额；数值统一折成约 USDT 口径供网页展示。"""
    if exchange_cfg.name == "okx":
        return await _get_okx_balance(exchange_cfg)
    if exchange_cfg.name == "binance":
        return await _get_binance_balance(exchange_cfg, app_cfg)
    if exchange_cfg.name in ("deribit", "deribit_linear"):
        return await _get_deribit_balance(exchange_cfg, app_cfg)
    return None


async def _get_okx_balance(cfg: "ExchangeConfig") -> dict | None:
    """OKX：totalEq/adjEq 为空时用 availEq 或按 details 的 eqUsd、availEq 折合 USD。"""
    if not cfg.api_key or not cfg.secret_key:
        return None
    try:
        from pcp_arbitrage.okx_client import OKXRestClient

        async with OKXRestClient(
            cfg.api_key, cfg.secret_key, cfg.passphrase, cfg.is_paper_trading
        ) as client:
            bal = await client.get_balance()
            total_eq = _sf(bal.get("totalEq"))
            adj_eq = _sf(bal.get("adjEq"))
            avail_top = _sf(bal.get("availEq"))
            imr = _sf(bal.get("imr"))
            mmr = _sf(bal.get("mmr"))
            details = bal.get("details", [])

            sum_eq_usd = _okx_details_sum_eq_usd(details)
            sum_avail_usd = _okx_details_sum_avail_usd(details)

            if total_eq <= 0 and sum_eq_usd > 0:
                total_eq = sum_eq_usd
            if adj_eq <= 0:
                if avail_top > 0:
                    adj_eq = avail_top
                elif sum_avail_usd > 0:
                    adj_eq = sum_avail_usd

            denom = adj_eq if adj_eq > 0 else total_eq
            im_pct = (imr / denom * 100) if denom > 0 else 0.0
            mm_pct = (mmr / denom * 100) if denom > 0 else 0.0

            currency_details: dict[str, dict] = {}
            for d in details:
                ccy = str(d.get("ccy", ""))
                eq = _sf(d.get("eq"))
                avail = _sf(d.get("availBal"))
                frozen = _sf(d.get("frozenBal"))
                if eq > 0 or _sf(d.get("eqUsd")) > 0:
                    currency_details[ccy] = {"equity": eq, "available": avail, "frozen": frozen}

            return {
                "total_eq_usdt": total_eq,
                "adj_eq_usdt": adj_eq,
                "imr_usdt": imr,
                "mmr_usdt": mmr,
                "im_pct": im_pct,
                "mm_pct": mm_pct,
                "currency_details": currency_details,
            }
    except Exception as exc:
        logger.warning("[account_fetcher] Failed to fetch OKX balance: %s", exc)
        return None


async def _get_binance_balance(
    ex_cfg: "ExchangeConfig", app_cfg: "AppConfig | None"
) -> dict | None:
    """Binance 期权 + U 本位合约：账户内 USDT 计价权益与可用。"""
    if not ex_cfg.api_key or not ex_cfg.secret_key:
        return None
    proxy = app_cfg.proxy if app_cfg else None
    try:
        from pcp_arbitrage.exchanges.binance import BinanceRestClient

        total = avail = im = mm = 0.0
        async with BinanceRestClient(ex_cfg.api_key, ex_cfg.secret_key, proxy) as rest:
            try:
                opt = await rest.get_option_account()
                total = _sf(opt.get("totalWalletBalance"))
                if total <= 0:
                    total = _sf(opt.get("totalMarginBalance"))
                im = _sf(opt.get("totalInitialMargin"))
                mm = _sf(opt.get("totalMaintenanceMargin"))
                avail = _sf(opt.get("availableBalance"))
                for a in opt.get("asset", []) or []:
                    if str(a.get("asset", "")).upper() == "USDT":
                        ab = _sf(a.get("availableBalance"))
                        if ab == 0:
                            ab = _sf(a.get("available"))
                        if ab > 0:
                            avail = ab
                        break
            except Exception as exc:
                logger.warning("[account_fetcher] Binance option account: %s", exc)

            try:
                fut = await rest.get_futures_account()
                ft = _sf(fut.get("totalWalletBalance"))
                fa = _sf(fut.get("availableBalance"))
                if total <= 0 and ft > 0:
                    total = ft
                if avail <= 0 and fa > 0:
                    avail = fa
                if im <= 0:
                    im = _sf(fut.get("totalInitialMargin"))
                if mm <= 0:
                    mm = _sf(fut.get("totalMaintMargin"))
            except Exception as exc:
                msg = str(exc)
                if "401" in msg or "Unauthorized" in msg:
                    logger.warning(
                        "[account_fetcher] Binance futures account 401：请在 API 管理中为密钥开启「合约 / Futures」权限。"
                    )
                else:
                    logger.warning("[account_fetcher] Binance futures account: %s", exc)

            # UTA 统一账户回退：期权(400)和合约(401)都无法获取时，从现货账户读取 USDT 余额
            if total <= 0 and avail <= 0:
                try:
                    spot = await rest.get_spot_account()
                    for b in spot.get("balances", []):
                        if str(b.get("asset", "")).upper() == "USDT":
                            free = _sf(b.get("free"))
                            locked = _sf(b.get("locked"))
                            avail = free
                            total = free + locked
                            logger.info(
                                "[account_fetcher] Binance UTA 模式：从现货账户读取 USDT 余额 total=%.2f avail=%.2f",
                                total, avail,
                            )
                            break
                except Exception as exc:
                    logger.warning("[account_fetcher] Binance spot account fallback: %s", exc)

        if total <= 0 and avail <= 0:
            return None

        denom = total if total > 0 else (avail if avail > 0 else 1.0)
        im_pct = (im / denom * 100) if denom > 0 else 0.0
        mm_pct = (mm / denom * 100) if denom > 0 else 0.0

        return {
            "total_eq_usdt": total,
            "adj_eq_usdt": avail,
            "imr_usdt": im,
            "mmr_usdt": mm,
            "im_pct": im_pct,
            "mm_pct": mm_pct,
            "currency_details": {},
        }
    except Exception as exc:
        logger.warning("[account_fetcher] Failed to fetch Binance balance: %s", exc)
        return None


async def _get_deribit_balance(
    ex_cfg: "ExchangeConfig", app_cfg: "AppConfig | None"
) -> dict | None:
    """Deribit：币本位按 btc_usd 指数折成美元权益；线性 USDC 按 1:1 近似 USDT。"""
    if not ex_cfg.api_key or not ex_cfg.secret_key:
        return None
    _ = app_cfg

    if ex_cfg.margin_type == "usdc":
        currency = "USDC"
    elif ex_cfg.margin_type == "coin":
        currency = "BTC"
    else:
        currency = "USDC"

    try:
        from pcp_arbitrage.exchanges.deribit import DeribitRestClient

        async with DeribitRestClient(ex_cfg.api_key, ex_cfg.secret_key) as rest:
            await rest._authenticate()
            summary = await rest.get_account_summary(currency)
            eq_coin = _sf(summary.get("equity"))
            avail_coin = _sf(summary.get("available_funds"))
            im_coin = _sf(summary.get("initial_margin"))
            mm_coin = _sf(summary.get("maintenance_margin"))

            if currency == "BTC":
                idx = await rest.get_index_price("btc_usd")
                px = _sf(idx.get("index_price"))
            else:
                px = 1.0

            total_eq = eq_coin * px
            adj_eq = avail_coin * px
            imr = im_coin * px
            mmr = mm_coin * px

        if total_eq <= 0 and adj_eq <= 0:
            return None

        denom = total_eq if total_eq > 0 else (adj_eq if adj_eq > 0 else 1.0)
        im_pct = (imr / denom * 100) if denom > 0 else 0.0
        mm_pct = (mmr / denom * 100) if denom > 0 else 0.0

        return {
            "total_eq_usdt": total_eq,
            "adj_eq_usdt": adj_eq,
            "imr_usdt": imr,
            "mmr_usdt": mmr,
            "im_pct": im_pct,
            "mm_pct": mm_pct,
            "currency_details": {},
        }
    except Exception as exc:
        logger.warning("[account_fetcher] Failed to fetch Deribit balance: %s", exc)
        return None


async def fetch_all_balances(cfg: "AppConfig") -> dict[str, dict]:
    """Fetch balances for all enabled exchanges with API keys. Returns {exchange_name: balance_info}."""
    results: dict[str, dict] = {}
    tasks = []
    names = []
    for name, ex_cfg in cfg.exchanges.items():
        if ex_cfg.enabled and ex_cfg.api_key:
            tasks.append(get_exchange_balance(ex_cfg, cfg))
            names.append(name)
    if tasks:
        balances = await asyncio.gather(*tasks, return_exceptions=True)
        for name, bal in zip(names, balances):
            if isinstance(bal, Exception):
                logger.warning("[account_fetcher] Error fetching %s balance: %s", name, bal)
            elif bal:
                results[name] = bal
    return results
