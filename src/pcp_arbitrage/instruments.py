import logging
import math

from pcp_arbitrage.exchange_symbols import format_quote_display
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
    min_days_to_expiry: float = 1.0,
    exchange: str = "",
) -> list[Triplet]:
    """
    Build (call, put, future) triplets from raw API payloads (exchange-normalized format).

    margin_type: "coin" → futures margin label "USD"  (BTC-USD-YYMMDD)
                 "usdt" → futures margin label "USDT" (BTC-USDT-YYMMDD)
                 "usdc" → futures margin label "USDC" (BTC-USDC-YYMMDD)
    min_days_to_expiry: 丢弃剩余到期（天）低于该值的期权；0 表示仅排除已到期（T≤0）。
    """
    if margin_type == "coin":
        margin_label = "USD"
    elif margin_type == "usdc":
        margin_label = "USDC"
    else:
        margin_label = "USDT"

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
        if days_to_expiry < min_days_to_expiry:
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
                exchange=exchange,
                symbol=sym,
                expiry=exp,
                strike=strike,
                call_id=call_opt["instId"],
                put_id=put_opt["instId"],
                future_id=fut_id,
            )
        )

    return triplets


def describe_unmatched_expiry(
    sym: str,
    exp: str,
    all_options: list[dict],
    margin_label: str,
    atm_prices: dict[str, float],
    atm_range: float,
    now_ms: int,
    min_days_to_expiry: float = 1.0,
) -> str:
    """
    有期权到期日 + 有同结算交割合约，但 build_triplets 未产出该日的 triplet 时，给出与 instruments 逻辑一致的原因说明。
    """
    strikes_c: set[float] = set()
    strikes_p: set[float] = set()
    exp_ms: int | None = None

    for opt in all_options:
        inst_id = opt["instId"]
        parts = inst_id.split("-")
        if len(parts) != 5:
            continue
        if parts[0] != sym or parts[1] != margin_label or parts[2] != exp:
            continue
        strike = float(opt.get("strike") or opt["stk"])
        if exp_ms is None:
            exp_ms = int(opt["expTime"])
        ot = opt["optType"]
        if ot == "C":
            strikes_c.add(strike)
        elif ot == "P":
            strikes_p.add(strike)

    paired = strikes_c & strikes_p
    if not paired:
        return "无同价 call+put 配对"

    if exp_ms is None:
        return "无期权数据"

    days = (exp_ms - now_ms) / 86_400_000
    if days < min_days_to_expiry:
        return (
            f"剩余到期 {days:.4f} 天 < 阈值 {min_days_to_expiry} 天（约 {max(0.0, days * 24):.1f} 小时）"
        )

    atm = atm_prices.get(sym)
    if atm is None or atm <= 0 or math.isnan(atm) or math.isinf(atm):
        return "无有效现指/现货价，无法计算平值带宽（请检查指数价是否拉取成功）"

    lo, hi = atm * (1 - atm_range), atm * (1 + atm_range)
    in_band = [s for s in paired if lo <= s <= hi]
    if not in_band:
        smin, smax = min(paired), max(paired)
        pct = int(round(atm_range * 100))
        if smax <= 0 and smin <= 0:
            return (
                "该到期日虽有 call+put 同价行权价，但行权价为 0（或异常）；"
                "无法与现指比对，请核对交易所合约数据"
            )
        return (
            f"平值带宽内无行权价：配置为现指 {format_quote_display(sym, atm)} 的 ±{pct}%"
            f"（约 [{format_quote_display(sym, lo)}, {format_quote_display(sym, hi)}]），"
            f"该到期日同价行权价范围为 [{format_quote_display(sym, smin)}, {format_quote_display(sym, smax)}]，"
            "与上述区间无交集"
        )

    return "其他（带宽内应有配对但仍未建 triplet，请对照日志与 instruments 过滤条件）"
