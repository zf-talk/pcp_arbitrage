from __future__ import annotations

from dataclasses import dataclass
import math
import time

from pcp_arbitrage.models import BookSnapshot, FeeRates, Triplet

# 币本位「USD 标价」交割/永续：单张合约的 USD 名义（与交易所 ctVal 一致，用于 taker 费 ≈ 张数 × 名义 × rate）
# OKX BTC-USD-* 常见 ctVal=100；Deribit 反向期货多为更小面值，见各所 instrument。
OKX_INVERSE_FUT_USD_FACE: dict[str, float] = {
    "BTC": 100.0,
    "ETH": 10.0,
}
DERIBIT_INVERSE_FUT_USD_FACE: dict[str, float] = {
    "BTC": 10.0,
    "ETH": 1.0,
}


def inverse_future_usd_face_for_exchange(
    exchange: str, symbol: str, settle_type: str = ""
) -> float | None:
    """币本位 USD 标价期货用张数×面值×费率；U 本位线性返回 None（用价×张数）。"""
    st = settle_type or ""
    if exchange == "okx":
        if "U本位" in st:
            return None
        return OKX_INVERSE_FUT_USD_FACE.get(symbol)
    if exchange == "binance":
        if "U本位" in st:
            return None
        return OKX_INVERSE_FUT_USD_FACE.get(symbol)
    if exchange == "deribit":
        if "U本位" in st or "USDC" in st.upper():
            return None
        return DERIBIT_INVERSE_FUT_USD_FACE.get(symbol)
    if exchange == "deribit_linear":
        return None
    return None


def per_leg_fees_from_stored_leg_px(
    direction_key: str,
    call_px_usdt: float,
    put_px_usdt: float,
    fut_px_usdt: float,
    *,
    lot_size: float,
    fee_rates: FeeRates,
    index_for_fee: float,
    future_inverse_usd_face: float | None = None,
) -> tuple[float, float, float] | None:
    """
    用库里已存的「本方向」三腿 USDT 价（与 record_evaluation 写入的 call/put/fut_px 一致）重算单边 C/P/F 手续费。
    direction_key: forward | reverse
    """
    if direction_key not in ("forward", "reverse"):
        return None
    if not math.isfinite(lot_size) or lot_size <= 0:
        return None
    if not math.isfinite(index_for_fee) or index_for_fee <= 0:
        return None
    for px in (call_px_usdt, put_px_usdt, fut_px_usdt):
        if not math.isfinite(px):
            return None
    opt_r = fee_rates.option_taker_rate
    fut_r = fee_rates.future_taker_rate
    if not math.isfinite(opt_r) or not math.isfinite(fut_r):
        return None
    # 库中三腿价已是该方向下用于套利的对手价（正向/反向各自一致）
    cf = _option_fee(call_px_usdt, lot_size, opt_r, index_for_fee)
    pf = _option_fee(put_px_usdt, lot_size, opt_r, index_for_fee)
    ff = _future_fee(
        fut_px_usdt,
        lot_size,
        fut_r,
        inverse_usd_face_per_contract=future_inverse_usd_face,
    )
    return (cf, pf, ff)


@dataclass
class ArbitrageSignal:
    direction: str  # "forward" or "reverse"
    triplet: Triplet
    call_price: float   # option price in USDT (coin price × spot)
    put_price: float    # option price in USDT (coin price × spot)
    future_price: float
    call_price_coin: float   # raw order book price (coin-denominated)
    put_price_coin: float
    spot_price: float        # spot price used for conversion
    gross_profit: float
    total_fee: float
    net_profit: float
    annualized_return: float  # e.g. 0.15 = 15%
    days_to_expiry: float
    # 三腿中与套利方向对应的对手盘挂量取最小（张数/数量，与交易所口径一致）
    tradeable_qty: float = 0.0
    call_fee: float = 0.0   # 单边 call 手续费 (USDT)
    put_fee: float = 0.0    # 单边 put 手续费 (USDT)
    fut_fee: float = 0.0    # 单边 future 手续费 (USDT)


def _option_fee(
    option_price: float,
    lot_size: float,
    option_rate: float,
    index_usdt: float,
) -> float:
    """
    OKX 官方期权手续费公式（币本位 / U 本位通用）：
      min(费率 × 合约乘数 × 合约面值 × 张数,
          7% × 期权费(币价) × 合约乘数 × 合约面值 × 张数)
    其中 7% 为 OKX 封顶系数；其他交易所封顶比例或计费方式可能不同，接入时需对照各所文档。
    其中合约乘数=1，合约面值(USDT) = index_usdt，期权费(币价) = option_price / index_usdt
    化简后两项均变为 lot_size × index_usdt 乘以各自系数：
      fee_by_face = lot_size × index_usdt × rate
      fee_cap     = lot_size × index_usdt × (option_price / index_usdt) × 7%
                  = lot_size × option_price × 0.07
    option_price: USDT（call_ask / put_bid 等已转换为 USDT）
    index_usdt: 标的指数价（USDT），用于名义价值那一项
    """
    fee_by_face = lot_size * index_usdt * option_rate
    fee_cap = option_price * lot_size * 0.07
    return min(fee_by_face, fee_cap)


def _future_fee(
    future_price: float,
    lot_size: float,
    future_rate: float,
    *,
    inverse_usd_face_per_contract: float | None = None,
) -> float:
    """期货手续费 = 价格 × 张数 × 费率（USDT）。
    lot_size 在本系统中始终以期权合约单位（BTC/ETH 数量）衡量；
    币本位反向合约 fee = n_contracts × face × rate = (lot_size × price / face) × face × rate = lot_size × price × rate，
    与 U 本位线性公式相同，inverse_usd_face_per_contract 不影响结果。"""
    return future_price * lot_size * future_rate


def _min_tradeable_forward(
    call_snap: BookSnapshot,
    put_snap: BookSnapshot,
    fut_snap: BookSnapshot,
) -> float:
    """正向：买 C@ask、卖 P@bid、卖 F@bid — 取三边对手盘量最小。"""
    return min(
        max(0.0, call_snap.ask_sz),
        max(0.0, put_snap.bid_sz),
        max(0.0, fut_snap.bid_sz),
    )


def _min_tradeable_reverse(
    call_snap: BookSnapshot,
    put_snap: BookSnapshot,
    fut_snap: BookSnapshot,
) -> float:
    """反向：卖 C@bid、买 P@ask、买 F@ask。"""
    return min(
        max(0.0, call_snap.bid_sz),
        max(0.0, put_snap.ask_sz),
        max(0.0, fut_snap.ask_sz),
    )


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
    spot_price: float,
    stale_threshold_ms: int = 5000,
    *,
    index_for_fee: float | None = None,
    future_inverse_usd_face: float | None = None,
) -> ArbitrageSignal | None:
    """Forward: buy Call + sell Put + sell Future."""
    inst_ids = [triplet.call_id, triplet.put_id, triplet.future_id]
    if not _integrity_check(inst_ids, books, stale_threshold_ms):
        return None

    call_snap = books[triplet.call_id]
    put_snap = books[triplet.put_id]
    fut_snap = books[triplet.future_id]

    # Option prices are coin-denominated; convert to USDT
    call_ask_coin = call_snap.ask
    put_bid_coin = put_snap.bid
    call_ask = call_ask_coin * spot_price
    put_bid = put_bid_coin * spot_price
    future_bid = fut_snap.bid
    K = triplet.strike

    gross = put_bid - call_ask + future_bid - K

    idx = index_for_fee if index_for_fee is not None else spot_price
    call_fee = _option_fee(call_ask, lot_size, fee_rates.option_taker_rate, idx)
    put_fee = _option_fee(put_bid, lot_size, fee_rates.option_taker_rate, idx)
    fut_fee = _future_fee(
        future_bid,
        lot_size,
        fee_rates.future_taker_rate,
        inverse_usd_face_per_contract=future_inverse_usd_face,
    )
    total_fee = 2 * (call_fee + put_fee + fut_fee)

    net = gross - total_fee
    if net <= 0:
        return None

    ann = (net / K) * (365 / days_to_expiry)
    tqty = _min_tradeable_forward(call_snap, put_snap, fut_snap)

    return ArbitrageSignal(
        direction="forward",
        triplet=triplet,
        call_price=call_ask,
        put_price=put_bid,
        future_price=future_bid,
        call_price_coin=call_ask_coin,
        put_price_coin=put_bid_coin,
        spot_price=spot_price,
        gross_profit=gross,
        total_fee=total_fee,
        net_profit=net,
        call_fee=call_fee,
        put_fee=put_fee,
        fut_fee=fut_fee,
        annualized_return=ann,
        days_to_expiry=days_to_expiry,
        tradeable_qty=tqty,
    )


def calculate_reverse(
    triplet: Triplet,
    books: dict[str, BookSnapshot],
    fee_rates: FeeRates,
    lot_size: float,
    days_to_expiry: float,
    spot_price: float,
    stale_threshold_ms: int = 5000,
    *,
    index_for_fee: float | None = None,
    future_inverse_usd_face: float | None = None,
) -> ArbitrageSignal | None:
    """Reverse: sell Call + buy Put + buy Future."""
    inst_ids = [triplet.call_id, triplet.put_id, triplet.future_id]
    if not _integrity_check(inst_ids, books, stale_threshold_ms):
        return None

    call_snap = books[triplet.call_id]
    put_snap = books[triplet.put_id]
    fut_snap = books[triplet.future_id]

    # Option prices are coin-denominated; convert to USDT
    call_bid_coin = call_snap.bid
    put_ask_coin = put_snap.ask
    call_bid = call_bid_coin * spot_price
    put_ask = put_ask_coin * spot_price
    future_ask = fut_snap.ask
    K = triplet.strike

    gross = call_bid - put_ask + K - future_ask

    idx = index_for_fee if index_for_fee is not None else spot_price
    call_fee = _option_fee(call_bid, lot_size, fee_rates.option_taker_rate, idx)
    put_fee = _option_fee(put_ask, lot_size, fee_rates.option_taker_rate, idx)
    fut_fee = _future_fee(
        future_ask,
        lot_size,
        fee_rates.future_taker_rate,
        inverse_usd_face_per_contract=future_inverse_usd_face,
    )
    total_fee = 2 * (call_fee + put_fee + fut_fee)

    net = gross - total_fee
    if net <= 0:
        return None

    ann = (net / K) * (365 / days_to_expiry)
    tqty = _min_tradeable_reverse(call_snap, put_snap, fut_snap)

    return ArbitrageSignal(
        direction="reverse",
        triplet=triplet,
        call_price=call_bid,
        put_price=put_ask,
        future_price=future_ask,
        call_price_coin=call_bid_coin,
        put_price_coin=put_ask_coin,
        spot_price=spot_price,
        gross_profit=gross,
        total_fee=total_fee,
        net_profit=net,
        call_fee=call_fee,
        put_fee=put_fee,
        fut_fee=fut_fee,
        annualized_return=ann,
        days_to_expiry=days_to_expiry,
        tradeable_qty=tqty,
    )
