from datetime import datetime

from pcp_arbitrage.exchange_symbols import format_strike_display
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

    # Show coin price and USDT equivalent for options
    call_coin_str = f"  ({sig.call_price_coin:.6f} {t.symbol} × {sig.spot_price:.2f})"
    put_coin_str  = f"  ({sig.put_price_coin:.6f} {t.symbol} × {sig.spot_price:.2f})"

    ks = format_strike_display(t.symbol, t.strike)
    lines = [
        f"[{now}] [{direction_cn}] {t.symbol}-{t.expiry}-{ks}",
        f"  期权组合  : {combo}",
        f"  {call_label}  : {sig.call_price:>10.2f} USDT  {call_note}{call_coin_str}",
        f"  {put_label}   : {sig.put_price:>10.2f} USDT  {put_note}{put_coin_str}",
        f"  {future_label}: {sig.future_price:>10.2f} USDT {future_note}",
        f"  K         : {ks:>10} USDT",
        f"  手续费    : {sig.total_fee:>10.2f} USDT  (进出场双边 × 3腿)",
        f"  净利润    : {sig.net_profit:>10.2f} USDT  {net_str}",
        f"  可交易量  : {sig.tradeable_qty:>10.4g}  (三腿对手盘最优档最小)",
        f"  年化收益  : {ann_pct:>9.2f}%      ({sig.net_profit:.2f} / {ks} × 365 / {sig.days_to_expiry:.0f} × 100)",
        SEP,
    ]
    return "\n".join(lines)


def print_signal(sig: ArbitrageSignal) -> None:
    print(format_signal(sig))
