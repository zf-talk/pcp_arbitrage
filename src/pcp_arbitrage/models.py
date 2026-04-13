from dataclasses import dataclass


def top_of_book_contracts(level: list) -> float:
    """Parse size from first depth level [price, qty, ...] (WS-specific)."""
    if not level or len(level) < 2:
        return 0.0
    try:
        return float(level[1])
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class BookSnapshot:
    bid: float
    ask: float
    ts: int  # millisecond timestamp from WebSocket push
    # 最优档挂单量（与交易所推送一致：期权/期货合约张数或标的数量）
    bid_sz: float = 0.0  # 卖单吃 bid 时可成交量
    ask_sz: float = 0.0  # 买单吃 ask 时可成交量


@dataclass(frozen=True)
class FeeRates:
    option_taker_rate: float  # option taker fee fraction (e.g. 0.0002)
    option_maker_rate: float  # option maker fee fraction (e.g. 0.0001)
    future_taker_rate: float  # future taker fee fraction (e.g. 0.0005)
    future_maker_rate: float  # future maker fee fraction (e.g. 0.0002)


@dataclass(frozen=True)
class Triplet:
    exchange: str  # "okx", "deribit", "deribit_linear", "binance"
    symbol: str  # "BTC" or "ETH"
    expiry: str  # "250425"
    strike: float  # strike price in USDT
    call_id: str  # key in order_books
    put_id: str
    future_id: str
