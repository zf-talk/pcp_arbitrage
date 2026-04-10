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
