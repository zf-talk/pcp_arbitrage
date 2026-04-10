from typing import Protocol


class ExchangeRunner(Protocol):
    async def run(self) -> None:
        """Start the full arbitrage listening loop (REST init + WS subscribe). Does not return."""
        ...
