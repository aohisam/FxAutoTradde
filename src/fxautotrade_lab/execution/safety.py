"""Safety utilities."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class DuplicateOrderGuard:
    """Prevents duplicate pending orders per symbol."""

    pending_symbols: set[str] = field(default_factory=set)

    def add(self, symbol: str) -> bool:
        symbol = symbol.upper()
        if symbol in self.pending_symbols:
            return False
        self.pending_symbols.add(symbol)
        return True

    def remove(self, symbol: str) -> None:
        self.pending_symbols.discard(symbol.upper())
