from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class StockData:
    symbol: str
    date: str  # ISO format date string, e.g., '2025-05-21'
    open: float
    high: float
    low: float
    close: float
    volume: int

    # Pattern analysis results
    one_candle_patterns_reversal: List[str] = field(default_factory=list)
    one_candle_patterns_continuation: List[str] = field(default_factory=list)

    two_candle_patterns_reversal: List[str] = field(default_factory=list)
    two_candle_patterns_continuation: List[str] = field(default_factory=list)

    three_candle_patterns_reversal: List[str] = field(default_factory=list)
    three_candle_patterns_continuation: List[str] = field(default_factory=list)

    # Distribution stock-pattern-enricher or other analyses can be added later
    distribution_patterns_bullish: List[str] = field(default_factory=list)
    distribution_patterns_bearish: List[str] = field(default_factory=list)
