from dataclasses import dataclass
import pandas as pd

@dataclass
class FundamentalData:
    quarterly: pd.DataFrame
    ratios: pd.DataFrame
    sector: pd.DataFrame
    market_cap: float | None = None
