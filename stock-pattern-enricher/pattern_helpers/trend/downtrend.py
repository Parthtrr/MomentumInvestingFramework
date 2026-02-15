from typing import List, Dict, Any
import numpy as np
from sklearn.linear_model import LinearRegression

def is_downtrend(candles: List[Dict[str, Any]], lookback: int) -> bool:
    """
    Detects if there is a downtrend in the last `lookback` candles.

    A downtrend is confirmed when:
    - At least 60% of consecutive candle pairs have lower highs and lower lows.
    - Linear regression on closing prices shows a negative slope.

    Args:
        candles: List of OHLCV dictionaries.
        lookback: Number of recent candles to consider.

    Returns:
        True if downtrend detected, False otherwise.
    """
    if len(candles) < lookback:
        return False

    recent = candles[-lookback:]
    highs = [c["high"] for c in recent]
    lows = [c["low"] for c in recent]
    closes = [c["close"] for c in recent]

    def percentage_consecutive_lower(arr: List[float]) -> float:
        count = 0
        for i in range(1, len(arr)):
            if arr[i] < arr[i - 1]:
                count += 1
        return count / (len(arr) - 1)

    # Use fixed threshold of 60%
    threshold = 0.8
    lower_highs_pct = percentage_consecutive_lower(highs)
    lower_lows_pct = percentage_consecutive_lower(lows)

    structure_downtrend = (
        lower_highs_pct >= threshold and lower_lows_pct >= threshold
    )

    # Linear regression on closes
    X = np.arange(len(closes)).reshape(-1, 1)
    y = np.array(closes).reshape(-1, 1)
    model = LinearRegression().fit(X, y)
    slope = model.coef_[0][0]

    return structure_downtrend and (slope < 0)
