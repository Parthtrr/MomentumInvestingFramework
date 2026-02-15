from typing import List, Dict, Any
import numpy as np
from sklearn.linear_model import LinearRegression

def is_uptrend(candles: List[Dict[str, Any]], lookback: int) -> bool:
    """
    Detects if there is an uptrend in the last `lookback` candles
    based on linear regression slope of closing prices.

    Logic:
    - Fits a linear regression line to the closing prices.
    - Normalizes slope by average price.
    - Returns True if slope indicates meaningful uptrend.

    Args:
        candles: List of OHLCV dictionaries.
        lookback: Number of recent candles to consider.

    Returns:
        True if uptrend detected, False otherwise.
    """
    if len(candles) < lookback:
        return False

    recent = candles[-lookback:]
    closes = [c["close"] for c in recent]

    # Linear regression on closing prices
    X = np.arange(len(closes)).reshape(-1, 1)
    y = np.array(closes).reshape(-1, 1)
    model = LinearRegression().fit(X, y)
    slope = model.coef_[0][0]

    if slope <= 0:
        return False

    # Normalize slope by average price
    avg_price = np.mean(closes)
    slope_pct = slope / avg_price  # approx % change per candle

    return slope_pct > 0.003
