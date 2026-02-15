from pattern_helpers.trend.downtrend import is_downtrend
from pattern_helpers.trend.uptrend import is_uptrend


def trend_appender(curr_candle, previous_candles):
    for days in [7, 10, 20, 50, 100, 150, 200]:
        if is_downtrend(previous_candles, days):
            if days not in curr_candle["downtrend"]:
                curr_candle["downtrend"].append(days)
        if is_uptrend(previous_candles, days):
            if days not in curr_candle["uptrend"]:
                curr_candle["uptrend"].append(days)
