import yfinance as yf
import pandas as pd
from logging_config import get_logger
import Constant

logger = get_logger(__name__)


def fetch_data(tickers, start_date, end_date, to_weekly=True):
    """
    Fetch OHLCV data for one or more tickers.
    - Single ticker → flat DataFrame
    - Multiple tickers → flattened columns: "Open/TICKER", "Close/TICKER", ...
    """
    try:
        logger.info(f"Downloading the data for {tickers} from {start_date} to {end_date}")
        data = yf.download(
            tickers,
            start=start_date,
            end=end_date,
            interval=Constant.interval,
            group_by="ticker" if len(tickers) > 1 else "column"
        )

        if data.empty:
            logger.warning(f"No data found for {tickers} in the given date range.")
            return None

        # Handle MultiIndex (typical for multiple tickers, sometimes for single ticker too)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [f"{col[0]}/{col[1]}" if col[1] else col[0] for col in data.columns]

        # Reset index → add "Date"
        data.reset_index(inplace=True)

        # If weekly conversion is needed
        if to_weekly:
            return _convert_to_weekly(data)

        return data

    except Exception as e:
        logger.error(f"Error downloading data for {tickers}: {e}")
        return None


def _convert_to_weekly(df: pd.DataFrame):
    """
    Convert daily data to weekly candles.
    Uses Monday as week start.
    """
    df["WeekStart"] = df["Date"] - pd.to_timedelta(df["Date"].dt.weekday, unit="d")

    agg_funcs = {
        col: ("first" if "Open" in col else
              "max" if "High" in col else
              "min" if "Low" in col else
              "last" if "Close" in col else
              "sum" if "Volume" in col else "last")
        for col in df.columns if col not in ["Date", "WeekStart"]
    }

    weekly = df.groupby("WeekStart").agg(agg_funcs).reset_index()
    weekly.rename(columns={"WeekStart": "Date"}, inplace=True)
    return weekly
