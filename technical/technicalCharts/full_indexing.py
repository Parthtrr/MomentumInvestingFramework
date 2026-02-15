from datetime import datetime, timedelta
import Constant
from data_fetcher import fetch_data
from indexer import index_data
import pandas as pd
from technical.fetchConstituents.fetchTickerToIndexMapping import build_reverse_dict, get_tickers_with_custom_flag


def get_nifty_df():
    """
    Fetches and returns Nifty 50 data with columns [Date, Close].
    """
    nifty_symbol = "^NSEI"
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    nifty_data = fetch_data([nifty_symbol], Constant.startDate, end_date)
    print(f"nifty data fetched from {Constant.startDate} to {end_date}")

    if nifty_data is None or nifty_data.empty:
        print("Nifty data not fetched.")
        return None

    if isinstance(nifty_data.columns, pd.MultiIndex):
        try:
            nifty_df = nifty_data.xs(nifty_symbol, axis=1, level=1).copy()
        except KeyError:
            print("Nifty symbol not found in fetched data.")
            return None
    else:
        nifty_cols = [col for col in nifty_data.columns if col.endswith(f"/{nifty_symbol}")]
        if not nifty_cols:
            print("Nifty columns not found.")
            return None
        nifty_df = nifty_data[nifty_cols].copy()
        nifty_df.columns = [col.split("/")[0] for col in nifty_cols]

    nifty_df = nifty_df.reset_index(drop=True)
    nifty_df["Date"] = nifty_data["Date"].values
    nifty_df["Date"] = pd.to_datetime(nifty_df["Date"])
    nifty_df = nifty_df[["Date", "Close"]].copy()
    return nifty_df


def full_index():
    batch_size = Constant.batch_size
    tickerDictionary = build_reverse_dict()
    indexDictionary = get_tickers_with_custom_flag()

    tickers = sorted(
        set(tickerDictionary.keys()) |
        {idx for indices in tickerDictionary.values() for idx in indices}
    )

    nifty_df = get_nifty_df()
    if nifty_df is None or nifty_df.empty:
        print("Nifty data unavailable. Skipping indexing.")
        return

    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"data fetched from {Constant.startDate} to {end_date}")

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        data_df = fetch_data(batch, Constant.startDate, end_date)

        if data_df is None or data_df.empty:
            continue

        if isinstance(data_df.columns, pd.MultiIndex):
            try:
                date_series = data_df[("Date", "")].copy()
            except KeyError:
                print("Date column not found in MultiIndex DataFrame")
                continue
        else:
            date_series = data_df["Date"].copy() if "Date" in data_df.columns else None

        if date_series is None:
            print("Date column is missing, skipping batch")
            continue

        for ticker in batch:
            if isinstance(data_df.columns, pd.MultiIndex):
                try:
                    ticker_data = data_df.xs(ticker, axis=1, level=1).copy()
                except KeyError:
                    continue
            else:
                ticker_cols = [col for col in data_df.columns if col.startswith(ticker)]
                if not ticker_cols:
                    continue
                ticker_data = data_df[ticker_cols].copy()
                ticker_data.columns = [col.split("/")[1] for col in ticker_cols]

            ticker_data = ticker_data.reset_index(drop=True)
            ticker_data["Date"] = date_series.values
            ticker_data["Ticker"] = ticker
            indices = tickerDictionary.get(ticker, [])

            type_ = "stock"
            isCustom = False
            if ticker in indexDictionary:
                type_ = "index"
                isCustom = indexDictionary[ticker]

            ticker_data["type"] = [type_] * len(ticker_data)
            ticker_data["isCustom"] = [isCustom] * len(ticker_data)
            ticker_data["indices"] = [indices] * len(ticker_data)

            index_data(Constant.index_name, ticker_data, ticker, nifty_df)
