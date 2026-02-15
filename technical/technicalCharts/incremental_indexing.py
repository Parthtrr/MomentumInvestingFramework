# from datetime import datetime, timedelta
# import pandas as pd
# import Constant
# from Constant import roc_period
# from data_fetcher import fetch_data
# from indexer import index_data
# from logging_config import get_logger
#
# logger = get_logger(__name__)
#
#
# # ✅ Added this function (same as in full_indexing) to fetch Nifty data
# def get_nifty_df():
#     """
#     Fetches and returns Nifty 50 data with columns [Date, Close].
#     """
#     nifty_symbol = "^NSEI"  # Adjust if your data source uses a different symbol
#
#     nifty_data = fetch_data([nifty_symbol], Constant.startDate, datetime.now().strftime("%Y-%m-%d"))
#     if nifty_data is None or nifty_data.empty:
#         logger.error("Nifty data not fetched.")
#         return None
#
#     if isinstance(nifty_data.columns, pd.MultiIndex):
#         try:
#             nifty_df = nifty_data.xs(nifty_symbol, axis=1, level=1).copy()
#         except KeyError:
#             logger.error("Nifty symbol not found in fetched data.")
#             return None
#     else:
#         nifty_cols = [col for col in nifty_data.columns if col.endswith(f"/{nifty_symbol}")]
#         if not nifty_cols:
#             logger.error("Nifty columns not found.")
#             return None
#         nifty_df = nifty_data[nifty_cols].copy()
#         nifty_df.columns = [col.split("/")[0] for col in nifty_cols]
#
#     nifty_df = nifty_df.reset_index(drop=True)
#     if isinstance(nifty_data.columns, pd.MultiIndex):
#         nifty_df["Date"] = nifty_data[("Date", "")].values
#     else:
#         nifty_df["Date"] = nifty_data["Date"].values
#
#     nifty_df["Date"] = pd.to_datetime(nifty_df["Date"])
#     nifty_df = nifty_df[["Date", "Close"]].copy()
#     return nifty_df
#
#
# def incremental_index(batch_size=50):
#     today = datetime.now()
#     date_from_index = (today - timedelta(days=(2*roc_period+3))).strftime("%Y-%m-%d")
#     date_to_index = (today + timedelta(days=1)).strftime("%Y-%m-%d")
#
#     logger.info(f"Starting incremental indexing from {date_from_index} to {date_to_index}")
#
#     nifty_df = get_nifty_df()  # ✅ Added: Fetch Nifty data once
#
#     if nifty_df is None or nifty_df.empty:
#         logger.warning("Nifty data unavailable. Skipping indexing.")
#         return
#
#     nifty500 = Constant.nifty500 + Constant.indices
#     for i in range(0, len(nifty500), batch_size):
#         batch = nifty500[i:i + batch_size]
#         logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} tickers")
#
#         data_df = fetch_data(batch, date_from_index, date_to_index)
#
#         if data_df is None or data_df.empty:
#             logger.warning("No data returned for batch, skipping...")
#             continue
#
#         # Extract Date column from MultiIndex or flat structure
#         if isinstance(data_df.columns, pd.MultiIndex):
#             try:
#                 date_series = data_df[("Date", "")].copy()
#             except KeyError:
#                 logger.error("Date column missing in MultiIndex DataFrame, skipping batch")
#                 continue
#         else:
#             date_series = data_df["Date"].copy() if "Date" in data_df.columns else None
#
#         if date_series is None:
#             logger.error("Date column missing, skipping batch")
#             continue
#
#         for ticker in batch:
#             logger.info(f"Indexing data for {ticker}")
#
#             # Extract ticker-specific data
#             if isinstance(data_df.columns, pd.MultiIndex):
#                 try:
#                     ticker_data = data_df.xs(ticker, axis=1, level=1).copy()
#                 except KeyError:
#                     logger.warning(f"No data found for {ticker}, skipping...")
#                     continue
#             else:
#                 ticker_cols = [col for col in data_df.columns if col.endswith(f"/{ticker}")]
#                 if not ticker_cols:
#                     logger.warning(f"No relevant columns found for {ticker}, skipping...")
#                     continue
#                 ticker_data = data_df[ticker_cols].copy()
#                 ticker_data.columns = [col.split("/")[0] for col in ticker_cols]
#
#             # Add Date & Ticker columns
#             ticker_data = ticker_data.reset_index(drop=True)
#             ticker_data["Date"] = date_series.values
#             ticker_data["Ticker"] = ticker
#
#             # ✅ Updated: Pass nifty_df so index_data can calculate and merge Nifty ROC
#             index_data_incremental("nifty_data_weekly", ticker_data, ticker, nifty_df)
#
#     logger.info("Incremental indexing completed successfully.")
