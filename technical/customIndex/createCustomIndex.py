from elasticsearch import Elasticsearch, helpers
import pandas as pd

ES = Elasticsearch("http://localhost:9200")
SRC_INDEX = "nifty_data_weekly"
META_INDEX = "indices"
BASE_VALUE = 1000.0


def get_custom_indices():
    query = {
        "_source": ["ticker", "Name", "constituents"],
        "query": {
            "term": {"IsCustom": {"value": "true"}}
        }
    }
    res = ES.search(index=META_INDEX, body=query, size=500)
    return [hit["_source"] for hit in res["hits"]["hits"]]


def fetch_ohlcv_for_constituents(tickers):
    query = {
        "_source": ["ticker", "close", "open", "high", "low", "volume", "date"],
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"ticker": tickers}}
                ]
            }
        },
        "sort": [{"date": {"order": "asc"}}]
    }

    res = ES.search(index=SRC_INDEX, body=query)
    df = pd.DataFrame([doc["_source"] for doc in res["hits"]["hits"]])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"])
    return df


def calculate_equal_weight_index(df, ticker):
    result = []
    tickers = df["ticker"].unique()

    prev_close_map = {}
    for t in tickers:
        prev_close_map[t] = None  # initialize previous close for each stock

    for date, group in df.groupby("date"):
        n_stocks = len(group)
        R_open = []
        R_high = []
        R_low = []
        R_close = []
        total_volume = 0

        for _, row in group.iterrows():
            prev_close = prev_close_map[row["ticker"]]
            if prev_close is None:
                # First available day for this stock
                prev_close_map[row["ticker"]] = row["close"]
                prev_close = row["close"]

            # Calculate returns relative to previous close
            R_open.append((row["open"] / prev_close) - 1)
            R_high.append((row["high"] / prev_close) - 1)
            R_low.append((row["low"] / prev_close) - 1)
            R_close.append((row["close"] / prev_close) - 1)
            total_volume += row["volume"]

            # Update previous close for next iteration
            prev_close_map[row["ticker"]] = row["close"]

        # Average returns across all constituents
        R_open_avg = sum(R_open) / n_stocks
        R_high_avg = sum(R_high) / n_stocks
        R_low_avg = sum(R_low) / n_stocks
        R_close_avg = sum(R_close) / n_stocks

        if not result:
            # Initialize first index value
            prev_index_close = BASE_VALUE
        else:
            prev_index_close = result[-1]["close"]

        index_open = round(prev_index_close * (1 + R_open_avg), 2)
        index_high = round(prev_index_close * (1 + R_high_avg), 2)
        index_low = round(prev_index_close * (1 + R_low_avg), 2)
        index_close = round(prev_index_close * (1 + R_close_avg), 2)

        result.append({
            "date": date.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "open": index_open,
            "high": index_high,
            "low": index_low,
            "close": index_close,
            "volume": int(total_volume),
            "type": "index",
            "isCustom": True
        })

    return result


def index_custom_index(data):
    actions = []
    for d in data:
        doc_id = f"{d['ticker']}_{d['date']}"
        actions.append({
            "_index": SRC_INDEX,
            "_id": doc_id,
            "_source": d
        })

    helpers.bulk(ES, actions)
    print(f"✔ Indexed {len(actions)} candles for {data[0]['ticker']}")


def main():
    custom_indices = get_custom_indices()
    print(f"Found {len(custom_indices)} custom indices")

    for idx in custom_indices:
        ticker = idx["ticker"]
        constituents = list(set(idx["constituents"]))

        print(f"\n📍 Building custom index: {ticker}")
        print(f"  → Constituents: {len(constituents)}")

        df = fetch_ohlcv_for_constituents(constituents)
        if df.empty:
            print(f"❌ No OHLCV found for {ticker}")
            continue

        result = calculate_equal_weight_index(df, ticker)
        index_custom_index(result)

    print("\n🎯 Completed custom index generation!")


if __name__ == "__main__":
    main()
