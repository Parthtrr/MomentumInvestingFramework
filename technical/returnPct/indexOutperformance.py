#!/usr/bin/env python3
import json
import pandas as pd
from elasticsearch import Elasticsearch

# ---------------------------
# CONFIGURATION VARIABLES
# ---------------------------
ES_HOST = "http://localhost:9200"
ES_INDEX = "nifty_data_weekly"
OUT_FILE = "returns.xlsx"

# Start date query
START_QUERY = {
    "_source": ["close", "ticker"],
    "query": {
        "bool": {
            "must": [
                {"term": {"date": {"value": "2025-06-23"}}},
                {"term": {"type": {"value": "index"}}}
            ]
        }
    }
}

# End date query
END_QUERY = {
    "_source": ["close", "ticker"],
    "query": {
        "bool": {
            "must": [
                {"term": {"date": {"value": "2025-12-01"}}},
                {"term": {"type": {"value": "index"}}}
            ]
        }
    }
}

# ---------------------------
# FUNCTIONS
# ---------------------------

def get_prices(es, query):
    res = es.search(index=ES_INDEX, body=query, size=10000)
    hits = res.get('hits', {}).get('hits', [])
    data = {}
    for h in hits:
        src = h.get('_source', {})
        ticker = src.get('ticker')
        close = src.get('close')
        if ticker and close:
            data[ticker] = float(close)
    return data

# ---------------------------
# MAIN LOGIC
# ---------------------------

def main():
    es = Elasticsearch(ES_HOST)

    print("Fetching start prices...")
    start_prices = get_prices(es, START_QUERY)
    print(f"Found {len(start_prices)} tickers at start date")

    print("Fetching end prices...")
    end_prices = get_prices(es, END_QUERY)
    print(f"Found {len(end_prices)} tickers at end date")

    results = []
    skipped = 0
    for t, s in start_prices.items():
        if t not in end_prices:
            skipped += 1
            continue
        e = end_prices[t]
        if s == 0:
            skipped += 1
            continue
        ret = ((e - s) / s) * 100.0
        results.append({
            "ticker": t,
            "startPrice": s,
            "endPrice": e,
            "return%": ret
        })

    print(f"Computed returns for {len(results)} stocks, skipped {skipped}")

    df = pd.DataFrame(results)
    df.sort_values(by="return%", ascending=False, inplace=True)

    df.to_excel(OUT_FILE, index=False)
    print(f"Saved: {OUT_FILE}")


if __name__ == "__main__":
    main()
