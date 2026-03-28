#!/usr/bin/env python3
import json
import pandas as pd
from elasticsearch import Elasticsearch

# ---------------------------
# CONFIGURATION VARIABLES
# ---------------------------
ES_HOST = "http://localhost:9200"
ES_INDEX = "nifty_data_weekly"
OUT_FILE = "returnsStocks.xlsx"
indices = ["^ENGINEERING",
           "^MOTORFINANCE",
           "^CNXPSUBANK",
           "^CONVENTIONAL2AND3WHEELERS",
           "^CNXAUTO",
           "MCX.NS",
           "M%26M.NS",
           "^SUTTA",
           "^ELECTRIC2AND3WHEELERS",
           "^DAARU",
           "HDFCGROWTH.NS",
           "^AUTOANCILLARY",
           "^FISHEXPORT",
           "^CNXMETAL",
           "^GOLDFINANCE",
           "VAL30IETF.NS",
           "EVINDIA.NS",
           "^CNXMNC",
           "NIFTYMIDLIQ15.NS",
           "^CNXPHARMA",
           "HDFCLOWVOL.NS",
           "^CNXCONSUM",
           "^ITPRODUCTS",
           "^EMS",
           "HDFCQUAL.NS",
           "^NSEBANK",
           "^HOTEL",
           "^MUTUALFUNDANDWEALTHMANAGEMENT",
           "SBINEQWETF.NS",
           "NETFDIVOPP.NS",
           "FINIETF.NS",
           "^FINTECH",
           "^ONLINEPLATFORM",
           "^NSEMDCP50",
           "^TRADINGANDBROKERAGE",
           "^INDUSTRIALCONGLOMERATE",
           "SDL26BEES.NS",
           "BSLNIFTY.NS",
           "^NIFTYEV&NEWAGEAUTOMOTIVE",
           "NIFTYBEES.NS",
           "NV20.NS",
           "^CNXCMDT",
           "NIFTY_MIDCAP_100.NS",
           "^DATACENTER"]

# Start date query
START_QUERY = {
    "_source": ["close", "ticker", "indices"],
    "query": {
        "bool": {
            "must": [
                {"term": {"date": {"value": "2025-06-23"}}},
                {
                    "terms": {
                        "indices": indices
                    }
                },
                {"term": {"type": {"value": "stock"}}}
            ]
        }
    }
}

# End date query
END_QUERY = {
    "_source": ["close", "ticker", "indices"],
    "query": {
        "bool": {
            "must": [
                {"term": {"date": {"value": "2025-12-01"}}},
                {
                    "terms": {
                        "indices": indices
                    }
                },
                {"term": {"type": {"value": "stock"}}}
            ]
        }
    }
}


# ---------------------------
# FUNCTIONS
# ---------------------------

def get_prices_and_sectors(es, query):
    """Fetch ticker → close price and ticker → sectors (indices list)."""
    res = es.search(index=ES_INDEX, body=query, size=10000)
    hits = res.get('hits', {}).get('hits', [])

    prices = {}
    sectors = {}

    for h in hits:
        src = h.get('_source', {})
        ticker = src.get("ticker")
        close = src.get("close")
        indices_list = src.get("indices", [])

        if ticker and close:
            prices[ticker] = float(close)
            sectors[ticker] = ", ".join(indices_list)  # join to readable string

    return prices, sectors


# ---------------------------
# MAIN LOGIC
# ---------------------------

def main():
    es = Elasticsearch(ES_HOST)

    print("Fetching start prices & sectors...")
    start_prices, start_sectors = get_prices_and_sectors(es, START_QUERY)
    print(f"Found {len(start_prices)} tickers at start date")

    print("Fetching end prices & sectors...")
    end_prices, end_sectors = get_prices_and_sectors(es, END_QUERY)
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

        sectors = start_sectors.get(t, "")  # take from start date doc

        results.append({
            "ticker": t,
            "startPrice": s,
            "endPrice": e,
            "return%": ret,
            "sectors": sectors
        })

    print(f"Computed returns for {len(results)} stocks, skipped {skipped}")

    df = pd.DataFrame(results)
    df.sort_values(by="return%", ascending=False, inplace=True)

    df.to_excel(OUT_FILE, index=False)

    print(f"Saved: {OUT_FILE}")


if __name__ == "__main__":
    main()
