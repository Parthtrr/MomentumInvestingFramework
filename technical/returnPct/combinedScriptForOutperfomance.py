#!/usr/bin/env python3
import pandas as pd
from elasticsearch import Elasticsearch

# ---------------------------
# CONFIG
# ---------------------------
ES_HOST = "http://localhost:9200"
ES_INDEX = "nifty_data_weekly"
OUT_FILE = "finalOutperformance.xlsx"

START_DATE = "2025-06-30"
END_DATE = "2026-02-09"

# ---------------------------
# ES QUERIES
# ---------------------------

def make_query(date, doc_type, indices=None):
    q = {
        "_source": ["close", "ticker", "indices"],
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "isCustom": {
                                "value": "false"
                            }
                        }
                    },
                    {"term": {"date": {"value": date}}},
                    {"term": {"type": {"value": doc_type}}}
                ]
            }
        }
    }

    if indices:
        q["query"]["bool"]["must"].append({"terms": {"ticker": indices}})

    return q


# ---------------------------
# HELPERS
# ---------------------------

def get_prices(es, query):
    res = es.search(index=ES_INDEX, body=query, size=10000)
    out = {}
    for h in res["hits"]["hits"]:
        src = h["_source"]
        out[src["ticker"]] = float(src["close"])
    return out


def get_prices_and_sectors(es, query):
    res = es.search(index=ES_INDEX, body=query, size=10000)
    prices, sectors = {}, {}
    for h in res["hits"]["hits"]:
        src = h["_source"]
        ticker = src["ticker"]
        prices[ticker] = float(src["close"])
        sectors[ticker] = ", ".join(src.get("indices", []))
    return prices, sectors


def compute_returns(start_data, end_data):
    rows = []
    for t, s in start_data.items():
        if t in end_data and s != 0:
            e = end_data[t]
            ret = ((e - s) / s) * 100
            rows.append((t, s, e, ret))
    return rows


# ---------------------------
# MAIN
# ---------------------------

def main():
    es = Elasticsearch(ES_HOST)

    # ---------------------------
    # 1. GET RETURNS FOR ALL INDICES
    # ---------------------------
    print("\n=== Calculating Index Returns ===")

    # First fetch all indices available on start date
    q_idx_start = make_query(START_DATE, "index")
    q_idx_end = make_query(END_DATE, "index")

    start_idx = get_prices(es, q_idx_start)
    end_idx = get_prices(es, q_idx_end)

    index_rows = compute_returns(start_idx, end_idx)

    df_index = pd.DataFrame(index_rows, columns=["ticker", "startPrice", "endPrice", "return%"])
    df_index.sort_values(by="return%", ascending=False, inplace=True)

    # NSEI return
    nsei_return = df_index[df_index["ticker"] == "^CRSLDX"]["return%"].values[0]

    print(f"\nNSEI Return = {nsei_return:.2f}%")

    # Outperformers
    outperforming_indices = df_index[df_index["return%"] > nsei_return]["ticker"].tolist()

    print("\nOutperforming Indices:")
    for idx in outperforming_indices:
        print("  ", idx)

    # ---------------------------
    # 2. GET STOCKS THAT BELONG TO OUTPERFORMING INDICES
    # ---------------------------
    print("\n=== Fetching Stocks of Outperforming Indices ===")

    q_stock_start = {
        "_source": ["close", "ticker", "indices"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"date": {"value": START_DATE}}},
                    {"term": {"type": {"value": "stock"}}},
                    {"terms": {"indices": outperforming_indices}}
                ]
            }
        }
    }

    q_stock_end = {
        "_source": ["close", "ticker", "indices"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"date": {"value": END_DATE}}},
                    {"term": {"type": {"value": "stock"}}},
                    {"terms": {"indices": outperforming_indices}}
                ]
            }
        }
    }

    start_stock, start_sectors = get_prices_and_sectors(es, q_stock_start)
    end_stock, _ = get_prices_and_sectors(es, q_stock_end)

    # Compute stock returns
    stock_rows = []
    for t, s in start_stock.items():
        if t in end_stock and s != 0:
            e = end_stock[t]
            ret = ((e - s) / s) * 100
            if ret > nsei_return:  # FILTER HERE
                stock_rows.append({
                    "ticker": t,
                    "startPrice": s,
                    "endPrice": e,
                    "return%": ret,
                    "sectors": start_sectors.get(t, "")
                })

    df_stock = pd.DataFrame(stock_rows)
    df_stock.sort_values(by="return%", ascending=False, inplace=True)

    # ---------------------------
    # SAVE EXCEL
    # ---------------------------
    print("\nSaving Excel...")

    with pd.ExcelWriter(OUT_FILE) as writer:
        df_index.to_excel(writer, sheet_name="IndexReturns", index=False)
        df_index[df_index["ticker"].isin(outperforming_indices)] \
            .to_excel(writer, sheet_name="OutperformingIndices", index=False)
        df_stock.to_excel(writer, sheet_name="StockReturns", index=False)

    print(f"\nSaved → {OUT_FILE}\n")


if __name__ == "__main__":
    main()
