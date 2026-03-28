from elasticsearch import Elasticsearch

ES_INDEX = "indices"

es = Elasticsearch("http://localhost:9200", verify_certs=False)

def build_reverse_dict():
    reverse_dict = {}

    # Scroll through all docs in the index
    resp = es.search(index=ES_INDEX, body={"query": {"match_all": {}}}, size=10000)

    hits = resp.get("hits", {}).get("hits", [])

    for hit in hits:
        source = hit["_source"]
        index_ticker = source.get("ticker")
        constituents = source.get("constituents", [])

        if not index_ticker:
            continue

        for stock in constituents:
            if stock not in reverse_dict:
                reverse_dict[stock] = []
            reverse_dict[stock].append(index_ticker)

    return reverse_dict

def get_tickers_with_custom_flag():
    """
    Returns a dictionary:
        { "ticker1": True, "ticker2": False, ... }
    where True/False is based on the IsCustom flag in the index.
    """
    tickers_dict = {}

    # Scroll through all documents in the index
    resp = es.search(
        index=ES_INDEX,
        body={"query": {"match_all": {}}},
        size=10000,  # adjust if you have more than 10k docs
        _source=["ticker", "IsCustom"]
    )

    hits = resp.get("hits", {}).get("hits", [])

    for hit in hits:
        source = hit["_source"]
        ticker = source.get("ticker")
        is_custom = source.get("IsCustom", False)

        if ticker:
            tickers_dict[ticker] = bool(is_custom)

    return tickers_dict


