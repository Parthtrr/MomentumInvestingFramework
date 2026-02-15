nifty_fundamental = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {

            "ticker": {
                "type": "keyword"
            },

            "company_name": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },

            "market_cap": {
                "type": "long"
            },

            "sector": {
                "properties": {
                    "broad_sector": {"type": "keyword"},
                    "sector": {"type": "keyword"},
                    "industry_group": {"type": "keyword"},
                    "industry": {"type": "keyword"}
                }
            },

            "ratios": {
                "properties": {
                    "roe": {"type": "float"},
                    "roce": {"type": "float"},
                    "book_value": {"type": "float"},
                    "dividend_yield": {"type": "float"},
                    "eps": {"type": "float"}
                }
            },

            "quarterly": {
                "type": "nested",
                "properties": {
                    "period": {
                        "type": "date",
                        "format": "yyyy-MM"
                    },
                    "metric": {
                        "type": "keyword"
                    },
                    "value": {
                        "type": "float"
                    }
                }
            }
        }
    }
}
