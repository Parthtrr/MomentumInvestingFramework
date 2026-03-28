import math
from elasticsearch import Elasticsearch
from fundamental.models.fundamental import FundamentalData
from fundamental.utils.logger import get_logger
import pandas as pd
from datetime import datetime


logger = get_logger(__name__)


class ElasticWriter:

    def __init__(self, index_name: str, host: str = "http://localhost:9200"):
        self.index = index_name
        self.es = Elasticsearch(host)

    def write(self, ticker: str, data: FundamentalData):
        existing = self._get_existing_doc(ticker)
        new_quarterly = self._quarterly_docs(data)

        if existing and "quarterly" in existing:
            quarterly = self._merge_quarterly(existing["quarterly"], new_quarterly)
        else:
            quarterly = new_quarterly

        document = {
            "ticker": ticker,
            "market_cap": data.market_cap,
            "sector": self._sector_doc(data),
            "ratios": self._ratios_doc(data),
            "quarterly": quarterly
        }

        document = self._sanitize_for_es(document)

        logger.info(f"Indexing (merge) {ticker}")
        self.es.index(index=self.index, id=ticker, document=document)

    def _get_existing_doc(self, ticker: str) -> dict | None:
        try:
            res = self.es.get(index=self.index, id=ticker)
            return res["_source"]
        except Exception:
            return None

    def _merge_quarterly(self, old: list, new: list) -> list:
        merged = {}

        # preserve old history
        for q in old or []:
            key = (q["metric"], q["period_date"])
            merged[key] = q

        # overwrite / add new data
        for q in new or []:
            key = (q["metric"], q["period_date"])
            merged[key] = q

        # return sorted (chronological)
        return sorted(
            merged.values(),
            key=lambda x: x["period_date"]
        )

    def _sector_doc(self, data: FundamentalData) -> dict:
        if data.sector.empty:
            return {}

        return {
            row["Category"].lower().replace(" ", "_"): row["Value"]
            for _, row in data.sector.iterrows()
        }

    def _ratios_doc(self, data: FundamentalData) -> dict:
        ALLOWED_RATIOS = {
            "roe",
            "roce",
            "book_value",
            "dividend_yield",
            "eps",
            "eps_in_rs"
        }

        ratios = {}

        for _, row in data.ratios.iterrows():
            key = row["Metric"].lower().replace(" ", "_")

            if key not in ALLOWED_RATIOS:
                continue

            val = self.safe_float(row["Value"])
            if val is None:
                continue

            ratios[key] = val

        return ratios

    def safe_float(self, value):
        if value is None:
            return None

        value = str(value).strip()

        if value in ("", "-", "--", "NA", "N/A"):
            return None

        value = value.replace(",", "").replace("%", "")

        try:
            return float(value)
        except ValueError:
            return None

    def _quarterly_docs(self, data: FundamentalData) -> list:
        records = []
        df = data.quarterly
        periods = df.columns[1:]

        for _, row in df.iterrows():
            metric = row["metric"]

            for period in periods:
                value = row[period]

                if pd.isna(value):
                    continue

                try:
                    value = float(str(value).replace("%", ""))
                except ValueError:
                    continue

                dt = datetime.strptime(period, "%b %Y")

                records.append({
                    "metric": metric,
                    "period_date": dt.strftime("%Y-%m"),
                    "period_label": dt.strftime("%Y-%B"),
                    "value": value
                })

        return records

    import math

    def _sanitize_for_es(self, obj):
        if isinstance(obj, dict):
            return {
                k: self._sanitize_for_es(v)
                for k, v in obj.items()
                if not (isinstance(v, float) and math.isnan(v))
            }

        if isinstance(obj, list):
            return [
                self._sanitize_for_es(v)
                for v in obj
                if not (isinstance(v, float) and math.isnan(v))
            ]

        if isinstance(obj, float) and math.isnan(obj):
            return None

        return obj
