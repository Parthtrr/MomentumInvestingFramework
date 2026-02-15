from time import sleep
from fundamental.config.settings import REQUEST_DELAY_SEC
from fundamental.models.fundamental import FundamentalData
from fundamental.utils.logger import get_logger

logger = get_logger(__name__)

class FundamentalService:

    def __init__(self, client, parser):
        self.client = client
        self.parser = parser

    def fetch_fundamentals(self, ticker: str) -> FundamentalData:
        logger.info(f"Processing fundamentals for {ticker}")

        try:
            html = self.client.fetch_company_page(ticker)
            parsed = self.parser.parse(html)
            sleep(REQUEST_DELAY_SEC)

            quarterly_df = parsed.get("quarterly")

            if quarterly_df is None or quarterly_df.empty:
                raise ValueError("Quarterly data missing")

            # ---- VALIDATE STRUCTURE ----

            columns = set(quarterly_df.columns)

            # Remove 'metric' column
            quarter_columns = columns - {"metric"}

            # Condition 1: Must have actual quarter columns (Dec 2023 etc)
            if len(quarter_columns) < 3:
                raise ValueError("Not enough quarter columns")

            # Condition 2: Must have revenue-type metric
            metrics = set(quarterly_df["metric"].tolist())
            revenue_aliases = {"Sales", "Revenue"}

            if not revenue_aliases.intersection(metrics):
                raise ValueError("Revenue metric missing")

            # Condition 3: Must contain Net Profit & EPS
            required = {"Net Profit", "EPS in Rs"}
            if not required.issubset(metrics):
                raise ValueError("Profit/EPS missing")

        except Exception as e:
            logger.warning(f"⚠️ Falling back to standalone for {ticker}: {e}")

            html = self.client.fetch_company_page_standalone(ticker)
            parsed = self.parser.parse(html)
            sleep(REQUEST_DELAY_SEC)

        return FundamentalData(
            quarterly=parsed.get("quarterly"),
            ratios=parsed.get("ratios"),
            sector=parsed.get("sector"),
            market_cap=parsed.get("market_cap"),
        )

