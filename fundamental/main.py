from fundamental.client.screener_client import ScreenerClient
from fundamental.parser.screener_parser import ScreenerParser
from fundamental.service.fundamental_service import FundamentalService
from fundamental.writer.elastic_writer import ElasticWriter
from fundamental.utils.http import HttpClient
from fundamental.config.settings import REQUEST_TIMEOUT
from fundamental.config.tickers import STOCK_SYMBOLS
from fundamental.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    http = HttpClient(timeout=REQUEST_TIMEOUT)
    client = ScreenerClient(http)
    parser = ScreenerParser()
    service = FundamentalService(client, parser)
    writer = ElasticWriter(index_name="nifty_fundamental")

    for ticker in STOCK_SYMBOLS:
        try:
            data = service.fetch_fundamentals(ticker)
            writer.write(ticker, data)
            logger.info(f"✅ Indexed {ticker}")
        except Exception as e:
            logger.error(f"❌ Failed {ticker}: {e}")

if __name__ == "__main__":
    main()
