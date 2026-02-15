from services.thread_executor import ThreadExecutor
from utils.logger import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting stock data enrichment process")
    executor = ThreadExecutor()
    executor.process_all_from_config()
    logger.info("Stock data enrichment process completed")

if __name__ == "__main__":
    main()
