from full_indexing import full_index
from logging_config import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting the full Indexing")
    full_index()

if __name__ == "__main__":
    main()