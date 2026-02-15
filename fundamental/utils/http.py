import requests
from fundamental.config.settings import HEADERS
from fundamental.utils.logger import get_logger

logger = get_logger(__name__)

class HttpClient:

    def __init__(self, timeout: int):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get(self, url: str) -> str:
        logger.info(f"HTTP GET {url}")
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text
