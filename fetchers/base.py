from abc import ABC, abstractmethod
from typing import List, Dict
import requests
from models import OHLCV
from utils import RateLimiter, get_logger


logger = get_logger("BaseFetcher")


class BaseFetcher(ABC):
    def __init__(self, name: str, config: dict, rate_limiter: RateLimiter):
        self.name = name
        self.config = config
        self.rate_limiter = rate_limiter
        self.logger = get_logger(name)
        self.max_retries = 3
        self.retry_delay = 2
        self.session = requests.Session()

    def fetch_ohlcv(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        return self._do_fetch(exchange_symbol, timeframe, start_ts, end_ts)

    @abstractmethod
    def get_symbol_mapping(self) -> Dict[str, str]:
        pass

    def fetch_with_backoff(self, *args, **kwargs) -> List[OHLCV]:
        import time

        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.wait()
                return self._do_fetch(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"请求失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
            except Exception as e:
                self.logger.warning(f"未知错误 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

    def _do_fetch(self, *args, **kwargs) -> List[OHLCV]:
        raise NotImplementedError

    def get_unified_symbol(self, exchange_symbol: str) -> str:
        mapping = self.get_symbol_mapping()
        for unified, ex_sym in mapping.items():
            if ex_sym == exchange_symbol:
                return unified
        return exchange_symbol