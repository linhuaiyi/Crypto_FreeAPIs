import time
from typing import List, Dict, Optional
from models import OHLCV
from .base import BaseFetcher


BINANCE_INTERVAL_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
    "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
    "6h": "6h", "8h": "8h", "12h": "12h", "1d": "1d",
    "3d": "3d", "1w": "1w", "1M": "1M",
}


class BinanceSpotFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("BinanceSpot", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://api.binance.com/api/v3')
        self.max_limit = 1000

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        interval = BINANCE_INTERVAL_MAP.get(timeframe, timeframe)
        all_data = []
        current_start = start_ts

        while current_start < end_ts:
            params = {
                'symbol': exchange_symbol,
                'interval': interval,
                'startTime': current_start,
                'endTime': end_ts,
                'limit': self.max_limit,
            }

            response = self.session.get(f"{self.base_url}/klines", params=params, timeout=30)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                self.logger.warning(f"触发限速，等待 {retry_after}s")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            if not data:
                break

            for k in data:
                all_data.append(OHLCV(
                    timestamp=int(k[0]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                    quote_volume=float(k[7]) if len(k) > 7 else 0.0,
                    exchange=self.name,
                    symbol=self.get_unified_symbol(exchange_symbol),
                    timeframe=timeframe,
                ))

            last_close_time = int(data[-1][6])
            current_start = last_close_time + 1

            if len(data) < self.max_limit:
                break

        return all_data


class BinanceUSDMFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("BinanceUSDM", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://fapi.binance.com/fapi/v1')
        self.max_limit = 1500

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        interval = BINANCE_INTERVAL_MAP.get(timeframe, timeframe)
        all_data = []
        current_start = start_ts

        while current_start < end_ts:
            params = {
                'symbol': exchange_symbol,
                'interval': interval,
                'startTime': current_start,
                'endTime': end_ts,
                'limit': self.max_limit,
            }

            response = self.session.get(f"{self.base_url}/klines", params=params, timeout=30)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                self.logger.warning(f"触发限速，等待 {retry_after}s")
                time.sleep(retry_after)
                continue

            if response.status_code == 400:
                error_data = response.json()
                error_code = error_data.get('code')
                if error_code == -1121:
                    self.logger.warning(f"BinanceUSDM 标的 {exchange_symbol} 不存在，跳过")
                    return []
                response.raise_for_status()

            response.raise_for_status()
            data = response.json()

            if not data:
                break

            for k in data:
                all_data.append(OHLCV(
                    timestamp=int(k[0]),
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                    quote_volume=float(k[7]) if len(k) > 7 else 0.0,
                    exchange=self.name,
                    symbol=self.get_unified_symbol(exchange_symbol),
                    timeframe=timeframe,
                ))

            last_close_time = int(data[-1][6])
            current_start = last_close_time + 1

            if len(data) < self.max_limit:
                break

        return all_data
