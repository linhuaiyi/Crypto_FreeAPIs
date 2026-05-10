from typing import List, Dict
from models import OHLCV
from .base import BaseFetcher


DERIBIT_RESOLUTION_MAP = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "1d": "1D",
    "1w": "1W", "1M": "1M",
}


class DeribitFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("Deribit", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://www.deribit.com/api/v2')

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        resolution = DERIBIT_RESOLUTION_MAP.get(timeframe, "1D")
        import time

        all_records = []
        chunk_size = 365 * 86400000
        current_start = start_ts

        while current_start < end_ts:
            chunk_end = min(current_start + chunk_size, end_ts)

            params = {
                'instrument_name': exchange_symbol,
                'start_timestamp': current_start,
                'end_timestamp': chunk_end,
                'resolution': resolution,
            }

            try:
                response = self.session.get(
                    f"{self.base_url}/public/get_tradingview_chart_data",
                    params=params,
                    timeout=30
                )

                if response.status_code == 429:
                    self.logger.warning("触发限速，等待 5s")
                    time.sleep(5)
                    response = self.session.get(
                        f"{self.base_url}/public/get_tradingview_chart_data",
                        params=params,
                        timeout=30
                    )

                if response.status_code == 400:
                    self.logger.warning(f"Deribit 标的 {exchange_symbol} 时间范围超出: {current_start} - {chunk_end}")
                    current_start = chunk_end + 1
                    continue

                response.raise_for_status()
            except Exception as e:
                self.logger.warning(f"Deribit 请求失败: {e}")
                current_start = chunk_end + 1
                continue

            result = response.json()

            if 'result' not in result:
                current_start = chunk_end + 1
                continue

            data = result['result']
            ticks = data.get('ticks', [])
            opens = data.get('open', [])
            highs = data.get('high', [])
            lows = data.get('low', [])
            closes = data.get('close', [])
            volumes = data.get('volume', [])

            for i in range(len(ticks)):
                all_records.append(OHLCV(
                    timestamp=int(ticks[i]),
                    open=float(opens[i]) if i < len(opens) else 0.0,
                    high=float(highs[i]) if i < len(highs) else 0.0,
                    low=float(lows[i]) if i < len(lows) else 0.0,
                    close=float(closes[i]) if i < len(closes) else 0.0,
                    volume=float(volumes[i]) if i < len(volumes) else 0.0,
                    quote_volume=0.0,
                    exchange=self.name,
                    symbol=self.get_unified_symbol(exchange_symbol),
                    timeframe=timeframe,
                ))

            current_start = chunk_end + 1

        return all_records
