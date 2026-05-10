from typing import List, Dict, Optional
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


class DeribitOptionsFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("DeribitOptions", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://www.deribit.com/api/v2')
        self.options_base = config.get('options_base_url', 'https://www.deribit.com/api/v2')

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('option_symbols', {})

    def get_all_option_instruments(self, currency: str, expired: str = 'false') -> List[Dict]:
        params = {
            'currency': currency,
            'kind': 'option',
            'expired': expired
        }

        response = self.session.get(
            f"{self.base_url}/public/get_instruments",
            params=params,
            timeout=30
        )
        response.raise_for_status()

        result = response.json()
        if 'result' in result:
            return result['result']
        return []

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
                    self.logger.warning(f"Deribit 期权 {exchange_symbol} 时间范围超出: {current_start} - {chunk_end}")
                    current_start = chunk_end + 1
                    continue

                response.raise_for_status()
            except Exception as e:
                self.logger.warning(f"Deribit 期权请求失败: {e}")
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

    def fetch_options_for_currency(
        self,
        currency: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
        include_expired: bool = False
    ) -> List[OHLCV]:
        all_records = []

        self.logger.info(f"获取 {currency} 期权列表...")
        instruments = self.get_all_option_instruments(currency, 'false')

        if not instruments:
            self.logger.warning(f"未找到 {currency} 的期权合约")
            return all_records

        active_instruments = [i for i in instruments if i.get('is_active', False)]
        self.logger.info(f"找到 {len(active_instruments)} 个活跃 {currency} 期权")

        for inst in active_instruments:
            inst_name = inst['instrument_name']
            try:
                records = self.fetch_with_backoff(inst_name, timeframe, start_ts, end_ts)
                if records:
                    all_records.extend(records)
                    self.logger.debug(f"  {inst_name}: 获取 {len(records)} 条记录")
            except Exception as e:
                self.logger.warning(f"获取 {inst_name} 失败: {e}")
                continue

        if include_expired:
            self.logger.info(f"获取已到期的 {currency} 期权...")
            expired_instruments = self.get_all_option_instruments(currency, 'true')
            self.logger.info(f"找到 {len(expired_instruments)} 个已到期 {currency} 期权")

            for inst in expired_instruments[:50]:
                inst_name = inst['instrument_name']
                exp_ts = inst.get('expiration_timestamp', 0)
                if exp_ts > start_ts and exp_ts < end_ts:
                    try:
                        records = self.fetch_with_backoff(inst_name, timeframe, start_ts, exp_ts)
                        if records:
                            all_records.extend(records)
                            self.logger.debug(f"  {inst_name}: 获取 {len(records)} 条记录")
                    except Exception as e:
                        self.logger.warning(f"获取 {inst_name} 失败: {e}")
                        continue

        return all_records
