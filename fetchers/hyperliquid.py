import time
from typing import List, Dict, Set, Optional
from models import OHLCV
from .base import BaseFetcher


HYPERLIQUID_INTERVAL_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
    "30m": "30m", "1h": "1h", "2h": "2h", "4h": "4h",
    "8h": "8h", "12h": "12h", "1d": "1d",
    "3d": "3d", "1w": "1w", "1M": "1M",
}


class HyperliquidFetcher(BaseFetcher):
    def __init__(self, config: dict, rate_limiter):
        super().__init__("Hyperliquid", config, rate_limiter)
        self.base_url = config.get('base_url', 'https://api.hyperliquid.xyz')
        self._available_coins: Optional[Set[str]] = None

    def get_symbol_mapping(self) -> Dict[str, str]:
        return self.config.get('symbols', {})

    def _get_available_coins(self) -> Set[str]:
        if self._available_coins is not None:
            return self._available_coins

        try:
            response = self.session.post(
                f"{self.base_url}/info",
                json={"type": "meta"},
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            coins = set()
            universe = data.get('universe', [])
            for item in universe:
                if isinstance(item, dict) and 'name' in item:
                    coins.add(item['name'])

            self._available_coins = coins
            self.logger.info(f"Hyperliquid 可用标的: {sorted(coins)[:20]}")
            return coins
        except Exception as e:
            self.logger.warning(f"获取可用标的失败，使用配置中的标的列表: {e}")
            mapping = self.get_symbol_mapping()
            self._available_coins = set(mapping.keys())
            return self._available_coins

    def _is_symbol_available(self, exchange_symbol: str) -> bool:
        available = self._get_available_coins()
        return exchange_symbol in available

    def _do_fetch(
        self,
        exchange_symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[OHLCV]:
        interval = HYPERLIQUID_INTERVAL_MAP.get(timeframe, "1d")
        unified_symbol = self.get_unified_symbol(exchange_symbol)

        if not self._is_symbol_available(exchange_symbol):
            self.logger.warning(f"标的 {unified_symbol} 在 Hyperliquid 不可用，跳过")
            return []

        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": exchange_symbol,
                "interval": interval,
                "startTime": start_ts,
                "endTime": end_ts,
            }
        }

        try:
            response = self.session.post(
                f"{self.base_url}/info",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code == 429:
                self.logger.warning("触发限速，等待 5s")
                time.sleep(5)
                response = self.session.post(
                    f"{self.base_url}/info",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )

            response.raise_for_status()
            candles = response.json()

            if not candles or candles is None:
                return []

            if not isinstance(candles, list):
                candles = candles.get('data', []) or []
            records = []
            for c in candles:
                volume = float(c.get('v', 0))
                if volume <= 0:
                    continue
                records.append(OHLCV(
                    timestamp=int(c['t']),
                    open=float(c['o']),
                    high=float(c['h']),
                    low=float(c['l']),
                    close=float(c['c']),
                    volume=volume,
                    quote_volume=0.0,
                    exchange=self.name,
                    symbol=unified_symbol,
                    timeframe=timeframe,
                    trades=int(c.get('n', 0)) if c.get('n') else None,
                ))

            return records
        except Exception as e:
            self.logger.error(f"Hyperliquid 获取 {unified_symbol} 数据失败: {e}")
            raise
