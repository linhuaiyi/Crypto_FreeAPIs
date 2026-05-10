"""
Mark price and index price fetcher for perpetual contracts.

Supports Binance, Deribit, and Hyperliquid exchanges.
"""

from dataclasses import dataclass
from typing import List, Optional
import time
import requests
from utils import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class MarkPrice:
    """Mark price data point."""
    timestamp: int  # milliseconds since epoch
    exchange: str  # 'binance', 'deribit', 'hyperliquid'
    symbol: str  # e.g., 'BTCUSDT', 'BTC-PERPETUAL'
    mark_price: float
    index_price: Optional[float] = None
    basis: Optional[float] = None  # mark_price - index_price

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
            "basis": self.basis,
        }


class MarkPriceFetcher:
    """Fetch mark price and index price from multiple exchanges."""

    def __init__(self):
        self.binance_base = "https://fapi.binance.com"
        self.deribit_base = "https://www.deribit.com"
        self.hyperliquid_base = "https://api.hyperliquid.xyz"

    def fetch_binance(self, symbol: str, start_ts: int, end_ts: int) -> List[MarkPrice]:
        """
        Fetch historical mark prices from Binance futures.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            start_ts: Start timestamp in milliseconds
            end_ts: End timestamp in milliseconds

        Returns:
            List of MarkPrice objects
        """
        results = []
        current_start = start_ts

        while current_start < end_ts:
            params = {
                "symbol": symbol,
                "interval": "1m",
                "startTime": current_start,
                "endTime": end_ts,
                "limit": 1000
            }

            try:
                response = requests.get(
                    f"{self.binance_base}/fapi/v1/markPriceKlines",
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                for item in data:
                    # Response: [time, open, high, low, close, volume, ...]
                    ts = int(item[0])
                    mark_price = float(item[4])  # Close price

                    results.append(MarkPrice(
                        timestamp=ts,
                        exchange="binance",
                        symbol=symbol,
                        mark_price=mark_price,
                        index_price=None,
                        basis=None
                    ))

                if len(data) < 1000:
                    break

                current_start = ts + 60000  # Next minute
                time.sleep(0.1)

            except Exception as e:
                logger.warning(f"Binance API error for {symbol}: {e}")
                break

        logger.info(f"Fetched {len(results)} mark prices from Binance for {symbol}")
        return results

    def fetch_deribit(self, symbol: str, start_ts: int, end_ts: int) -> List[MarkPrice]:
        """
        Fetch mark price history from Deribit via tradingview chart data.

        Deribit's get_mark_price_history returns empty for perpetuals.
        get_tradingview_chart_data returns OHLC ticks usable as mark prices.

        Args:
            symbol: Instrument name (e.g., 'BTC-PERPETUAL')
            start_ts: Start timestamp in milliseconds
            end_ts: End timestamp in milliseconds

        Returns:
            List of MarkPrice objects
        """
        params = {
            "instrument_name": symbol,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "resolution": "1",
        }

        try:
            response = requests.get(
                f"{self.deribit_base}/api/v2/public/get_tradingview_chart_data",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            if data.get("error"):
                logger.warning(f"Deribit API error for {symbol}: {data['error']}")
                return []

            result = data.get("result", {})
            ticks = result.get("ticks", [])
            closes = result.get("close", [])

            results = []
            for i, ts in enumerate(ticks):
                if i >= len(closes):
                    break
                results.append(MarkPrice(
                    timestamp=int(ts),
                    exchange="deribit",
                    symbol=symbol,
                    mark_price=float(closes[i]),
                ))

            logger.info(f"Fetched {len(results)} mark prices from Deribit for {symbol}")
            return results

        except Exception as e:
            logger.warning(f"Deribit request failed for {symbol}: {e}")
            return []

    def fetch_hyperliquid(self, symbol: Optional[str] = None) -> List[MarkPrice]:
        """
        Fetch current mid prices from Hyperliquid.

        Args:
            symbol: Optional filter for specific symbol

        Returns:
            List of MarkPrice objects with current mid prices
        """
        try:
            response = requests.post(
                f"{self.hyperliquid_base}/info",
                json={"type": "allMids"},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            results = []
            for sym, price in data.items():
                if symbol and sym != symbol:
                    continue

                results.append(MarkPrice(
                    timestamp=int(time.time() * 1000),
                    exchange="hyperliquid",
                    symbol=sym,
                    mark_price=float(price),
                    index_price=None,
                    basis=None
                ))

            logger.info(f"Fetched {len(results)} mid prices from Hyperliquid")
            return results

        except Exception as e:
            logger.warning(f"Hyperliquid request failed: {e}")
            return []
