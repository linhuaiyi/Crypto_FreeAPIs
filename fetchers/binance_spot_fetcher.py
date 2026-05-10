"""
Binance spot price fetcher for BTCUSDT / ETHUSDT.

Provides real-time spot prices via REST polling for basis calculation.
"""

import time
from dataclasses import dataclass
from typing import List

import requests

from utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class SpotPrice:
    """Single spot price observation."""
    timestamp: int
    exchange: str
    symbol: str
    price: float
    bid_price: float
    ask_price: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "price": self.price,
            "bid_price": self.bid_price,
            "ask_price": self.ask_price,
        }


class BinanceSpotPriceFetcher:
    """Fetch real-time spot prices from Binance."""

    def __init__(self) -> None:
        self.base_url = "https://api.binance.com/api/v3"
        self.session = requests.Session()

    def fetch_prices(self, symbols: List[str]) -> List[SpotPrice]:
        """Fetch current bookTicker for each symbol.

        Args:
            symbols: e.g. ['BTCUSDT', 'ETHUSDT']

        Returns:
            List of SpotPrice objects (one per symbol).
        """
        results: List[SpotPrice] = []

        for symbol in symbols:
            try:
                resp = self.session.get(
                    f"{self.base_url}/ticker/bookTicker",
                    params={"symbol": symbol},
                    timeout=5,
                )
                resp.raise_for_status()
                data = resp.json()

                bid = float(data.get("bidPrice", 0))
                ask = float(data.get("askPrice", 0))
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
                ts = int(data.get("T", 0)) or resp.headers.get("Date")

                if mid > 0:
                    ts_ms = int(data.get("time", 0)) or int(time.time() * 1000)
                    results.append(SpotPrice(
                        timestamp=ts_ms,
                        exchange="binance",
                        symbol=symbol,
                        price=mid,
                        bid_price=bid,
                        ask_price=ask,
                    ))

            except Exception as e:
                logger.warning(f"Binance spot bookTicker error for {symbol}: {e}")

        if results:
            logger.debug(
                f"Binance spot: fetched {len(results)} prices "
                f"({', '.join(f'{r.symbol}={r.price:.2f}' for r in results)})"
            )
        return results
