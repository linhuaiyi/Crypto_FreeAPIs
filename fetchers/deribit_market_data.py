"""Deribit market-level data fetchers: DVOL index and spot index price.

Two endpoints that the options collector was not previously calling:

- ``public/get_volatility_index_data`` — Deribit DVOL (volatility index)
  historical time series. We poll the latest minute and take the last
  close as the current DVOL value.
- ``public/ticker`` on ``{currency}-PERPETUAL`` — returns ``index_price``,
  the real Deribit index used for option settlement. This is NOT the
  perpetual mark price (which carries basis) and is what the 0-1DTE
  strategy needs for delivery settlement.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

from utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class SpotTicker:
    """Single spot ticker observation from Deribit (e.g. BTC_USDC)."""
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


@dataclass(frozen=True)
class DvolSnapshot:
    """Single DVOL observation."""
    timestamp: int  # ms since epoch
    symbol: str     # 'BTC' or 'ETH'
    dvol: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "dvol": self.dvol,
        }


@dataclass(frozen=True)
class IndexPriceSnapshot:
    """Single index price observation."""
    timestamp: int  # ms since epoch
    symbol: str     # 'BTC' or 'ETH'
    index_price: float
    mark_price: float
    estimated_delivery_price: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "index_price": self.index_price,
            "mark_price": self.mark_price,
            "estimated_delivery_price": self.estimated_delivery_price,
        }


class DeribitMarketDataFetcher:
    """Fetch DVOL and index price from Deribit REST API."""

    def __init__(self) -> None:
        self.base_url = "https://www.deribit.com/api/v2"
        self.session = requests.Session()

    def fetch_dvol(self, currency: str) -> Optional[DvolSnapshot]:
        """Fetch the latest DVOL value for a currency.

        Uses ``public/get_volatility_index_data`` with a 2-minute lookback
        window and takes the last close. Returns None on failure.
        """
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 120_000  # last 2 minutes
        try:
            resp = self.session.get(
                f"{self.base_url}/public/get_volatility_index_data",
                params={
                    "currency": currency,
                    "resolution": "1",
                    "start_timestamp": start_ms,
                    "end_timestamp": now_ms,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("error"):
                logger.warning(
                    f"DVOL fetch error for {currency}: {data['error']}"
                )
                return None

            result = data.get("result", {})
            rows = result.get("data") or []
            if not rows:
                # Fallback: some responses use the ticks/close shape
                ticks = result.get("ticks") or []
                closes = result.get("close") or []
                if ticks and closes:
                    return DvolSnapshot(
                        timestamp=int(ticks[-1]),
                        symbol=currency,
                        dvol=float(closes[-1]),
                    )
                logger.debug(f"DVOL empty for {currency}")
                return None

            # rows are [ts, o, h, l, c]
            last = rows[-1]
            return DvolSnapshot(
                timestamp=int(last[0]),
                symbol=currency,
                dvol=float(last[4]),
            )
        except Exception as e:
            logger.warning(f"DVOL fetch failed for {currency}: {e}")
            return None

    def fetch_spot_ticker(self, instrument: str) -> Optional[SpotTicker]:
        """Fetch spot ticker for a USDC-settled instrument (e.g. BTC_USDC)."""
        try:
            resp = self.session.get(
                f"{self.base_url}/public/ticker",
                params={"instrument_name": instrument},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("error"):
                logger.warning(
                    f"Spot ticker fetch error for {instrument}: {data['error']}"
                )
                return None

            result = data.get("result", {})
            bid = float(result.get("best_bid_price", 0))
            ask = float(result.get("best_ask_price", 0))
            last = float(result.get("last_price", 0))
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else last

            if mid <= 0:
                return None

            return SpotTicker(
                timestamp=int(result.get("timestamp", time.time() * 1000)),
                exchange="deribit",
                symbol=instrument,
                price=mid,
                bid_price=bid,
                ask_price=ask,
            )
        except Exception as e:
            logger.warning(f"Spot ticker fetch failed for {instrument}: {e}")
            return None

    def fetch_index_price(self, currency: str) -> Optional[IndexPriceSnapshot]:
        """Fetch the current Deribit index price for a currency.

        Uses ``public/ticker`` on ``{currency}-PERPETUAL`` which returns
        ``index_price`` (the real settlement index, not the perp mark).
        """
        instrument = f"{currency}-PERPETUAL"
        try:
            resp = self.session.get(
                f"{self.base_url}/public/ticker",
                params={"instrument_name": instrument},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("error"):
                logger.warning(
                    f"Index price fetch error for {instrument}: {data['error']}"
                )
                return None

            result = data.get("result", {})
            index_price = result.get("index_price")
            if index_price is None or index_price <= 0:
                return None

            return IndexPriceSnapshot(
                timestamp=int(result.get("timestamp", time.time() * 1000)),
                symbol=currency,
                index_price=float(index_price),
                mark_price=float(result.get("mark_price", 0.0)),
                estimated_delivery_price=float(
                    result.get("estimated_delivery_price", 0.0)
                ),
            )
        except Exception as e:
            logger.warning(f"Index price fetch failed for {instrument}: {e}")
            return None
