"""
Funding rate fetcher for Binance, Deribit, and Hyperliquid.

Collects historical and real-time funding rates for perpetual contracts.
"""

from dataclasses import dataclass
from typing import List, Optional
import time

import requests

from utils import get_logger

logger = get_logger("FundingRateFetcher")


@dataclass(frozen=True)
class FundingRate:
    """Single funding rate data point."""
    timestamp: int
    exchange: str
    symbol: str
    funding_rate: float
    mark_price: Optional[float] = None
    index_price: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "funding_rate": self.funding_rate,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FundingRate":
        return cls(
            timestamp=d["timestamp"],
            exchange=d["exchange"],
            symbol=d["symbol"],
            funding_rate=d["funding_rate"],
            mark_price=d.get("mark_price"),
            index_price=d.get("index_price"),
        )


class FundingRateFetcher:
    """Fetch funding rates from Binance, Deribit, and Hyperliquid."""

    def __init__(self) -> None:
        self.binance_base = "https://fapi.binance.com/fapi/v1"
        self.deribit_base = "https://www.deribit.com/api/v2"
        self.hyperliquid_base = "https://api.hyperliquid.xyz"
        self.session = requests.Session()

    # ── Binance ──

    def fetch_binance(
        self, symbol: str, start_ts: int, end_ts: int
    ) -> List[FundingRate]:
        """Fetch historical funding rates from Binance USDT-M futures."""
        results: List[FundingRate] = []
        current_start = start_ts
        limit = 1000

        while current_start < end_ts:
            params = {
                "symbol": symbol,
                "startTime": current_start,
                "endTime": end_ts,
                "limit": limit,
            }

            try:
                resp = self.session.get(
                    f"{self.binance_base}/fundingRate",
                    params=params,
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()

                if not data:
                    break

                for item in data:
                    results.append(FundingRate(
                        timestamp=int(item["fundingTime"]),
                        exchange="binance",
                        symbol=symbol,
                        funding_rate=float(item["fundingRate"]),
                    ))

                if len(data) < limit:
                    break

                current_start = int(data[-1]["fundingTime"]) + 1
                time.sleep(0.1)

            except Exception as e:
                logger.warning(f"Binance funding rate error for {symbol}: {e}")
                break

        logger.info(f"Binance {symbol}: fetched {len(results)} funding rate records")
        return results

    def fetch_binance_realtime(self, symbol: str) -> Optional[FundingRate]:
        """Fetch current funding rate from Binance premium index."""
        try:
            resp = self.session.get(
                f"{self.binance_base}/premiumIndex",
                params={"symbol": symbol},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            return FundingRate(
                timestamp=int(data["time"]),
                exchange="binance",
                symbol=symbol,
                funding_rate=float(data["lastFundingRate"]),
                mark_price=float(data["markPrice"]),
                index_price=float(data["indexPrice"]),
            )

        except Exception as e:
            logger.warning(f"Binance realtime funding error for {symbol}: {e}")
            return None

    # ── Deribit ──

    def fetch_deribit(
        self, instrument_name: str, start_ts: int, end_ts: int
    ) -> List[FundingRate]:
        """Fetch funding rate history from Deribit perpetual."""
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
        }

        try:
            resp = self.session.get(
                f"{self.deribit_base}/public/get_funding_rate_history",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json()

            if result.get("error"):
                logger.warning(
                    f"Deribit funding error for {instrument_name}: {result['error']}"
                )
                return []

            results: List[FundingRate] = []
            for item in result.get("result", []):
                results.append(FundingRate(
                    timestamp=int(item["timestamp"]),
                    exchange="deribit",
                    symbol=instrument_name,
                    funding_rate=float(item["interest_8h"]),
                    index_price=float(item.get("index_price", 0)) or None,
                ))

            logger.info(
                f"Deribit {instrument_name}: fetched {len(results)} funding records"
            )
            return results

        except Exception as e:
            logger.warning(f"Deribit funding rate error for {instrument_name}: {e}")
            return []

    # ── Hyperliquid ──

    def fetch_hyperliquid(
        self, coin: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None
    ) -> List[FundingRate]:
        """Fetch funding rate history from Hyperliquid.

        startTime is required by the API. If not provided, defaults to 30 days ago.
        """
        try:
            if start_ts is None:
                start_ts = int(time.time() * 1000) - 30 * 86400 * 1000

            body: dict = {
                "type": "fundingHistory",
                "coin": coin,
                "startTime": start_ts,
            }
            if end_ts is not None:
                body["endTime"] = end_ts

            resp = self.session.post(
                f"{self.hyperliquid_base}/info",
                json=body,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            results: List[FundingRate] = []
            for item in data:
                results.append(FundingRate(
                    timestamp=int(item["time"]),
                    exchange="hyperliquid",
                    symbol=coin,
                    funding_rate=float(item["fundingRate"]),
                    mark_price=float(item.get("premium", 0)) or None,
                ))

            logger.info(f"Hyperliquid {coin}: fetched {len(results)} funding records")
            return results

        except Exception as e:
            logger.warning(f"Hyperliquid funding rate error for {coin}: {e}")
            return []
