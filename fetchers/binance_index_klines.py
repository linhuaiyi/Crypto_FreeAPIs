"""
REST fallback for Binance USDM futures index price klines.

Used for the current UTC day when data.binance.vision daily zip has not yet
been published. Endpoint: GET /fapi/v1/indexPriceKlines

Response is the same 12-element array shape as /fapi/v1/klines, but volume /
trades / taker columns are zeros / nulls. We strip them to match the
mark/index schema produced by fetchers.binance_archive._parse_klines.
"""

from __future__ import annotations

import time
from typing import List

import requests

from utils import get_logger

logger = get_logger("BinanceIndexKlines")


class BinanceIndexKlinesFetcher:
    BASE_URL = "https://fapi.binance.com/fapi/v1"
    MAX_LIMIT = 1500

    def __init__(self, timeout_sec: int = 30, max_retries: int = 3) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.session = requests.Session()

    def fetch(
        self,
        symbol: str,
        interval: str,
        start_ts: int,
        end_ts: int,
    ) -> List[dict]:
        """Paginate /indexPriceKlines across [start_ts, end_ts] (ms).

        Returns list of dicts with keys: timestamp, open, high, low, close, close_time.
        """
        out: List[dict] = []
        cursor = start_ts
        while cursor < end_ts:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ts,
                "limit": self.MAX_LIMIT,
            }
            data = self._get_with_retry("/indexPriceKlines", params)
            if not data:
                break
            for row in data:
                out.append({
                    "timestamp": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "close_time": int(row[6]),
                })
            last_close_ts = int(data[-1][6])
            if last_close_ts <= cursor:
                break
            cursor = last_close_ts + 1
            if len(data) < self.MAX_LIMIT:
                break
        return out

    def _get_with_retry(self, path: str, params: dict) -> list:
        url = f"{self.BASE_URL}{path}"
        last_err: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_sec)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning(f"429 on {url}; sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue
                if resp.status_code >= 500:
                    backoff = 2 ** attempt
                    logger.warning(
                        f"HTTP {resp.status_code} on {url} (attempt {attempt+1}); sleeping {backoff}s"
                    )
                    time.sleep(backoff)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = e
                backoff = 2 ** attempt
                logger.warning(
                    f"network error on {url} (attempt {attempt+1}): {e}; sleeping {backoff}s"
                )
                time.sleep(backoff)
        if last_err:
            logger.error(f"giving up on {url}: {last_err}")
        return []
