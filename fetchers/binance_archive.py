"""
Bulk historical downloader for Binance public data (data.binance.vision).

Static CDN, no API key, no rate limit. Returns None on 404 (future date /
halted trading). All kline functions return a unified schema DataFrame.

URL templates (see docs/binance/STRATEGY_DATA_REQUIREMENTS_BINANCE.md §4.1):
    spot  klines:  /data/spot/daily/klines/{SYM}/{IV}/{SYM}-{IV}-{DATE}.zip
    um    klines:  /data/futures/um/daily/klines/{SYM}/{IV}/{SYM}-{IV}-{DATE}.zip
    mark  klines:  /data/futures/um/daily/markPriceKlines/{SYM}/{IV}/{SYM}-{IV}-{DATE}.zip
    index klines:  /data/futures/um/daily/indexPriceKlines/{SYM}/{IV}/{SYM}-{IV}-{DATE}.zip
    fundingRate:   /data/futures/um/monthly/fundingRate/{SYM}/{SYM}-fundingRate-{YYYY-MM}.zip
"""

from __future__ import annotations

import io
import time
import zipfile
from datetime import date
from typing import Literal, Optional

import pandas as pd
import requests

from utils import get_logger

logger = get_logger("BinanceArchive")

_VISION_BASE = "https://data.binance.vision"

_KL_FULL_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote",
]
_KL_PRICE_COLS = ["timestamp", "open", "high", "low", "close", "close_time"]

Kind = Literal["spot", "um"]


def _fetch_zip(url: str, timeout: int = 60, max_retries: int = 3) -> Optional[bytes]:
    """GET a zip from data.binance.vision. Returns None on 404.

    Retries with exponential backoff on transient errors.
    Raises HTTPError for non-404 HTTP errors.
    """
    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                last_err = requests.exceptions.HTTPError(
                    f"HTTP {resp.status_code} on {url}", response=resp
                )
                logger.warning(
                    f"transient HTTP {resp.status_code} on {url} "
                    f"(attempt {attempt + 1}/{max_retries}); sleeping {retry_after}s"
                )
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp.content
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            backoff = 2 ** attempt
            logger.warning(
                f"network error on {url} (attempt {attempt + 1}/{max_retries}): {e}; "
                f"sleeping {backoff}s"
            )
            time.sleep(backoff)
    if last_err:
        logger.error(f"giving up on {url}: {last_err}")
    return None


def _extract_csv(zip_bytes: bytes) -> str:
    """Extract the first (only) CSV from a vision zip and return its text."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise ValueError(f"no CSV inside zip; entries={zf.namelist()}")
        with zf.open(names[0]) as f:
            return f.read().decode("utf-8")


def _parse_klines(csv_text: str, has_volume: bool) -> pd.DataFrame:
    """Parse klines CSV into unified schema.

    Reality on data.binance.vision (verified 2026-07):
      - spot klines: NO header, timestamp in MICROSECONDS (16 digits)
      - um klines / markPriceKlines / indexPriceKlines: HAS header
        ('open_time,open,high,low,close,volume,close_time,...'), timestamp in
        MILLISECONDS (13 digits)

    We auto-detect: drop header row if present; convert μs → ms when needed.
    """
    raw = pd.read_csv(io.StringIO(csv_text), header=None, dtype=str)
    # Drop header row if the first cell isn't numeric
    first_cell = str(raw.iloc[0, 0]).strip()
    try:
        float(first_cell)
    except (ValueError, TypeError):
        raw = raw.iloc[1:].reset_index(drop=True)

    if has_volume:
        if raw.shape[1] < 11:
            raise ValueError(f"expected >=11 kline cols, got {raw.shape[1]}")
        df = raw.iloc[:, :11].copy()
        df.columns = _KL_FULL_COLS
    else:
        if raw.shape[1] < 7:
            raise ValueError(f"expected >=7 price-kline cols, got {raw.shape[1]}")
        df = raw.iloc[:, [0, 1, 2, 3, 4, 6]].copy()
        df.columns = _KL_PRICE_COLS

    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in ("timestamp", "close_time"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("int64")
    if has_volume:
        for c in ("volume", "quote_volume", "taker_buy_base", "taker_buy_quote"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
        df["trades"] = pd.to_numeric(df["trades"], errors="coerce").astype("int64")

    # Auto-detect μs vs ms on timestamp (spot uses μs, others use ms).
    # Any value >= 1e14 must be μs.
    if (df["timestamp"].abs() >= 10**14).any():
        df["timestamp"] = (df["timestamp"] // 1000).astype("int64")
        if "close_time" in df.columns and (df["close_time"].abs() >= 10**14).any():
            df["close_time"] = (df["close_time"] // 1000).astype("int64")
    return df


def _parse_funding(csv_text: str) -> pd.DataFrame:
    """Parse funding rate monthly CSV.

    Actual format on vision (verified 2026-07):
        calc_time,funding_interval_hours,last_funding_rate
        1780272000001,8,0.00005703
        ...

    Output schema: timestamp (ms int64), funding_rate (float64).
    """
    raw = pd.read_csv(io.StringIO(csv_text), header=None, dtype=str)
    first_cell = str(raw.iloc[0, 0]).strip()
    try:
        float(first_cell)
    except (ValueError, TypeError):
        raw = raw.iloc[1:].reset_index(drop=True)
    if raw.shape[1] < 3:
        raise ValueError(f"expected >=3 funding cols, got {raw.shape[1]}")
    df = pd.DataFrame({
        "timestamp": pd.to_numeric(raw.iloc[:, 0], errors="coerce").astype("int64"),
        "funding_rate": pd.to_numeric(raw.iloc[:, 2], errors="coerce").astype("float64"),
    })
    return df


def download_daily_klines(
    symbol: str,
    kind: Kind,
    interval: str,
    day: date,
) -> Optional[pd.DataFrame]:
    """Fetch one day's klines from vision CDN.

    Returns None if the day is not yet published (HTTP 404) or transient failure.
    Caller decides whether to fall back to REST.
    """
    kind_path = "spot" if kind == "spot" else "futures/um"
    d = day.strftime("%Y-%m-%d")
    url = (
        f"{_VISION_BASE}/data/{kind_path}/daily/klines/"
        f"{symbol}/{interval}/{symbol}-{interval}-{d}.zip"
    )
    blob = _fetch_zip(url)
    if blob is None:
        return None
    return _parse_klines(_extract_csv(blob), has_volume=True)


def download_daily_mark_klines(
    symbol: str,
    interval: str,
    day: date,
) -> Optional[pd.DataFrame]:
    d = day.strftime("%Y-%m-%d")
    url = (
        f"{_VISION_BASE}/data/futures/um/daily/markPriceKlines/"
        f"{symbol}/{interval}/{symbol}-{interval}-{d}.zip"
    )
    blob = _fetch_zip(url)
    if blob is None:
        return None
    return _parse_klines(_extract_csv(blob), has_volume=False)


def download_daily_index_klines(
    symbol: str,
    interval: str,
    day: date,
) -> Optional[pd.DataFrame]:
    d = day.strftime("%Y-%m-%d")
    url = (
        f"{_VISION_BASE}/data/futures/um/daily/indexPriceKlines/"
        f"{symbol}/{interval}/{symbol}-{interval}-{d}.zip"
    )
    blob = _fetch_zip(url)
    if blob is None:
        return None
    return _parse_klines(_extract_csv(blob), has_volume=False)


def download_monthly_funding(
    symbol: str,
    month: date,
) -> Optional[pd.DataFrame]:
    """Fetch a month of funding rate rows. Pass any date in the target month."""
    ym = month.strftime("%Y-%m")
    url = (
        f"{_VISION_BASE}/data/futures/um/monthly/fundingRate/"
        f"{symbol}/{symbol}-fundingRate-{ym}.zip"
    )
    blob = _fetch_zip(url)
    if blob is None:
        return None
    return _parse_funding(_extract_csv(blob))
