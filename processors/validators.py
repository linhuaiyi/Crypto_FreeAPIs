"""Pre-flush validation gates for collected snapshots.

Each validator filters a DataFrame down to rows that pass sanity checks
and logs how many were dropped. This prevents corrupted data from
reaching parquet — the downstream strategy depends on these invariants.

Sanity bounds are conservative (catch garbage, not edges):

- ``iv`` in (0, 1000) percent — realistic range is 10-200
- ``|delta|`` <= 1.0001 — BS can overshoot by FP epsilon
- ``gamma`` in [0, 1e6) — ATM gamma near expiry can be large but not astronomical
- ``vega``/``theta`` finite (not NaN/inf)
- ``mid_price``/``bid_price``/``ask_price`` >= 0, ask >= bid
- ``dvol`` in (0, 400) — Deribit DVOL historically 20-200
- ``index_price`` > 0 when present
"""

from __future__ import annotations

import math
from typing import Tuple

import numpy as np
import pandas as pd

from utils import get_logger

logger = get_logger(__name__)


def _finite(series: pd.Series) -> pd.Series:
    """Return boolean mask: value is finite (not NaN/inf)."""
    s = pd.to_numeric(series, errors="coerce")
    return s.replace([np.inf, -np.inf], np.nan).notna()


def validate_options_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """Filter options_greeks rows that fail sanity checks.

    Returns the cleaned DataFrame (possibly empty). Logs drop count.
    """
    if df.empty:
        return df

    n0 = len(df)
    mask = pd.Series(True, index=df.index)

    if "iv" in df.columns:
        iv = pd.to_numeric(df["iv"], errors="coerce")
        mask &= (iv > 0) & (iv < 1000)

    if "delta" in df.columns:
        delta = pd.to_numeric(df["delta"], errors="coerce")
        mask &= delta.abs() <= 1.0001

    if "gamma" in df.columns:
        gamma = pd.to_numeric(df["gamma"], errors="coerce")
        mask &= (gamma >= 0) & (gamma < 1e6) & _finite(df["gamma"])

    for col in ("vega", "theta"):
        if col in df.columns:
            mask &= _finite(df[col])

    # Price consistency
    for col in ("mid_price", "bid_price", "ask_price"):
        if col in df.columns:
            price = pd.to_numeric(df[col], errors="coerce")
            mask &= (price >= 0) | price.isna()  # allow NaN, reject negative

    if "bid_price" in df.columns and "ask_price" in df.columns:
        bid = pd.to_numeric(df["bid_price"], errors="coerce")
        ask = pd.to_numeric(df["ask_price"], errors="coerce")
        # ask >= bid when both are positive
        both_positive = (bid > 0) & (ask > 0)
        violated = both_positive & (ask < bid)
        mask &= ~violated

    out = df[mask].reset_index(drop=True)
    dropped = n0 - len(out)
    if dropped > 0:
        bad_rate = dropped / n0 if n0 else 0.0
        level = logger.warning if bad_rate > 0.01 else logger.info
        level(
            f"validate_options_greeks: dropped {dropped}/{n0} rows "
            f"(bad_rate={bad_rate:.4%})"
        )
    return out


def validate_dvol(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DVOL rows: dvol in (0, 400) and finite."""
    if df.empty:
        return df

    n0 = len(df)
    dvol = pd.to_numeric(df["dvol"], errors="coerce")
    mask = (dvol > 0) & (dvol < 400) & dvol.replace([np.inf, -np.inf], np.nan).notna()
    out = df[mask].reset_index(drop=True)
    dropped = n0 - len(out)
    if dropped > 0:
        logger.warning(f"validate_dvol: dropped {dropped}/{n0} rows")
    return out


def validate_index_price(df: pd.DataFrame) -> pd.DataFrame:
    """Filter index_price rows: index_price > 0 and finite."""
    if df.empty:
        return df

    n0 = len(df)
    ip = pd.to_numeric(df["index_price"], errors="coerce")
    mask = (ip > 0) & ip.replace([np.inf, -np.inf], np.nan).notna()
    out = df[mask].reset_index(drop=True)
    dropped = n0 - len(out)
    if dropped > 0:
        logger.warning(f"validate_index_price: dropped {dropped}/{n0} rows")
    return out


def validate_mark_price(df: pd.DataFrame) -> pd.DataFrame:
    """Filter mark_price rows: mark_price > 0."""
    if df.empty:
        return df

    n0 = len(df)
    mp = pd.to_numeric(df["mark_price"], errors="coerce")
    mask = mp > 0
    out = df[mask].reset_index(drop=True)
    dropped = n0 - len(out)
    if dropped > 0:
        logger.warning(f"validate_mark_price: dropped {dropped}/{n0} rows")
    return out
