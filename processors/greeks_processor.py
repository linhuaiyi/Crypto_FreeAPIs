"""
Vectorized Greeks processor for Deribit options chain data.

Computes Black-Scholes Greeks (delta, gamma, vega, theta, rho) for all
active options using NumPy vectorized operations. Zero Python loops for math.

Fetches option chain via Deribit REST API (public/get_book_summary_by_currency)
which returns bid_iv/ask_iv/mark_iv directly, avoiding expensive Newton-Raphson.

Hardware constraints:
  - scipy.special.ndtr for CDF (10x faster than scipy.stats.norm.cdf)
  - Inline PDF formula: exp(-x^2/2) / sqrt(2*pi)
  - All outputs cast to float32 for 50% storage savings
  - Zombie option filtering before computation
  - Explicit del + gc.collect() for memory control
"""

from __future__ import annotations

import gc
import math
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from scipy.special import ndtr

from utils import get_logger

logger = get_logger(__name__)

# Constants
_DAYS_PER_YEAR = 365.25
_MS_PER_DAY = 86400 * 1000
_SQRT_2PI = math.sqrt(2.0 * math.pi)

# Zombie filter thresholds
_MAX_OTM_PCT = 0.50       # skip options >50% OTM
_MIN_TTE_YEARS = 5.0 / (365.25 * 24 * 60)  # skip <5min to expiry
_FALLBACK_IV = 0.5


def _norm_pdf(x: np.ndarray) -> np.ndarray:
    """Vectorized standard normal PDF — no scipy.stats.norm.pdf."""
    return np.exp(-x * x / 2.0) / _SQRT_2PI


@dataclass(frozen=True)
class InstrumentMeta:
    """Parsed instrument metadata."""
    currency: str
    expiry_str: str
    expiry_timestamp: int  # ms since epoch
    strike: float
    option_type: str  # 'C' or 'P'


@dataclass(frozen=True)
class GreeksSnapshot:
    """Greeks for a single option instrument."""
    timestamp: int
    instrument_name: str
    exchange: str
    underlying_price: float
    strike: float
    time_to_expiry_years: float
    option_type: str
    iv: float
    iv_source: str
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float
    mid_price: float
    bid_price: float
    ask_price: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "instrument_name": self.instrument_name,
            "exchange": self.exchange,
            "underlying_price": self.underlying_price,
            "strike": self.strike,
            "time_to_expiry_years": self.time_to_expiry_years,
            "option_type": self.option_type,
            "iv": self.iv,
            "iv_source": self.iv_source,
            "delta": self.delta,
            "gamma": self.gamma,
            "vega": self.vega,
            "theta": self.theta,
            "rho": self.rho,
            "mid_price": self.mid_price,
            "bid_price": self.bid_price,
            "ask_price": self.ask_price,
        }


class DeribitOptionsChainFetcher:
    """Fetch all option tickers for a currency via Deribit REST API."""

    def __init__(self) -> None:
        self.base_url = "https://www.deribit.com/api/v2"
        self.session = requests.Session()

    def fetch_option_chain(self, currency: str) -> List[Dict]:
        """Fetch option chain summary for a currency.

        Endpoint: public/get_book_summary_by_currency
        Returns list of dicts with: instrument_name, mid_price, bid_price,
        ask_price, underlying_price, mark_iv, bid_iv, ask_iv, open_interest, etc.
        """
        try:
            resp = self.session.get(
                f"{self.base_url}/public/get_book_summary_by_currency",
                params={"currency": currency, "kind": "option"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("error"):
                logger.warning(
                    f"Deribit option chain error for {currency}: {data['error']}"
                )
                return []

            result = data.get("result", [])
            logger.info(f"Deribit {currency} option chain: {len(result)} instruments")
            return result

        except Exception as e:
            logger.warning(f"Deribit option chain fetch failed for {currency}: {e}")
            return []


class GreeksProcessor:
    """Vectorized Black-Scholes Greeks computation for option chains.

    All BS math uses NumPy arrays with scipy.special.ndtr (C-level CDF)
    and inline PDF formula. Zero Python loops for pricing math.
    """

    def __init__(self, risk_free_rate: float = 0.05) -> None:
        self._default_rfr = risk_free_rate

    @staticmethod
    def parse_instrument_name(name: str) -> Optional[InstrumentMeta]:
        """Parse Deribit instrument name to metadata.

        Format: '{currency}-{DDMonYY}-{strike}-{C/P}'
        Example: 'BTC-9MAY26-100000-C'
        """
        parts = name.split("-")
        if len(parts) < 4:
            return None

        try:
            currency = parts[0]
            expiry_str = parts[1]
            strike = float(parts[2])
            option_type = parts[3].upper()

            if option_type not in ("C", "P"):
                return None

            expiry_date = datetime.strptime(expiry_str, "%d%b%y")
            expiry_timestamp = int(expiry_date.timestamp() * 1000)

            return InstrumentMeta(
                currency=currency,
                expiry_str=expiry_str,
                expiry_timestamp=expiry_timestamp,
                strike=strike,
                option_type=option_type,
            )
        except (ValueError, IndexError):
            return None

    def compute_batch(
        self,
        chain_data: List[Dict],
        risk_free_rate: Optional[float] = None,
        now_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """Compute Greeks for an entire option chain — fully vectorized.

        Args:
            chain_data: List of dicts from DeribitOptionsChainFetcher.
            risk_free_rate: Annualized continuous rate. Falls back to default.
            now_ms: Current timestamp in ms. Defaults to now.

        Returns:
            DataFrame with all Greeks columns, float32, zombie-filtered.
            Empty DataFrame if no valid data.
        """
        if not chain_data:
            return pd.DataFrame()

        r = risk_free_rate if risk_free_rate is not None else self._default_rfr
        now = now_ms or int(time.time() * 1000)

        # ── Step 1: Build DataFrame ──
        df = pd.DataFrame(chain_data)

        # Required columns
        required = ["instrument_name", "bid_price", "ask_price"]
        for col in required:
            if col not in df.columns:
                logger.warning(f"Missing required column '{col}' in chain data")
                return pd.DataFrame()

        # Fill optional columns
        for col in ["mid_price", "underlying_price", "mark_iv", "bid_iv", "ask_iv",
                     "open_interest"]:
            if col not in df.columns:
                df[col] = np.nan

        # ── Step 2: Vectorized instrument name parsing ──
        extracted = df["instrument_name"].str.extract(
            r"^([A-Z]+)-(\d{1,2}[A-Z]{3}\d{2})-(\d+)-([CP])$"
        )
        if extracted is None or extracted[0].isna().all():
            logger.warning("No valid instrument names found in chain data")
            return pd.DataFrame()

        df["_currency"] = extracted[0]
        df["_expiry_str"] = extracted[1]
        df["_strike"] = pd.to_numeric(extracted[2], errors="coerce")
        df["_option_type"] = extracted[3]

        # Drop unparseable rows
        df = df.dropna(subset=["_strike", "_option_type"]).copy()

        if df.empty:
            return pd.DataFrame()

        # ── Step 3: Vectorized expiry parsing ──
        try:
            expiry_dates = pd.to_datetime(
                df["_expiry_str"], format="%d%b%y", errors="coerce"
            )
            df["_expiry_ts"] = (expiry_dates.astype("int64") // 10**3).astype("int64")
        except Exception:
            logger.warning("Failed to parse expiry dates")
            return pd.DataFrame()

        # Drop rows with unparseable expiry
        df = df.dropna(subset=["_expiry_ts"]).copy()
        if df.empty:
            return pd.DataFrame()

        # ── Step 4: Time to expiry (years) ──
        df["time_to_expiry_years"] = (
            (df["_expiry_ts"] - now) / (_MS_PER_DAY * _DAYS_PER_YEAR)
        ).astype(np.float64)

        # ── Step 5: Underlying price ──
        # Use underlying_price from API, fallback to mid_price of perpetuals
        df["underlying_price"] = pd.to_numeric(
            df["underlying_price"], errors="coerce"
        ).fillna(0)

        # ── Step 6: Zombie filter ──
        bid = pd.to_numeric(df["bid_price"], errors="coerce").fillna(0).values
        ask = pd.to_numeric(df["ask_price"], errors="coerce").fillna(0).values
        tte = df["time_to_expiry_years"].values
        strike = df["_strike"].values
        underlying = df["underlying_price"].values

        has_liquidity = (bid > 0) | (ask > 0)
        not_expired = tte > _MIN_TTE_YEARS
        not_deep_otm = np.abs(strike - underlying) / np.maximum(underlying, 1.0) < _MAX_OTM_PCT

        mask = has_liquidity & not_expired & not_deep_otm
        df = df[mask].copy()

        if df.empty:
            logger.debug("All options filtered out by zombie filter")
            return pd.DataFrame()

        # ── Step 7: IV resolution (vectorized) ──
        mark_iv = pd.to_numeric(df["mark_iv"], errors="coerce").fillna(0).values
        bid_iv = pd.to_numeric(df["bid_iv"], errors="coerce").fillna(0).values
        ask_iv = pd.to_numeric(df["ask_iv"], errors="coerce").fillna(0).values

        # Priority: mark_iv > mid_iv(bid+ask)/2 > fallback
        mid_iv = (bid_iv + ask_iv) / 2.0
        has_mark = mark_iv > 0
        has_mid = mid_iv > 0

        iv = np.where(has_mark, mark_iv, np.where(has_mid, mid_iv, _FALLBACK_IV))
        iv_source = np.where(
            has_mark, "rest_api",
            np.where(has_mid, "rest_api", "fallback")
        )

        # Ensure IV > 0
        iv = np.maximum(iv, 0.001)

        # ── Step 8: Vectorized Black-Scholes Greeks ──
        S = df["underlying_price"].values.astype(np.float64)
        K = df["_strike"].values.astype(np.float64)
        T = df["time_to_expiry_years"].values.astype(np.float64)
        sigma = iv.astype(np.float64)
        is_call = (df["_option_type"] == "C").values

        # Clamp T > 0 to avoid division by zero
        T = np.maximum(T, 1e-10)

        sqrt_T = np.sqrt(T)
        d1 = (np.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T

        pdf_d1 = _norm_pdf(d1)
        cdf_d1 = ndtr(d1)
        cdf_d2 = ndtr(d2)

        # Delta
        delta = np.where(is_call, cdf_d1, cdf_d1 - 1.0)

        # Gamma
        gamma = pdf_d1 / (S * sigma * sqrt_T)

        # Vega (per 1% move)
        vega = S * pdf_d1 * sqrt_T / 100.0

        # Theta (per day)
        theta_common = -S * pdf_d1 * sigma / (2.0 * sqrt_T)
        theta_call = theta_common - r * K * np.exp(-r * T) * cdf_d2
        theta_put = theta_common + r * K * np.exp(-r * T) * ndtr(-d2)
        theta = np.where(is_call, theta_call, theta_put) / _DAYS_PER_YEAR

        # Rho (per 1% rate move)
        rho_call = K * T * np.exp(-r * T) * cdf_d2 / 100.0
        rho_put = -K * T * np.exp(-r * T) * ndtr(-d2) / 100.0
        rho = np.where(is_call, rho_call, rho_put)

        # ── Step 9: Assemble output DataFrame ──
        mid_price = pd.to_numeric(df["mid_price"], errors="coerce").fillna(0)

        result = pd.DataFrame({
            "timestamp": now,
            "instrument_name": df["instrument_name"].values,
            "exchange": "deribit",
            "underlying_price": S,
            "strike": K,
            "expiry": df["_expiry_str"].values,
            "time_to_expiry_years": T,
            "option_type": df["_option_type"].values,
            "iv": iv,
            "iv_source": iv_source,
            "delta": delta,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
            "rho": rho,
            "mid_price": mid_price.values,
            "bid_price": bid[mask],
            "ask_price": ask[mask],
        })

        # ── Step 10: Cast to float32 ──
        float_cols = [
            "underlying_price", "strike", "time_to_expiry_years",
            "iv", "delta", "gamma", "vega", "theta", "rho",
            "mid_price", "bid_price", "ask_price",
        ]
        for col in float_cols:
            result[col] = result[col].astype(np.float32)

        # Cleanup intermediate arrays
        del d1, d2, pdf_d1, cdf_d1, cdf_d2, S, K, T, sigma
        del delta, gamma, vega, theta, rho, iv, bid, ask, strike, underlying, tte
        del mark_iv, bid_iv, ask_iv, mid_iv, mask, sqrt_T

        logger.info(
            f"GreeksProcessor: computed {len(result)} Greeks "
            f"(filtered from {len(chain_data)} instruments)"
        )

        return result
