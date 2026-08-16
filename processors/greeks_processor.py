"""
Vectorized Greeks processor for Deribit options chain data.

Computes Black-Scholes Greeks (delta, gamma, vega, theta, rho) for all
active options using NumPy vectorized operations. Zero Python loops for math.

Fetches option chain via Deribit REST API (public/get_book_summary_by_currency)
which returns mark_iv directly, avoiding expensive Newton-Raphson.
bid_iv/ask_iv are NOT returned by this endpoint (only by public/ticker).

Unit conventions
----------------
- ``iv`` / ``mark_iv`` / ``bid_iv`` / ``ask_iv`` are stored **in percent**
  (e.g. 55.0 means 55%). This matches Deribit's REST API.
- Internally the BS formula receives ``sigma = iv / 100`` (decimal, e.g. 0.55).
- ``T`` is in years, clamped to >= 1 hour to prevent gamma blowup near expiry.
- ``vega`` is per 1% IV move (NOT per 1.0 move). ``theta`` is per day.

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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

# T clamp: 1 hour in years — prevents gamma→∞ as T→0 (e.g. 0-1DTE near close).
# 1 / 8760 ≈ 1.14e-4 years.
_MIN_T_CLAMP_YEARS = 1.0 / 8760.0

# How many near-ATM instruments to enrich with top-of-book sizes via
# public/get_order_book. Full chain is too many for a 5s cadence; ATM
# subset is what the 0-1DTE strategy actually needs.
_ORDER_BOOK_ATM_LIMIT = 40
_ORDER_BOOK_ATM_MONEYNESS = 0.10  # 10% from underlying
_ORDER_BOOK_WORKERS = 8


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
    mark_iv: float = 0.0
    open_interest: float = 0.0
    volume_24h: float = 0.0
    volume_usd: float = 0.0
    bid_size: float = 0.0
    ask_size: float = 0.0

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
            "mark_iv": self.mark_iv,
            "open_interest": self.open_interest,
            "volume_24h": self.volume_24h,
            "volume_usd": self.volume_usd,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
        }


class DeribitOptionsChainFetcher:
    """Fetch all option tickers for a currency via Deribit REST API."""

    def __init__(
        self,
        ws_engine: Optional["DeribitOptionsQuoteEngine"] = None,  # noqa: F821
    ) -> None:
        self.base_url = "https://www.deribit.com/api/v2"
        self.session = requests.Session()
        self._ws_engine = ws_engine

    def fetch_option_chain(self, currency: str) -> List[Dict]:
        """Fetch option chain summary for a currency, enriched with top-of-book sizes.

        Endpoint: public/get_book_summary_by_currency returns most fields
        (mid_price, bid_price, ask_price, mark_iv, open_interest, volume,
        volume_usd) but NOT bid_size/ask_size. For near-ATM instruments we
        additionally call public/get_order_book (parallel, depth=1) and
        attach ``bid_size``/``ask_size`` to each chain entry. Deep OTM /
        illiquid options get 0.

        Returns list of dicts with all summary fields plus ``bid_size`` and
        ``ask_size``.
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

            chain = data.get("result", [])
            if not chain:
                return []

            self._enrich_with_top_of_book_sizes(chain)

            logger.info(
                f"Deribit {currency} option chain: {len(chain)} instruments"
            )
            return chain

        except Exception as e:
            logger.warning(f"Deribit option chain fetch failed for {currency}: {e}")
            return []

    def fetch_usdc_option_chains(self) -> Dict[str, List[Dict]]:
        """Fetch all USDC-settled linear option chains, grouped by base currency.

        Returns ``{"BTC": [...], "ETH": [...], "SOL": [...]}`` where each
        entry uses the ``BTC_USDC`` family naming so ``compute_batch`` sees
        ``_currency="BTC_USDC"``.
        """
        try:
            resp = self.session.get(
                f"{self.base_url}/public/get_book_summary_by_currency",
                params={"currency": "USDC", "kind": "option"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("error"):
                logger.warning(f"Deribit USDC option chain error: {data['error']}")
                return {}

            chain = data.get("result", [])
            if not chain:
                return {}

            self._enrich_with_top_of_book_sizes(chain)

            _WANTED = frozenset({"BTC_USDC", "ETH_USDC", "SOL_USDC"})
            grouped: Dict[str, List[Dict]] = {}
            for entry in chain:
                name = entry.get("instrument_name", "")
                parts = name.split("-", 1)
                base = parts[0] if parts else "UNKNOWN"
                if base not in _WANTED:
                    continue
                grouped.setdefault(base, []).append(entry)

            logger.info(
                f"Deribit USDC option chains: {len(chain)} instruments total, "
                f"kept {sum(len(v) for v in grouped.values())} "
                f"across {list(grouped.keys())}"
            )
            return grouped

        except Exception as e:
            logger.warning(f"Deribit USDC option chain fetch failed: {e}")
            return {}

    def _enrich_with_top_of_book_sizes(self, chain: List[Dict]) -> None:
        """Attach bid_size/ask_size to chain entries in-place.

        When a WS engine is wired in (production), sizes come from the
        ``quote.{instrument}`` subscription for every chain member — full
        coverage instead of the REST ATM-only subset. Without a WS engine
        we fall back to ``public/get_order_book`` for near-ATM instruments
        only (rate-limit budget ~40 calls / 5s cadence).
        """
        for entry in chain:
            entry.setdefault("bid_size", 0.0)
            entry.setdefault("ask_size", 0.0)

        if self._ws_engine is not None:
            names = [
                e.get("instrument_name") for e in chain
                if e.get("instrument_name")
            ]
            sizes = self._ws_engine.get_sizes(names)
            # Per-strike bid/ask IV from the ticker-subscribed near-term ATM
            # subset. Instruments outside that subset keep bid_iv/ask_iv = 0
            # (only mark_iv is available for them, from the batch endpoint).
            ivs = self._ws_engine.get_ivs(names) if hasattr(self._ws_engine, "get_ivs") else {}
            # Deribit-published greeks (same ticker subset) — coexist with our
            # BS-computed greeks for cross-validation / margin-consistent risk.
            dgs = self._ws_engine.get_deribit_greeks(names) if hasattr(self._ws_engine, "get_deribit_greeks") else {}
            for entry in chain:
                n = entry.get("instrument_name")
                if n in sizes:
                    entry["bid_size"], entry["ask_size"] = sizes[n]
                if n in ivs:
                    bid_iv, ask_iv, _mark_iv = ivs[n]
                    if bid_iv is not None:
                        entry["bid_iv"] = bid_iv
                    if ask_iv is not None:
                        entry["ask_iv"] = ask_iv
                if n in dgs:
                    dd, dg, dv, dt, dr = dgs[n]
                    entry["deribit_delta"] = dd if dd is not None else float("nan")
                    entry["deribit_gamma"] = dg if dg is not None else float("nan")
                    entry["deribit_vega"] = dv if dv is not None else float("nan")
                    entry["deribit_theta"] = dt if dt is not None else float("nan")
                    entry["deribit_rho"] = dr if dr is not None else float("nan")
            return

        underlying = self._median_underlying(chain)
        if not underlying or underlying <= 0:
            return

        # Rank by moneyness, take the ATM subset
        ranked = sorted(
            chain,
            key=lambda e: abs(
                self._strike_from_name(e.get("instrument_name", "")) - underlying
            ) / underlying if underlying else float("inf"),
        )
        atm_subset = []
        for entry in ranked:
            if len(atm_subset) >= _ORDER_BOOK_ATM_LIMIT:
                break
            strike = self._strike_from_name(entry.get("instrument_name", ""))
            if strike <= 0:
                continue
            moneyness = abs(strike - underlying) / underlying
            if moneyness > _ORDER_BOOK_ATM_MONEYNESS:
                break
            atm_subset.append(entry["instrument_name"])

        sizes_map = self.fetch_top_of_book_sizes(atm_subset)
        for entry in chain:
            name = entry.get("instrument_name")
            if name in sizes_map:
                bid_size, ask_size = sizes_map[name]
                entry["bid_size"] = bid_size
                entry["ask_size"] = ask_size

    @staticmethod
    def _median_underlying(chain: List[Dict]) -> float:
        prices = [
            float(e["underlying_price"])
            for e in chain
            if e.get("underlying_price") not in (None, 0, "", "0")
        ]
        if not prices:
            return 0.0
        prices.sort()
        n = len(prices)
        return prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) / 2.0

    @staticmethod
    def _strike_from_name(name: str) -> float:
        parts = name.split("-")
        if len(parts) < 3:
            return 0.0
        try:
            return float(parts[2])
        except (ValueError, IndexError):
            return 0.0

    def fetch_top_of_book_sizes(
        self,
        instrument_names: List[str],
    ) -> Dict[str, Tuple[float, float]]:
        """Fetch top-of-book bid/ask sizes for a subset of instruments.

        Uses public/get_order_book with depth=1. Parallelized via a thread
        pool to stay within the 5s cadence budget. Returns a mapping
        ``{instrument_name: (bid_size, ask_size)}``. Missing or failed
        entries are omitted; callers should treat absence as 0.
        """
        if not instrument_names:
            return {}

        out: Dict[str, Tuple[float, float]] = {}

        def _fetch_one(name: str) -> Tuple[str, Optional[Tuple[float, float]]]:
            try:
                resp = self.session.get(
                    f"{self.base_url}/public/get_order_book",
                    params={"instrument_name": name, "depth": 1},
                    timeout=5,
                )
                resp.raise_for_status()
                data = resp.json()
                if data.get("error"):
                    return name, None
                result = data.get("result", {})
                bids = result.get("bids") or []
                asks = result.get("asks") or []
                bid_size = float(bids[0][1]) if bids else 0.0
                ask_size = float(asks[0][1]) if asks else 0.0
                return name, (bid_size, ask_size)
            except Exception:
                return name, None

        with ThreadPoolExecutor(max_workers=_ORDER_BOOK_WORKERS) as pool:
            futures = {pool.submit(_fetch_one, n): n for n in instrument_names}
            for fut in as_completed(futures):
                name, sizes = fut.result()
                if sizes is not None:
                    out[name] = sizes

        return out


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
        Example: 'BTC-9MAY26-100000-C' or 'BTC_USDC-9MAY26-100000-C' (linear)
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

        # Fill optional columns (also ensures bid_size/ask_size/deribit_* exist,
        # so the later .values extraction never hits a missing column).
        for col in ["mid_price", "underlying_price", "mark_iv", "bid_iv", "ask_iv",
                     "bid_size", "ask_size",
                     "deribit_delta", "deribit_gamma", "deribit_vega",
                     "deribit_theta", "deribit_rho",
                     "open_interest", "volume", "volume_usd"]:
            if col not in df.columns:
                df[col] = np.nan

        # ── Step 2: Vectorized instrument name parsing ──
        extracted = df["instrument_name"].str.extract(
            r"^([A-Z_]+)-(\d{1,2}[A-Z]{3}\d{2})-(\d+)-([CP])$"
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
        # CRITICAL: Deribit mark_iv/bid_iv/ask_iv are in PERCENT (e.g. 55.0 = 55%).
        # BS formula requires sigma as a decimal (0.55). Dividing by 100 here
        # fixes the prior bug where delta collapsed to 0/1 and gamma→0.
        S = df["underlying_price"].values.astype(np.float64)
        K = df["_strike"].values.astype(np.float64)
        T = df["time_to_expiry_years"].values.astype(np.float64)
        sigma = iv.astype(np.float64) / 100.0
        is_call = (df["_option_type"] == "C").values

        # Clamp T to >= 1 hour to prevent gamma→∞ near expiry. The prior
        # 1e-10 clamp was too small: gamma ~ 1/(S*sigma*sqrt(T)) blows up
        # as T→0, producing nonsensical values for 0-1DTE options.
        T = np.maximum(T, _MIN_T_CLAMP_YEARS)

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

        # ── Step 9: Extract bid_size/ask_size + OI/volume (T4, T5) ──
        # The chain fetcher already enriched near-ATM instruments with
        # top-of-book sizes via public/get_order_book. Deep OTM entries
        # carry 0. open_interest / volume / volume_usd come directly from
        # get_book_summary_by_currency.
        bid_size = pd.to_numeric(df.get("bid_size", 0), errors="coerce").fillna(0).values
        ask_size = pd.to_numeric(df.get("ask_size", 0), errors="coerce").fillna(0).values
        open_interest = pd.to_numeric(df.get("open_interest", 0), errors="coerce").fillna(0).values
        volume_24h = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).values
        volume_usd = pd.to_numeric(df.get("volume_usd", 0), errors="coerce").fillna(0).values

        # ── Step 10: Assemble output DataFrame ──
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
            "mark_iv": mark_iv,
            "bid_iv": bid_iv,
            "ask_iv": ask_iv,
            "iv_source": iv_source,
            "delta": delta,
            "gamma": gamma,
            "vega": vega,
            "theta": theta,
            "rho": rho,
            "mid_price": mid_price.values,
            "bid_price": bid[mask],
            "ask_price": ask[mask],
            "bid_size": bid_size,
            "ask_size": ask_size,
            # Deribit-published greeks (coexist with our BS greeks above).
            # 0 where not ticker-subscribed (far-DTE / deep-OTM).
            "deribit_delta": pd.to_numeric(df["deribit_delta"], errors="coerce").fillna(0).values,
            "deribit_gamma": pd.to_numeric(df["deribit_gamma"], errors="coerce").fillna(0).values,
            "deribit_vega": pd.to_numeric(df["deribit_vega"], errors="coerce").fillna(0).values,
            "deribit_theta": pd.to_numeric(df["deribit_theta"], errors="coerce").fillna(0).values,
            "deribit_rho": pd.to_numeric(df["deribit_rho"], errors="coerce").fillna(0).values,
            "open_interest": open_interest,
            "volume_24h": volume_24h,
            "volume_usd": volume_usd,
        })

        # ── Step 11: Cast to float32 ──
        float_cols = [
            "underlying_price", "strike", "time_to_expiry_years",
            "iv", "mark_iv", "bid_iv", "ask_iv", "delta", "gamma", "vega", "theta", "rho",
            "deribit_delta", "deribit_gamma", "deribit_vega", "deribit_theta", "deribit_rho",
            "mid_price", "bid_price", "ask_price",
            "bid_size", "ask_size", "open_interest", "volume_24h", "volume_usd",
        ]
        for col in float_cols:
            result[col] = result[col].astype(np.float32)

        # Cleanup intermediate arrays
        del d1, d2, pdf_d1, cdf_d1, cdf_d2, S, K, T, sigma
        del delta, gamma, vega, theta, rho, iv, bid, ask, strike, underlying, tte
        del mark_iv, bid_iv, ask_iv, mid_iv, mask, sqrt_T
        del bid_size, ask_size, open_interest, volume_24h, volume_usd, mid_price

        logger.info(
            f"GreeksProcessor: computed {len(result)} Greeks "
            f"(filtered from {len(chain_data)} instruments)"
        )

        return result
