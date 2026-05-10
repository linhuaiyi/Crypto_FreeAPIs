"""Volatility surface builder for options data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

from utils import get_logger

logger = get_logger(__name__)

# ATM search widening thresholds (fraction of underlying price)
_ATM_TIGHT = 0.05       # 5% moneyness — ideal
_ATM_MEDIUM = 0.15      # 15% — acceptable
_ATM_WIDE = 0.50        # 50% — last resort


@dataclass(frozen=True)
class VolSurfacePoint:
    """Single snapshot of the volatility surface for a symbol."""

    timestamp: int
    symbol: str
    atm_iv: float
    skew_25d: float
    butterfly_25d: float
    iv_rank: float
    term_structure: Dict[str, float]
    quality: str = "good"  # "good", "degraded", "estimated"


class VolatilitySurfaceBuilder:
    """Builds volatility surface metrics from options chain data.

    Parameters
    ----------
    lookback_days : int
        Number of trading days used for historical IV rank calculation.
    """

    def __init__(self, lookback_days: int = 252) -> None:
        self._lookback_days = lookback_days

    def _filter_near_atm(
        self, options_df: pd.DataFrame, underlying_price: float,
    ) -> pd.DataFrame:
        """Filter options near ATM with progressive widening.

        Tries tight (5%), then medium (15%), then wide (50%) moneyness.
        Returns the tightest band that has at least 1 option with valid IV.
        """
        if options_df.empty or underlying_price <= 0:
            return options_df

        iv_col = options_df["iv"] if "iv" in options_df.columns else None
        for threshold, label in [
            (_ATM_TIGHT, "tight"),
            (_ATM_MEDIUM, "medium"),
            (_ATM_WIDE, "wide"),
        ]:
            moneyness = (options_df["strike"] - underlying_price).abs() / underlying_price
            near = options_df[moneyness <= threshold]
            if iv_col is not None:
                valid = near[near["iv"] > 0]
                if not valid.empty:
                    if label != "tight":
                        logger.debug(
                            "ATM search widened to %s (%d options)", label, len(valid)
                        )
                    return valid
            elif not near.empty:
                return near

        return options_df

    def build_atm_iv(self, options_df: pd.DataFrame) -> float:
        """Return the implied volatility of the option closest to at-the-money.

        Expects columns: ``strike``, ``underlying_price``, ``iv``.
        """
        if options_df.empty:
            logger.warning("Empty options DataFrame passed to build_atm_iv")
            return 0.0

        strike_diff = (options_df["strike"] - options_df["underlying_price"]).abs()
        atm_idx = strike_diff.idxmin()
        atm_iv = float(options_df.loc[atm_idx, "iv"])
        logger.debug("ATM IV=%.4f at strike=%.2f", atm_iv, options_df.loc[atm_idx, "strike"])
        return atm_iv

    def build_skew(self, options_df: pd.DataFrame) -> float:
        """Compute 25-delta risk reversal: 25d Call IV minus 25d Put IV.

        Expects columns: ``delta``, ``iv``.
        Approximate by finding options whose delta is closest to +0.25 and -0.25.
        """
        if options_df.empty or "delta" not in options_df.columns:
            logger.warning("Cannot compute skew: missing delta column or empty DataFrame")
            return 0.0

        call_25d_idx = (options_df["delta"] - 0.25).abs().idxmin()
        put_25d_idx = (options_df["delta"] - (-0.25)).abs().idxmin()

        call_iv = float(options_df.loc[call_25d_idx, "iv"])
        put_iv = float(options_df.loc[put_25d_idx, "iv"])

        skew = call_iv - put_iv
        logger.debug("Skew 25d=%.4f (call_iv=%.4f, put_iv=%.4f)", skew, call_iv, put_iv)
        return skew

    def build_butterfly(self, options_df: pd.DataFrame) -> float:
        """Compute 25-delta butterfly: (25d Call IV + 25d Put IV) / 2 - ATM IV.

        Expects columns: ``delta``, ``iv``.
        """
        if options_df.empty or "delta" not in options_df.columns:
            logger.warning("Cannot compute butterfly: missing delta column or empty DataFrame")
            return 0.0

        call_25d_idx = (options_df["delta"] - 0.25).abs().idxmin()
        put_25d_idx = (options_df["delta"] - (-0.25)).abs().idxmin()

        call_iv = float(options_df.loc[call_25d_idx, "iv"])
        put_iv = float(options_df.loc[put_25d_idx, "iv"])
        atm_iv = self.build_atm_iv(options_df)

        butterfly = (call_iv + put_iv) / 2.0 - atm_iv
        logger.debug("Butterfly 25d=%.4f", butterfly)
        return butterfly

    def build_term_structure(self, options_df: pd.DataFrame) -> Dict[str, float]:
        """Group by expiry and compute ATM IV per expiry.

        Expects columns: ``expiry``, ``strike``, ``underlying_price``, ``iv``.
        Returns a dict mapping expiry date string to ATM IV.
        """
        if options_df.empty:
            logger.warning("Empty options DataFrame passed to build_term_structure")
            return {}

        term_structure: Dict[str, float] = {}
        for expiry, group in options_df.groupby("expiry"):
            strike_diff = (group["strike"] - group["underlying_price"]).abs()
            atm_idx = strike_diff.idxmin()
            term_structure[str(expiry)] = float(group.loc[atm_idx, "iv"])

        logger.debug("Term structure built for %d expiries", len(term_structure))
        return term_structure

    def compute_iv_rank(self, current_iv: float, historical_ivs: pd.Series) -> float:
        """Percentile rank of *current_iv* within *historical_ivs* (0-100)."""
        if historical_ivs.empty:
            logger.warning("Empty historical IV series passed to compute_iv_rank")
            return 50.0

        count_below = int((historical_ivs <= current_iv).sum())
        iv_rank = (count_below / len(historical_ivs)) * 100.0
        logger.debug("IV rank=%.1f (current_iv=%.4f)", iv_rank, current_iv)
        return iv_rank

    def build_surface(
        self,
        options_df: pd.DataFrame,
        underlying_price: float,
        historical_ivs: pd.Series | None = None,
        symbol: str = "UNKNOWN",
    ) -> VolSurfacePoint:
        """Orchestrate all surface-building methods into a single VolSurfacePoint.

        Never returns None. If data is sparse or fitting quality is poor,
        returns estimated values with quality="degraded" or "estimated".

        Parameters
        ----------
        options_df : pd.DataFrame
            Options chain data. Must include ``strike``, ``iv``, ``delta``,
            ``expiry``, and optionally ``underlying_price``.
        underlying_price : float
            Current underlying price; overrides any column value.
        historical_ivs : pd.Series, optional
            Historical ATM IV series for IV rank calculation.
        symbol : str
            Ticker symbol to embed in the result.
        """
        quality = "good"
        enriched = options_df.copy()
        enriched["underlying_price"] = underlying_price

        # Use widened ATM filter for robustness
        atm_subset = self._filter_near_atm(enriched, underlying_price)

        if atm_subset.empty:
            atm_subset = enriched
            quality = "estimated"

        atm_iv = self.build_atm_iv(atm_subset)

        # Skew / butterfly use full dataset for wider delta coverage
        skew = self.build_skew(enriched)
        butterfly = self.build_butterfly(enriched)
        term_structure = self.build_term_structure(enriched)

        if historical_ivs is not None and not historical_ivs.empty:
            iv_rank = self.compute_iv_rank(atm_iv, historical_ivs)
        else:
            iv_rank = 50.0
            logger.debug("No historical IVs provided; defaulting iv_rank to 50.0")

        # Degrade quality if key metrics are zero (likely insufficient data)
        if atm_iv == 0.0 and skew == 0.0 and butterfly == 0.0:
            quality = "estimated"

        timestamp = int(enriched["timestamp"].max()) if "timestamp" in enriched.columns else 0

        point = VolSurfacePoint(
            timestamp=timestamp,
            symbol=symbol,
            atm_iv=atm_iv,
            skew_25d=skew,
            butterfly_25d=butterfly,
            iv_rank=iv_rank,
            term_structure=term_structure,
            quality=quality,
        )
        logger.info(
            "Surface built for %s [%s]: atm_iv=%.4f skew=%.4f butterfly=%.4f iv_rank=%.1f",
            symbol,
            quality,
            atm_iv,
            skew,
            butterfly,
            iv_rank,
        )
        return point
