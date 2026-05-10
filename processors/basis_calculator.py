"""Basis calculator for spot-perp, synthetic, and cross-exchange relationships."""

from __future__ import annotations

from dataclasses import dataclass
from math import exp
from typing import List

import pandas as pd

from utils import get_logger

logger = get_logger(__name__)

DAYS_PER_YEAR = 365


@dataclass(frozen=True)
class BasisPoint:
    """Single basis measurement snapshot."""

    timestamp: int
    symbol: str
    basis_type: str
    spot_price: float
    perp_price: float
    basis: float
    basis_pct: float
    annualized_basis: float
    days_to_expiry: int

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "basis_type": self.basis_type,
            "spot_price": self.spot_price,
            "perp_price": self.perp_price,
            "basis": self.basis,
            "basis_pct": self.basis_pct,
            "annualized_basis": self.annualized_basis,
            "days_to_expiry": self.days_to_expiry,
        }


class BasisCalculator:
    """Computes basis across spot-perp, synthetic, and cross-exchange pairs."""

    def calc_spot_perp(
        self,
        spot_df: pd.DataFrame,
        perp_df: pd.DataFrame,
        symbol: str,
        days_to_expiry: int = DAYS_PER_YEAR,
    ) -> List[BasisPoint]:
        """Calculate spot-perpetual basis over aligned timestamps.

        Parameters
        ----------
        spot_df : pd.DataFrame
            Must contain ``timestamp`` and ``price`` columns.
        perp_df : pd.DataFrame
            Must contain ``timestamp`` and ``price`` columns.
        symbol : str
            Trading pair symbol (e.g. ``"BTC_USDT"``).
        days_to_expiry : int
            Notional days to expiry for annualization. Defaults to 365 for perps.
        """
        merged = pd.merge_asof(
            spot_df.sort_values("timestamp"),
            perp_df[["timestamp", "price"]].sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            suffixes=("_spot", "_perp"),
        )

        results: List[BasisPoint] = []
        for row in merged.itertuples(index=False):
            spot_price = float(row.price_spot)
            perp_price = float(row.price_perp)
            basis = perp_price - spot_price
            basis_pct = (basis / spot_price * 100.0) if spot_price != 0 else 0.0
            annualized = basis_pct * (DAYS_PER_YEAR / days_to_expiry)

            results.append(
                BasisPoint(
                    timestamp=int(row.timestamp),
                    symbol=symbol,
                    basis_type="spot_perp",
                    spot_price=spot_price,
                    perp_price=perp_price,
                    basis=basis,
                    basis_pct=basis_pct,
                    annualized_basis=annualized,
                    days_to_expiry=days_to_expiry,
                )
            )

        logger.info("Calculated %d spot-perp basis points for %s", len(results), symbol)
        return results

    def calc_synthetic(
        self,
        call_mid: float,
        put_mid: float,
        strike: float,
        perp_price: float,
        rate: float,
        days_to_expiry: int,
    ) -> BasisPoint:
        """Compute synthetic futures basis via put-call parity.

        synthetic_long = call_mid - put_mid + strike * exp(-rate * dte / 365)
        basis = perp_price - synthetic_long
        """
        dte_fraction = days_to_expiry / DAYS_PER_YEAR
        synthetic_long = call_mid - put_mid + strike * exp(-rate * dte_fraction)

        basis = perp_price - synthetic_long
        basis_pct = (basis / perp_price * 100.0) if perp_price != 0 else 0.0
        annualized = basis_pct * (DAYS_PER_YEAR / days_to_expiry) if days_to_expiry > 0 else 0.0

        point = BasisPoint(
            timestamp=0,
            symbol="SYNTHETIC",
            basis_type="synthetic",
            spot_price=synthetic_long,
            perp_price=perp_price,
            basis=basis,
            basis_pct=basis_pct,
            annualized_basis=annualized,
            days_to_expiry=days_to_expiry,
        )
        logger.debug(
            "Synthetic basis: call=%.4f put=%.4f strike=%.2f -> basis=%.4f (%.4f%%)",
            call_mid,
            put_mid,
            strike,
            basis,
            basis_pct,
        )
        return point

    def calc_cross_exchange(
        self,
        perp_a_df: pd.DataFrame,
        perp_b_df: pd.DataFrame,
        symbol: str = "CROSS_EXCHANGE",
        days_to_expiry: int = DAYS_PER_YEAR,
    ) -> List[BasisPoint]:
        """Calculate perp price difference between two exchanges.

        Parameters
        ----------
        perp_a_df : pd.DataFrame
            Exchange A perps with ``timestamp`` and ``price`` columns.
        perp_b_df : pd.DataFrame
            Exchange B perps with ``timestamp`` and ``price`` columns.
        symbol : str
            Label for the pair.
        days_to_expiry : int
            Notional days for annualization. Defaults to 365 for perps.
        """
        merged = pd.merge_asof(
            perp_a_df.sort_values("timestamp"),
            perp_b_df[["timestamp", "price"]].sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            suffixes=("_a", "_b"),
        )

        results: List[BasisPoint] = []
        for row in merged.itertuples(index=False):
            price_a = float(row.price_a)
            price_b = float(row.price_b)
            basis = price_a - price_b
            basis_pct = (basis / price_b * 100.0) if price_b != 0 else 0.0
            annualized = basis_pct * (DAYS_PER_YEAR / days_to_expiry)

            results.append(
                BasisPoint(
                    timestamp=int(row.timestamp),
                    symbol=symbol,
                    basis_type="cross_exchange",
                    spot_price=price_b,
                    perp_price=price_a,
                    basis=basis,
                    basis_pct=basis_pct,
                    annualized_basis=annualized,
                    days_to_expiry=days_to_expiry,
                )
            )

        logger.info("Calculated %d cross-exchange basis points for %s", len(results), symbol)
        return results
