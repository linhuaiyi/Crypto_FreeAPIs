"""Data gap detector for time series."""

from dataclasses import dataclass
from typing import List

import pandas as pd

from utils import get_logger

logger = get_logger("GapDetector")


@dataclass(frozen=True)
class Gap:
    """Represents a detected gap in time series data."""

    gap_start: int
    gap_end: int
    gap_duration_ms: int
    affected_instruments: List[str]


class GapDetector:
    """Detects and fills gaps in timestamped DataFrames."""

    def detect(
        self,
        df: pd.DataFrame,
        threshold_ms: int = 60000,
        time_col: str = "timestamp",
    ) -> List[Gap]:
        """Sort by time_col and return gaps where consecutive diffs exceed threshold_ms."""
        if df.empty:
            return []

        sorted_df = df.sort_values(time_col).reset_index(drop=True)
        times = sorted_df[time_col].values
        diffs = times[1:] - times[:-1]

        gap_indices = [i for i, d in enumerate(diffs) if d > threshold_ms]
        instrument_col = "instrument" if "instrument" in sorted_df.columns else None

        gaps: List[Gap] = []
        for idx in gap_indices:
            affected = (
                sorted_df.iloc[idx : idx + 2]["instrument"].unique().tolist()
                if instrument_col
                else []
            )
            gaps.append(
                Gap(
                    gap_start=int(times[idx]),
                    gap_end=int(times[idx + 1]),
                    gap_duration_ms=int(diffs[idx]),
                    affected_instruments=affected,
                )
            )

        logger.info("Detected %d gap(s) above %d ms threshold", len(gaps), threshold_ms)
        return gaps

    def fill_gaps(self, df: pd.DataFrame, method: str = "ffill") -> pd.DataFrame:
        """Fill gaps using forward fill for OHLCV columns, linear interpolation otherwise."""
        if df.empty:
            return df.copy()

        ohlcv_cols = {"open", "high", "low", "close", "volume"}
        ohlcv_present = [c for c in df.columns if c in ohlcv_cols]
        numeric_cols = [
            c for c in df.select_dtypes(include="number").columns
            if c not in ohlcv_present
        ]

        result = df.copy()
        if ohlcv_present:
            result[ohlcv_present] = result[ohlcv_present].ffill()
        if numeric_cols:
            result[numeric_cols] = result[numeric_cols].interpolate(method="linear")

        logger.info("Filled gaps using method=%s", method)
        return result
