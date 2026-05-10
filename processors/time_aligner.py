"""Time alignment via pandas merge_asof with _age_ms freshness markers."""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from utils import get_logger

logger = get_logger(__name__)


class TimeAligner:
    """Aligns asynchronously sampled data streams onto a common timeline.

    Uses ``pd.merge_asof`` to join data sources onto a target time index and
    annotates each aligned column with an ``_age_ms`` freshness marker that
    records how stale the aligned value is in milliseconds.
    """

    def align_to_target(
        self,
        target_df: pd.DataFrame,
        source_df: pd.DataFrame,
        on: str = "timestamp",
        direction: str = "backward",
        tolerance: int = 60000,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Join *source_df* columns onto *target_df* using merge_asof.

        Parameters
        ----------
        target_df : pd.DataFrame
            Base timeline to align onto. Must contain the ``on`` column.
        source_df : pd.DataFrame
            Data source to merge. Must contain the ``on`` column.
        on : str
            Timestamp column name used for alignment.
        direction : str
            merge_asof direction: ``"backward"``, ``"forward"``, or ``"nearest"``.
        tolerance : int
            Maximum staleness in milliseconds.
        columns : list of str, optional
            Subset of columns to align. If ``None``, aligns all source columns
            except the ``on`` column.

        Returns
        -------
        pd.DataFrame
            Target DataFrame with aligned source columns and corresponding
            ``_age_ms`` columns.
        """
        target_sorted = target_df.sort_values(on).copy()
        source_sorted = source_df.sort_values(on).copy()

        align_cols = columns if columns is not None else [
            c for c in source_sorted.columns if c != on
        ]

        subset = source_sorted[[on] + align_cols].copy()
        merged = pd.merge_asof(
            target_sorted,
            subset,
            on=on,
            direction=direction,
            tolerance=tolerance,
        )

        # Compute _age_ms: time difference between target and matched source timestamp
        source_ts_aligned = pd.merge_asof(
            target_sorted[[on]].rename(columns={on: "_target_ts"}),
            source_sorted[[on]].rename(columns={on: "_source_ts"}),
            left_on="_target_ts",
            right_on="_source_ts",
            direction=direction,
            tolerance=tolerance,
        )["_source_ts"]

        for col in align_cols:
            age_col = f"{col}_age_ms"
            merged[age_col] = (merged[on] - source_ts_aligned.values).astype("Int64")

        logger.debug(
            "Aligned %d columns with tolerance=%dms direction=%s",
            len(align_cols),
            tolerance,
            direction,
        )
        return merged

    def build_strategy_slice(
        self,
        base_timestamps: pd.Series,
        data_sources: Dict[str, pd.DataFrame],
        tolerance_ms: Optional[Dict[str, int]] = None,
    ) -> pd.DataFrame:
        """Build a wide DataFrame by merging multiple sources onto base timestamps.

        Parameters
        ----------
        base_timestamps : pd.Series
            Target timestamps for the strategy slice.
        data_sources : dict
            Mapping of source name to DataFrame. Each must contain a
            ``timestamp`` column.
        tolerance_ms : dict, optional
            Per-source tolerance overrides. Sources not listed default to 60000ms.

        Returns
        -------
        pd.DataFrame
            Wide DataFrame with one row per base timestamp and all source
            columns aligned, plus ``_age_ms`` freshness markers.
        """
        if tolerance_ms is None:
            tolerance_ms = {}

        base_df = pd.DataFrame({"timestamp": base_timestamps.values})
        result = base_df.copy()

        for source_name, source_df in data_sources.items():
            tol = tolerance_ms.get(source_name, 60000)
            source_cols = [c for c in source_df.columns if c != "timestamp"]

            source_sorted = source_df.sort_values("timestamp")
            merged = pd.merge_asof(
                result.sort_values("timestamp"),
                source_sorted,
                on="timestamp",
                direction="backward",
                tolerance=tol,
            )

            # Compute _age_ms freshness for each column from this source
            source_ts_aligned = pd.merge_asof(
                result.sort_values("timestamp")[["timestamp"]].rename(
                    columns={"timestamp": "_target_ts"}
                ),
                source_sorted[["timestamp"]].rename(
                    columns={"timestamp": "_source_ts"}
                ),
                left_on="_target_ts",
                right_on="_source_ts",
                direction="backward",
                tolerance=tol,
            )["_source_ts"]

            for col in source_cols:
                age_col = f"{col}_age_ms"
                merged[age_col] = (
                    merged["timestamp"] - source_ts_aligned.values
                ).astype("Int64")

            result = merged
            logger.debug(
                "Merged source '%s' (%d cols, tolerance=%dms)",
                source_name,
                len(source_cols),
                tol,
            )

        logger.info(
            "Strategy slice built: %d rows, %d columns from %d sources",
            len(result),
            len(result.columns),
            len(data_sources),
        )
        return result
