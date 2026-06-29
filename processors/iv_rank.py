"""IV Rank computation and rolling-window tracking.

Shared logic used by both the real-time collector
(``deribit-options-data-collector``) and the live trading process so that
both compute identical numbers for the same input.

The collector writes ``iv_rank`` into ``data/deribit/vol_surface/`` parquet
files; the live process reads the same parquet to bootstrap its own window
and then updates per tick. Both call ``compute_rank`` /
``daily_representative_iv`` so a fix here propagates everywhere.

State persistence
-----------------

When the collector's ``data/`` directory is cleared (e.g., by
``pull_data.sh`` after a sync), an in-memory-only window would be lost on
the next process restart. Pass a ``state_file`` path to ``IVRankTracker``
and the window is:

1. Loaded from JSON on ``bootstrap_from_parquet`` (merged with parquet,
   state takes precedence for overlapping dates).
2. Appended on every UTC daily rollover inside ``update``.
3. Explicitly flushed via ``save_state`` on shutdown.

The state file lives outside ``data/`` (typically ``state/``) so it is
not removed by pull/cleanup scripts.
"""

from __future__ import annotations

import glob
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from utils import get_logger

logger = get_logger(__name__)


def compute_rank(current_iv: float, historical_ivs: pd.Series) -> float:
    """Percentile rank of ``current_iv`` within ``historical_ivs`` (0-100).

    Returns 50.0 when ``historical_ivs`` is empty or ``current_iv`` is
    invalid (NaN, <= 0). This matches the prior behavior of
    ``VolatilitySurfaceBuilder.compute_iv_rank`` so existing callers and
    tests are unaffected.
    """
    if historical_ivs is None or len(historical_ivs) == 0:
        return 50.0
    if current_iv is None or not (current_iv > 0):
        return 50.0
    count_below = int((historical_ivs <= current_iv).sum())
    return (count_below / len(historical_ivs)) * 100.0


def compute_rank_batch(
    current_ivs: "pd.Series | np.ndarray",
    historical_ivs: pd.Series,
) -> np.ndarray:
    """Vectorized rank for many ``current_ivs`` against one ``historical_ivs``.

    Same semantics as ``compute_rank`` applied element-wise, but uses
    ``np.searchsorted`` on a sorted copy of the history for ~100x speedup
    when back-filling thousands of ticks per file.

    Returns a ``float64`` numpy array of the same length as ``current_ivs``.
    Invalid entries (NaN or <= 0) and empty-history cases return 50.0.
    """
    ivs = np.asarray(current_ivs, dtype=np.float64)
    if historical_ivs is None or len(historical_ivs) == 0:
        return np.full(len(ivs), 50.0, dtype=np.float64)

    sorted_hist = np.sort(np.asarray(historical_ivs, dtype=np.float64))
    n = len(sorted_hist)

    out = np.full(len(ivs), 50.0, dtype=np.float64)
    valid = (~np.isnan(ivs)) & (ivs > 0)
    if valid.any():
        # side='right' -> index after equals -> count of elements <= x
        n_below = np.searchsorted(sorted_hist, ivs[valid], side="right")
        out[valid] = (n_below / n) * 100.0
    return out


def daily_representative_iv(day_df: pd.DataFrame) -> float:
    """Return the representative ATM IV for one UTC day.

    Uses the last valid observation of the day (analogous to a daily
    close). Rows with ``atm_iv <= 0`` are dropped before picking the
    latest by ``timestamp``.

    Returns NaN if the frame is empty or lacks the required columns.
    """
    if day_df is None or day_df.empty:
        return float("nan")
    if "atm_iv" not in day_df.columns or "timestamp" not in day_df.columns:
        return float("nan")
    valid = day_df[day_df["atm_iv"] > 0]
    if valid.empty:
        return float("nan")
    sorted_df = valid.sort_values("timestamp")
    return float(sorted_df["atm_iv"].iloc[-1])


class IVRankTracker:
    """Rolling window of daily representative ATM IVs.

    The window holds one IV per past UTC day (each day's last observed
    ``atm_iv``). On UTC rollover, the previous day's final IV is pushed
    into the window and the oldest day is evicted once ``lookback_days``
    is exceeded.

    ``rank(current_iv)`` returns the percentile of ``current_iv`` vs the
    window. The current day is NOT in the window — rank reflects where
    today's IV sits relative to past N days, with no look-ahead.

    Pass ``state_file`` to persist the window across process restarts
    (see module docstring). When unset, the tracker is purely in-memory.

    Thread-safety: instances are not thread-safe. The collector uses one
    instance per currency, accessed only from ``BasisVolProcessorThread``.
    The live process is expected to do the same.
    """

    def __init__(
        self,
        symbol: str,
        lookback_days: int = 252,
        state_file: Optional[str] = None,
    ) -> None:
        self._symbol = symbol
        self._lookback_days = lookback_days
        self._state_file = state_file
        # Oldest -> newest, (date_str, iv) tuples.
        self._history: List[Tuple[str, float]] = []
        self._current_day: Optional[str] = None
        self._current_day_iv: Optional[float] = None

    def bootstrap_from_parquet(
        self,
        data_dir: str,
        exchange: str = "deribit",
        data_type: str = "vol_surface",
    ) -> int:
        """Load historical daily representative IVs into the window.

        Sources are merged in this precedence (later overrides earlier
        for the same date):

        1. Parquet scan of ``{data_dir}/{exchange}/{data_type}/{symbol}_*.parquet``
        2. State file (if ``state_file`` was provided at construction)

        Today's entry is dropped from both sources (still being written).
        Result is sorted by date and capped to ``lookback_days``.

        Returns the number of days loaded.
        """
        parquet_daily = self._scan_parquet(data_dir, exchange, data_type)
        state_daily = self._load_state()

        merged: Dict[str, float] = {**parquet_daily, **state_daily}

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prior = sorted(
            (d, iv) for d, iv in merged.items() if d < today_str
        )
        if len(prior) > self._lookback_days:
            prior = prior[-self._lookback_days:]

        self._history = prior

        sources: List[str] = []
        if parquet_daily:
            sources.append(f"parquet={len(parquet_daily)}")
        if state_daily:
            sources.append(f"state={len(state_daily)}")
        src_str = ", ".join(sources) if sources else "empty"

        if prior:
            ivs = [iv for _, iv in prior]
            logger.info(
                "[IVRankTracker:%s] bootstrapped %d days (%s ~ %s) "
                "from %s, iv min=%.4f max=%.4f",
                self._symbol,
                len(prior),
                prior[0][0],
                prior[-1][0],
                src_str,
                min(ivs),
                max(ivs),
            )
        else:
            logger.info(
                "[IVRankTracker:%s] 0 historical days (sources: %s)",
                self._symbol,
                src_str,
            )
        return len(prior)

    def update(self, ts_ms: int, atm_iv: float) -> None:
        """Ingest a new ATM IV observation.

        On UTC daily rollover, the previous day's final IV is appended
        to history (and the window trimmed to ``lookback_days``). When
        a ``state_file`` is configured, the new day is also persisted.

        Invalid IV values (NaN, <= 0) are silently ignored so a bad tick
        never pollutes the window.
        """
        if atm_iv is None or not (atm_iv > 0):
            return

        day = self._utc_day_from_ms(ts_ms)

        if self._current_day is None:
            self._current_day = day
            self._current_day_iv = atm_iv
            return

        if day != self._current_day:
            if self._current_day_iv is not None:
                self._history.append((self._current_day, self._current_day_iv))
                if len(self._history) > self._lookback_days:
                    self._history = self._history[-self._lookback_days:]
                # Persist the freshly closed day. Best-effort: a failed
                # save does not roll back the in-memory update.
                try:
                    self._save_state()
                except Exception as e:
                    logger.warning(
                        "[IVRankTracker:%s] state save failed: %s",
                        self._symbol,
                        e,
                    )
            self._current_day = day
            self._current_day_iv = atm_iv
        else:
            self._current_day_iv = atm_iv

    def rank(self, current_iv: Optional[float] = None) -> float:
        """Percentile rank of ``current_iv`` vs the window (0-100).

        If ``current_iv`` is None, the most recent observed IV is used.
        Returns 50.0 when history is empty (insufficient data).
        """
        iv = current_iv if current_iv is not None else self._current_day_iv
        return compute_rank(iv, self.historical_ivs())

    def historical_ivs(self) -> pd.Series:
        """Return history IVs as a ``pd.Series`` for direct use by
        ``VolatilitySurfaceBuilder.build_surface``."""
        return pd.Series([iv for _, iv in self._history])

    def window_days(self) -> int:
        """Number of past days currently held in the window."""
        return len(self._history)

    def save_state(self) -> None:
        """Public hook for shutdown flushes. No-op without ``state_file``."""
        self._save_state()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _scan_parquet(
        self,
        data_dir: str,
        exchange: str,
        data_type: str,
    ) -> Dict[str, float]:
        """Read each past day's parquet -> ``{date_str: daily_iv}``.

        Today's file is skipped because the collector is still writing it.
        """
        pattern = os.path.join(
            data_dir, exchange, data_type, f"{self._symbol}_*.parquet"
        )
        files = sorted(glob.glob(pattern))
        if not files:
            return {}

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        prefix = f"{self._symbol}_"
        suffix = ".parquet"
        out: Dict[str, float] = {}

        for fp in files:
            fname = os.path.basename(fp)
            if not fname.startswith(prefix) or not fname.endswith(suffix):
                continue
            date_str = fname[len(prefix):-len(suffix)]
            if date_str >= today_str:
                continue

            try:
                df = pd.read_parquet(fp, columns=["timestamp", "atm_iv"])
            except Exception as e:
                logger.warning(
                    "[IVRankTracker:%s] skip %s: %s",
                    self._symbol,
                    fp,
                    e,
                )
                continue

            iv = daily_representative_iv(df)
            if iv > 0:
                out[date_str] = iv
        return out

    def _load_state(self) -> Dict[str, float]:
        """Read ``state_file`` if configured and present.

        Returns ``{date_str: iv}`` or ``{}`` on missing / invalid file.
        """
        if not self._state_file or not os.path.exists(self._state_file):
            return {}
        try:
            with open(self._state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning(
                "[IVRankTracker:%s] state load failed (%s): %s",
                self._symbol,
                self._state_file,
                e,
            )
            return {}

        out: Dict[str, float] = {}
        for entry in data.get("history", []):
            try:
                out[entry["date"]] = float(entry["iv"])
            except (KeyError, ValueError, TypeError):
                continue
        return out

    def _save_state(self) -> None:
        """Atomically write the window to ``state_file``.

        No-op when ``state_file`` is unset. Writes to a ``.tmp`` sibling
        first and uses ``os.replace`` so a crash mid-write leaves the
        previous file intact.
        """
        if not self._state_file:
            return

        payload = {
            "symbol": self._symbol,
            "lookback_days": self._lookback_days,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "history": [
                {"date": d, "iv": float(iv)} for d, iv in self._history
            ],
        }

        parent = os.path.dirname(self._state_file) or "."
        os.makedirs(parent, exist_ok=True)
        tmp = self._state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, self._state_file)

    @staticmethod
    def _utc_day_from_ms(ts_ms: int) -> str:
        return datetime.fromtimestamp(
            ts_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")
