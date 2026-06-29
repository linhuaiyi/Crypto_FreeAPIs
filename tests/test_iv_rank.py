"""Tests for processors/iv_rank.py"""

from __future__ import annotations

import os
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

from processors.iv_rank import (
    IVRankTracker,
    compute_rank,
    daily_representative_iv,
)


# ---------------------------------------------------------------------------
# compute_rank
# ---------------------------------------------------------------------------


class TestComputeRank:
    def test_correct_percentile(self):
        historical = pd.Series([0.10, 0.15, 0.20, 0.25, 0.30])
        # 3 values <= 0.20 -> 60%
        assert compute_rank(0.20, historical) == pytest.approx(60.0)

    def test_lowest_iv_returns_zero(self):
        historical = pd.Series([0.20, 0.25, 0.30])
        # nothing below 0.10
        assert compute_rank(0.10, historical) == pytest.approx(0.0)

    def test_highest_iv_returns_100(self):
        historical = pd.Series([0.20, 0.25, 0.30])
        # everything <= 0.40
        assert compute_rank(0.40, historical) == pytest.approx(100.0)

    def test_empty_series_returns_50(self):
        assert compute_rank(0.25, pd.Series(dtype=float)) == 50.0

    def test_none_series_returns_50(self):
        assert compute_rank(0.25, None) == 50.0

    def test_invalid_current_iv_returns_50(self):
        historical = pd.Series([0.20, 0.25, 0.30])
        assert compute_rank(0.0, historical) == 50.0
        assert compute_rank(-0.1, historical) == 50.0
        assert compute_rank(float("nan"), historical) == 50.0
        assert compute_rank(None, historical) == 50.0

    def test_min_of_history_is_not_zero(self):
        # percentile of the min value is 1/N, not 0
        historical = pd.Series([0.10, 0.20, 0.30])
        assert compute_rank(0.10, historical) == pytest.approx(100.0 / 3)


# ---------------------------------------------------------------------------
# compute_rank_batch
# ---------------------------------------------------------------------------


class TestComputeRankBatch:
    def test_matches_scalar_version(self):
        from processors.iv_rank import compute_rank_batch

        historical = pd.Series([0.10, 0.15, 0.20, 0.25, 0.30])
        current = pd.Series([0.05, 0.18, 0.20, 0.30, 0.40])
        expected = [compute_rank(v, historical) for v in current]
        out = compute_rank_batch(current, historical)
        assert np.allclose(out, expected)

    def test_empty_history_returns_all_50(self):
        from processors.iv_rank import compute_rank_batch

        out = compute_rank_batch(
            pd.Series([0.10, 0.20, 0.30]), pd.Series(dtype=float)
        )
        assert np.allclose(out, [50.0, 50.0, 50.0])

    def test_invalid_ivs_return_50(self):
        from processors.iv_rank import compute_rank_batch

        historical = pd.Series([0.20, 0.30])
        out = compute_rank_batch(
            pd.Series([0.0, -0.1, float("nan"), None, 0.40]),
            historical,
        )
        # last value (0.40) is the only valid one; 2/2 <= 0.40 -> 100
        assert np.allclose(out, [50.0, 50.0, 50.0, 50.0, 100.0])

    def test_accepts_numpy_array(self):
        from processors.iv_rank import compute_rank_batch

        historical = pd.Series([0.20, 0.30])
        out = compute_rank_batch(np.array([0.10, 0.40]), historical)
        # 0/2 <= 0.10 -> 0; 2/2 <= 0.40 -> 100
        assert np.allclose(out, [0.0, 100.0])

    def test_empty_current_returns_empty(self):
        from processors.iv_rank import compute_rank_batch

        out = compute_rank_batch(np.array([]), pd.Series([0.20, 0.30]))
        assert len(out) == 0


# ---------------------------------------------------------------------------
# daily_representative_iv
# ---------------------------------------------------------------------------


class TestDailyRepresentativeIv:
    def test_picks_last_by_timestamp(self):
        df = pd.DataFrame({
            "timestamp": [100, 200, 300],
            "atm_iv": [0.30, 0.25, 0.28],
        })
        assert daily_representative_iv(df) == pytest.approx(0.28)

    def test_unsorted_input_picks_latest(self):
        df = pd.DataFrame({
            "timestamp": [300, 100, 200],
            "atm_iv": [0.28, 0.30, 0.25],
        })
        # latest by timestamp is 300 -> 0.28
        assert daily_representative_iv(df) == pytest.approx(0.28)

    def test_drops_zero_and_negative_iv(self):
        df = pd.DataFrame({
            "timestamp": [100, 200, 300],
            "atm_iv": [0.0, -0.1, 0.28],
        })
        assert daily_representative_iv(df) == pytest.approx(0.28)

    def test_empty_df_returns_nan(self):
        df = pd.DataFrame(columns=["timestamp", "atm_iv"])
        assert np.isnan(daily_representative_iv(df))

    def test_missing_columns_returns_nan(self):
        df = pd.DataFrame({"timestamp": [100]})
        assert np.isnan(daily_representative_iv(df))

    def test_all_invalid_returns_nan(self):
        df = pd.DataFrame({
            "timestamp": [100, 200],
            "atm_iv": [0.0, -0.1],
        })
        assert np.isnan(daily_representative_iv(df))

    def test_none_df_returns_nan(self):
        assert np.isnan(daily_representative_iv(None))


# ---------------------------------------------------------------------------
# IVRankTracker
# ---------------------------------------------------------------------------


def _ms(day_offset_sec: float) -> int:
    """Return a millisecond timestamp offset from a fixed base."""
    base = 1_700_000_000  # arbitrary fixed base
    return int((base + day_offset_sec) * 1000)


class TestIVRankTrackerUpdate:
    def test_same_day_updates_current_iv_only(self):
        t = IVRankTracker("BTC")
        t.update(_ms(0), 0.30)
        t.update(_ms(3600), 0.35)  # same UTC day
        assert t.window_days() == 0
        assert t.rank() == 50.0  # history empty -> fallback

    def test_day_rollover_pushes_to_history(self):
        t = IVRankTracker("BTC")
        # day 1
        t.update(_ms(0), 0.30)
        t.update(_ms(3600), 0.35)
        # day 2 (UTC rollover) - 90_000 sec > 1 day
        t.update(_ms(90_000), 0.40)

        assert t.window_days() == 1
        # history holds day-1 final IV (0.35)
        hist = t.historical_ivs()
        assert len(hist) == 1
        assert hist.iloc[0] == pytest.approx(0.35)

    def test_invalid_iv_ignored(self):
        t = IVRankTracker("BTC")
        t.update(_ms(0), 0.0)
        t.update(_ms(10), -0.1)
        t.update(_ms(20), None)
        # all invalid -> nothing ingested
        assert t.window_days() == 0
        assert t.rank() == 50.0

    def test_lookback_window_trims(self):
        """History never exceeds lookback_days; oldest entries evicted."""
        t = IVRankTracker("BTC", lookback_days=3)
        # 5 day rollovers; day 4 final IV becomes current_day, days 0-3 push.
        # Without trim history would be [0.10, 0.20, 0.30, 0.40];
        # with lookback=3 only the last 3 are kept.
        for day in range(5):
            t.update(_ms(day * 90_000), 0.10 * (day + 1))
        assert t.window_days() == 3
        hist = t.historical_ivs()
        assert list(hist) == pytest.approx([0.20, 0.30, 0.40])


class TestIVRankTrackerRank:
    def test_rank_uses_history_not_current_day(self):
        """Current day IV is NOT in the baseline used for ranking."""
        t = IVRankTracker("BTC")
        # Day 1: observe two ticks; only the last (0.25) will be pushed.
        t.update(_ms(0), 0.20)
        t.update(_ms(60), 0.25)
        # Day 2: rollover pushes 0.25 into history.
        t.update(_ms(90_000), 0.50)

        assert t.window_days() == 1
        # current_day_iv (0.50) is not in history=[0.25]
        # rank of 0.50 vs [0.25] -> 1/1 = 100
        assert t.rank() == pytest.approx(100.0)
        # explicit query of 0.20 vs [0.25] -> 0/1 = 0
        assert t.rank(0.20) == pytest.approx(0.0)

    def test_rank_empty_history_returns_50(self):
        t = IVRankTracker("BTC")
        assert t.rank(0.50) == 50.0

    def test_rank_explicit_iv_overrides_internal(self):
        t = IVRankTracker("BTC")
        t.update(_ms(0), 0.30)
        t.update(_ms(90_000), 0.40)  # rollover -> history=[0.30]
        # rank a different IV explicitly
        assert t.rank(0.30) == pytest.approx(100.0)  # 1 of 1


# ---------------------------------------------------------------------------
# IVRankTracker.bootstrap_from_parquet
# ---------------------------------------------------------------------------


def _write_daily_parquet(dir_path: str, symbol: str, date_str: str, ivs):
    """Write a fake daily parquet with the given IV series."""
    os.makedirs(dir_path, exist_ok=True)
    ts_base = int(
        time.mktime(time.strptime(date_str, "%Y-%m-%d"))
    ) * 1000
    rows = [
        {"timestamp": ts_base + i * 10_000, "atm_iv": float(iv)}
        for i, iv in enumerate(ivs)
    ]
    df = pd.DataFrame(rows)
    fp = os.path.join(dir_path, f"{symbol}_{date_str}.parquet")
    df.to_parquet(fp, index=False)
    return fp


class TestBootstrapFromParquet:
    def test_loads_multiple_days_excluding_today(self, tmp_path):
        data_dir = str(tmp_path)
        surface_dir = data_dir + "/deribit/vol_surface"
        _write_daily_parquet(surface_dir, "BTC", "2026-01-01", [0.20, 0.22])
        _write_daily_parquet(surface_dir, "BTC", "2026-01-02", [0.25, 0.30])

        # today file should be skipped — patch by writing today's date
        today_str = time.strftime("%Y-%m-%d", time.gmtime())
        _write_daily_parquet(surface_dir, "BTC", today_str, [0.99, 0.99])

        t = IVRankTracker("BTC")
        n = t.bootstrap_from_parquet(data_dir)
        assert n == 2
        hist = t.historical_ivs()
        # last obs of each day: 0.22, 0.30
        assert list(hist) == pytest.approx([0.22, 0.30])

    def test_missing_dir_returns_zero(self, tmp_path):
        t = IVRankTracker("BTC")
        n = t.bootstrap_from_parquet(str(tmp_path / "nonexistent"))
        assert n == 0
        assert t.window_days() == 0
        assert t.rank() == 50.0

    def test_lookback_caps_loaded_days(self, tmp_path):
        data_dir = str(tmp_path)
        surface_dir = data_dir + "/deribit/vol_surface"
        # write 5 past days
        for d in range(1, 6):
            date_str = f"2026-01-{d:02d}"
            _write_daily_parquet(surface_dir, "BTC", date_str, [0.10 * d])

        t = IVRankTracker("BTC", lookback_days=3)
        n = t.bootstrap_from_parquet(data_dir)
        assert n == 3
        # last 3 days: 0.30, 0.40, 0.50
        assert list(t.historical_ivs()) == pytest.approx([0.30, 0.40, 0.50])

    def test_filters_invalid_iv_days(self, tmp_path):
        data_dir = str(tmp_path)
        surface_dir = data_dir + "/deribit/vol_surface"
        _write_daily_parquet(surface_dir, "BTC", "2026-01-01", [0.0, -0.1])  # all invalid
        _write_daily_parquet(surface_dir, "BTC", "2026-01-02", [0.25, 0.30])

        t = IVRankTracker("BTC")
        n = t.bootstrap_from_parquet(data_dir)
        assert n == 1
        assert t.historical_ivs().iloc[0] == pytest.approx(0.30)


# ---------------------------------------------------------------------------
# State file persistence
# ---------------------------------------------------------------------------


def _state_path(tmp_path, symbol: str) -> str:
    return str(tmp_path / f"iv_rank_{symbol}.json")


class TestStateFile:
    def test_save_and_reload_round_trip(self, tmp_path):
        state_file = _state_path(tmp_path, "BTC")
        t = IVRankTracker("BTC", state_file=state_file)
        # Seed history via 3 rollovers (pushes days 0,1,2; day 3 = current).
        for day in range(4):
            t.update(_ms(day * 90_000), 0.10 * (day + 1))
        t.save_state()

        # New tracker picks up the same window from state file alone.
        t2 = IVRankTracker("BTC", state_file=state_file)
        loaded = t2._load_state()
        assert set(loaded.keys()) == {"d0", "d1", "d2"} or len(loaded) == 3

    def test_save_state_noop_without_file(self, tmp_path):
        # No state_file -> save_state is a no-op (no exception).
        t = IVRankTracker("BTC")
        t.update(_ms(0), 0.20)
        t.update(_ms(90_000), 0.30)  # rollover
        t.save_state()

    def test_load_missing_state_returns_empty(self, tmp_path):
        t = IVRankTracker("BTC", state_file=_state_path(tmp_path, "BTC"))
        assert t._load_state() == {}

    def test_load_corrupt_state_returns_empty(self, tmp_path):
        state_file = _state_path(tmp_path, "BTC")
        with open(state_file, "w") as f:
            f.write("{not valid json")
        t = IVRankTracker("BTC", state_file=state_file)
        assert t._load_state() == {}

    def test_bootstrap_merges_parquet_and_state(self, tmp_path):
        data_dir = str(tmp_path)
        surface_dir = data_dir + "/deribit/vol_surface"
        # Parquet has 2 days
        _write_daily_parquet(surface_dir, "BTC", "2026-01-01", [0.10, 0.11])
        _write_daily_parquet(surface_dir, "BTC", "2026-01-02", [0.20, 0.21])
        # State file has 2 different (older) days
        state_file = _state_path(tmp_path, "BTC")
        import json
        with open(state_file, "w") as f:
            json.dump({
                "symbol": "BTC",
                "history": [
                    {"date": "2025-12-30", "iv": 0.50},
                    {"date": "2025-12-31", "iv": 0.60},
                ],
            }, f)

        t = IVRankTracker("BTC", state_file=state_file)
        n = t.bootstrap_from_parquet(data_dir)
        assert n == 4  # 2 from parquet + 2 from state, all < today
        ivs = list(t.historical_ivs())
        # daily_representative_iv picks last tick: 0.11 (day1), 0.21 (day2)
        assert min(ivs) == pytest.approx(0.11)
        assert max(ivs) == pytest.approx(0.60)

    def test_state_overrides_parquet_on_date_conflict(self, tmp_path):
        """State wins for the same date (canonical long-term memory)."""
        data_dir = str(tmp_path)
        surface_dir = data_dir + "/deribit/vol_surface"
        _write_daily_parquet(surface_dir, "BTC", "2026-01-01", [0.10])  # parquet: 0.10
        state_file = _state_path(tmp_path, "BTC")
        import json
        with open(state_file, "w") as f:
            json.dump({
                "symbol": "BTC",
                "history": [{"date": "2026-01-01", "iv": 0.99}],  # state: 0.99
            }, f)

        t = IVRankTracker("BTC", state_file=state_file)
        t.bootstrap_from_parquet(data_dir)
        # Single day; rank of any value vs [0.99] is the count-below.
        # If state won: history=[0.99], rank(0.50)=100 (0.99 > 0.50).
        # If parquet won: history=[0.10], rank(0.50)=100 too — ambiguous.
        # Use a direct check instead: history_ivs should be [0.99].
        assert t.historical_ivs().iloc[0] == pytest.approx(0.99)

    def test_bootstrap_drops_today_from_state(self, tmp_path):
        """State may include today (e.g., pushed from a different TZ);
        bootstrap must drop it to avoid look-ahead."""
        state_file = _state_path(tmp_path, "BTC")
        today_str = time.strftime("%Y-%m-%d", time.gmtime())
        import json
        with open(state_file, "w") as f:
            json.dump({
                "symbol": "BTC",
                "history": [
                    {"date": "2025-12-31", "iv": 0.40},
                    {"date": today_str, "iv": 0.99},  # today, must drop
                ],
            }, f)
        t = IVRankTracker("BTC", state_file=state_file)
        t.bootstrap_from_parquet(str(tmp_path))  # no parquet, just state
        assert t.window_days() == 1
        assert t.historical_ivs().iloc[0] == pytest.approx(0.40)

    def test_update_persists_on_rollover(self, tmp_path):
        state_file = _state_path(tmp_path, "BTC")
        t = IVRankTracker("BTC", state_file=state_file)
        t.update(_ms(0), 0.20)
        # No file yet — nothing to save before any rollover.
        import os
        assert not os.path.exists(state_file)

        t.update(_ms(90_000), 0.30)  # rollover -> save
        assert os.path.exists(state_file)

        import json
        with open(state_file) as f:
            data = json.load(f)
        assert len(data["history"]) == 1

    def test_save_state_atomic(self, tmp_path):
        """Save writes .tmp then renames — no partial file left on crash."""
        state_file = _state_path(tmp_path, "BTC")
        t = IVRankTracker("BTC", state_file=state_file)
        t.update(_ms(0), 0.20)
        t.update(_ms(90_000), 0.30)
        t.save_state()

        # Final file exists, no .tmp leftover.
        import os
        assert os.path.exists(state_file)
        assert not os.path.exists(state_file + ".tmp")
