"""Tests for processors/gap_detector.py"""

import pandas as pd
import pytest

from processors.gap_detector import GapDetector


@pytest.fixture
def detector():
    return GapDetector()


class TestDetect:
    def test_no_gaps_returns_empty_list(self, detector):
        df = pd.DataFrame({
            "timestamp": [1000, 2000, 3000, 4000, 5000],
            "price": [100, 101, 102, 103, 104],
        })
        gaps = detector.detect(df, threshold_ms=60000)
        assert gaps == []

    def test_single_gap_above_threshold(self, detector):
        df = pd.DataFrame({
            "timestamp": [1000, 2000, 3000, 70000, 71000],
            "price": [100, 101, 102, 103, 104],
        })
        gaps = detector.detect(df, threshold_ms=60000)
        assert len(gaps) == 1
        assert gaps[0].gap_start == 3000
        assert gaps[0].gap_end == 70000
        assert gaps[0].gap_duration_ms == 67000

    def test_multiple_gaps(self, detector):
        df = pd.DataFrame({
            "timestamp": [1000, 2000, 80000, 81000, 200000, 201000],
            "price": [100, 101, 102, 103, 104, 105],
        })
        gaps = detector.detect(df, threshold_ms=10000)
        assert len(gaps) == 2
        assert gaps[0].gap_start == 2000
        assert gaps[1].gap_start == 81000

    def test_empty_dataframe(self, detector):
        df = pd.DataFrame({"timestamp": pd.Series(dtype=int), "price": pd.Series(dtype=float)})
        gaps = detector.detect(df)
        assert gaps == []

    def test_single_row_no_diffs(self, detector):
        df = pd.DataFrame({"timestamp": [1000], "price": [100]})
        gaps = detector.detect(df)
        assert gaps == []

    def test_gap_with_instrument_column(self, detector):
        df = pd.DataFrame({
            "timestamp": [1000, 2000, 80000],
            "price": [100, 101, 102],
            "instrument": ["BTC", "ETH", "BTC"],
        })
        gaps = detector.detect(df, threshold_ms=10000)
        assert len(gaps) == 1
        assert "BTC" in gaps[0].affected_instruments
        assert "ETH" in gaps[0].affected_instruments


class TestFillGaps:
    def test_ffill_fills_ohlcv(self, detector):
        df = pd.DataFrame({
            "timestamp": [1, 2, 3, 4, 5],
            "open": [100.0, None, None, 103.0, 104.0],
            "close": [100.0, None, None, 103.0, 104.0],
        })
        result = detector.fill_gaps(df, method="ffill")
        assert result["open"].iloc[2] == 100.0
        assert result["close"].iloc[2] == 100.0

    def test_empty_dataframe_returns_empty(self, detector):
        df = pd.DataFrame()
        result = detector.fill_gaps(df)
        assert result.empty
