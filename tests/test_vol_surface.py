"""Tests for processors/vol_surface.py"""

import pandas as pd
import pytest

from processors.vol_surface import VolatilitySurfaceBuilder


@pytest.fixture
def builder():
    return VolatilitySurfaceBuilder(lookback_days=252)


def _make_options_df():
    return pd.DataFrame({
        "strike": [95, 100, 105],
        "underlying_price": [100, 100, 100],
        "iv": [0.30, 0.25, 0.28],
        "delta": [0.60, 0.50, 0.35],
        "option_type": ["call", "call", "call"],
        "expiry": ["2025-12-31", "2025-12-31", "2025-12-31"],
        "timestamp": [1700000000, 1700000000, 1700000000],
    })


def _make_multi_expiry_df():
    df1 = _make_options_df().copy()
    df2 = _make_options_df().copy()
    df2["expiry"] = "2026-03-31"
    return pd.concat([df1, df2], ignore_index=True)


class TestBuildAtmIv:
    def test_finds_closest_to_atm(self, builder):
        df = _make_options_df()
        atm_iv = builder.build_atm_iv(df)
        assert atm_iv == pytest.approx(0.25, abs=1e-6)

    def test_empty_dataframe_returns_zero(self, builder):
        df = pd.DataFrame(columns=["strike", "underlying_price", "iv"])
        assert builder.build_atm_iv(df) == 0.0


class TestComputeIvRank:
    def test_correct_percentile(self, builder):
        historical = pd.Series([0.10, 0.15, 0.20, 0.25, 0.30])
        rank = builder.compute_iv_rank(0.20, historical)
        assert rank == pytest.approx(60.0)

    def test_lowest_iv(self, builder):
        historical = pd.Series([0.20, 0.25, 0.30])
        rank = builder.compute_iv_rank(0.10, historical)
        assert rank == pytest.approx(0.0)

    def test_empty_series_returns_50(self, builder):
        rank = builder.compute_iv_rank(0.25, pd.Series(dtype=float))
        assert rank == 50.0


class TestBuildTermStructure:
    def test_multi_expiry(self, builder):
        df = _make_multi_expiry_df()
        ts = builder.build_term_structure(df)
        assert len(ts) == 2
        assert "2025-12-31" in ts
        assert "2026-03-31" in ts

    def test_empty_returns_empty_dict(self, builder):
        df = pd.DataFrame(columns=["strike", "underlying_price", "iv", "expiry"])
        assert builder.build_term_structure(df) == {}
