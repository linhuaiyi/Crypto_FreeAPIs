"""Tests for processors/outlier_filter.py"""

import numpy as np
import pandas as pd
import pytest

from processors.outlier_filter import OutlierFilter


@pytest.fixture
def filter_default():
    return OutlierFilter(z_threshold=3.0, window_size=20)


def _make_no_outlier_df():
    return pd.DataFrame({
        "mark_price": [100.0] * 100,
        "timestamp": range(100),
    })


def _make_outlier_df():
    prices = [100.0] * 50 + [999.0] + [100.0] * 50
    return pd.DataFrame({"mark_price": prices, "timestamp": range(len(prices))})


class TestFilter:
    def test_no_outliers_all_false(self, filter_default):
        df = _make_no_outlier_df()
        result = filter_default.filter(df, columns=["mark_price"])
        assert not result["is_outlier"].any()

    def test_extreme_value_flagged(self, filter_default):
        df = _make_outlier_df()
        result = filter_default.filter(df, columns=["mark_price"])
        assert result["is_outlier"].sum() >= 1
        assert result.loc[50, "is_outlier"]

    def test_get_outliers_returns_only_flagged(self, filter_default):
        df = _make_outlier_df()
        outliers = filter_default.get_outliers(df, columns=["mark_price"])
        assert all(outliers["is_outlier"])
        assert len(outliers) < len(df)

    def test_custom_z_threshold(self):
        f = OutlierFilter(z_threshold=1.0, window_size=20)
        prices = [100.0] * 30 + [105.0] + [100.0] * 30
        df = pd.DataFrame({"price": prices, "timestamp": range(len(prices))})
        result = f.filter(df, columns=["price"])
        assert result["is_outlier"].sum() >= 1

    def test_multiple_columns(self, filter_default):
        df = pd.DataFrame({
            "price_a": [100.0] * 50 + [999.0] + [100.0] * 50,
            "price_b": [100.0] * 101,
            "timestamp": range(101),
        })
        result = filter_default.filter(df, columns=["price_a", "price_b"])
        assert result["is_outlier"].sum() >= 1

    def test_small_dataframe_less_than_window(self):
        f = OutlierFilter(z_threshold=3.0, window_size=100)
        df = pd.DataFrame({"price": [100.0, 100.0, 999.0], "timestamp": [0, 1, 2]})
        result = f.filter(df, columns=["price"])
        assert "is_outlier" in result.columns
