"""Tests for fetchers/risk_free_rate.py — FRED API with calendar ffill."""

import json
import math
import os
import tempfile
import shutil
import pytest
from unittest.mock import patch, MagicMock

from fetchers.risk_free_rate import RiskFreeRate, RiskFreeRateFetcher, FRED_SERIES


@pytest.fixture
def cache_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def fetcher(cache_dir):
    return RiskFreeRateFetcher(api_key="test_key", cache_dir=cache_dir)


def _fred_response(observations):
    return {"observations": observations}


class TestRiskFreeRateDataclass:
    def test_creation(self):
        r = RiskFreeRate("2026-01-15", 1.0, 0.045, math.log(1.045), True)
        assert r.rate_annual == 0.045
        assert r.is_trading_day is True

    def test_frozen(self):
        r = RiskFreeRate("2026-01-15", 1.0, 0.045, math.log(1.045), True)
        with pytest.raises(AttributeError):
            r.rate_annual = 0.05


class TestFetchSeries:
    def test_success(self, fetcher):
        mock_data = _fred_response([
            {"date": "2026-01-02", "value": "4.50"},
            {"date": "2026-01-03", "value": "4.55"},
            {"date": "2026-01-04", "value": "."},  # missing
        ])
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_data
        mock_resp.raise_for_status = MagicMock()

        with patch.object(fetcher._session, "get", return_value=mock_resp):
            result = fetcher.fetch_series("DGS1", "2026-01-01", "2026-01-31")

        assert len(result) == 2
        assert result[0] == ("2026-01-02", 0.045)
        assert result[1] == ("2026-01-03", 0.0455)

    def test_network_timeout(self, fetcher):
        import requests
        with patch.object(fetcher._session, "get", side_effect=requests.Timeout("timeout")):
            result = fetcher.fetch_series("DGS1", "2026-01-01", "2026-01-31")
        assert result == []

    def test_http_500_error(self, fetcher):
        import requests as req
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("500 Server Error")

        with patch.object(fetcher._session, "get", return_value=mock_resp):
            result = fetcher.fetch_series("DGS1", "2026-01-01", "2026-01-31")
        assert result == []

    def test_invalid_json_response(self, fetcher):
        mock_resp = MagicMock()
        mock_resp.json.side_effect = ValueError("invalid json")
        mock_resp.raise_for_status = MagicMock()

        with patch.object(fetcher._session, "get", return_value=mock_resp):
            result = fetcher.fetch_series("DGS1", "2026-01-01", "2026-01-31")
        assert result == []

    def test_uses_cache(self, fetcher, cache_dir):
        # Write cache file
        cache_data = [["2026-01-02", 0.045]]
        path = os.path.join(cache_dir, "DGS1_2026-01-01_2026-01-31.json")
        with open(path, "w") as f:
            json.dump(cache_data, f)

        result = fetcher.fetch_series("DGS1", "2026-01-01", "2026-01-31")
        assert result == [("2026-01-02", 0.045)]


class TestCalendarHelpers:
    def test_generate_calendar_full_year(self, fetcher):
        cal = fetcher._generate_calendar(2026)
        assert len(cal) == 365
        assert cal[0] == "2026-01-01"
        assert cal[-1] == "2026-12-31"

    def test_is_us_holiday_new_year(self, fetcher):
        # 2026-01-01 is Thursday
        assert fetcher._is_us_holiday("2026-01-01") is True

    def test_is_us_holiday_christmas(self, fetcher):
        # 2026-12-25 is Friday
        assert fetcher._is_us_holiday("2026-12-25") is True

    def test_is_us_holiday_regular_day(self, fetcher):
        assert fetcher._is_us_holiday("2026-01-05") is False  # Monday, not a holiday

    def test_is_trading_day_weekday(self, fetcher):
        assert fetcher._is_trading_day("2026-01-05") is True  # Monday

    def test_is_trading_day_weekend(self, fetcher):
        assert fetcher._is_trading_day("2026-01-04") is False  # Sunday

    def test_is_trading_day_holiday(self, fetcher):
        assert fetcher._is_trading_day("2026-01-01") is False  # New Year

    def test_ffill_calendar(self, fetcher):
        raw = {"DGS1": [("2026-01-02", 0.045), ("2026-01-05", 0.046)]}
        cal = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
        filled = fetcher._ffill_calendar(raw, cal)

        assert "2026-01-01" not in filled["DGS1"]  # no data before 01-02
        assert filled["DGS1"]["2026-01-02"] == 0.045
        assert filled["DGS1"]["2026-01-03"] == 0.045  # ffill
        assert filled["DGS1"]["2026-01-04"] == 0.045  # ffill weekend
        assert filled["DGS1"]["2026-01-05"] == 0.046


class TestBuildYieldCurve:
    def test_fallback_when_no_data(self, fetcher):
        with patch.object(fetcher, "fetch_series", return_value=[]):
            curve = fetcher.build_yield_curve("2026-01-15")

        assert len(curve) == len(FRED_SERIES)
        assert all(r.rate_annual == 0.05 for r in curve)
        assert curve[0].rate_continuous == pytest.approx(math.log(1.05))

    def test_with_mock_data(self, fetcher):
        mock_obs = [
            {"date": "2026-01-15", "value": "4.50"},
        ]

        def fake_fetch(series_id, start, end):
            return [(obs["date"], float(obs["value"]) / 100) for obs in mock_obs]

        with patch.object(fetcher, "fetch_series", side_effect=fake_fetch):
            curve = fetcher.build_yield_curve("2026-01-15")

        assert len(curve) == len(FRED_SERIES)
        assert curve[0].rate_annual == 0.045
        assert curve[0].rate_continuous == pytest.approx(math.log(1.045))


class TestGetRateForTenor:
    def test_interpolation(self, fetcher):
        with patch.object(fetcher, "build_yield_curve") as mock_curve:
            # Use strictly increasing tenors to satisfy CubicSpline
            mock_curve.return_value = [
                RiskFreeRate("2026-01-15", 1/12, 0.04, math.log(1.04), True),
                RiskFreeRate("2026-01-15", 3/12, 0.041, math.log(1.041), True),
                RiskFreeRate("2026-01-15", 6/12, 0.043, math.log(1.043), True),
                RiskFreeRate("2026-01-15", 1.0, 0.045, math.log(1.045), True),
                RiskFreeRate("2026-01-15", 2.0, 0.046, math.log(1.046), True),
                RiskFreeRate("2026-01-15", 5.0, 0.048, math.log(1.048), True),
                RiskFreeRate("2026-01-15", 10.0, 0.05, math.log(1.05), True),
                RiskFreeRate("2026-01-15", 30.0, 0.055, math.log(1.055), True),
            ]
            rate = fetcher.get_rate_for_tenor("2026-01-15", 0.5)
            assert isinstance(rate, float)
            assert 0.03 < rate < 0.10


class TestCacheIO:
    def test_cache_write_and_read(self, fetcher, cache_dir):
        data = [("2026-01-02", 0.045), ("2026-01-03", 0.046)]
        fetcher._save_cache("DGS1", "2026-01-01", "2026-01-31", data)

        loaded = fetcher._load_cache("DGS1", "2026-01-01", "2026-01-31")
        assert loaded == data

    def test_cache_read_nonexistent(self, fetcher):
        result = fetcher._load_cache("NONEXISTENT", "2026-01-01", "2026-01-31")
        assert result is None

    def test_cache_read_corrupt(self, fetcher, cache_dir):
        path = os.path.join(cache_dir, "DGS1_2026-01-01_2026-01-31.json")
        with open(path, "w") as f:
            f.write("not valid json {{{")
        result = fetcher._load_cache("DGS1", "2026-01-01", "2026-01-31")
        assert result is None
