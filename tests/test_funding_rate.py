"""Tests for fetchers/funding_rate.py"""

import pytest
from unittest.mock import patch, MagicMock

from fetchers.funding_rate import FundingRate, FundingRateFetcher


def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    return mock


class TestFundingRateDataclass:
    def test_creation(self):
        fr = FundingRate(
            timestamp=1700000000000, exchange="binance",
            symbol="BTCUSDT", funding_rate=0.0001,
            mark_price=50000.0, index_price=49999.0,
        )
        assert fr.timestamp == 1700000000000
        assert fr.funding_rate == 0.0001

    def test_to_dict_from_dict_roundtrip(self):
        fr = FundingRate(1700000000000, "binance", "BTCUSDT", 0.0001, 50000.0, 49999.0)
        d = fr.to_dict()
        fr2 = FundingRate.from_dict(d)
        assert fr2 == fr

    def test_optional_fields_none(self):
        fr = FundingRate(1700000000000, "binance", "BTCUSDT", 0.0001)
        assert fr.mark_price is None
        assert fr.index_price is None


class TestFetchBinance:
    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_binance_success(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.binance_base = "https://fapi.binance.com/fapi/v1"

        mock_data = [
            {"fundingTime": "1700000000000", "fundingRate": "0.0001"},
            {"fundingTime": "1700028800000", "fundingRate": "0.0002"},
        ]
        fetcher.session.get.return_value = _mock_response(mock_data)

        result = fetcher.fetch_binance("BTCUSDT", 1700000000000, 1700100000000)
        assert len(result) == 2
        assert result[0].funding_rate == pytest.approx(0.0001)

    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_binance_error_returns_empty(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.binance_base = "https://fapi.binance.com/fapi/v1"
        fetcher.session.get.side_effect = Exception("network error")

        result = fetcher.fetch_binance("BTCUSDT", 1700000000000, 1700100000000)
        assert result == []

    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_binance_realtime(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.binance_base = "https://fapi.binance.com/fapi/v1"

        mock_data = {
            "time": 1700000000000,
            "lastFundingRate": "0.0001",
            "markPrice": "50000.0",
            "indexPrice": "49999.0",
        }
        fetcher.session.get.return_value = _mock_response(mock_data)

        result = fetcher.fetch_binance_realtime("BTCUSDT")
        assert result is not None
        assert result.mark_price == 50000.0


class TestFetchDeribit:
    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_deribit_success(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.deribit_base = "https://www.deribit.com/api/v2"

        mock_data = {
            "result": [
                {"timestamp": 1700000000000, "interest_8h": 0.0001, "index_price": 50000.0, "prev_index_price": 49999.0},
                {"timestamp": 1700086400000, "interest_8h": 0.0002, "index_price": 50100.0, "prev_index_price": 50000.0},
            ]
        }
        fetcher.session.get.return_value = _mock_response(mock_data)

        result = fetcher.fetch_deribit("BTC-PERPETUAL", 1700000000000, 1700200000000)
        assert len(result) == 2
        assert result[0].exchange == "deribit"

    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_deribit_error(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.deribit_base = "https://www.deribit.com/api/v2"

        mock_data = {"error": {"message": "not found"}, "result": None}
        fetcher.session.get.return_value = _mock_response(mock_data)

        result = fetcher.fetch_deribit("BTC-PERPETUAL", 1700000000000, 1700200000000)
        assert result == []


class TestFetchHyperliquid:
    @patch.object(FundingRateFetcher, '__init__', lambda self: None)
    def test_fetch_hyperliquid_success(self):
        fetcher = FundingRateFetcher()
        fetcher.session = MagicMock()
        fetcher.hyperliquid_base = "https://api.hyperliquid.xyz"

        mock_data = [
            {"time": 1700000000000, "fundingRate": "0.0001", "premium": "0.0002"},
        ]
        fetcher.session.post.return_value = _mock_response(mock_data)

        result = fetcher.fetch_hyperliquid("BTC", start_ts=1700000000000)
        assert len(result) == 1
        assert result[0].exchange == "hyperliquid"
