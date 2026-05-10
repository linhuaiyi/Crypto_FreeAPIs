"""Tests for fetchers/mark_price.py"""

import pytest
from unittest.mock import patch, MagicMock

from fetchers.mark_price import MarkPrice, MarkPriceFetcher


def _mock_response(json_data):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.status_code = 200
    mock.raise_for_status = MagicMock()
    return mock


class TestMarkPriceDataclass:
    def test_creation(self):
        mp = MarkPrice(
            timestamp=1700000000000, exchange="binance",
            symbol="BTCUSDT", mark_price=50000.0,
            index_price=49999.0, basis=1.0,
        )
        assert mp.basis == 1.0

    def test_optional_fields(self):
        mp = MarkPrice(1700000000000, "hyperliquid", "BTC", 50000.0)
        assert mp.index_price is None
        assert mp.basis is None


class TestMarkPriceBinance:
    @patch("fetchers.mark_price.requests.get")
    def test_fetch_binance(self, mock_get):
        mock_data = [
            [1700000000000, "50000", "50100", "49900", "50050", "1000"],
        ]
        mock_get.return_value = _mock_response(mock_data)

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_binance("BTCUSDT", 1700000000000, 1700100000000)
        assert len(result) == 1
        assert result[0].mark_price == 50050.0

    @patch("fetchers.mark_price.requests.get")
    def test_fetch_binance_empty(self, mock_get):
        mock_get.return_value = _mock_response([])

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_binance("BTCUSDT", 1700000000000, 1700100000000)
        assert result == []

    @patch("fetchers.mark_price.requests.get")
    def test_fetch_binance_error(self, mock_get):
        mock_get.side_effect = Exception("fail")

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_binance("BTCUSDT", 1700000000000, 1700100000000)
        assert result == []


class TestMarkPriceDeribit:
    @patch("fetchers.mark_price.requests.get")
    def test_fetch_deribit(self, mock_get):
        mock_data = {
            "result": {
                "ticks": [1700000000000, 1700000600000],
                "close": [50000.0, 50100.0],
                "open": [49900.0, 50050.0],
                "high": [50100.0, 50200.0],
                "low": [49800.0, 49900.0],
                "volume": [100.0, 200.0],
                "cost": [5000000.0, 10020000.0],
            }
        }
        mock_get.return_value = _mock_response(mock_data)

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_deribit("BTC-PERPETUAL", 1700000000000, 1700100000000)
        assert len(result) == 2
        assert result[0].mark_price == 50000.0


class TestMarkPriceHyperliquid:
    @patch("fetchers.mark_price.requests.post")
    def test_fetch_hyperliquid(self, mock_post):
        mock_post.return_value = _mock_response({"BTC": 50000.0, "ETH": 3000.0})

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_hyperliquid()
        assert len(result) == 2

    @patch("fetchers.mark_price.requests.post")
    def test_fetch_hyperliquid_filtered(self, mock_post):
        mock_post.return_value = _mock_response({"BTC": 50000.0, "ETH": 3000.0})

        fetcher = MarkPriceFetcher()
        result = fetcher.fetch_hyperliquid(symbol="BTC")
        assert len(result) == 1
        assert result[0].symbol == "BTC"
