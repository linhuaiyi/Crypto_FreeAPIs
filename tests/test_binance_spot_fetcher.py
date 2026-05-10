"""Tests for fetchers/binance_spot_fetcher.py"""

import pytest
from unittest.mock import patch, MagicMock

from fetchers.binance_spot_fetcher import SpotPrice, BinanceSpotPriceFetcher


def _mock_bookticker_response(symbol, bid, ask, ts):
    return {
        "symbol": symbol,
        "bidPrice": str(bid),
        "askPrice": str(ask),
        "bidQty": "1.0",
        "askQty": "1.0",
        "T": ts,
    }


class TestSpotPrice:
    def test_creation(self):
        sp = SpotPrice(
            timestamp=1700000000000, exchange="binance",
            symbol="BTCUSDT", price=50000.0,
            bid_price=49999.0, ask_price=50001.0,
        )
        assert sp.price == 50000.0
        assert sp.exchange == "binance"

    def test_frozen(self):
        sp = SpotPrice(1700000000000, "binance", "BTCUSDT", 50000.0, 49999.0, 50001.0)
        with pytest.raises(AttributeError):
            sp.price = 51000.0  # type: ignore[misc]

    def test_to_dict(self):
        sp = SpotPrice(1700000000000, "binance", "BTCUSDT", 50000.0, 49999.0, 50001.0)
        d = sp.to_dict()
        assert d["symbol"] == "BTCUSDT"
        assert d["price"] == 50000.0
        assert d["bid_price"] == 49999.0


class TestBinanceSpotPriceFetcher:
    @patch("fetchers.binance_spot_fetcher.requests.Session")
    def test_fetch_prices_single(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_resp = MagicMock()
        mock_resp.json.return_value = _mock_bookticker_response(
            "BTCUSDT", 50000.0, 50002.0, 1700000000000
        )
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        fetcher = BinanceSpotPriceFetcher()
        result = fetcher.fetch_prices(["BTCUSDT"])

        assert len(result) == 1
        assert result[0].symbol == "BTCUSDT"
        assert result[0].price == pytest.approx(50001.0)
        assert result[0].bid_price == 50000.0
        assert result[0].ask_price == 50002.0

    @patch("fetchers.binance_spot_fetcher.requests.Session")
    def test_fetch_prices_multiple(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        responses = [
            _mock_bookticker_response("BTCUSDT", 50000.0, 50002.0, 1700000000000),
            _mock_bookticker_response("ETHUSDT", 3000.0, 3001.0, 1700000000000),
        ]
        mock_session.get.side_effect = [
            MagicMock(
                json=MagicMock(return_value=r),
                raise_for_status=MagicMock(),
            )
            for r in responses
        ]

        fetcher = BinanceSpotPriceFetcher()
        result = fetcher.fetch_prices(["BTCUSDT", "ETHUSDT"])

        assert len(result) == 2
        symbols = {r.symbol for r in result}
        assert symbols == {"BTCUSDT", "ETHUSDT"}

    @patch("fetchers.binance_spot_fetcher.requests.Session")
    def test_fetch_prices_error_returns_empty(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.side_effect = Exception("network error")

        fetcher = BinanceSpotPriceFetcher()
        result = fetcher.fetch_prices(["BTCUSDT"])
        assert result == []

    @patch("fetchers.binance_spot_fetcher.requests.Session")
    def test_fetch_prices_zero_bid_skipped(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_resp = MagicMock()
        mock_resp.json.return_value = _mock_bookticker_response(
            "BTCUSDT", 0.0, 0.0, 1700000000000
        )
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        fetcher = BinanceSpotPriceFetcher()
        result = fetcher.fetch_prices(["BTCUSDT"])
        assert len(result) == 0
