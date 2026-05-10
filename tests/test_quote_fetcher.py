"""Tests for fetchers/quote_fetcher.py — REST + WS dual-mode quote collector."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from fetchers.quote_fetcher import QuoteFetcher, QuoteSnapshot, _make_snapshot
from fetchers.ws_orderbook import L1Quote


def _mock_response(json_data, status_code=200):
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    if status_code >= 400:
        mock.raise_for_status.side_effect = Exception(f"{status_code} error")
    return mock


class TestQuoteSnapshot:
    def test_creation(self):
        snap = QuoteSnapshot(
            timestamp=1, instrument_name="BTC", exchange="deribit",
            source="rest", bid_price=100.0, ask_price=101.0,
            bid_size=1.0, ask_size=2.0, mid_price=100.5,
            spread=1.0, spread_bps=99.5,
        )
        assert snap.mid_price == 100.5

    def test_to_dict_roundtrip(self):
        snap = QuoteSnapshot(
            1, "BTC", "deribit", "rest", 100, 101, 1, 2, 100.5, 1, 99.5,
            bid_iv=0.5, ask_iv=0.6,
        )
        d = snap.to_dict()
        assert d["bid_iv"] == 0.5
        assert d["ask_iv"] == 0.6


class TestMakeSnapshot:
    def test_normal_spread(self):
        q = L1Quote(1, "BTC", 100.0, 102.0, 1.0, 2.0)
        snap = _make_snapshot(q, "deribit", "rest")
        assert snap.mid_price == 101.0
        assert snap.spread == 2.0
        assert snap.spread_bps == pytest.approx(198.02, rel=0.01)

    def test_zero_bid(self):
        q = L1Quote(1, "BTC", 0.0, 102.0, 0, 2.0)
        snap = _make_snapshot(q, "deribit", "rest")
        assert snap.mid_price == 0.0
        assert snap.spread_bps == 0.0


class TestQuoteFetcherREST:
    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_deribit_success(self, mock_session_cls):
        fetcher = QuoteFetcher("deribit")
        mock_data = {
            "result": {
                "timestamp": 1700000000000,
                "best_bid_price": 50000.0,
                "best_ask_price": 50100.0,
                "best_bid_amount": 1.5,
                "best_ask_amount": 2.0,
                "last_price": 50050.0,
                "bid_iv": 0.45,
                "ask_iv": 0.55,
            }
        }
        fetcher.session = MagicMock()
        fetcher.session.get.return_value = _mock_response(mock_data)

        results = fetcher.fetch_rest_snapshot(["BTC-PERPETUAL"])
        assert len(results) == 1
        assert results[0].bid_price == 50000.0
        assert results[0].bid_iv == 0.45

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_deribit_empty_result(self, mock_session_cls):
        fetcher = QuoteFetcher("deribit")
        fetcher.session = MagicMock()
        fetcher.session.get.return_value = _mock_response({"result": {}})

        results = fetcher.fetch_rest_snapshot(["BTC-PERPETUAL"])
        assert len(results) == 0

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_deribit_500_error(self, mock_session_cls):
        fetcher = QuoteFetcher("deribit")
        fetcher.session = MagicMock()
        fetcher.session.get.return_value = _mock_response({"error": "internal"}, 500)

        results = fetcher.fetch_rest_snapshot(["BTC-PERPETUAL"])
        assert len(results) == 0

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_deribit_timeout(self, mock_session_cls):
        fetcher = QuoteFetcher("deribit")
        fetcher.session = MagicMock()
        fetcher.session.get.side_effect = TimeoutError("connection timed out")

        results = fetcher.fetch_rest_snapshot(["BTC-PERPETUAL"])
        assert len(results) == 0

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_binance_success(self, mock_session_cls):
        fetcher = QuoteFetcher("binance")
        fetcher.session = MagicMock()
        mock_data = {
            "bidPrice": "50000.0", "askPrice": "50100.0",
            "bidQty": "1.5", "askQty": "2.0", "time": 1700000000000,
        }
        fetcher.session.get.return_value = _mock_response(mock_data)

        results = fetcher.fetch_rest_snapshot(["BTCUSDT"])
        assert len(results) == 1
        assert results[0].exchange == "binance"

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_binance_502_error(self, mock_session_cls):
        fetcher = QuoteFetcher("binance")
        fetcher.session = MagicMock()
        fetcher.session.get.return_value = _mock_response({"code": -1, "msg": "error"}, 502)

        results = fetcher.fetch_rest_snapshot(["BTCUSDT"])
        assert len(results) == 0

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_hyperliquid_success(self, mock_session_cls):
        fetcher = QuoteFetcher("hyperliquid")
        fetcher.session = MagicMock()
        mock_data = {"levels": [[{"px": "50000", "sz": "1"}], [{"px": "50100", "sz": "2"}]]}
        fetcher.session.post.return_value = _mock_response(mock_data)

        results = fetcher.fetch_rest_snapshot(["BTC"])
        assert len(results) == 1
        assert results[0].bid_price == 50000.0

    @patch("fetchers.quote_fetcher.requests.Session")
    def test_rest_hyperliquid_invalid_json(self, mock_session_cls):
        fetcher = QuoteFetcher("hyperliquid")
        fetcher.session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.side_effect = ValueError("invalid json")
        mock_resp.raise_for_status = MagicMock()
        fetcher.session.post.return_value = mock_resp

        results = fetcher.fetch_rest_snapshot(["BTC"])
        assert len(results) == 0

    def test_rest_no_instruments_returns_empty(self):
        fetcher = QuoteFetcher("deribit")
        results = fetcher.fetch_rest_snapshot([])
        assert results == []


class TestQuoteFetcherWS:
    def test_collect_ws_no_engine(self):
        fetcher = QuoteFetcher("deribit")
        assert fetcher.collect_ws_snapshots() == []

    def test_collect_ws_snapshots(self):
        fetcher = QuoteFetcher("deribit")
        engine = MagicMock()
        engine.get_all_quotes.return_value = {
            "BTC": L1Quote(1, "BTC", 100.0, 101.0, 1.0, 2.0, 100.5),
        }
        fetcher._ws_engine = engine

        snaps = fetcher.collect_ws_snapshots()
        assert len(snaps) == 1
        assert snaps[0].source == "ws"

    def test_start_ws_no_channels_warning(self):
        fetcher = QuoteFetcher("deribit")
        # No channels added → should not start
        fetcher.start_ws()
        assert fetcher._ws_engine is None

    def test_add_ws_channels(self):
        fetcher = QuoteFetcher("deribit")
        fetcher.add_ws_channels(["ticker.BTC.100ms"])
        assert "ticker.BTC.100ms" in fetcher._ws_channels

    def test_add_rest_instruments(self):
        fetcher = QuoteFetcher("deribit")
        fetcher.add_rest_instruments(["BTC-PERPETUAL"])
        assert "BTC-PERPETUAL" in fetcher._rest_instruments
