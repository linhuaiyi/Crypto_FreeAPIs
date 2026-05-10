"""Tests for fetchers/ws_orderbook.py — async WS engine with heartbeat, memory guard, reconnect."""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from fetchers.ws_orderbook import (
    WSOrderbookEngine, L1Quote, APP_HEARTBEAT_CHECK_SEC, GC_INTERVAL_SEC,
)


# ── Unit tests (no real WS) ──

class TestL1Quote:
    def test_creation(self):
        q = L1Quote(1700000000000, "BTC-PERPETUAL", 50000.0, 50100.0, 1.5, 2.0, 50050.0)
        assert q.instrument_name == "BTC-PERPETUAL"
        assert q.bid_price == 50000.0


class TestSubscribe:
    def test_subscribe_deribit(self):
        engine = WSOrderbookEngine("deribit")
        engine.subscribe("ticker.BTC-PERPETUAL.100ms")
        assert "ticker.BTC-PERPETUAL.100ms" in engine._subscriptions
        assert engine._subscriptions["ticker.BTC-PERPETUAL.100ms"] == "BTC-PERPETUAL"

    def test_subscribe_binance(self):
        engine = WSOrderbookEngine("binance")
        engine.subscribe("btcusdt@bookTicker")
        assert "btcusdt@bookTicker" in engine._subscriptions
        assert engine._subscriptions["btcusdt@bookTicker"] == "BTCUSDT"

    def test_subscribe_many(self):
        engine = WSOrderbookEngine("deribit")
        engine.subscribe_many(["ticker.BTC-PERPETUAL.100ms", "ticker.ETH-PERPETUAL.100ms"])
        assert len(engine._subscriptions) == 2


class TestGetQuote:
    def test_get_existing(self):
        engine = WSOrderbookEngine("deribit")
        q = L1Quote(1700000000000, "BTC-PERPETUAL", 50000.0, 50100.0, 1.0, 2.0)
        engine._state["BTC-PERPETUAL"] = q
        assert engine.get_quote("BTC-PERPETUAL") == q

    def test_get_missing_returns_none(self):
        engine = WSOrderbookEngine("deribit")
        assert engine.get_quote("UNKNOWN") is None

    def test_get_all_quotes_snapshot(self):
        engine = WSOrderbookEngine("deribit")
        engine._state["A"] = L1Quote(1, "A", 1, 2, 3, 4)
        snap = engine.get_all_quotes()
        assert "A" in snap
        snap.clear()
        assert "A" in engine._state  # snapshot is a copy


class TestStop:
    def test_stop_sets_running_false(self):
        engine = WSOrderbookEngine("deribit")
        engine._running = True
        engine.stop()
        assert engine._running is False


class TestMaxInstruments:
    def test_memory_guard_drops_updates(self):
        engine = WSOrderbookEngine("deribit", max_instruments=2)
        engine._ws = MagicMock()

        # Fill to limit
        engine._state["A"] = L1Quote(1, "A", 1, 2, 3, 4)
        engine._state["B"] = L1Quote(1, "B", 1, 2, 3, 4)

        # Third instrument should be dropped
        msg = {
            "method": "subscription",
            "params": {
                "channel": "ticker.C.100ms",
                "data": {
                    "instrument_name": "C",
                    "timestamp": 1,
                    "best_bid_price": 100,
                    "best_ask_price": 101,
                    "best_bid_amount": 1,
                    "best_ask_amount": 1,
                    "last_price": 100.5,
                },
            },
        }
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        assert "C" not in engine._state
        assert len(engine._state) == 2


class TestMessageHandling:
    def _make_engine(self, exchange="deribit"):
        engine = WSOrderbookEngine(exchange)
        engine._ws = MagicMock()
        engine._ws.send = AsyncMock()
        return engine

    def test_deribit_ticker_updates_state(self):
        engine = self._make_engine()
        msg = {
            "method": "subscription",
            "params": {
                "channel": "ticker.BTC-PERPETUAL.100ms",
                "data": {
                    "instrument_name": "BTC-PERPETUAL",
                    "timestamp": 1700000000000,
                    "best_bid_price": 50000.0,
                    "best_ask_price": 50100.0,
                    "best_bid_amount": 1.5,
                    "best_ask_amount": 2.0,
                    "last_price": 50050.0,
                },
            },
        }
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        q = engine.get_quote("BTC-PERPETUAL")
        assert q is not None
        assert q.bid_price == 50000.0
        assert q.ask_price == 50100.0

    def test_deribit_non_ticker_ignored(self):
        engine = self._make_engine()
        msg = {"method": "subscription", "params": {"channel": "trades.BTC-PERPETUAL.100ms", "data": {}}}
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        assert len(engine._state) == 0

    def test_deribit_heartbeat_responds(self):
        engine = self._make_engine()
        msg = {"method": "public/heartbeat", "id": 42}
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        engine._ws.send.assert_called_once()
        sent = json.loads(engine._ws.send.call_args[0][0])
        assert sent["method"] == "public/test"

    def test_deribit_test_request_responds(self):
        engine = self._make_engine()
        msg = {"method": "public/test_request", "id": 7}
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        sent = json.loads(engine._ws.send.call_args[0][0])
        assert sent["id"] == 7
        assert sent["method"] == "public/test"

    def test_binance_bookticker_updates_state(self):
        engine = self._make_engine("binance")
        msg = {"e": "bookTicker", "s": "BTCUSDT", "b": "50000", "a": "50100", "B": "1", "A": "2", "T": 1700000000000}
        asyncio.get_event_loop().run_until_complete(engine._handle_binance(msg))
        q = engine.get_quote("BTCUSDT")
        assert q is not None
        assert q.bid_price == 50000.0

    def test_binance_non_bookticker_ignored(self):
        engine = self._make_engine("binance")
        msg = {"e": "aggTrade", "s": "BTCUSDT"}
        asyncio.get_event_loop().run_until_complete(engine._handle_binance(msg))
        assert len(engine._state) == 0

    def test_invalid_json_ignored(self):
        engine = self._make_engine()
        asyncio.get_event_loop().run_until_complete(engine._handle_message("not json {{{"))
        assert len(engine._state) == 0

    def test_on_quote_callback(self):
        received = []
        engine = WSOrderbookEngine("deribit", on_quote=lambda q: received.append(q))
        engine._ws = MagicMock()
        msg = {
            "method": "subscription",
            "params": {
                "channel": "ticker.BTC-PERPETUAL.100ms",
                "data": {"instrument_name": "BTC-PERPETUAL", "timestamp": 1, "best_bid_price": 100, "best_ask_price": 101, "best_bid_amount": 1, "best_ask_amount": 1, "last_price": 100.5},
            },
        }
        asyncio.get_event_loop().run_until_complete(engine._handle_deribit(msg))
        assert len(received) == 1
        assert received[0].instrument_name == "BTC-PERPETUAL"


class TestHandleMessage:
    def test_unknown_exchange_noop(self):
        engine = WSOrderbookEngine("unknown_exchange")
        # Should not raise
        asyncio.get_event_loop().run_until_complete(engine._handle_message('{"test": 1}'))


class TestCleanup:
    def test_cleanup_clears_state(self):
        engine = WSOrderbookEngine("deribit")
        engine._state["A"] = L1Quote(1, "A", 1, 2, 3, 4)
        engine._subscriptions["ch"] = "A"
        engine._ws = MagicMock()
        engine._ws.closed = False
        engine._ws.close = AsyncMock()

        asyncio.get_event_loop().run_until_complete(engine._cleanup())

        assert len(engine._state) == 0
        assert len(engine._subscriptions) == 0
        assert engine._ws is None
