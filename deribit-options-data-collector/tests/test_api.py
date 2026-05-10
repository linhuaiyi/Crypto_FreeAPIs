"""Tests for API clients."""

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from deribit_options_collector.api.rest_client import (
    DeribitAPIError,
    DeribitRestClient,
    RateLimitError,
)
from deribit_options_collector.api.websocket_client import (
    DeribitWebSocketClient,
    WebSocketError,
)
from deribit_options_collector.models import (
    CollectedData,
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionTicker,
    TradeData,
)


class TestDeribitRestClient:
    """Tests for DeribitRestClient."""

    @pytest.fixture
    def client(self, mock_settings: Any) -> DeribitRestClient:
        """Create REST client for testing."""
        return DeribitRestClient(mock_settings)

    @pytest.mark.asyncio
    async def test_client_start(self, client: DeribitRestClient) -> None:
        """Test starting the client."""
        await client.start()
        assert client._session is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_client_context_manager(self, mock_settings: Any) -> None:
        """Test client as context manager."""
        async with DeribitRestClient(mock_settings) as client:
            assert client._session is not None

    @pytest.mark.asyncio
    async def test_client_close(self, client: DeribitRestClient) -> None:
        """Test closing the client."""
        await client.start()
        await client.close()
        assert client._session is None

    @pytest.mark.asyncio
    async def test_get_instruments(self, client: DeribitRestClient) -> None:
        """Test fetching instruments."""
        mock_response = {
            "result": [
                {
                    "instrument_name": "BTC-28MAR26-80000-C",
                    "base_currency": "BTC",
                    "kind": "option",
                    "quote_currency": "USD",
                    "contract_size": 1.0,
                    "option_type": "call",
                    "strike": 80000.0,
                    "expiration_timestamp": 1743206400000,
                    "settlement_period": "month",
                    "is_active": True,
                    "min_trade_amount": 0.1,
                    "tick_size": 0.0001,
                    "maker_commission": 0.0003,
                    "taker_commission": 0.0005,
                }
            ]
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            instruments = await client.get_instruments("BTC", "option")

            assert len(instruments) == 1
            assert instruments[0].instrument_name == "BTC-28MAR26-80000-C"
            assert instruments[0].currency == "BTC"
            assert instruments[0].option_type == "call"

    @pytest.mark.asyncio
    async def test_get_ticker(self, client: DeribitRestClient) -> None:
        """Test fetching ticker."""
        mock_response = {
            "result": {
                "instrument_name": "BTC-28MAR26-80000-C",
                "timestamp": 1743206400000,
                "underlying_price": 85000.0,
                "mark_price": 0.0254,
                "bid_price": 0.0248,
                "ask_price": 0.0260,
                "bid_iv": 0.62,
                "ask_iv": 0.64,
                "mark_iv": 0.63,
                "open_interest": 1250.0,
                "stats": {"volume": 350.0},
                "settlement_period": "month",
            }
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            ticker = await client.get_ticker("BTC-28MAR26-80000-C")

            assert ticker.instrument_name == "BTC-28MAR26-80000-C"
            assert ticker.underlying_price == 85000.0
            assert ticker.mark_price == 0.0254
            assert ticker.bid_iv == 0.62
            assert ticker.open_interest == 1250.0

    @pytest.mark.asyncio
    async def test_get_order_book(self, client: DeribitRestClient) -> None:
        """Test fetching order book."""
        mock_response = {
            "result": {
                "instrument_name": "BTC-28MAR26-80000-C",
                "timestamp": 1743206400000,
                "underlying_price": 85000.0,
                "settlement_price": 0.025,
                "current_best_bid": 0.0248,
                "current_best_ask": 0.0260,
                "current_timestamp": 1743206400000,
                "state": "open",
                "bids": [[0.0248, 10.0, 5], [0.0245, 20.0, 8]],
                "asks": [[0.0260, 15.0, 6], [0.0265, 25.0, 10]],
            }
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            book = await client.get_order_book("BTC-28MAR26-80000-C", depth=20)

            assert book.instrument_name == "BTC-28MAR26-80000-C"
            assert len(book.bids) == 2
            assert len(book.asks) == 2
            assert book.bids[0].price == 0.0248

    @pytest.mark.asyncio
    async def test_get_trades(self, client: DeribitRestClient) -> None:
        """Test fetching trades."""
        mock_response = {
            "result": {
                "trades": [
                    {
                        "trade_seq": 100,
                        "trade_id": "100-abc",
                        "timestamp": 1743206400000,
                        "instrument_name": "BTC-28MAR26-80000-C",
                        "direction": "buy",
                        "price": 0.0254,
                        "amount": 1.0,
                        "trade_volume_usd": 850.0,
                        "trade_index_price": 85000.0,
                        "inventory_index": 85000.0,
                    }
                ]
            }
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            trades = await client.get_trades("BTC-28MAR26-80000-C")

            assert len(trades) == 1
            assert trades[0].trade_id == "100-abc"
            assert trades[0].direction == "buy"

    @pytest.mark.asyncio
    async def test_get_greeks(self, client: DeribitRestClient) -> None:
        """Test fetching Greeks."""
        mock_response = {
            "result": {
                "instrument_name": "BTC-28MAR26-80000-C",
                "underlying_price": 85000.0,
                "mark_price": 0.0254,
                "open_interest": 1250.0,
                "delta": 0.45,
                "gamma": 0.0012,
                "rho": 0.0035,
                "theta": -0.015,
                "vega": 0.25,
            }
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            greeks = await client.get_greeks("BTC-28MAR26-80000-C")

            assert greeks.instrument_name == "BTC-28MAR26-80000-C"
            assert greeks.delta == 0.45
            assert greeks.gamma == 0.0012
            assert greeks.vega == 0.25

    @pytest.mark.asyncio
    async def test_get_mark_price(self, client: DeribitRestClient) -> None:
        """Test fetching mark price."""
        mock_response = {
            "result": [
                {
                    "timestamp": 1743206400000,
                    "instrument_name": "BTC-28MAR26-80000-C",
                    "mark_price": 0.0254,
                    "index_price": 85000.0,
                    "settlement_price": 0.025,
                    "underlying_price": 85000.0,
                }
            ]
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response
            mark_price = await client.get_mark_price("BTC-28MAR26-80000-C")

            assert mark_price.instrument_name == "BTC-28MAR26-80000-C"
            assert mark_price.mark_price == 0.0254

    @pytest.mark.asyncio
    async def test_batch_get_tickers(self, client: DeribitRestClient) -> None:
        """Test batch fetching tickers."""
        instrument_names = ["BTC-28MAR26-80000-C", "BTC-28MAR26-85000-P"]

        mock_ticker_response = {
            "result": {
                "instrument_name": "BTC-28MAR26-80000-C",
                "timestamp": 1743206400000,
                "underlying_price": 85000.0,
                "mark_price": 0.0254,
                "bid_price": 0.0248,
                "ask_price": 0.0260,
                "bid_iv": 0.62,
                "ask_iv": 0.64,
                "mark_iv": 0.63,
                "open_interest": 1250.0,
                "stats": {"volume": 350.0},
                "settlement_period": "month",
            }
        }

        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_ticker_response
            tickers = await client.batch_get_tickers(instrument_names)

            assert len(tickers) == 2
            assert all(isinstance(t, OptionTicker) for t in tickers)

    @pytest.mark.asyncio
    async def test_api_error_handling(self, client: DeribitRestClient) -> None:
        """Test API error handling."""
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = aiohttp.ClientError("Connection failed")

            with pytest.raises(aiohttp.ClientError):
                await client.get_ticker("BTC-28MAR26-80000-C")

    @pytest.mark.asyncio
    async def test_rate_limit_error(self, client: DeribitRestClient) -> None:
        """Test rate limit error handling."""
        with patch.object(client, "_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = RateLimitError("Rate limited", code=429)

            with pytest.raises(RateLimitError):
                await client.get_ticker("BTC-28MAR26-80000-C")


class TestDeribitWebSocketClient:
    """Tests for DeribitWebSocketClient."""

    @pytest.fixture
    def ws_client(self, mock_settings: Any) -> DeribitWebSocketClient:
        """Create WebSocket client for testing."""
        return DeribitWebSocketClient(mock_settings)

    @pytest.mark.asyncio
    async def test_connect(self, ws_client: DeribitWebSocketClient) -> None:
        """Test WebSocket connection."""
        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_connect.return_value = mock_ws

            await ws_client.connect()

            assert ws_client._socket is not None
            assert ws_client._running is True

    @pytest.mark.asyncio
    async def test_disconnect(self, ws_client: DeribitWebSocketClient) -> None:
        """Test WebSocket disconnection."""
        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_connect.return_value = mock_ws

            await ws_client.connect()
            await ws_client.disconnect()

            assert ws_client._socket is None
            assert ws_client._running is False

    @pytest.mark.asyncio
    async def test_subscribe(self, ws_client: DeribitWebSocketClient) -> None:
        """Test subscribing to channels."""
        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_connect.return_value = mock_ws

            await ws_client.connect()
            await ws_client.subscribe(["ticker.BTC-28MAR26-80000-C.1s"])

            assert "ticker.BTC-28MAR26-80000-C.1s" in ws_client._subscriptions
            mock_ws.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_unsubscribe(self, ws_client: DeribitWebSocketClient) -> None:
        """Test unsubscribing from channels."""
        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_ws = AsyncMock()
            mock_connect.return_value = mock_ws

            await ws_client.connect()
            await ws_client.subscribe(["ticker.BTC-28MAR26-80000-C.1s"])
            await ws_client.unsubscribe(["ticker.BTC-28MAR26-80000-C.1s"])

            assert "ticker.BTC-28MAR26-80000-C.1s" not in ws_client._subscriptions

    def test_is_connected(self, ws_client: DeribitWebSocketClient) -> None:
        """Test connection status check."""
        assert ws_client.is_connected() is False

        ws_client._running = True
        assert ws_client.is_connected() is False

        ws_client._socket = MagicMock()
        assert ws_client.is_connected() is True

    def test_get_subscriptions(self, ws_client: DeribitWebSocketClient) -> None:
        """Test getting subscriptions."""
        ws_client._subscriptions = ["ticker.BTC-28MAR26-80000-C.1s"]
        assert ws_client.get_subscriptions() == ["ticker.BTC-28MAR26-80000-C.1s"]

    def test_process_ticker_message(self, ws_client: DeribitWebSocketClient) -> None:
        """Test processing ticker message."""
        received_data: list[CollectedData] = []

        def on_data(data: CollectedData) -> None:
            received_data.append(data)

        ws_client._on_data = on_data

        message = {
            "method": "subscription",
            "params": {
                "channel": "ticker.BTC-28MAR26-80000-C.1s",
                "data": {
                    "instrument_name": "BTC-28MAR26-80000-C",
                    "timestamp": 1743206400000,
                    "underlying_price": 85000.0,
                    "mark_price": 0.0254,
                    "best_bid_price": 0.0248,
                    "best_ask_price": 0.0260,
                    "bid_iv": 0.62,
                    "ask_iv": 0.64,
                    "mark_iv": 0.63,
                    "open_interest": 1250.0,
                    "volume": 350.0,
                    "settlement_period": "month",
                },
            },
        }

        ws_client._process_message(message)

        assert len(received_data) == 1
        assert len(received_data[0].tickers) == 1
        assert received_data[0].tickers[0].instrument_name == "BTC-28MAR26-80000-C"

    def test_process_order_book_message(self, ws_client: DeribitWebSocketClient) -> None:
        """Test processing order book message."""
        received_data: list[CollectedData] = []

        def on_data(data: CollectedData) -> None:
            received_data.append(data)

        ws_client._on_data = on_data

        message = {
            "method": "subscription",
            "params": {
                "channel": "book.BTC-28MAR26-80000-C.20.1s",
                "data": {
                    "instrument_name": "BTC-28MAR26-80000-C",
                    "timestamp": 1743206400000,
                    "underlying_price": 85000.0,
                    "settlement_price": 0.025,
                    "best_bid_price": 0.0248,
                    "best_ask_price": 0.0260,
                    "bids": [[0.0248, 10.0, 5], [0.0245, 20.0, 8]],
                    "asks": [[0.0260, 15.0, 6], [0.0265, 25.0, 10]],
                },
            },
        }

        ws_client._process_message(message)

        assert len(received_data) == 1
        assert len(received_data[0].books) == 1
        assert received_data[0].books[0].instrument_name == "BTC-28MAR26-80000-C"

    def test_generate_id(self, ws_client: DeribitWebSocketClient) -> None:
        """Test ID generation."""
        id1 = ws_client._generate_id()
        id2 = ws_client._generate_id()
        assert isinstance(id1, int)
        assert isinstance(id2, int)


class TestRateLimitError:
    """Tests for RateLimitError."""

    def test_rate_limit_error_creation(self) -> None:
        """Test creating RateLimitError."""
        error = RateLimitError("Rate limited", code=429)
        assert str(error) == "Rate limited"
        assert error.code == 429

    def test_deribit_api_error_creation(self) -> None:
        """Test creating DeribitAPIError."""
        error = DeribitAPIError("API error", code=500, data={"error": "test"})
        assert str(error) == "API error"
        assert error.code == 500
        assert error.data == {"error": "test"}


class TestWebSocketError:
    """Tests for WebSocketError."""

    def test_websocket_error_creation(self) -> None:
        """Test creating WebSocketError."""
        error = WebSocketError("Connection failed")
        assert str(error) == "Connection failed"
