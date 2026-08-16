"""Deribit WebSocket API client with auto-reconnection."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import orjson
import structlog
import websockets
from websockets.client import WebSocketClientProtocol

from deribit_options_collector.config import DeribitConfig, Settings
from deribit_options_collector.models import (
    CollectedData,
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionTicker,
    OrderBookEntry,
    SettlementPriceData,
    TradeData,
)

logger = structlog.get_logger(__name__)


class WebSocketError(Exception):
    """WebSocket connection error."""

    pass


class DeribitWebSocketClient:
    """Asynchronous Deribit WebSocket API client."""

    def __init__(
        self,
        settings: Settings,
        on_data: Callable[[CollectedData], None] | None = None,
    ) -> None:
        self._settings = settings
        self._config = settings.deribit
        self._ws_url = self._config.ws_url
        self._socket: Optional[WebSocketClientProtocol] = None
        self._running = False
        self._subscriptions: list[str] = []
        self._on_data = on_data
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0
        self._last_message_time: float = 0.0
        self._message_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._producer_task: Optional[asyncio.Task[None]] = None

    async def connect(self) -> None:
        """Connect to the WebSocket server."""
        try:
            self._socket = await websockets.connect(
                self._ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            )
            self._running = True
            self._reconnect_delay = 1.0
            self._last_message_time = asyncio.get_event_loop().time()
            logger.info("websocket_connected", url=self._ws_url)
        except Exception as e:
            logger.error("websocket_connect_failed", error=str(e))
            raise WebSocketError(f"Failed to connect: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from the WebSocket server."""
        self._running = False
        if self._socket is not None:
            try:
                await self._socket.close()
            except Exception as e:
                logger.warning("websocket_close_error", error=str(e))
            finally:
                self._socket = None

    async def subscribe(
        self,
        channels: list[str],
    ) -> None:
        """Subscribe to WebSocket channels."""
        if self._socket is None:
            raise WebSocketError("Not connected")

        subscribe_msg = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "params": {"channels": channels},
            "id": self._generate_id(),
        }

        await self._socket.send(json.dumps(subscribe_msg))
        self._subscriptions.extend(channels)
        logger.info("channels_subscribed", channels=channels)

    async def unsubscribe(self, channels: list[str]) -> None:
        """Unsubscribe from WebSocket channels."""
        if self._socket is None:
            raise WebSocketError("Not connected")

        unsubscribe_msg = {
            "jsonrpc": "2.0",
            "method": "public/unsubscribe",
            "params": {"channels": channels},
            "id": self._generate_id(),
        }

        await self._socket.send(json.dumps(unsubscribe_msg))
        for channel in channels:
            if channel in self._subscriptions:
                self._subscriptions.remove(channel)
        logger.info("channels_unsubscribed", channels=channels)

    async def listen(self) -> None:
        """Start listening for messages."""
        self._consumer_task = asyncio.create_task(self._consume_messages())
        self._producer_task = asyncio.create_task(self._produce_messages())

        while self._running:
            try:
                if self._socket is None:
                    await self.connect()
                    if self._subscriptions:
                        await self.subscribe(self._subscriptions)

                async for message in self._socket:  # type: ignore[union-attr]
                    self._last_message_time = asyncio.get_event_loop().time()
                    try:
                        data = orjson.loads(message)
                        await self._message_queue.put(data)
                    except orjson.JSONDecodeError as e:
                        logger.warning("invalid_json", error=str(e))

            except websockets.ConnectionClosed:
                logger.warning("websocket_disconnected", will_reconnect=True)
                await self._handle_disconnect()
            except Exception as e:
                logger.error("websocket_error", error=str(e))
                await self._handle_disconnect()

    async def _handle_disconnect(self) -> None:
        """Handle WebSocket disconnection with reconnection."""
        self._running = False
        if self._socket is not None:
            try:
                await self._socket.close()
            except Exception:
                pass
            self._socket = None

        if self._running:
            logger.info(
                "reconnecting",
                delay=self._reconnect_delay,
            )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2, self._max_reconnect_delay
            )
            self._running = True
            await self.connect()
            if self._subscriptions:
                await self.subscribe(self._subscriptions)

    async def _produce_messages(self) -> None:
        """Process incoming WebSocket messages."""
        while self._running:
            try:
                message = await asyncio.wait_for(
                    self._message_queue.get(),
                    timeout=1.0,
                )
                self._process_message(message)
            except asyncio.TimeoutError:
                continue

    async def _consume_messages(self) -> None:
        """Consumer task placeholder."""
        while self._running:
            await asyncio.sleep(0.1)

    def _process_message(self, message: dict[str, Any]) -> None:
        """Process a WebSocket message based on its type."""
        if "method" not in message:
            return

        method = message.get("method", "")
        params = message.get("params", {})
        channel = params.get("channel", "")

        if "ticker" in channel:
            self._process_ticker(params.get("data", {}))
        elif "book" in channel:
            self._process_order_book(params.get("data", {}))
        elif "trades" in channel:
            self._process_trades(params.get("data", {}))
        elif "markprice" in channel:
            self._process_mark_price(params.get("data", {}))
        elif "greeks" in channel:
            self._process_greeks(params.get("data", {}))

    def _process_ticker(self, data: dict[str, Any]) -> None:
        """Process ticker data from WebSocket."""
        if not data:
            return

        ticker = OptionTicker(
            instrument_name=data.get("instrument_name", ""),
            timestamp=datetime.fromtimestamp(
                data.get("timestamp", 0) / 1000, tz=timezone.utc
            ),
            underlying_price=data.get("underlying_price", 0.0),
            mark_price=data.get("mark_price", 0.0),
            bid_price=data.get("best_bid_price", data.get("bid_price", 0.0)),
            ask_price=data.get("best_ask_price", data.get("ask_price", 0.0)),
            bid_iv=data.get("bid_iv", 0.0),
            ask_iv=data.get("ask_iv", 0.0),
            mark_iv=data.get("mark_iv", 0.0),
            open_interest=data.get("open_interest", 0.0),
            volume_24h=data.get("volume", data.get("stats", {}).get("volume", 0.0)),
            settlement_period=data.get("settlement_period", "day"),
            last=data.get("last_price"),
            high=data.get("high"),
            low=data.get("low"),
        )

        collected = CollectedData(tickers=[ticker])
        if self._on_data:
            self._on_data(collected)

    def _process_order_book(self, data: dict[str, Any]) -> None:
        """Process order book data from WebSocket."""
        if not data:
            return

        bids = [
            OrderBookEntry(
                price=bid[0] if isinstance(bid, list) else bid.get("price", 0),
                amount=bid[1] if isinstance(bid, list) else bid.get("amount", 0),
                order_count=bid[2] if isinstance(bid, list) and len(bid) > 2 else 0,
            )
            for bid in data.get("bids", [])
        ]

        asks = [
            OrderBookEntry(
                price=ask[0] if isinstance(ask, list) else ask.get("price", 0),
                amount=ask[1] if isinstance(ask, list) else ask.get("amount", 0),
                order_count=ask[2] if isinstance(ask, list) and len(ask) > 2 else 0,
            )
            for ask in data.get("asks", [])
        ]

        book = OptionBook(
            instrument_name=data.get("instrument_name", ""),
            timestamp=datetime.fromtimestamp(
                data.get("timestamp", 0) / 1000, tz=timezone.utc
            ),
            underlying_price=data.get("underlying_price", 0.0),
            settlement_price=data.get("settlement_price", 0.0),
            bids=bids,
            asks=asks,
            current_best_bid=data.get("best_bid_price"),
            current_best_ask=data.get("best_ask_price"),
            current_timestamp=data.get("current_timestamp"),
            state=data.get("state"),
        )

        collected = CollectedData(books=[book])
        if self._on_data:
            self._on_data(collected)

    def _process_trades(self, data: list[dict[str, Any]]) -> None:
        """Process trades data from WebSocket."""
        if not data:
            return

        trades = []
        for item in data:
            trades.append(
                TradeData(
                    trade_seq=item.get("trade_seq", 0),
                    trade_id=item.get("trade_id", ""),
                    timestamp=datetime.fromtimestamp(
                        item.get("timestamp", 0) / 1000, tz=timezone.utc
                    ),
                    instrument_name=item.get("instrument_name", ""),
                    direction=item.get("direction", ""),
                    price=item.get("price", 0.0),
                    amount=item.get("amount", 0.0),
                    trade_volume_usd=item.get("trade_volume_usd", 0.0),
                    trade_index_price=item.get("trade_index_price", 0.0),
                    inventory_index=item.get("inventory_index", 0.0),
                    volatility=item.get("volatility"),
                    interest_rate=item.get("interest_rate"),
                    mark_price=item.get("mark_price"),
                    index_price=item.get("index_price"),
                )
            )

        collected = CollectedData(trades=trades)
        if self._on_data:
            self._on_data(collected)

    def _process_mark_price(self, data: dict[str, Any]) -> None:
        """Process mark price data from WebSocket."""
        if not data:
            return

        mark_price = MarkPriceData(
            timestamp=datetime.fromtimestamp(
                data.get("timestamp", 0) / 1000, tz=timezone.utc
            ),
            instrument_name=data.get("instrument_name", ""),
            mark_price=data.get("mark_price", 0.0),
            index_price=data.get("index_price", 0.0),
            settlement_price=data.get("settlement_price", 0.0),
            underlying_price=data.get("underlying_price", 0.0),
        )

        collected = CollectedData(mark_prices=[mark_price])
        if self._on_data:
            self._on_data(collected)

    def _process_greeks(self, data: dict[str, Any]) -> None:
        """Process Greeks data from WebSocket."""
        if not data:
            return

        greeks = GreeksData(
            timestamp=datetime.fromtimestamp(
                data.get("timestamp", 0) / 1000, tz=timezone.utc
            ),
            instrument_name=data.get("instrument_name", ""),
            underlying_price=data.get("underlying_price", 0.0),
            mark_price=data.get("mark_price", 0.0),
            open_interest=data.get("open_interest", 0.0),
            delta=data.get("delta", 0.0),
            gamma=data.get("gamma", 0.0),
            rho=data.get("rho", 0.0),
            theta=data.get("theta", 0.0),
            vega=data.get("vega", 0.0),
        )

        collected = CollectedData(greeks=[greeks])
        if self._on_data:
            self._on_data(collected)

    def _generate_id(self) -> int:
        """Generate a unique request ID."""
        import time

        return int(time.time() * 1000) % 1000000

    def get_last_message_time(self) -> float:
        """Get the timestamp of the last received message."""
        return self._last_message_time

    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._socket is not None and self._running

    def get_subscriptions(self) -> list[str]:
        """Get current subscriptions."""
        return self._subscriptions.copy()

    async def send_heartbeat(self) -> None:
        """Send a heartbeat/ping message."""
        if self._socket is not None and self._running:
            heartbeat_msg = {
                "jsonrpc": "2.0",
                "method": "public/ping",
                "id": self._generate_id(),
            }
            try:
                await self._socket.send(json.dumps(heartbeat_msg))
            except Exception as e:
                logger.warning("heartbeat_failed", error=str(e))
