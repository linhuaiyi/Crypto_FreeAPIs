"""
WebSocket incremental orderbook engine for L1 quote data.

Maintains local L1 state from WS ticker subscriptions,
with application-layer heartbeat, Deribit JSON-RPC heartbeat,
automatic reconnection, memory guards, and clean lifecycle.
"""

import asyncio
import gc
import json
import time
from typing import Dict, Optional, Callable
from dataclasses import dataclass

import websockets

from utils import get_logger

logger = get_logger("WSOrderbookEngine")


def _ws_is_closed(ws) -> bool:
    """Check if a WebSocket connection is closed (compatible with websockets 14+)."""
    if hasattr(ws, "closed"):
        return ws.closed
    if hasattr(ws, "state"):
        from websockets.protocol import State
        return ws.state >= State.CLOSING
    return True

HEARTBEAT_TIMEOUT_SEC = 30
RECONNECT_DELAY_SEC = 5
APP_HEARTBEAT_CHECK_SEC = 15
APP_HEARTBEAT_PONG_TIMEOUT_SEC = 10
GC_INTERVAL_SEC = 300


@dataclass
class L1Quote:
    """Best bid/ask snapshot for a single instrument."""
    timestamp: int  # ms
    instrument_name: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    last_price: float = 0.0


class WSOrderbookEngine:
    """
    Maintains local L1 orderbook state via WebSocket ticker subscriptions.

    Usage:
        engine = WSOrderbookEngine("deribit")
        engine.subscribe("ticker.BTC-PERPETUAL.100ms")
        engine.start()  # blocking, or run in thread

        # Poll latest state
        quote = engine.get_quote("BTC-PERPETUAL")
    """

    def __init__(
        self,
        exchange: str,
        on_quote: Optional[Callable[[L1Quote], None]] = None,
        max_instruments: int = 2000,
    ) -> None:
        self.exchange = exchange
        self.on_quote = on_quote
        self._max_instruments = max_instruments

        self._state: Dict[str, L1Quote] = {}
        self._subscriptions: Dict[str, str] = {}  # channel -> instrument_name
        self._last_message_time: float = 0.0
        self._last_pong_time: float = 0.0
        self._last_gc_time: float = 0.0
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._deribit_req_id: int = 0

        self._ws_urls: Dict[str, str] = {
            "deribit": "wss://www.deribit.com/ws/api/v2",
            "binance": "wss://fstream.binance.com/ws",
        }

    def subscribe(self, channel: str) -> None:
        """Register a subscription channel."""
        instrument = self._parse_instrument(channel)
        if instrument:
            self._subscriptions[channel] = instrument

    def subscribe_many(self, channels: list[str]) -> None:
        for ch in channels:
            self.subscribe(ch)

    def get_quote(self, instrument_name: str) -> Optional[L1Quote]:
        """Get the latest L1 quote for an instrument."""
        return self._state.get(instrument_name)

    def get_all_quotes(self) -> Dict[str, L1Quote]:
        """Snapshot of all current L1 states."""
        return dict(self._state)

    def stop(self) -> None:
        """Signal shutdown and clear state."""
        self._running = False

    async def _cleanup(self) -> None:
        """Gracefully close WebSocket, cancel tasks, release memory."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._ws and not _ws_is_closed(self._ws):
            try:
                await self._ws.close(1000, "shutdown")
            except Exception:
                pass
        self._ws = None

        self._state.clear()
        self._subscriptions.clear()
        gc.collect()
        logger.info(f"[{self.exchange}] Cleanup complete")

    async def run(self) -> None:
        """Main event loop with auto-reconnect."""
        self._running = True

        while self._running:
            try:
                url = self._ws_urls.get(self.exchange)
                if not url:
                    logger.error(f"No WS URL configured for {self.exchange}")
                    break

                async with websockets.connect(
                    url,
                    ping_interval=None,  # we handle heartbeats ourselves
                    ping_timeout=None,
                    max_size=2**22,
                ) as ws:
                    self._ws = ws
                    await self._send_subscriptions(ws)
                    now = time.time()
                    self._last_message_time = now
                    self._last_pong_time = now
                    self._last_gc_time = now

                    self._heartbeat_task = asyncio.create_task(
                        self._app_heartbeat_loop(ws)
                    )

                    async for raw in ws:
                        self._last_message_time = time.time()
                        await self._handle_message(raw)

            except websockets.ConnectionClosed as e:
                logger.warning(
                    f"[{self.exchange}] WS closed (code={e.code}), "
                    f"reconnecting in {RECONNECT_DELAY_SEC}s"
                )
            except asyncio.CancelledError:
                logger.info(f"[{self.exchange}] Run cancelled")
                break
            except Exception as e:
                logger.error(f"[{self.exchange}] WS error: {e}")

            if self._running:
                await asyncio.sleep(RECONNECT_DELAY_SEC)

        await self._cleanup()

    # ── Application-layer heartbeat ──

    async def _app_heartbeat_loop(self, ws: object) -> None:
        """Periodically check liveness and send pings if needed."""
        try:
            while self._running and not _ws_is_closed(ws):
                await asyncio.sleep(APP_HEARTBEAT_CHECK_SEC)
                now = time.time()
                silence = now - self._last_message_time

                # Periodic GC
                if now - self._last_gc_time >= GC_INTERVAL_SEC:
                    gc.collect()
                    self._last_gc_time = now

                # Deribit JSON-RPC keepalive
                if self.exchange == "deribit" and silence < APP_HEARTBEAT_CHECK_SEC:
                    await self._send_deribit_test(ws)

                # No message for 15s: send WS Ping
                if silence >= APP_HEARTBEAT_CHECK_SEC:
                    pong_wait = now - self._last_pong_time
                    if pong_wait >= APP_HEARTBEAT_CHECK_SEC + APP_HEARTBEAT_PONG_TIMEOUT_SEC:
                        logger.warning(
                            f"[{self.exchange}] No pong for "
                            f"{APP_HEARTBEAT_PONG_TIMEOUT_SEC}s, forcing reconnect"
                        )
                        raise websockets.ConnectionClosed(
                            rcvd=None, sent=None
                        )
                    try:
                        pong = await ws.ping()
                        await asyncio.wait_for(
                            pong, timeout=APP_HEARTBEAT_PONG_TIMEOUT_SEC
                        )
                        self._last_pong_time = time.time()
                    except (asyncio.TimeoutError, Exception):
                        logger.warning(
                            f"[{self.exchange}] Ping pong timeout, forcing reconnect"
                        )
                        raise websockets.ConnectionClosed(
                            rcvd=None, sent=None
                        )
                else:
                    self._last_pong_time = now
        except asyncio.CancelledError:
            pass

    async def _send_deribit_test(self, ws: object) -> None:
        """Send public/test as Deribit JSON-RPC keepalive."""
        self._deribit_req_id += 1
        msg = {
            "jsonrpc": "2.0",
            "id": self._deribit_req_id,
            "method": "public/test",
            "params": {},
        }
        try:
            await ws.send(json.dumps(msg))
        except Exception:
            pass

    # ── Internal ──

    async def _send_subscriptions(self, ws: object) -> None:
        if not self._subscriptions:
            return

        if self.exchange == "deribit":
            msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "public/subscribe",
                "params": {
                    "channels": list(self._subscriptions.keys()),
                },
            }
            await ws.send(json.dumps(msg))
            logger.info(
                f"[deribit] Subscribed to {len(self._subscriptions)} channels"
            )

        elif self.exchange == "binance":
            for channel in self._subscriptions:
                stream_url = f"{self._ws_urls['binance']}/{channel}"
                logger.info(f"[binance] Would connect to {stream_url}")

    async def _handle_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if self.exchange == "deribit":
            await self._handle_deribit(msg)
        elif self.exchange == "binance":
            await self._handle_binance(msg)

    async def _handle_deribit(self, msg: dict) -> None:
        method = msg.get("method")

        # Deribit server heartbeat: respond with public/test
        if method == "public/heartbeat":
            await self._send_deribit_test(self._ws)
            return

        # Deribit test_request: respond with test
        if method == "public/test_request":
            req_id = msg.get("id", 0)
            resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "public/test",
                "params": {},
            }
            try:
                await self._ws.send(json.dumps(resp))
            except Exception:
                pass
            return

        if method != "subscription":
            return

        params = msg.get("params", {})
        channel = params.get("channel", "")
        data = params.get("data", {})

        if "ticker" not in channel:
            return

        instrument = data.get("instrument_name", "")

        if len(self._state) >= self._max_instruments:
            logger.warning(
                f"[deribit] State limit ({self._max_instruments}) reached, "
                f"dropping update for {instrument}"
            )
            return

        quote = L1Quote(
            timestamp=int(data.get("timestamp", time.time() * 1000)),
            instrument_name=instrument,
            bid_price=float(data.get("best_bid_price", 0)),
            ask_price=float(data.get("best_ask_price", 0)),
            bid_size=float(data.get("best_bid_amount", 0)),
            ask_size=float(data.get("best_ask_amount", 0)),
            last_price=float(data.get("last_price", 0)),
        )

        self._state[instrument] = quote

        if self.on_quote:
            self.on_quote(quote)

    async def _handle_binance(self, msg: dict) -> None:
        evt_type = msg.get("e")
        if evt_type != "bookTicker":
            return

        symbol = msg.get("s", "")

        if len(self._state) >= self._max_instruments:
            logger.warning(
                f"[binance] State limit ({self._max_instruments}) reached, "
                f"dropping update for {symbol}"
            )
            return

        quote = L1Quote(
            timestamp=int(msg.get("T", time.time() * 1000)),
            instrument_name=symbol,
            bid_price=float(msg.get("b", 0)),
            ask_price=float(msg.get("a", 0)),
            bid_size=float(msg.get("B", 0)),
            ask_size=float(msg.get("A", 0)),
        )

        self._state[symbol] = quote

        if self.on_quote:
            self.on_quote(quote)

    def _parse_instrument(self, channel: str) -> Optional[str]:
        if self.exchange == "deribit":
            parts = channel.split(".")
            if len(parts) >= 2:
                return parts[1]
        elif self.exchange == "binance":
            return channel.split("@")[0].upper()
        return None
