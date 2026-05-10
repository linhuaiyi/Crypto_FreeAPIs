"""
L1 quote fetcher with WebSocket-first + REST fallback architecture.

Dual-mode collector:
  - WS path: subscribe to ticker channels, poll local state at 1s intervals
  - REST path: batch snapshot via REST API (startup calibration, fallback, backfill)
"""

import time
import asyncio
import threading
from typing import Dict, List, Optional
from dataclasses import dataclass

import pandas as pd
import requests

from utils import get_logger
from fetchers.ws_orderbook import WSOrderbookEngine, L1Quote

logger = get_logger("QuoteFetcher")


@dataclass(frozen=True)
class QuoteSnapshot:
    """Normalized L1 quote, unified regardless of source."""
    timestamp: int
    instrument_name: str
    exchange: str
    source: str  # 'ws' or 'rest'
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    mid_price: float
    spread: float
    spread_bps: float
    bid_iv: Optional[float] = None
    ask_iv: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "instrument_name": self.instrument_name,
            "exchange": self.exchange,
            "source": self.source,
            "bid_price": self.bid_price,
            "ask_price": self.ask_price,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "mid_price": self.mid_price,
            "spread": self.spread,
            "spread_bps": self.spread_bps,
            "bid_iv": self.bid_iv,
            "ask_iv": self.ask_iv,
        }


def _make_snapshot(quote: L1Quote, exchange: str, source: str) -> QuoteSnapshot:
    bid = quote.bid_price
    ask = quote.ask_price
    mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
    spread = ask - bid if bid > 0 and ask > 0 else 0.0
    spread_bps = (spread / mid * 10000) if mid > 0 else 0.0

    return QuoteSnapshot(
        timestamp=quote.timestamp,
        instrument_name=quote.instrument_name,
        exchange=exchange,
        source=source,
        bid_price=bid,
        ask_price=ask,
        bid_size=quote.bid_size,
        ask_size=quote.ask_size,
        mid_price=mid,
        spread=spread,
        spread_bps=spread_bps,
    )


class QuoteFetcher:
    """
    L1 quote collector with WS primary + REST fallback.

    Usage:
        fetcher = QuoteFetcher("deribit")
        fetcher.add_ws_channels(["ticker.BTC-PERPETUAL.100ms"])
        fetcher.start_ws()  # starts WS in background thread

        # Collect 1s snapshots
        snapshots = fetcher.collect_ws_snapshots()
    """

    def __init__(self, exchange: str) -> None:
        self.exchange = exchange
        self._ws_engine: Optional[WSOrderbookEngine] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_channels: List[str] = []
        self._rest_instruments: List[str] = []

        self.deribit_base = "https://www.deribit.com/api/v2"
        self.binance_base = "https://fapi.binance.com/fapi/v1"
        self.session = requests.Session()

    def add_ws_channels(self, channels: List[str]) -> None:
        self._ws_channels.extend(channels)

    def add_rest_instruments(self, instruments: List[str]) -> None:
        self._rest_instruments.extend(instruments)

    # ── WebSocket Path ──

    def start_ws(self) -> None:
        """Start WS engine in a background thread."""
        if not self._ws_channels:
            logger.warning(f"[{self.exchange}] No WS channels configured")
            return

        self._ws_engine = WSOrderbookEngine(self.exchange)
        self._ws_engine.subscribe_many(self._ws_channels)

        self._ws_thread = threading.Thread(target=self._run_ws_loop, daemon=True)
        self._ws_thread.start()
        logger.info(
            f"[{self.exchange}] WS engine started "
            f"({len(self._ws_channels)} channels)"
        )

    def stop_ws(self) -> None:
        if self._ws_engine:
            self._ws_engine.stop()
        if self._ws_loop and self._ws_loop.is_running():
            self._ws_loop.call_soon_threadsafe(self._ws_loop.stop)
        if self._ws_thread:
            self._ws_thread.join(timeout=10)

    def collect_ws_snapshots(self) -> List[QuoteSnapshot]:
        """Take a snapshot from current WS state (call at ~1s intervals)."""
        if not self._ws_engine:
            return []

        now_ms = int(time.time() * 1000)
        results: List[QuoteSnapshot] = []

        for instrument, quote in self._ws_engine.get_all_quotes().items():
            snap = _make_snapshot(quote, self.exchange, "ws")
            results.append(snap)

        return results

    # ── REST Path ──

    def fetch_rest_snapshot(self, instruments: Optional[List[str]] = None) -> List[QuoteSnapshot]:
        """Fetch REST batch snapshot (startup calibration / fallback)."""
        targets = instruments or self._rest_instruments
        if not targets:
            return []

        if self.exchange == "deribit":
            return self._rest_deribit(targets)
        elif self.exchange == "binance":
            return self._rest_binance(targets)
        else:
            return self._rest_hyperliquid(targets)

    def _rest_deribit(self, instruments: List[str]) -> List[QuoteSnapshot]:
        results: List[QuoteSnapshot] = []
        now_ms = int(time.time() * 1000)

        for instrument in instruments:
            try:
                resp = self.session.get(
                    f"{self.deribit_base}/public/ticker",
                    params={"instrument_name": instrument},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json().get("result", {})
                if not data:
                    continue

                quote = L1Quote(
                    timestamp=int(data.get("timestamp", now_ms)),
                    instrument_name=instrument,
                    bid_price=float(data.get("best_bid_price", 0)),
                    ask_price=float(data.get("best_ask_price", 0)),
                    bid_size=float(data.get("best_bid_amount", 0)),
                    ask_size=float(data.get("best_ask_amount", 0)),
                    last_price=float(data.get("last_price", 0)),
                )
                snap = _make_snapshot(quote, "deribit", "rest")
                bid_iv = data.get("bid_iv")
                ask_iv = data.get("ask_iv")
                if bid_iv or ask_iv:
                    snap = QuoteSnapshot(
                        **{**snap.to_dict(),
                           "bid_iv": float(bid_iv) if bid_iv else None,
                           "ask_iv": float(ask_iv) if ask_iv else None}
                    )
                results.append(snap)

            except Exception as e:
                logger.warning(f"Deribit REST ticker error for {instrument}: {e}")

        return results

    def _rest_binance(self, instruments: List[str]) -> List[QuoteSnapshot]:
        results: List[QuoteSnapshot] = []
        now_ms = int(time.time() * 1000)

        for symbol in instruments:
            try:
                resp = self.session.get(
                    f"{self.binance_base}/ticker/bookTicker",
                    params={"symbol": symbol},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()

                bid = float(data.get("bidPrice", 0))
                ask = float(data.get("askPrice", 0))
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
                spread = ask - bid if bid > 0 and ask > 0 else 0.0
                spread_bps = (spread / mid * 10000) if mid > 0 else 0.0

                results.append(QuoteSnapshot(
                    timestamp=int(data.get("time", now_ms)),
                    instrument_name=symbol,
                    exchange="binance",
                    source="rest",
                    bid_price=bid,
                    ask_price=ask,
                    bid_size=float(data.get("bidQty", 0)),
                    ask_size=float(data.get("askQty", 0)),
                    mid_price=mid,
                    spread=spread,
                    spread_bps=spread_bps,
                ))

            except Exception as e:
                logger.warning(f"Binance REST bookTicker error for {symbol}: {e}")

        return results

    def _rest_hyperliquid(self, instruments: List[str]) -> List[QuoteSnapshot]:
        results: List[QuoteSnapshot] = []
        now_ms = int(time.time() * 1000)

        try:
            resp = self.session.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "l2Book", "coin": "BTC"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            levels = data.get("levels", [[]])
            if levels and levels[0]:
                best_bid = levels[0][0]
                best_ask = levels[1][0] if len(levels) > 1 and levels[1] else best_bid

                bid = float(best_bid.get("px", 0))
                ask = float(best_ask.get("px", 0))
                mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
                spread = ask - bid if bid > 0 and ask > 0 else 0.0
                spread_bps = (spread / mid * 10000) if mid > 0 else 0.0

                for coin in instruments:
                    results.append(QuoteSnapshot(
                        timestamp=now_ms,
                        instrument_name=coin,
                        exchange="hyperliquid",
                        source="rest",
                        bid_price=bid,
                        ask_price=ask,
                        bid_size=float(best_bid.get("sz", 0)),
                        ask_size=float(best_ask.get("sz", 0)),
                        mid_price=mid,
                        spread=spread,
                        spread_bps=spread_bps,
                    ))
        except Exception as e:
            logger.warning(f"Hyperliquid REST error: {e}")

        return results

    # ── Internal ──

    def _run_ws_loop(self) -> None:
        self._ws_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._ws_loop)
        try:
            self._ws_loop.run_until_complete(self._ws_engine.run())
        except Exception as e:
            logger.error(f"[{self.exchange}] WS loop exited: {e}")
