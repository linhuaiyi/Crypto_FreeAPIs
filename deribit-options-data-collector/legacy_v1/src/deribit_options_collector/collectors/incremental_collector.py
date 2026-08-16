"""Incremental streaming collector for real-time data."""

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

import structlog

from deribit_options_collector.api.rest_client import DeribitRestClient
from deribit_options_collector.api.websocket_client import DeribitWebSocketClient
from deribit_options_collector.collectors.base import BaseCollector
from deribit_options_collector.config import Settings
from deribit_options_collector.models import CollectedData, OptionInstrument
from deribit_options_collector.storage.parquet_store import ParquetStore
from deribit_options_collector.storage.sqlite_store import SQLiteStore

if TYPE_CHECKING:
    from deribit_options_collector.metrics.prometheus import MetricsCollector

logger = structlog.get_logger(__name__)


class IncrementalCollector(BaseCollector):
    """Incremental streaming collector using REST polling and WebSocket."""

    def __init__(
        self,
        settings: Settings,
        rest_client: DeribitRestClient,
        ws_client: DeribitWebSocketClient,
        parquet_store: ParquetStore,
        sqlite_store: SQLiteStore,
        metrics: Optional["MetricsCollector"] = None,
    ) -> None:
        super().__init__("incremental_collector")
        self._settings = settings
        self._rest_client = rest_client
        self._ws_client = ws_client
        self._parquet_store = parquet_store
        self._sqlite_store = sqlite_store
        self._metrics = metrics
        self._instruments: list[OptionInstrument] = []
        self._buffer: CollectedData = CollectedData()
        self._buffer_lock = asyncio.Lock()
        self._collection_interval = settings.collection.incremental_interval_seconds
        self._running = False
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._poll_task: Optional[asyncio.Task[None]] = None
        self._flush_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Start the incremental collector."""
        logger.info("starting_incremental_collector")

        await self._load_instruments()

        self._running = True
        self._ws_task = asyncio.create_task(self._ws_listener())
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._flush_task = asyncio.create_task(self._flush_loop())

        logger.info("incremental_collector_started", instruments=len(self._instruments))

    async def stop(self) -> None:
        """Stop the collector gracefully."""
        logger.info("stopping_incremental_collector")
        self._running = False

        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self.flush_buffer()
        await self._ws_client.disconnect()

        logger.info("incremental_collector_stopped")

    async def _load_instruments(self) -> None:
        """Load available instruments from Deribit."""
        currencies = self._settings.get_whitelisted_currencies()
        whitelist = self._settings.get_whitelisted_instruments()

        for currency in currencies:
            try:
                instruments = await self._rest_client.get_instruments(
                    currency=currency,
                    kind=self._settings.collection.kind,
                )
                if whitelist:
                    instruments = [
                        i
                        for i in instruments
                        if i.instrument_name in whitelist or not whitelist
                    ]
                self._instruments.extend(instruments)
                logger.info(
                    "instruments_loaded",
                    currency=currency,
                    count=len(instruments),
                )
            except Exception as e:
                logger.error("instrument_load_failed", currency=currency, error=str(e))

        logger.info("total_instruments", count=len(self._instruments))

    def _on_ws_data(self, data: CollectedData) -> None:
        """Handle WebSocket data callback."""
        asyncio.create_task(self._buffer_data(data))

    async def _buffer_data(self, data: CollectedData) -> None:
        """Buffer incoming data."""
        async with self._buffer_lock:
            self._buffer.tickers.extend(data.tickers)
            self._buffer.books.extend(data.books)
            self._buffer.trades.extend(data.trades)
            self._buffer.greeks.extend(data.greeks)
            self._buffer.mark_prices.extend(data.mark_prices)
            self._buffer.settlement_prices.extend(data.settlement_prices)

    async def _ws_listener(self) -> None:
        """Listen to WebSocket for real-time data."""
        self._ws_client._on_data = self._on_ws_data

        try:
            await self._ws_client.connect()
            channels = self._build_channels()
            await self._ws_client.subscribe(channels)
            await self._ws_client.listen()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("ws_listener_error", error=str(e))

    def _build_channels(self) -> list[str]:
        """Build WebSocket channel subscriptions."""
        channels: list[str] = []
        depth = self._settings.collection.snapshot_depth
        interval = f"{self._collection_interval}s"

        for channel_type in self._settings.collection.channels:
            for instrument in self._instruments[:100]:
                if channel_type == "ticker":
                    channels.append(f"ticker.{instrument.instrument_name}.{interval}")
                elif channel_type == "book":
                    channels.append(f"book.{instrument.instrument_name}.{depth}.{interval}")
                elif channel_type == "trades":
                    channels.append(f"trades.{instrument.instrument_name}.{interval}")
                elif channel_type == "markprice":
                    channels.append(f"markprice.{instrument.instrument_name}.{interval}")
                elif channel_type == "greeks":
                    channels.append(f"greeks.{instrument.instrument_name}.{interval}")

        return channels[:200]

    async def _poll_loop(self) -> None:
        """REST polling loop as fallback/supplement to WebSocket."""
        while self._running:
            try:
                await self._poll_tickers()
                await asyncio.sleep(self._collection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("poll_loop_error", error=str(e))
                await asyncio.sleep(5)

    async def _poll_tickers(self) -> None:
        """Poll ticker data via REST API."""
        instrument_names = [i.instrument_name for i in self._instruments]

        try:
            tickers = await self._rest_client.batch_get_tickers(instrument_names)
            data = CollectedData(tickers=tickers)
            await self._buffer_data(data)
            self._last_collection_time = datetime.now(timezone.utc)

            if self._metrics:
                self._metrics.record_ticker_count(len(tickers))

            logger.debug("tickers_polled", count=len(tickers))
        except Exception as e:
            logger.error("ticker_poll_failed", error=str(e))
            if self._metrics:
                self._metrics.increment_write_errors()

    async def _flush_loop(self) -> None:
        """Periodic buffer flush loop."""
        while self._running:
            try:
                await asyncio.sleep(self._collection_interval)
                await self.flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("flush_loop_error", error=str(e))

    async def flush_buffer(self) -> None:
        """Flush buffered data to storage."""
        async with self._buffer_lock:
            if self._buffer.is_empty():
                return

            data = CollectedData(
                tickers=self._buffer.tickers.copy(),
                books=self._buffer.books.copy(),
                trades=self._buffer.trades.copy(),
                greeks=self._buffer.greeks.copy(),
                mark_prices=self._buffer.mark_prices.copy(),
                settlement_prices=self._buffer.settlement_prices.copy(),
            )

            self._buffer = CollectedData()

        try:
            self._parquet_store.save_collected_data(data)
            self._sqlite_store.save_collected_data(data)

            if self._metrics:
                self._metrics.record_data_flush(data.record_count())

            logger.info("buffer_flushed", record_count=data.record_count())
        except Exception as e:
            logger.error("buffer_flush_failed", error=str(e))
            if self._metrics:
                self._metrics.increment_write_errors()

    async def collect(self) -> None:
        """Perform a single collection cycle."""
        await self._poll_tickers()
        await self.flush_buffer()

    async def on_shutdown(self) -> None:
        """Flush remaining data on shutdown."""
        await self.flush_buffer()
        await super().on_shutdown()
