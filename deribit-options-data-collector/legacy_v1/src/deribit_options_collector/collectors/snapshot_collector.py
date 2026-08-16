"""Daily snapshot collector for full data capture."""

import asyncio
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import structlog

from deribit_options_collector.api.rest_client import DeribitRestClient
from deribit_options_collector.collectors.base import BaseCollector
from deribit_options_collector.config import Settings
from deribit_options_collector.models import OptionInstrument
from deribit_options_collector.storage.parquet_store import ParquetStore
from deribit_options_collector.storage.sqlite_store import SQLiteStore

if TYPE_CHECKING:
    from deribit_options_collector.metrics.prometheus import MetricsCollector

logger = structlog.get_logger(__name__)


class SnapshotCollector(BaseCollector):
    """Daily full snapshot collector at 08:00 UTC."""

    def __init__(
        self,
        settings: Settings,
        rest_client: DeribitRestClient,
        parquet_store: ParquetStore,
        sqlite_store: SQLiteStore,
        metrics: Optional["MetricsCollector"] = None,
    ) -> None:
        super().__init__("snapshot_collector")
        self._settings = settings
        self._rest_client = rest_client
        self._parquet_store = parquet_store
        self._sqlite_store = sqlite_store
        self._metrics = metrics
        self._instruments: list[OptionInstrument] = []
        self._running = False
        self._scheduler_task: Optional[asyncio.Task[None]] = None
        self._snapshot_depth = settings.collection.snapshot_depth

    async def start(self) -> None:
        """Start the snapshot collector scheduler."""
        logger.info("starting_snapshot_collector")
        self._running = True
        self._scheduler_task = asyncio.create_task(self._schedule_loop())
        logger.info("snapshot_collector_started")

    async def stop(self) -> None:
        """Stop the snapshot collector."""
        logger.info("stopping_snapshot_collector")
        self._running = False

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        logger.info("snapshot_collector_stopped")

    async def _schedule_loop(self) -> None:
        """Main scheduler loop waiting for 08:00 UTC trigger."""
        while self._running:
            now = datetime.now(timezone.utc)
            target_hour = 8
            target_minute = 0
            target_second = 0

            next_snapshot = datetime(
                now.year,
                now.month,
                now.day,
                target_hour,
                target_minute,
                target_second,
                tzinfo=timezone.utc,
            )

            if now >= next_snapshot:
                next_snapshot = next_snapshot.replace(day=now.day + 1)

            wait_seconds = (next_snapshot - now).total_seconds()

            logger.info(
                "next_snapshot_scheduled",
                next_snapshot=next_snapshot.isoformat(),
                wait_seconds=wait_seconds,
            )

            try:
                await asyncio.sleep(wait_seconds)
            except asyncio.CancelledError:
                break

            if self._running:
                await self.collect()

    async def collect(self) -> None:
        """Collect full snapshot of all active instruments."""
        logger.info("snapshot_collection_started")
        start_time = datetime.now(timezone.utc)

        await self._load_instruments()

        try:
            await self._collect_order_books()
            await self._collect_greeks()
            await self._collect_mark_prices()

            if self._metrics:
                self._metrics.record_snapshot_timestamp()

            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            logger.info(
                "snapshot_collection_completed",
                instruments=len(self._instruments),
                elapsed_seconds=elapsed,
            )
        except Exception as e:
            logger.error("snapshot_collection_failed", error=str(e))
            if self._metrics:
                self._metrics.increment_write_errors()

        self._last_collection_time = datetime.now(timezone.utc)

    async def _load_instruments(self) -> None:
        """Load all active instruments."""
        currencies = self._settings.get_whitelisted_currencies()
        whitelist = self._settings.get_whitelisted_instruments()

        self._instruments = []

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
            except Exception as e:
                logger.error("snapshot_instrument_load_failed", currency=currency, error=str(e))

        logger.info("snapshot_instruments_loaded", count=len(self._instruments))

    async def _collect_order_books(self) -> None:
        """Collect order books with depth 20 for all instruments."""
        logger.info("collecting_order_books", count=len(self._instruments))
        instrument_names = [i.instrument_name for i in self._instruments]

        batch_size = 50
        total_collected = 0

        for i in range(0, len(instrument_names), batch_size):
            batch = instrument_names[i : i + batch_size]

            books = await self._rest_client.batch_get_order_books(
                batch, depth=self._snapshot_depth
            )

            for book in books:
                self._parquet_store.save_order_books([book])
                self._sqlite_store.save_order_book(book)
                total_collected += 1

            logger.info(
                "orderbooks_batch_collected",
                batch=i // batch_size + 1,
                collected=total_collected,
            )

            await asyncio.sleep(0.5)

        logger.info("orderbooks_collection_completed", total=total_collected)

    async def _collect_greeks(self) -> None:
        """Collect Greeks data for all instruments."""
        logger.info("collecting_greeks", count=len(self._instruments))
        total_collected = 0

        for instrument in self._instruments:
            try:
                greeks = await self._rest_client.get_greeks(instrument.instrument_name)
                self._parquet_store.save_greeks([greeks])
                self._sqlite_store.save_greeks(greeks)
                total_collected += 1
            except Exception as e:
                logger.warning("greeks_fetch_failed", instrument=instrument.instrument_name, error=str(e))

            if total_collected % 20 == 0:
                await asyncio.sleep(0.1)

        logger.info("greeks_collection_completed", total=total_collected)

    async def _collect_mark_prices(self) -> None:
        """Collect mark prices for all instruments."""
        logger.info("collecting_mark_prices", count=len(self._instruments))
        total_collected = 0

        for instrument in self._instruments:
            try:
                mark_price = await self._rest_client.get_mark_price(instrument.instrument_name)
                self._parquet_store.save_mark_prices([mark_price])
                self._sqlite_store.save_mark_price(mark_price)
                total_collected += 1
            except Exception as e:
                logger.warning("markprice_fetch_failed", instrument=instrument.instrument_name, error=str(e))

            if total_collected % 20 == 0:
                await asyncio.sleep(0.1)

        logger.info("mark_prices_collection_completed", total=total_collected)

    async def force_snapshot(self) -> None:
        """Force an immediate snapshot collection."""
        logger.info("forcing_immediate_snapshot")
        await self.collect()
