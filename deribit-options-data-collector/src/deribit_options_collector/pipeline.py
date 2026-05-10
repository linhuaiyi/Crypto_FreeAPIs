"""Main pipeline orchestration with graceful shutdown."""

import asyncio
import signal
import sys
from pathlib import Path
from typing import Optional

import structlog

from deribit_options_collector.api.rest_client import DeribitRestClient
from deribit_options_collector.api.websocket_client import DeribitWebSocketClient
from deribit_options_collector.collectors.incremental_collector import IncrementalCollector
from deribit_options_collector.collectors.snapshot_collector import SnapshotCollector
from deribit_options_collector.config import Settings, load_settings
from deribit_options_collector.metrics.prometheus import MetricsCollector, MetricsMonitor
from deribit_options_collector.storage.parquet_store import ParquetStore
from deribit_options_collector.storage.sqlite_store import SQLiteStore

logger = structlog.get_logger(__name__)


class DataPipeline:
    """Main data collection pipeline orchestrator."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._rest_client: Optional[DeribitRestClient] = None
        self._ws_client: Optional[DeribitWebSocketClient] = None
        self._parquet_store: Optional[ParquetStore] = None
        self._sqlite_store: Optional[SQLiteStore] = None
        self._metrics: Optional[MetricsCollector] = None
        self._metrics_monitor: Optional[MetricsMonitor] = None
        self._incremental_collector: Optional[IncrementalCollector] = None
        self._snapshot_collector: Optional[SnapshotCollector] = None
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._collectors: list[object] = []

    async def start(self) -> None:
        """Start the data pipeline."""
        logger.info("pipeline_starting")

        self._setup_signal_handlers()

        self._rest_client = DeribitRestClient(self._settings)
        await self._rest_client.start()

        self._ws_client = DeribitWebSocketClient(self._settings)

        self._parquet_store = ParquetStore(self._settings)
        self._sqlite_store = SQLiteStore(self._settings)

        self._metrics = MetricsCollector(self._settings)
        await self._metrics.start()

        self._metrics_monitor = MetricsMonitor(self._metrics)
        await self._metrics_monitor.start()

        self._incremental_collector = IncrementalCollector(
            settings=self._settings,
            rest_client=self._rest_client,
            ws_client=self._ws_client,
            parquet_store=self._parquet_store,
            sqlite_store=self._sqlite_store,
            metrics=self._metrics,
        )

        self._snapshot_collector = SnapshotCollector(
            settings=self._settings,
            rest_client=self._rest_client,
            parquet_store=self._parquet_store,
            sqlite_store=self._sqlite_store,
            metrics=self._metrics,
        )

        self._collectors = [self._incremental_collector, self._snapshot_collector]

        await self._incremental_collector.start()
        await self._snapshot_collector.start()

        self._running = True
        logger.info("pipeline_started")

        await self._shutdown_event.wait()

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._handle_shutdown(s)),
                )
        except NotImplementedError:
            pass

    async def _handle_shutdown(self, sig: signal.Signals) -> None:
        """Handle shutdown signals gracefully."""
        logger.info("shutdown_signal_received", signal=sig.name)
        await self.stop()

    async def stop(self) -> None:
        """Stop the pipeline gracefully."""
        if not self._running:
            return

        logger.info("pipeline_stopping")
        self._running = False

        for collector in self._collectors:
            try:
                if hasattr(collector, "on_shutdown"):
                    await collector.on_shutdown()
                if hasattr(collector, "stop"):
                    await collector.stop()
            except Exception as e:
                logger.error("collector_shutdown_error", collector=type(collector).__name__, error=str(e))

        if self._metrics_monitor:
            await self._metrics_monitor.stop()

        if self._metrics:
            await self._metrics.stop()

        if self._ws_client:
            await self._ws_client.disconnect()

        if self._rest_client:
            await self._rest_client.close()

        if self._sqlite_store:
            self._sqlite_store.close()

        self._shutdown_event.set()
        logger.info("pipeline_stopped")

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()


def setup_logging(settings: Settings) -> None:
    """Setup structured logging."""
    import logging

    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)

    if settings.logging.format == "json":
        import json
        from datetime import datetime

        class JSONFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                log_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if hasattr(record, "_structlog_data"):
                    log_data.update(record._structlog_data)
                return json.dumps(log_data)

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def run_pipeline(config_path: str | None = None) -> None:
    """Run the data pipeline."""
    settings = load_settings(config_path)
    setup_logging(settings)

    pipeline = DataPipeline(settings)

    try:
        await pipeline.start()
    except Exception as e:
        logger.error("pipeline_error", error=str(e))
        await pipeline.stop()
        raise


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Deribit Options Data Collector")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=None,
        help="Path to configuration file",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_pipeline(args.config))
    except KeyboardInterrupt:
        logger.info("interrupted")
    except Exception as e:
        logger.error("fatal_error", error=str(e))
        sys.exit(1)
