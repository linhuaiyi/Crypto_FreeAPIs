"""Base collector abstract class."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from deribit_options_collector.storage.parquet_store import ParquetStore
    from deribit_options_collector.storage.sqlite_store import SQLiteStore

logger = structlog.get_logger(__name__)


class BaseCollector(ABC):
    """Abstract base class for data collectors."""

    def __init__(self, name: str) -> None:
        self._name = name
        self._running = False
        self._last_collection_time: datetime | None = None

    @property
    def name(self) -> str:
        """Get collector name."""
        return self._name

    @property
    def last_collection_time(self) -> datetime | None:
        """Get last collection timestamp."""
        return self._last_collection_time

    @abstractmethod
    async def start(self) -> None:
        """Start the collector."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the collector gracefully."""
        pass

    @abstractmethod
    async def collect(self) -> None:
        """Perform a single collection cycle."""
        pass

    def mark_running(self, running: bool) -> None:
        """Set running state."""
        self._running = running

    def is_running(self) -> bool:
        """Check if collector is running."""
        return self._running

    async def on_shutdown(self) -> None:
        """Hook called during shutdown to flush buffers."""
        logger.info("collector_shutdown_hook", collector=self._name)
