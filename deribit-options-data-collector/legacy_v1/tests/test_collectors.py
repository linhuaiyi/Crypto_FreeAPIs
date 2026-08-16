"""Tests for collectors."""

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from deribit_options_collector.collectors.base import BaseCollector
from deribit_options_collector.collectors.incremental_collector import IncrementalCollector
from deribit_options_collector.collectors.snapshot_collector import SnapshotCollector
from deribit_options_collector.models import (
    CollectedData,
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionInstrument,
    OptionTicker,
    TradeData,
)


class TestBaseCollector:
    """Tests for BaseCollector."""

    def test_collector_name(self) -> None:
        """Test collector name property."""
        collector = BaseCollector.__new__(BaseCollector)
        collector._name = "test_collector"
        collector._running = False
        collector._last_collection_time = None

        assert collector.name == "test_collector"

    def test_is_running(self) -> None:
        """Test is_running check."""
        collector = BaseCollector.__new__(BaseCollector)
        collector._name = "test"
        collector._running = False
        collector._last_collection_time = None

        assert collector.is_running() is False

        collector.mark_running(True)
        assert collector.is_running() is True

    def test_last_collection_time(self) -> None:
        """Test last collection time."""
        collector = BaseCollector.__new__(BaseCollector)
        collector._name = "test"
        collector._running = False
        collector._last_collection_time = None

        assert collector.last_collection_time is None

        now = datetime.now(timezone.utc)
        collector._last_collection_time = now
        assert collector.last_collection_time == now


class TestIncrementalCollector:
    """Tests for IncrementalCollector."""

    @pytest.fixture
    def incremental_collector(
        self,
        mock_settings: Any,
        mock_rest_client: AsyncMock,
        mock_ws_client: AsyncMock,
    ) -> IncrementalCollector:
        """Create IncrementalCollector for testing."""
        from deribit_options_collector.storage.parquet_store import ParquetStore
        from deribit_options_collector.storage.sqlite_store import SQLiteStore

        settings = mock_settings
        settings.storage.parquet.base_path = "/tmp/test_parquet"
        settings.storage.sqlite.path = "/tmp/test.db"

        parquet_store = MagicMock(spec=ParquetStore)
        sqlite_store = MagicMock(spec=SQLiteStore)
        metrics = MagicMock()

        return IncrementalCollector(
            settings=settings,
            rest_client=mock_rest_client,
            ws_client=mock_ws_client,
            parquet_store=parquet_store,
            sqlite_store=sqlite_store,
            metrics=metrics,
        )

    @pytest.mark.asyncio
    async def test_load_instruments(
        self,
        incremental_collector: IncrementalCollector,
        sample_instrument: OptionInstrument,
    ) -> None:
        """Test loading instruments."""
        incremental_collector._rest_client.get_instruments = AsyncMock(
            return_value=[sample_instrument]
        )

        await incremental_collector._load_instruments()

        assert len(incremental_collector._instruments) == 1
        assert incremental_collector._instruments[0].instrument_name == sample_instrument.instrument_name

    @pytest.mark.asyncio
    async def test_load_instruments_multiple_currencies(
        self,
        incremental_collector: IncrementalCollector,
        sample_instrument: OptionInstrument,
    ) -> None:
        """Test loading instruments from multiple currencies."""
        btc_instrument = sample_instrument
        eth_instrument = OptionInstrument(
            instrument_name="ETH-28MAR26-3000-C",
            currency="ETH",
            kind="option",
            base_currency="ETH",
            quote_currency="USD",
            contract_size=1.0,
            option_type="call",
            strike=3000.0,
            expiration_timestamp=1743206400000,
            expiration_date=datetime(2026, 3, 28, tzinfo=timezone.utc),
            settlement_period="month",
            is_active=True,
            min_trade_amount=0.1,
            tick_size=0.0001,
            maker_commission=0.0003,
            taker_commission=0.0005,
        )

        async def mock_get_instruments(currency: str, kind: str):
            if currency == "BTC":
                return [btc_instrument]
            elif currency == "ETH":
                return [eth_instrument]
            return []

        incremental_collector._rest_client.get_instruments = mock_get_instruments

        await incremental_collector._load_instruments()

        assert len(incremental_collector._instruments) == 2

    def test_on_ws_data(self, incremental_collector: IncrementalCollector) -> None:
        """Test WebSocket data callback."""
        data = CollectedData()
        data.tickers.append(
            OptionTicker(
                instrument_name="BTC-28MAR26-80000-C",
                timestamp=datetime.now(timezone.utc),
                underlying_price=85000.0,
                mark_price=0.0254,
                bid_price=0.0248,
                ask_price=0.0260,
                bid_iv=0.62,
                ask_iv=0.64,
                mark_iv=0.63,
                open_interest=1250.0,
                volume_24h=350.0,
                settlement_period="month",
            )
        )

        incremental_collector._on_ws_data(data)

        assert len(incremental_collector._buffer.tickers) == 1

    @pytest.mark.asyncio
    async def test_buffer_data(self, incremental_collector: IncrementalCollector) -> None:
        """Test buffering data."""
        data = CollectedData()
        data.tickers.append(
            OptionTicker(
                instrument_name="BTC-28MAR26-80000-C",
                timestamp=datetime.now(timezone.utc),
                underlying_price=85000.0,
                mark_price=0.0254,
                bid_price=0.0248,
                ask_price=0.0260,
                bid_iv=0.62,
                ask_iv=0.64,
                mark_iv=0.63,
                open_interest=1250.0,
                volume_24h=350.0,
                settlement_period="month",
            )
        )

        await incremental_collector._buffer_data(data)

        assert len(incremental_collector._buffer.tickers) == 1

    def test_build_channels(
        self,
        incremental_collector: IncrementalCollector,
        sample_instrument: OptionInstrument,
    ) -> None:
        """Test building WebSocket channels."""
        incremental_collector._instruments = [sample_instrument]

        channels = incremental_collector._build_channels()

        assert len(channels) > 0
        assert any("ticker" in ch for ch in channels)
        assert any("book" in ch for ch in channels)

    @pytest.mark.asyncio
    async def test_flush_buffer(
        self,
        incremental_collector: IncrementalCollector,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test flushing buffer."""
        incremental_collector._buffer.tickers.append(sample_ticker)

        await incremental_collector.flush_buffer()

        incremental_collector._parquet_store.save_collected_data.assert_called_once()
        incremental_collector._sqlite_store.save_collected_data.assert_called_once()
        assert len(incremental_collector._buffer.tickers) == 0

    @pytest.mark.asyncio
    async def test_flush_buffer_empty(self, incremental_collector: IncrementalCollector) -> None:
        """Test flushing empty buffer."""
        await incremental_collector.flush_buffer()

        incremental_collector._parquet_store.save_collected_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll_tickers(
        self,
        incremental_collector: IncrementalCollector,
        sample_instrument: OptionInstrument,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test polling tickers."""
        incremental_collector._instruments = [sample_instrument]
        incremental_collector._rest_client.batch_get_tickers = AsyncMock(
            return_value=[sample_ticker]
        )

        await incremental_collector._poll_tickers()

        incremental_collector._rest_client.batch_get_tickers.assert_called_once()
        incremental_collector._metrics.record_ticker_count.assert_called()

    @pytest.mark.asyncio
    async def test_collect(
        self,
        incremental_collector: IncrementalCollector,
        sample_instrument: OptionInstrument,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test collect method."""
        incremental_collector._instruments = [sample_instrument]
        incremental_collector._rest_client.batch_get_tickers = AsyncMock(
            return_value=[sample_ticker]
        )

        await incremental_collector.collect()

        incremental_collector._rest_client.batch_get_tickers.assert_called_once()


class TestSnapshotCollector:
    """Tests for SnapshotCollector."""

    @pytest.fixture
    def snapshot_collector(
        self,
        mock_settings: Any,
        mock_rest_client: AsyncMock,
    ) -> SnapshotCollector:
        """Create SnapshotCollector for testing."""
        from deribit_options_collector.storage.parquet_store import ParquetStore
        from deribit_options_collector.storage.sqlite_store import SQLiteStore

        settings = mock_settings
        settings.storage.parquet.base_path = "/tmp/test_parquet"
        settings.storage.sqlite.path = "/tmp/test.db"

        parquet_store = MagicMock(spec=ParquetStore)
        sqlite_store = MagicMock(spec=SQLiteStore)
        metrics = MagicMock()

        return SnapshotCollector(
            settings=settings,
            rest_client=mock_rest_client,
            parquet_store=parquet_store,
            sqlite_store=sqlite_store,
            metrics=metrics,
        )

    @pytest.mark.asyncio
    async def test_load_instruments(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
    ) -> None:
        """Test loading instruments."""
        snapshot_collector._rest_client.get_instruments = AsyncMock(
            return_value=[sample_instrument]
        )

        await snapshot_collector._load_instruments()

        assert len(snapshot_collector._instruments) == 1

    @pytest.mark.asyncio
    async def test_collect_order_books(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
        sample_order_book: OptionBook,
    ) -> None:
        """Test collecting order books."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.batch_get_order_books = AsyncMock(
            return_value=[sample_order_book]
        )

        await snapshot_collector._collect_order_books()

        snapshot_collector._rest_client.batch_get_order_books.assert_called_once()
        snapshot_collector._parquet_store.save_order_books.assert_called_once()
        snapshot_collector._sqlite_store.save_order_book.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_greeks(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
        sample_greeks: GreeksData,
    ) -> None:
        """Test collecting Greeks."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.get_greeks = AsyncMock(
            return_value=sample_greeks
        )

        await snapshot_collector._collect_greeks()

        snapshot_collector._rest_client.get_greeks.assert_called()
        snapshot_collector._parquet_store.save_greeks.assert_called()
        snapshot_collector._sqlite_store.save_greeks.assert_called()

    @pytest.mark.asyncio
    async def test_collect_mark_prices(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
        sample_mark_price: MarkPriceData,
    ) -> None:
        """Test collecting mark prices."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.get_mark_price = AsyncMock(
            return_value=sample_mark_price
        )

        await snapshot_collector._collect_mark_prices()

        snapshot_collector._rest_client.get_mark_price.assert_called()
        snapshot_collector._parquet_store.save_mark_prices.assert_called()
        snapshot_collector._sqlite_store.save_mark_price.assert_called()

    @pytest.mark.asyncio
    async def test_collect_full_snapshot(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
        sample_order_book: OptionBook,
        sample_greeks: GreeksData,
        sample_mark_price: MarkPriceData,
    ) -> None:
        """Test full snapshot collection."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.get_instruments = AsyncMock(
            return_value=[sample_instrument]
        )
        snapshot_collector._rest_client.batch_get_order_books = AsyncMock(
            return_value=[sample_order_book]
        )
        snapshot_collector._rest_client.get_greeks = AsyncMock(
            return_value=sample_greeks
        )
        snapshot_collector._rest_client.get_mark_price = AsyncMock(
            return_value=sample_mark_price
        )

        await snapshot_collector.collect()

        assert snapshot_collector.last_collection_time is not None
        snapshot_collector._metrics.record_snapshot_timestamp.assert_called()

    @pytest.mark.asyncio
    async def test_collect_handles_errors(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
    ) -> None:
        """Test that collect handles errors gracefully."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.get_instruments = AsyncMock(
            return_value=[sample_instrument]
        )
        snapshot_collector._rest_client.batch_get_order_books = AsyncMock(
            side_effect=Exception("API Error")
        )

        await snapshot_collector.collect()

        snapshot_collector._metrics.increment_write_errors.assert_called()

    @pytest.mark.asyncio
    async def test_force_snapshot(
        self,
        snapshot_collector: SnapshotCollector,
        sample_instrument: OptionInstrument,
        sample_order_book: OptionBook,
        sample_greeks: GreeksData,
        sample_mark_price: MarkPriceData,
    ) -> None:
        """Test forcing a snapshot."""
        snapshot_collector._instruments = [sample_instrument]
        snapshot_collector._rest_client.get_instruments = AsyncMock(
            return_value=[sample_instrument]
        )
        snapshot_collector._rest_client.batch_get_order_books = AsyncMock(
            return_value=[sample_order_book]
        )
        snapshot_collector._rest_client.get_greeks = AsyncMock(
            return_value=sample_greeks
        )
        snapshot_collector._rest_client.get_mark_price = AsyncMock(
            return_value=sample_mark_price
        )

        await snapshot_collector.force_snapshot()

        assert snapshot_collector.last_collection_time is not None
