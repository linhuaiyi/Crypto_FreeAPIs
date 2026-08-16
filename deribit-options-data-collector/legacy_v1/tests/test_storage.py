"""Tests for storage modules."""

import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

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
from deribit_options_collector.storage.parquet_store import ParquetStore
from deribit_options_collector.storage.sqlite_store import SQLiteStore


class TestParquetStore:
    """Tests for ParquetStore."""

    @pytest.fixture
    def parquet_store(self, temp_dir: Path, mock_settings: Any) -> ParquetStore:
        """Create ParquetStore for testing."""
        mock_settings.storage.parquet.base_path = str(temp_dir / "parquet_data")
        return ParquetStore(mock_settings)

    def test_get_partition_path(self, parquet_store: ParquetStore) -> None:
        """Test getting partition path."""
        path = parquet_store._get_partition_path(
            "BTC-28MAR26-80000-C",
            datetime(2026, 3, 28, tzinfo=timezone.utc),
        )
        assert "BTC-28MAR26-80000-C" in str(path)
        assert "2026-03-28" in str(path)

    def test_ensure_dir(self, parquet_store: ParquetStore, temp_dir: Path) -> None:
        """Test directory creation."""
        test_path = temp_dir / "test" / "nested" / "path"
        parquet_store._ensure_dir(test_path)
        assert test_path.exists()
        assert test_path.is_dir()

    def test_save_tickers_empty(self, parquet_store: ParquetStore) -> None:
        """Test saving empty tickers list."""
        parquet_store.save_tickers([])
        assert True

    def test_save_single_ticker(
        self,
        parquet_store: ParquetStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test saving a single ticker."""
        parquet_store._save_ticker_single(sample_ticker)

        path = parquet_store._get_partition_path(sample_ticker.instrument_name)
        file_path = path / "tickers.parquet"
        assert file_path.exists()

    def test_save_multiple_tickers(
        self,
        parquet_store: ParquetStore,
        sample_tickers: list[OptionTicker],
    ) -> None:
        """Test saving multiple tickers."""
        parquet_store.save_tickers(sample_tickers)

        for ticker in sample_tickers:
            path = parquet_store._get_partition_path(ticker.instrument_name)
            file_path = path / "tickers.parquet"
            assert file_path.exists()

    def test_save_order_books_empty(self, parquet_store: ParquetStore) -> None:
        """Test saving empty order books list."""
        parquet_store.save_order_books([])
        assert True

    def test_save_single_order_book(
        self,
        parquet_store: ParquetStore,
        sample_order_book: OptionBook,
    ) -> None:
        """Test saving a single order book."""
        parquet_store._save_order_book_single(sample_order_book)

        path = parquet_store._get_partition_path(sample_order_book.instrument_name)
        file_path = path / "orderbook.parquet"
        assert file_path.exists()

    def test_save_trades(
        self,
        parquet_store: ParquetStore,
        sample_trade: TradeData,
    ) -> None:
        """Test saving trades."""
        parquet_store.save_trades([sample_trade])

        path = parquet_store._get_partition_path(sample_trade.instrument_name)
        file_path = path / "trades.parquet"
        assert file_path.exists()

    def test_save_greeks(
        self,
        parquet_store: ParquetStore,
        sample_greeks: GreeksData,
    ) -> None:
        """Test saving Greeks data."""
        parquet_store.save_greeks([sample_greeks])

        path = parquet_store._get_partition_path(sample_greeks.instrument_name)
        file_path = path / "greeks.parquet"
        assert file_path.exists()

    def test_save_mark_prices(
        self,
        parquet_store: ParquetStore,
        sample_mark_price: MarkPriceData,
    ) -> None:
        """Test saving mark prices."""
        parquet_store.save_mark_prices([sample_mark_price])

        path = parquet_store._get_partition_path(sample_mark_price.instrument_name)
        file_path = path / "markprice.parquet"
        assert file_path.exists()

    def test_save_settlement_prices(
        self,
        parquet_store: ParquetStore,
        sample_settlement_price: SettlementPriceData,
    ) -> None:
        """Test saving settlement prices."""
        parquet_store.save_settlement_prices([sample_settlement_price])

        path = parquet_store._get_partition_path(sample_settlement_price.instrument_name)
        file_path = path / "settlement.parquet"
        assert file_path.exists()

    def test_save_collected_data(
        self,
        parquet_store: ParquetStore,
        sample_ticker: OptionTicker,
        sample_order_book: OptionBook,
        sample_trade: TradeData,
        sample_greeks: GreeksData,
        sample_mark_price: MarkPriceData,
        sample_settlement_price: SettlementPriceData,
    ) -> None:
        """Test saving collected data."""
        data = CollectedData(
            tickers=[sample_ticker],
            books=[sample_order_book],
            trades=[sample_trade],
            greeks=[sample_greeks],
            mark_prices=[sample_mark_price],
            settlement_prices=[sample_settlement_price],
        )

        parquet_store.save_collected_data(data)

        path = parquet_store._get_partition_path(sample_ticker.instrument_name)
        assert (path / "tickers.parquet").exists()
        assert (path / "orderbook.parquet").exists()
        assert (path / "trades.parquet").exists()
        assert (path / "greeks.parquet").exists()
        assert (path / "markprice.parquet").exists()
        assert (path / "settlement.parquet").exists()

    def test_save_collected_data_empty(self, parquet_store: ParquetStore) -> None:
        """Test saving empty collected data."""
        data = CollectedData()
        parquet_store.save_collected_data(data)
        assert True

    def test_load_data(
        self,
        parquet_store: ParquetStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test loading data."""
        parquet_store.save_tickers([sample_ticker])

        df = parquet_store.load(
            sample_ticker.instrument_name,
            "tickers",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 12, 31, tzinfo=timezone.utc),
        )

        assert not df.empty
        assert len(df) >= 1

    def test_load_latest(
        self,
        parquet_store: ParquetStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test loading latest data."""
        parquet_store.save_tickers([sample_ticker])

        df = parquet_store.load_latest(sample_ticker.instrument_name, "tickers")
        assert df is not None
        assert not df.empty

    def test_load_no_data(self, parquet_store: ParquetStore) -> None:
        """Test loading when no data exists."""
        df = parquet_store.load(
            "NONEXISTENT-INSTRUMENT",
            "tickers",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 12, 31, tzinfo=timezone.utc),
        )
        assert df.empty

    def test_append_to_parquet_deduplication(
        self,
        parquet_store: ParquetStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test that appending duplicates doesn't create duplicates."""
        parquet_store._save_ticker_single(sample_ticker)
        parquet_store._save_ticker_single(sample_ticker)

        df = parquet_store.load_latest(sample_ticker.instrument_name, "tickers")
        assert df is not None


class TestSQLiteStore:
    """Tests for SQLiteStore."""

    @pytest.fixture
    def sqlite_store(self, temp_dir: Path, mock_settings: Any) -> SQLiteStore:
        """Create SQLiteStore for testing."""
        db_path = temp_dir / "test_deribit.db"
        mock_settings.storage.sqlite.path = str(db_path)
        return SQLiteStore(mock_settings)

    def test_init_database(self, sqlite_store: SQLiteStore) -> None:
        """Test database initialization."""
        conn = sqlite_store._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        assert "option_tickers" in tables
        assert "order_books" in tables
        assert "trades" in tables
        assert "greeks" in tables
        assert "mark_prices" in tables
        assert "settlement_prices" in tables
        assert "instruments" in tables

    def test_save_ticker(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test saving a ticker."""
        sqlite_store.save_ticker(sample_ticker)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT instrument_name, mark_price FROM option_tickers WHERE instrument_name = ?",
            (sample_ticker.instrument_name,),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == sample_ticker.instrument_name

    def test_save_tickers_batch(
        self,
        sqlite_store: SQLiteStore,
        sample_tickers: list[OptionTicker],
    ) -> None:
        """Test batch saving tickers."""
        count = sqlite_store.save_tickers_batch(sample_tickers)
        assert count == 2

    def test_save_tickers_batch_empty(self, sqlite_store: SQLiteStore) -> None:
        """Test batch saving empty list."""
        count = sqlite_store.save_tickers_batch([])
        assert count == 0

    def test_save_order_book(
        self,
        sqlite_store: SQLiteStore,
        sample_order_book: OptionBook,
    ) -> None:
        """Test saving an order book."""
        sqlite_store.save_order_book(sample_order_book)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT instrument_name FROM order_books WHERE instrument_name = ?",
            (sample_order_book.instrument_name,),
        )
        row = cursor.fetchone()
        assert row is not None

    def test_save_trade(
        self,
        sqlite_store: SQLiteStore,
        sample_trade: TradeData,
    ) -> None:
        """Test saving a trade."""
        sqlite_store.save_trade(sample_trade)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT trade_id FROM trades WHERE trade_id = ?",
            (sample_trade.trade_id,),
        )
        row = cursor.fetchone()
        assert row is not None

    def test_save_trade_duplicate_ignored(
        self,
        sqlite_store: SQLiteStore,
        sample_trade: TradeData,
    ) -> None:
        """Test that duplicate trades are ignored."""
        sqlite_store.save_trade(sample_trade)
        sqlite_store.save_trade(sample_trade)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM trades WHERE trade_id = ?",
            (sample_trade.trade_id,),
        )
        count = cursor.fetchone()[0]
        assert count == 1

    def test_save_greeks(
        self,
        sqlite_store: SQLiteStore,
        sample_greeks: GreeksData,
    ) -> None:
        """Test saving Greeks data."""
        sqlite_store.save_greeks(sample_greeks)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT instrument_name, delta FROM greeks WHERE instrument_name = ?",
            (sample_greeks.instrument_name,),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[1] == sample_greeks.delta

    def test_save_mark_price(
        self,
        sqlite_store: SQLiteStore,
        sample_mark_price: MarkPriceData,
    ) -> None:
        """Test saving mark price."""
        sqlite_store.save_mark_price(sample_mark_price)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT instrument_name FROM mark_prices WHERE instrument_name = ?",
            (sample_mark_price.instrument_name,),
        )
        row = cursor.fetchone()
        assert row is not None

    def test_save_settlement_price(
        self,
        sqlite_store: SQLiteStore,
        sample_settlement_price: SettlementPriceData,
    ) -> None:
        """Test saving settlement price."""
        sqlite_store.save_settlement_price(sample_settlement_price)

        cursor = sqlite_store._get_connection().cursor()
        cursor.execute(
            "SELECT instrument_name FROM settlement_prices WHERE instrument_name = ?",
            (sample_settlement_price.instrument_name,),
        )
        row = cursor.fetchone()
        assert row is not None

    def test_save_collected_data(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
        sample_order_book: OptionBook,
        sample_trade: TradeData,
        sample_greeks: GreeksData,
        sample_mark_price: MarkPriceData,
        sample_settlement_price: SettlementPriceData,
    ) -> None:
        """Test saving collected data."""
        data = CollectedData(
            tickers=[sample_ticker],
            books=[sample_order_book],
            trades=[sample_trade],
            greeks=[sample_greeks],
            mark_prices=[sample_mark_price],
            settlement_prices=[sample_settlement_price],
        )

        count = sqlite_store.save_collected_data(data)
        assert count >= 2

    def test_query_tickers(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test querying tickers."""
        sqlite_store.save_ticker(sample_ticker)

        df = sqlite_store.query_tickers(
            instrument_name=sample_ticker.instrument_name,
            limit=10,
        )

        assert not df.empty
        assert len(df) == 1

    def test_query_tickers_with_time_range(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test querying tickers with time range."""
        sqlite_store.save_ticker(sample_ticker)

        df = sqlite_store.query_tickers(
            instrument_name=sample_ticker.instrument_name,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
        )

        assert len(df) >= 1

    def test_get_latest_ticker(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test getting latest ticker."""
        sqlite_store.save_ticker(sample_ticker)

        result = sqlite_store.get_latest_ticker(sample_ticker.instrument_name)
        assert result is not None
        assert result["instrument_name"] == sample_ticker.instrument_name

    def test_get_latest_ticker_nonexistent(self, sqlite_store: SQLiteStore) -> None:
        """Test getting latest ticker for nonexistent instrument."""
        result = sqlite_store.get_latest_ticker("NONEXISTENT")
        assert result is None

    def test_get_ticker_count(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test getting ticker count."""
        sqlite_store.save_ticker(sample_ticker)

        count = sqlite_store.get_ticker_count()
        assert count >= 1

    def test_get_ticker_count_with_date(
        self,
        sqlite_store: SQLiteStore,
        sample_ticker: OptionTicker,
    ) -> None:
        """Test getting ticker count for specific date."""
        sqlite_store.save_ticker(sample_ticker)

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        count = sqlite_store.get_ticker_count(today)
        assert count >= 1

    def test_close(self, sqlite_store: SQLiteStore) -> None:
        """Test closing the store."""
        sqlite_store.close()
        assert len(sqlite_store._connection_pool) == 0

    def test_vacuum(self, sqlite_store: SQLiteStore) -> None:
        """Test vacuum operation."""
        sqlite_store.vacuum()
        assert True

    def test_indexes_created(self, sqlite_store: SQLiteStore) -> None:
        """Test that indexes are created."""
        conn = sqlite_store._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
        indexes = [row[0] for row in cursor.fetchall()]

        assert "idx_tickers_instrument_timestamp" in indexes
        assert "idx_trades_trade_id" in indexes
        assert "idx_greeks_instrument_timestamp" in indexes
