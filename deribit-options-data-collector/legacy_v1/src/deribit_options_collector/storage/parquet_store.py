"""Parquet storage for Deribit options data."""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from deribit_options_collector.config import Settings
from deribit_options_collector.models import (
    CollectedData,
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionTicker,
    SettlementPriceData,
    TradeData,
)

logger = structlog.get_logger(__name__)


class ParquetStore:
    """Parquet file storage with partition by instrument and date."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._config = settings.storage.parquet
        self._base_path = Path(self._config.base_path)
        self._compression = self._config.compression
        self._block_size = self._config.block_size_bytes
        self._row_group_size = self._config.row_group_size

    def _get_partition_path(
        self,
        instrument_name: str,
        date: datetime | None = None,
    ) -> Path:
        """Get partition path for an instrument and date."""
        if date is None:
            date = datetime.now(timezone.utc)

        date_str = date.strftime("%Y-%m-%d")
        instrument_dir = instrument_name.replace("/", "-").replace("_", "-")
        return self._base_path / instrument_dir / date_str

    def _ensure_dir(self, path: Path) -> None:
        """Ensure directory exists."""
        path.mkdir(parents=True, exist_ok=True)

    def save_tickers(
        self,
        tickers: list[OptionTicker],
        date: datetime | None = None,
    ) -> None:
        """Save ticker data to Parquet file."""
        if not tickers:
            return

        for ticker in tickers:
            self._save_ticker_single(ticker, date)

    def _save_ticker_single(
        self,
        ticker: OptionTicker,
        date: datetime | None = None,
    ) -> None:
        """Save a single ticker to its partition file."""
        if date is None:
            date = ticker.timestamp

        partition_path = self._get_partition_path(ticker.instrument_name, date)
        self._ensure_dir(partition_path)
        file_path = partition_path / "tickers.parquet"

        data = {
            "instrument_name": [ticker.instrument_name],
            "timestamp": [pd.Timestamp(ticker.timestamp)],
            "underlying_price": [ticker.underlying_price],
            "mark_price": [ticker.mark_price],
            "bid_price": [ticker.bid_price],
            "ask_price": [ticker.ask_price],
            "bid_iv": [ticker.bid_iv],
            "ask_iv": [ticker.ask_iv],
            "mark_iv": [ticker.mark_iv],
            "open_interest": [ticker.open_interest],
            "volume_24h": [ticker.volume_24h],
            "settlement_period": [ticker.settlement_period],
            "last": [ticker.last],
            "high": [ticker.high],
            "low": [ticker.low],
            "total_volume": [ticker.total_volume],
        }

        df = pd.DataFrame(data)
        self._append_to_parquet(df, file_path, ["instrument_name", "timestamp"])

    def save_order_books(
        self,
        books: list[OptionBook],
        date: datetime | None = None,
    ) -> None:
        """Save order book data to Parquet file."""
        if not books:
            return

        for book in books:
            self._save_order_book_single(book, date)

    def _save_order_book_single(
        self,
        book: OptionBook,
        date: datetime | None = None,
    ) -> None:
        """Save a single order book to its partition file."""
        if date is None:
            date = book.timestamp

        partition_path = self._get_partition_path(book.instrument_name, date)
        self._ensure_dir(partition_path)
        file_path = partition_path / "orderbook.parquet"

        bids_data = [
            {
                "price": b.price,
                "amount": b.amount,
                "order_count": b.order_count,
            }
            for b in book.bids
        ]
        asks_data = [
            {
                "price": a.price,
                "amount": a.amount,
                "order_count": a.order_count,
            }
            for a in book.asks
        ]

        data = {
            "instrument_name": [book.instrument_name],
            "timestamp": [pd.Timestamp(book.timestamp)],
            "underlying_price": [book.underlying_price],
            "settlement_price": [book.settlement_price],
            "current_best_bid": [book.current_best_bid],
            "current_best_ask": [book.current_best_ask],
            "current_timestamp": [book.current_timestamp],
            "state": [book.state],
            "bids": [bids_data],
            "asks": [asks_data],
        }

        df = pd.DataFrame(data)
        self._append_to_parquet(df, file_path, ["instrument_name", "timestamp"])

    def save_trades(
        self,
        trades: list[TradeData],
        date: datetime | None = None,
    ) -> None:
        """Save trade data to Parquet file."""
        if not trades:
            return

        if date is None:
            date = trades[0].timestamp

        for trade in trades:
            partition_path = self._get_partition_path(trade.instrument_name, date)
            self._ensure_dir(partition_path)
            file_path = partition_path / "trades.parquet"

            data = {
                "trade_seq": [trade.trade_seq],
                "trade_id": [trade.trade_id],
                "timestamp": [pd.Timestamp(trade.timestamp)],
                "instrument_name": [trade.instrument_name],
                "direction": [trade.direction],
                "price": [trade.price],
                "amount": [trade.amount],
                "trade_volume_usd": [trade.trade_volume_usd],
                "trade_index_price": [trade.trade_index_price],
                "inventory_index": [trade.inventory_index],
                "volatility": [trade.volatility],
                "interest_rate": [trade.interest_rate],
                "mark_price": [trade.mark_price],
                "index_price": [trade.index_price],
            }

            df = pd.DataFrame(data)
            self._append_to_parquet(df, file_path, ["instrument_name", "trade_id"])

    def save_greeks(
        self,
        greeks_list: list[GreeksData],
        date: datetime | None = None,
    ) -> None:
        """Save Greeks data to Parquet file."""
        if not greeks_list:
            return

        for greeks in greeks_list:
            partition_path = self._get_partition_path(greeks.instrument_name, date)
            self._ensure_dir(partition_path)
            file_path = partition_path / "greeks.parquet"

            data = {
                "timestamp": [pd.Timestamp(greeks.timestamp)],
                "instrument_name": [greeks.instrument_name],
                "underlying_price": [greeks.underlying_price],
                "mark_price": [greeks.mark_price],
                "open_interest": [greeks.open_interest],
                "delta": [greeks.delta],
                "gamma": [greeks.gamma],
                "rho": [greeks.rho],
                "theta": [greeks.theta],
                "vega": [greeks.vega],
            }

            df = pd.DataFrame(data)
            self._append_to_parquet(df, file_path, ["instrument_name", "timestamp"])

    def save_mark_prices(
        self,
        mark_prices: list[MarkPriceData],
        date: datetime | None = None,
    ) -> None:
        """Save mark price data to Parquet file."""
        if not mark_prices:
            return

        for mp in mark_prices:
            partition_path = self._get_partition_path(mp.instrument_name, date)
            self._ensure_dir(partition_path)
            file_path = partition_path / "markprice.parquet"

            data = {
                "timestamp": [pd.Timestamp(mp.timestamp)],
                "instrument_name": [mp.instrument_name],
                "mark_price": [mp.mark_price],
                "index_price": [mp.index_price],
                "settlement_price": [mp.settlement_price],
                "underlying_price": [mp.underlying_price],
            }

            df = pd.DataFrame(data)
            self._append_to_parquet(df, file_path, ["instrument_name", "timestamp"])

    def save_settlement_prices(
        self,
        settlements: list[SettlementPriceData],
        date: datetime | None = None,
    ) -> None:
        """Save settlement price data to Parquet file."""
        if not settlements:
            return

        for sp in settlements:
            partition_path = self._get_partition_path(sp.instrument_name, date)
            self._ensure_dir(partition_path)
            file_path = partition_path / "settlement.parquet"

            data = {
                "timestamp": [pd.Timestamp(sp.timestamp)],
                "instrument_name": [sp.instrument_name],
                "settlement_price": [sp.settlement_price],
                "delivery_price": [sp.delivery_price],
                "settlement_type": [sp.settlement_type],
            }

            df = pd.DataFrame(data)
            self._append_to_parquet(df, file_path, ["instrument_name", "timestamp"])

    def save_collected_data(
        self,
        data: CollectedData,
        date: datetime | None = None,
    ) -> None:
        """Save all collected data to appropriate Parquet files."""
        if data.is_empty():
            return

        self.save_tickers(data.tickers, date)
        self.save_order_books(data.books, date)
        self.save_trades(data.trades, date)
        self.save_greeks(data.greeks, date)
        self.save_mark_prices(data.mark_prices, date)
        self.save_settlement_prices(data.settlement_prices, date)

        logger.info(
            "data_saved_to_parquet",
            ticker_count=len(data.tickers),
            book_count=len(data.books),
            trade_count=len(data.trades),
            greeks_count=len(data.greeks),
            mark_price_count=len(data.mark_prices),
            settlement_count=len(data.settlement_prices),
        )

    def _append_to_parquet(
        self,
        df: pd.DataFrame,
        file_path: Path,
        unique_columns: list[str],
    ) -> None:
        """Append data to Parquet file with deduplication."""
        if df.empty:
            return

        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=unique_columns,
                keep="last",
            )
        else:
            combined_df = df

        table = pa.Table.from_pandas(combined_df)

        writer = pq.ParquetWriter(
            file_path,
            table.schema,
            compression=self._compression.upper(),
            use_dictionary=True,
            write_statistics=True,
        )
        writer.write_table(table)
        writer.close()

    def load(
        self,
        instrument_name: str,
        data_type: str,
        start_date: datetime | str,
        end_date: datetime | str,
    ) -> pd.DataFrame:
        """Load data for a date range."""
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        dfs = []
        current_date = start_date

        while current_date <= end_date:
            partition_path = self._get_partition_path(instrument_name, current_date)
            file_path = partition_path / f"{data_type}.parquet"

            if file_path.exists():
                df = pd.read_parquet(file_path)
                dfs.append(df)

            current_date = pd.Timedelta(days=1) + current_date  # type: ignore[assignment]

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def load_latest(
        self,
        instrument_name: str,
        data_type: str,
    ) -> Optional[pd.DataFrame]:
        """Load the latest data for an instrument."""
        date = datetime.now(timezone.utc)
        for _ in range(30):
            partition_path = self._get_partition_path(instrument_name, date)
            file_path = partition_path / f"{data_type}.parquet"

            if file_path.exists():
                return pd.read_parquet(file_path)

            date = date - pd.Timedelta(days=1)

        return None

    def get_partition_path(self) -> Path:
        """Get the base partition path."""
        return self._base_path
