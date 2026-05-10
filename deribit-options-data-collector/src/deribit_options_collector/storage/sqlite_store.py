"""SQLite storage for Deribit options data with schema and indexes."""

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import pandas as pd
import structlog

from deribit_options_collector.config import Settings
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


class SQLiteStore:
    """SQLite storage with proper schema, indexes, and partitioning."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS option_tickers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instrument_name TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        underlying_price REAL,
        mark_price REAL,
        bid_price REAL,
        ask_price REAL,
        bid_iv REAL,
        ask_iv REAL,
        mark_iv REAL,
        open_interest REAL,
        volume_24h REAL,
        settlement_period TEXT,
        last_price REAL,
        high_price REAL,
        low_price REAL,
        total_volume REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(instrument_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS order_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instrument_name TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        underlying_price REAL,
        settlement_price REAL,
        current_best_bid REAL,
        current_best_ask REAL,
        current_timestamp INTEGER,
        state TEXT,
        bids_json TEXT,
        asks_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(instrument_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_seq INTEGER,
        trade_id TEXT NOT NULL UNIQUE,
        timestamp TEXT NOT NULL,
        instrument_name TEXT NOT NULL,
        direction TEXT,
        price REAL,
        amount REAL,
        trade_volume_usd REAL,
        trade_index_price REAL,
        inventory_index REAL,
        volatility REAL,
        interest_rate REAL,
        mark_price REAL,
        index_price REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS greeks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        instrument_name TEXT NOT NULL,
        underlying_price REAL,
        mark_price REAL,
        open_interest REAL,
        delta REAL,
        gamma REAL,
        rho REAL,
        theta REAL,
        vega REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(instrument_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS mark_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        instrument_name TEXT NOT NULL,
        mark_price REAL,
        index_price REAL,
        settlement_price REAL,
        underlying_price REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(instrument_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS settlement_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        instrument_name TEXT NOT NULL,
        settlement_price REAL,
        delivery_price REAL,
        settlement_type TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(instrument_name, timestamp)
    );

    CREATE TABLE IF NOT EXISTS instruments (
        instrument_name TEXT PRIMARY KEY,
        currency TEXT,
        kind TEXT,
        base_currency TEXT,
        quote_currency TEXT,
        contract_size REAL,
        option_type TEXT,
        strike REAL,
        expiration_timestamp INTEGER,
        expiration_date TEXT,
        settlement_period TEXT,
        is_active INTEGER,
        min_trade_amount REAL,
        tick_size REAL,
        maker_commission REAL,
        taker_commission REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_tickers_instrument_timestamp
        ON option_tickers(instrument_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_tickers_timestamp
        ON option_tickers(timestamp);
    CREATE INDEX IF NOT EXISTS idx_tickers_date
        ON option_tickers(substr(timestamp, 1, 10));

    CREATE INDEX IF NOT EXISTS idx_orderbooks_instrument_timestamp
        ON order_books(instrument_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_orderbooks_timestamp
        ON order_books(timestamp);

    CREATE INDEX IF NOT EXISTS idx_trades_instrument_timestamp
        ON trades(instrument_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_trades_timestamp
        ON trades(timestamp);
    CREATE INDEX IF NOT EXISTS idx_trades_trade_id
        ON trades(trade_id);

    CREATE INDEX IF NOT EXISTS idx_greeks_instrument_timestamp
        ON greeks(instrument_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_greeks_timestamp
        ON greeks(timestamp);

    CREATE INDEX IF NOT EXISTS idx_markprices_instrument_timestamp
        ON mark_prices(instrument_name, timestamp);
    CREATE INDEX IF NOT EXISTS idx_markprices_timestamp
        ON mark_prices(timestamp);

    CREATE INDEX IF NOT EXISTS idx_settlements_instrument_timestamp
        ON settlement_prices(instrument_name, timestamp);

    CREATE INDEX IF NOT EXISTS idx_instruments_currency
        ON instruments(currency);
    CREATE INDEX IF NOT EXISTS idx_instruments_expiration
        ON instruments(expiration_timestamp);
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._config = settings.storage.sqlite
        self._db_path = Path(self._config.path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection_pool: dict[int, sqlite3.Connection] = {}
        self._lock = threading.Lock()
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        thread_id = threading.get_ident()
        if thread_id not in self._connection_pool:
            conn = sqlite3.connect(
                self._db_path,
                timeout=self._config.timeout,
                check_same_thread=self._config.check_same_thread,
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            conn.execute("PRAGMA temp_store=MEMORY")
            self._connection_pool[thread_id] = conn
        return self._connection_pool[thread_id]

    def _init_database(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.executescript(self.SCHEMA)
        conn.commit()
        logger.info("database_initialized", path=str(self._db_path))

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for transactions."""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def save_ticker(self, ticker: OptionTicker) -> None:
        """Save a ticker record."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO option_tickers
            (instrument_name, timestamp, underlying_price, mark_price,
             bid_price, ask_price, bid_iv, ask_iv, mark_iv,
             open_interest, volume_24h, settlement_period,
             last_price, high_price, low_price, total_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker.instrument_name,
                ticker.timestamp.isoformat(),
                ticker.underlying_price,
                ticker.mark_price,
                ticker.bid_price,
                ticker.ask_price,
                ticker.bid_iv,
                ticker.ask_iv,
                ticker.mark_iv,
                ticker.open_interest,
                ticker.volume_24h,
                ticker.settlement_period,
                ticker.last,
                ticker.high,
                ticker.low,
                ticker.total_volume,
            ),
        )

    def save_tickers_batch(self, tickers: list[OptionTicker]) -> int:
        """Save multiple tickers in a batch."""
        if not tickers:
            return 0

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO option_tickers
            (instrument_name, timestamp, underlying_price, mark_price,
             bid_price, ask_price, bid_iv, ask_iv, mark_iv,
             open_interest, volume_24h, settlement_period,
             last_price, high_price, low_price, total_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    t.instrument_name,
                    t.timestamp.isoformat(),
                    t.underlying_price,
                    t.mark_price,
                    t.bid_price,
                    t.ask_price,
                    t.bid_iv,
                    t.ask_iv,
                    t.mark_iv,
                    t.open_interest,
                    t.volume_24h,
                    t.settlement_period,
                    t.last,
                    t.high,
                    t.low,
                    t.total_volume,
                )
                for t in tickers
            ],
        )
        return len(tickers)

    def save_order_book(self, book: OptionBook) -> None:
        """Save an order book record."""
        import json

        conn = self._get_connection()
        cursor = conn.cursor()

        bids_json = json.dumps(
            [
                {"price": b.price, "amount": b.amount, "order_count": b.order_count}
                for b in book.bids
            ]
        )
        asks_json = json.dumps(
            [
                {"price": a.price, "amount": a.amount, "order_count": a.order_count}
                for a in book.asks
            ]
        )

        cursor.execute(
            """
            INSERT OR REPLACE INTO order_books
            (instrument_name, timestamp, underlying_price, settlement_price,
             current_best_bid, current_best_ask, current_timestamp, state,
             bids_json, asks_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                book.instrument_name,
                book.timestamp.isoformat(),
                book.underlying_price,
                book.settlement_price,
                book.current_best_bid,
                book.current_best_ask,
                book.current_timestamp,
                book.state,
                bids_json,
                asks_json,
            ),
        )

    def save_trade(self, trade: TradeData) -> None:
        """Save a trade record."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO trades
                (trade_seq, trade_id, timestamp, instrument_name, direction,
                 price, amount, trade_volume_usd, trade_index_price,
                 inventory_index, volatility, interest_rate, mark_price, index_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.trade_seq,
                    trade.trade_id,
                    trade.timestamp.isoformat(),
                    trade.instrument_name,
                    trade.direction,
                    trade.price,
                    trade.amount,
                    trade.trade_volume_usd,
                    trade.trade_index_price,
                    trade.inventory_index,
                    trade.volatility,
                    trade.interest_rate,
                    trade.mark_price,
                    trade.index_price,
                ),
            )
        except sqlite3.IntegrityError:
            logger.debug("duplicate_trade_skipped", trade_id=trade.trade_id)

    def save_greeks(self, greeks: GreeksData) -> None:
        """Save a Greeks record."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO greeks
            (timestamp, instrument_name, underlying_price, mark_price,
             open_interest, delta, gamma, rho, theta, vega)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                greeks.timestamp.isoformat(),
                greeks.instrument_name,
                greeks.underlying_price,
                greeks.mark_price,
                greeks.open_interest,
                greeks.delta,
                greeks.gamma,
                greeks.rho,
                greeks.theta,
                greeks.vega,
            ),
        )

    def save_mark_price(self, mark_price: MarkPriceData) -> None:
        """Save a mark price record."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO mark_prices
            (timestamp, instrument_name, mark_price, index_price,
             settlement_price, underlying_price)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                mark_price.timestamp.isoformat(),
                mark_price.instrument_name,
                mark_price.mark_price,
                mark_price.index_price,
                mark_price.settlement_price,
                mark_price.underlying_price,
            ),
        )

    def save_settlement_price(self, settlement: SettlementPriceData) -> None:
        """Save a settlement price record."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO settlement_prices
            (timestamp, instrument_name, settlement_price, delivery_price, settlement_type)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                settlement.timestamp.isoformat(),
                settlement.instrument_name,
                settlement.settlement_price,
                settlement.delivery_price,
                settlement.settlement_type,
            ),
        )

    def save_collected_data(self, data: CollectedData) -> int:
        """Save all collected data to SQLite."""
        total = 0

        if data.tickers:
            total += self.save_tickers_batch(data.tickers)

        for book in data.books:
            self.save_order_book(book)
            total += 1

        for trade in data.trades:
            self.save_trade(trade)
            total += 1

        for greeks in data.greeks:
            self.save_greeks(greeks)
            total += 1

        for mp in data.mark_prices:
            self.save_mark_price(mp)
            total += 1

        for sp in data.settlement_prices:
            self.save_settlement_price(sp)
            total += 1

        logger.info("data_saved_to_sqlite", record_count=total)
        return total

    def query_tickers(
        self,
        instrument_name: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Query ticker data."""
        conn = self._get_connection()
        query = "SELECT * FROM option_tickers WHERE 1=1"
        params: list[Any] = []

        if instrument_name:
            query += " AND instrument_name = ?"
            params.append(instrument_name)
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        return pd.read_sql_query(query, conn, params=params)

    def get_latest_ticker(self, instrument_name: str) -> pd.Series | None:
        """Get the latest ticker for an instrument."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM option_tickers
            WHERE instrument_name = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (instrument_name,),
        )
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]  # type: ignore[union-attr]
            return pd.Series(dict(zip(columns, row)))
        return None

    def get_ticker_count(self, date: str | None = None) -> int:
        """Get ticker count for a date."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if date:
            cursor.execute(
                "SELECT COUNT(*) FROM option_tickers WHERE timestamp LIKE ?",
                (f"{date}%",),
            )
        else:
            cursor.execute("SELECT COUNT(*) FROM option_tickers")
        return cursor.fetchone()[0]

    def close(self) -> None:
        """Close all connections."""
        with self._lock:
            for conn in self._connection_pool.values():
                try:
                    conn.close()
                except Exception:
                    pass
            self._connection_pool.clear()

    def vacuum(self) -> None:
        """Optimize database with VACUUM."""
        conn = self._get_connection()
        conn.execute("VACUUM")
        logger.info("database_vacuumed")
