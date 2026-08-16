"""Test fixtures and configuration."""

import asyncio
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from deribit_options_collector.config import Settings
from deribit_options_collector.models import (
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionInstrument,
    OptionTicker,
    OrderBookEntry,
    SettlementPriceData,
    TradeData,
)


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def settings(temp_dir: Path) -> Settings:
    """Create test settings."""
    return Settings(
        deribit=MagicMock(
            base_url="https://test.deribit.com",
            ws_url="wss://test.deribit.com/ws/api/v2",
            api_key="test_key",
            api_secret="test_secret",
            timeout_seconds=30,
            max_retries=3,
            retry_base_delay=0.1,
            rate_limit=MagicMock(
                requests_per_second=20.0,
                batch_delay_ms=50,
            ),
        ),
        collection=MagicMock(
            currencies=["BTC", "ETH"],
            kind="option",
            incremental_interval_seconds=1,
            snapshot_cron="0 8 * * *",
            snapshot_depth=20,
            channels=["ticker", "book", "trades", "markprice", "greeks"],
        ),
        storage=MagicMock(
            parquet=MagicMock(
                base_path=str(temp_dir / "data" / "raw" / "option"),
                compression="snappy",
                block_size_mb=128,
                row_group_size=100000,
                block_size_bytes=128 * 1024 * 1024,
            ),
            sqlite=MagicMock(
                path=str(temp_dir / "db" / "test.db"),
                pool_size=5,
                timeout=30.0,
                check_same_thread=False,
            ),
        ),
        metrics=MagicMock(
            enabled=True,
            port=9090,
            path="/metrics",
            health_port=8080,
            health_path="/health",
            labels=MagicMock(
                service="test-collector",
                environment="test",
            ),
        ),
        alerts=MagicMock(
            pagerduty=MagicMock(
                enabled=False,
                routing_key="",
                severity="critical",
            ),
            ws_disconnect_threshold_seconds=30,
            write_failure_threshold=5,
        ),
        logging=MagicMock(
            level="DEBUG",
            format="json",
            output="stdout",
            file_path="",
            rotation=MagicMock(
                max_bytes=100 * 1024 * 1024,
                backup_count=5,
            ),
        ),
        whitelist=MagicMock(
            instruments=[],
            currencies=["BTC", "ETH"],
        ),
    )


@pytest.fixture
def mock_settings() -> Settings:
    """Create mock settings without temp_dir dependency."""
    return Settings()


@pytest.fixture
def sample_instrument() -> OptionInstrument:
    """Create sample instrument."""
    return OptionInstrument(
        instrument_name="BTC-28MAR26-80000-C",
        currency="BTC",
        kind="option",
        base_currency="BTC",
        quote_currency="USD",
        contract_size=1.0,
        option_type="call",
        strike=80000.0,
        expiration_timestamp=1743206400000,
        expiration_date=datetime(2026, 3, 28, 0, 0, 0, tzinfo=timezone.utc),
        settlement_period="month",
        is_active=True,
        min_trade_amount=0.1,
        tick_size=0.0001,
        maker_commission=0.0003,
        taker_commission=0.0005,
    )


@pytest.fixture
def sample_ticker() -> OptionTicker:
    """Create sample ticker."""
    return OptionTicker(
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
        last=0.0255,
        high=0.0280,
        low=0.0230,
    )


@pytest.fixture
def sample_tickers() -> list[OptionTicker]:
    """Create sample tickers list."""
    return [
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
        ),
        OptionTicker(
            instrument_name="BTC-28MAR26-85000-P",
            timestamp=datetime.now(timezone.utc),
            underlying_price=85000.0,
            mark_price=0.0300,
            bid_price=0.0295,
            ask_price=0.0305,
            bid_iv=0.65,
            ask_iv=0.67,
            mark_iv=0.66,
            open_interest=1000.0,
            volume_24h=250.0,
            settlement_period="month",
        ),
    ]


@pytest.fixture
def sample_order_book() -> OptionBook:
    """Create sample order book."""
    return OptionBook(
        instrument_name="BTC-28MAR26-80000-C",
        timestamp=datetime.now(timezone.utc),
        underlying_price=85000.0,
        settlement_price=0.025,
        bids=[
            OrderBookEntry(price=0.0248, amount=10.0, order_count=5),
            OrderBookEntry(price=0.0245, amount=20.0, order_count=8),
            OrderBookEntry(price=0.0240, amount=30.0, order_count=12),
        ],
        asks=[
            OrderBookEntry(price=0.0260, amount=15.0, order_count=6),
            OrderBookEntry(price=0.0265, amount=25.0, order_count=10),
            OrderBookEntry(price=0.0270, amount=35.0, order_count=14),
        ],
        current_best_bid=0.0248,
        current_best_ask=0.0260,
        current_timestamp=1234567890,
        state="open",
    )


@pytest.fixture
def sample_trade() -> TradeData:
    """Create sample trade."""
    return TradeData(
        trade_seq=100,
        trade_id="100-abc",
        timestamp=datetime.now(timezone.utc),
        instrument_name="BTC-28MAR26-80000-C",
        direction="buy",
        price=0.0254,
        amount=1.0,
        trade_volume_usd=850.0,
        trade_index_price=85000.0,
        inventory_index=85000.0,
    )


@pytest.fixture
def sample_greeks() -> GreeksData:
    """Create sample Greeks."""
    return GreeksData(
        timestamp=datetime.now(timezone.utc),
        instrument_name="BTC-28MAR26-80000-C",
        underlying_price=85000.0,
        mark_price=0.0254,
        open_interest=1250.0,
        delta=0.45,
        gamma=0.0012,
        rho=0.0035,
        theta=-0.015,
        vega=0.25,
    )


@pytest.fixture
def sample_mark_price() -> MarkPriceData:
    """Create sample mark price."""
    return MarkPriceData(
        timestamp=datetime.now(timezone.utc),
        instrument_name="BTC-28MAR26-80000-C",
        mark_price=0.0254,
        index_price=85000.0,
        settlement_price=0.0250,
        underlying_price=85000.0,
    )


@pytest.fixture
def sample_settlement_price() -> SettlementPriceData:
    """Create sample settlement price."""
    return SettlementPriceData(
        timestamp=datetime.now(timezone.utc),
        instrument_name="BTC-28MAR26-80000-C",
        settlement_price=0.0250,
        delivery_price=0.0252,
        settlement_type="final",
    )


@pytest.fixture
def mock_rest_client() -> AsyncMock:
    """Create mock REST client."""
    client = AsyncMock()
    client.get_instruments = AsyncMock(return_value=[])
    client.get_ticker = AsyncMock()
    client.batch_get_tickers = AsyncMock(return_value=[])
    client.get_order_book = AsyncMock()
    client.batch_get_order_books = AsyncMock(return_value=[])
    client.get_greeks = AsyncMock()
    client.get_mark_price = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_ws_client() -> AsyncMock:
    """Create mock WebSocket client."""
    client = AsyncMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.subscribe = AsyncMock()
    client.listen = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    client._on_data = None
    return client
