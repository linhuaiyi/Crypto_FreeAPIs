"""Tests for data models."""

from datetime import datetime, timezone

import pytest

from deribit_options_collector.models import (
    CollectedData,
    GreeksData,
    MarkPriceData,
    OptionBook,
    OptionInstrument,
    OptionTicker,
    OptionType,
    OrderBookEntry,
    SettlementPeriod,
    SettlementPriceData,
    TradeData,
)


class TestOptionInstrument:
    """Tests for OptionInstrument model."""

    def test_create_instrument(self, sample_instrument: OptionInstrument) -> None:
        """Test creating an instrument."""
        assert sample_instrument.instrument_name == "BTC-28MAR26-80000-C"
        assert sample_instrument.currency == "BTC"
        assert sample_instrument.option_type == "call"
        assert sample_instrument.strike == 80000.0

    def test_instrument_is_frozen(self, sample_instrument: OptionInstrument) -> None:
        """Test that instrument is immutable."""
        with pytest.raises(AttributeError):
            sample_instrument.instrument_name = "new-name"

    def test_option_type_enum(self) -> None:
        """Test OptionType enum values."""
        assert OptionType.CALL == "call"
        assert OptionType.PUT == "put"

    def test_settlement_period_enum(self) -> None:
        """Test SettlementPeriod enum values."""
        assert SettlementPeriod.DAY == "day"
        assert SettlementPeriod.WEEK == "week"
        assert SettlementPeriod.MONTH == "month"
        assert SettlementPeriod.QUARTER == "quarter"


class TestOptionTicker:
    """Tests for OptionTicker model."""

    def test_create_ticker(self, sample_ticker: OptionTicker) -> None:
        """Test creating a ticker."""
        assert sample_ticker.instrument_name == "BTC-28MAR26-80000-C"
        assert sample_ticker.mark_price == 0.0254
        assert sample_ticker.bid_iv == 0.62
        assert sample_ticker.open_interest == 1250.0

    def test_ticker_is_frozen(self, sample_ticker: OptionTicker) -> None:
        """Test that ticker is immutable."""
        with pytest.raises(AttributeError):
            sample_ticker.mark_price = 0.03


class TestOrderBookEntry:
    """Tests for OrderBookEntry model."""

    def test_create_entry(self) -> None:
        """Test creating an order book entry."""
        entry = OrderBookEntry(price=0.025, amount=10.0, order_count=5)
        assert entry.price == 0.025
        assert entry.amount == 10.0
        assert entry.order_count == 5


class TestOptionBook:
    """Tests for OptionBook model."""

    def test_create_order_book(self, sample_order_book: OptionBook) -> None:
        """Test creating an order book."""
        assert sample_order_book.instrument_name == "BTC-28MAR26-80000-C"
        assert len(sample_order_book.bids) == 3
        assert len(sample_order_book.asks) == 3
        assert sample_order_book.current_best_bid == 0.0248

    def test_order_book_mutable(self, sample_order_book: OptionBook) -> None:
        """Test that order book is mutable."""
        sample_order_book.bids.append(OrderBookEntry(price=0.0235, amount=40.0, order_count=15))
        assert len(sample_order_book.bids) == 4


class TestTradeData:
    """Tests for TradeData model."""

    def test_create_trade(self, sample_trade: TradeData) -> None:
        """Test creating trade data."""
        assert sample_trade.trade_id == "100-abc"
        assert sample_trade.direction == "buy"
        assert sample_trade.price == 0.0254

    def test_trade_is_frozen(self, sample_trade: TradeData) -> None:
        """Test that trade is immutable."""
        with pytest.raises(AttributeError):
            sample_trade.price = 0.03


class TestGreeksData:
    """Tests for GreeksData model."""

    def test_create_greeks(self, sample_greeks: GreeksData) -> None:
        """Test creating Greeks data."""
        assert sample_greeks.delta == 0.45
        assert sample_greeks.gamma == 0.0012
        assert sample_greeks.vega == 0.25

    def test_greeks_is_frozen(self, sample_greeks: GreeksData) -> None:
        """Test that Greeks is immutable."""
        with pytest.raises(AttributeError):
            sample_greeks.delta = 0.5


class TestMarkPriceData:
    """Tests for MarkPriceData model."""

    def test_create_mark_price(self, sample_mark_price: MarkPriceData) -> None:
        """Test creating mark price."""
        assert sample_mark_price.mark_price == 0.0254
        assert sample_mark_price.index_price == 85000.0


class TestSettlementPriceData:
    """Tests for SettlementPriceData model."""

    def test_create_settlement(self, sample_settlement_price: SettlementPriceData) -> None:
        """Test creating settlement price."""
        assert sample_settlement_price.settlement_price == 0.0250
        assert sample_settlement_price.settlement_type == "final"


class TestCollectedData:
    """Tests for CollectedData container."""

    def test_empty_collection(self) -> None:
        """Test empty collection."""
        data = CollectedData()
        assert data.is_empty()
        assert data.record_count() == 0

    def test_collection_with_data(
        self,
        sample_ticker: OptionTicker,
        sample_order_book: OptionBook,
        sample_trade: TradeData,
        sample_greeks: GreeksData,
        sample_mark_price: MarkPriceData,
        sample_settlement_price: SettlementPriceData,
    ) -> None:
        """Test collection with various data types."""
        data = CollectedData(
            tickers=[sample_ticker],
            books=[sample_order_book],
            trades=[sample_trade],
            greeks=[sample_greeks],
            mark_prices=[sample_mark_price],
            settlement_prices=[sample_settlement_price],
        )
        assert not data.is_empty()
        assert data.record_count() == 6

    def test_collection_partial_data(self, sample_tickers: list[OptionTicker]) -> None:
        """Test collection with partial data."""
        data = CollectedData(tickers=sample_tickers)
        assert not data.is_empty()
        assert data.record_count() == 2

    def test_add_data_to_collection(self, sample_ticker: OptionTicker) -> None:
        """Test adding data to collection."""
        data = CollectedData()
        data.tickers.append(sample_ticker)
        assert len(data.tickers) == 1
        assert not data.is_empty()
