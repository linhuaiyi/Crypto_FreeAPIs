"""Data models for Deribit options data collection."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class OptionType(str, Enum):
    """Option type enumeration."""

    CALL = "call"
    PUT = "put"


class SettlementPeriod(str, Enum):
    """Settlement period enumeration."""

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"


@dataclass(frozen=True)
class OptionInstrument:
    """Deribit option instrument metadata."""

    instrument_name: str
    currency: str
    kind: str
    base_currency: str
    quote_currency: str
    contract_size: float
    option_type: OptionType
    strike: float
    expiration_timestamp: int
    expiration_date: datetime
    settlement_period: SettlementPeriod
    is_active: bool
    min_trade_amount: float
    tick_size: float
    maker_commission: float
    taker_commission: float


@dataclass(frozen=True)
class OptionTicker:
    """Single option contract snapshot data."""

    instrument_name: str
    timestamp: datetime
    underlying_price: float
    mark_price: float
    bid_price: float
    ask_price: float
    bid_iv: float
    ask_iv: float
    mark_iv: float
    open_interest: float
    volume_24h: float
    settlement_period: str
    last: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    total_volume: Optional[float] = None


@dataclass(frozen=True)
class OrderBookEntry:
    """Single order book level."""

    price: float
    amount: float
    order_count: int


@dataclass
class OptionBook:
    """Full order book data for an option."""

    instrument_name: str
    timestamp: datetime
    underlying_price: float
    settlement_price: float
    bids: list[OrderBookEntry] = field(default_factory=list)
    asks: list[OrderBookEntry] = field(default_factory=list)
    current_best_bid: Optional[float] = None
    current_best_ask: Optional[float] = None
    current_timestamp: Optional[int] = None
    state: Optional[str] = None


@dataclass(frozen=True)
class TradeData:
    """Trade data for an option."""

    trade_seq: int
    trade_id: str
    timestamp: datetime
    instrument_name: str
    direction: str
    price: float
    amount: float
    trade_volume_usd: float
    trade_index_price: float
    inventory_index: float
    volatility: Optional[float] = None
    interest_rate: Optional[float] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None


@dataclass(frozen=True)
class GreeksData:
    """Greeks data for an option."""

    timestamp: datetime
    instrument_name: str
    underlying_price: float
    mark_price: float
    open_interest: float
    delta: float
    gamma: float
    rho: float
    theta: float
    vega: float


@dataclass(frozen=True)
class MarkPriceData:
    """Mark price data for an option."""

    timestamp: datetime
    instrument_name: str
    mark_price: float
    index_price: float
    settlement_price: float
    underlying_price: float


@dataclass(frozen=True)
class SettlementPriceData:
    """Settlement price data for an option."""

    timestamp: datetime
    instrument_name: str
    settlement_price: float
    delivery_price: float
    settlement_type: str


@dataclass
class CollectedData:
    """Container for collected data from multiple sources."""

    tickers: list[OptionTicker] = field(default_factory=list)
    books: list[OptionBook] = field(default_factory=list)
    trades: list[TradeData] = field(default_factory=list)
    greeks: list[GreeksData] = field(default_factory=list)
    mark_prices: list[MarkPriceData] = field(default_factory=list)
    settlement_prices: list[SettlementPriceData] = field(default_factory=list)

    def is_empty(self) -> bool:
        """Check if no data has been collected."""
        return (
            len(self.tickers) == 0
            and len(self.books) == 0
            and len(self.trades) == 0
            and len(self.greeks) == 0
            and len(self.mark_prices) == 0
            and len(self.settlement_prices) == 0
        )

    def record_count(self) -> int:
        """Get total record count across all data types."""
        return (
            len(self.tickers)
            + len(self.books)
            + len(self.trades)
            + len(self.greeks)
            + len(self.mark_prices)
            + len(self.settlement_prices)
        )
