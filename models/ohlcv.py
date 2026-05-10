from dataclasses import dataclass
from typing import Optional


@dataclass
class OHLCV:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    exchange: str
    symbol: str
    timeframe: str
    trades: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'quote_volume': self.quote_volume,
            'exchange': self.exchange,
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'trades': self.trades,
        }

    @classmethod
    def from_dict(cls, d: dict) -> 'OHLCV':
        return cls(
            timestamp=d['timestamp'],
            open=d['open'],
            high=d['high'],
            low=d['low'],
            close=d['close'],
            volume=d['volume'],
            quote_volume=d['quote_volume'],
            exchange=d['exchange'],
            symbol=d['symbol'],
            timeframe=d['timeframe'],
            trades=d.get('trades'),
        )
