"""Deribit REST API client with retry logic."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import structlog
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tenacity.asyncio import AsyncRetrying

from deribit_options_collector.config import DeribitConfig, Settings
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

logger = structlog.get_logger(__name__)


class DeribitAPIError(Exception):
    """Deribit API error."""

    def __init__(self, message: str, code: int | None = None, data: Any = None):
        super().__init__(message)
        self.code = code
        self.data = data


class RateLimitError(DeribitAPIError):
    """Rate limit exceeded error."""

    pass


class DeribitRestClient:
    """Asynchronous Deribit REST API client."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._config = settings.deribit
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter_lock = asyncio.Lock()
        self._last_request_time = 0.0
        self._request_interval = 1.0 / self._config.rate_limit.requests_per_second

    async def __aenter__(self) -> "DeribitRestClient":
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        await self.close()

    async def start(self) -> None:
        """Start the HTTP session."""
        if self._session is None:
            timeout = aiohttp.ClientTimeout(
                total=self._config.timeout_seconds,
                connect=self._config.timeout_seconds / 2,
            )
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Make an authenticated API request with retry logic."""
        if self._session is None:
            await self.start()

        url = f"{self._config.base_url}/api/v2{endpoint}"

        async with AsyncRetrying(
            retry=retry_if_exception_type((aiohttp.ClientError, RateLimitError)),
            stop=stop_after_attempt(self._config.max_retries),
            wait=wait_exponential(
                multiplier=self._config.retry_base_delay,
                max=30,
            ),
            reraise=True,
        ):
            try:
                await self._rate_limit()

                async with self._session.request(  # type: ignore[union-attr]
                    method=method,
                    url=url,
                    params=params,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    data = await response.json()

                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After", "60")
                        wait_time = float(retry_after)
                        logger.warning(
                            "rate_limit_exceeded",
                            url=url,
                            retry_after=wait_time,
                        )
                        await asyncio.sleep(wait_time)
                        raise RateLimitError("Rate limit exceeded", code=429)

                    if response.status >= 400:
                        error_msg = data.get("error", {})
                        raise DeribitAPIError(
                            message=error_msg.get("message", "Unknown error"),
                            code=error_msg.get("code"),
                            data=error_msg,
                        )

                    return data

            except aiohttp.ClientError as e:
                logger.error("api_request_failed", url=url, error=str(e))
                raise

    async def _rate_limit(self) -> None:
        """Apply rate limiting to requests."""
        async with self._rate_limiter_lock:
            now = time.monotonic()
            time_since_last = now - self._last_request_time
            if time_since_last < self._request_interval:
                await asyncio.sleep(self._request_interval - time_since_last)
            self._last_request_time = time.monotonic()

    async def get_instruments(
        self,
        currency: str,
        kind: str = "option",
    ) -> list[OptionInstrument]:
        """Get all available instruments for a currency."""
        params: dict[str, Any] = {"currency": currency, "kind": kind}
        response = await self._request("GET", "/public/get_instruments", params)

        instruments = []
        for item in response.get("result", []):
            expiration_dt = datetime.fromtimestamp(
                item["expiration_timestamp"] / 1000, tz=timezone.utc
            )
            instruments.append(
                OptionInstrument(
                    instrument_name=item["instrument_name"],
                    currency=item["base_currency"],
                    kind=item["kind"],
                    base_currency=item["base_currency"],
                    quote_currency=item["quote_currency"],
                    contract_size=item["contract_size"],
                    option_type=item["option_type"],
                    strike=item["strike"],
                    expiration_timestamp=item["expiration_timestamp"],
                    expiration_date=expiration_dt,
                    settlement_period=item["settlement_period"],
                    is_active=item.get("is_active", True),
                    min_trade_amount=item["min_trade_amount"],
                    tick_size=item["tick_size"],
                    maker_commission=item.get("maker_commission", 0.0),
                    taker_commission=item.get("taker_commission", 0.0),
                )
            )
        return instruments

    async def get_ticker(self, instrument_name: str) -> OptionTicker:
        """Get ticker data for an instrument."""
        params = {"instrument_name": instrument_name}
        response = await self._request("GET", "/public/ticker", params)
        result = response["result"]

        timestamp = datetime.fromtimestamp(
            result["timestamp"] / 1000, tz=timezone.utc
        )

        return OptionTicker(
            instrument_name=result["instrument_name"],
            timestamp=timestamp,
            underlying_price=result.get("underlying_price", 0.0),
            mark_price=result.get("mark_price", 0.0),
            bid_price=result.get("bid_price", 0.0),
            ask_price=result.get("ask_price", 0.0),
            bid_iv=result.get("bid_iv", 0.0),
            ask_iv=result.get("ask_iv", 0.0),
            mark_iv=result.get("mark_iv", 0.0),
            open_interest=result.get("open_interest", 0.0),
            volume_24h=result.get("stats", {}).get("volume", 0.0),
            settlement_period=result.get("settlement_period", "day"),
            last=result.get("last", None),
            high=result.get("high", None),
            low=result.get("low", None),
            total_volume=result.get("total_volume", None),
        )

    async def get_order_book(
        self,
        instrument_name: str,
        depth: int = 20,
    ) -> OptionBook:
        """Get order book for an instrument."""
        params = {
            "instrument_name": instrument_name,
            "depth": depth,
        }
        response = await self._request("GET", "/public/get_order_book", params)
        result = response["result"]

        timestamp = datetime.fromtimestamp(
            result.get("timestamp", 0) / 1000, tz=timezone.utc
        )

        bids = [
            OrderBookEntry(
                price=bid[0],
                amount=bid[1],
                order_count=bid[2] if len(bid) > 2 else 0,
            )
            for bid in result.get("bids", [])
        ]

        asks = [
            OrderBookEntry(
                price=ask[0],
                amount=ask[1],
                order_count=ask[2] if len(ask) > 2 else 0,
            )
            for ask in result.get("asks", [])
        ]

        return OptionBook(
            instrument_name=result["instrument_name"],
            timestamp=timestamp,
            underlying_price=result.get("underlying_price", 0.0),
            settlement_price=result.get("settlement_price", 0.0),
            bids=bids,
            asks=asks,
            current_best_bid=result.get("current_best_bid"),
            current_best_ask=result.get("current_best_ask"),
            current_timestamp=result.get("current_timestamp"),
            state=result.get("state"),
        )

    async def get_trades(
        self,
        instrument_name: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        count: int = 100,
    ) -> list[TradeData]:
        """Get recent trades for an instrument."""
        params: dict[str, Any] = {
            "instrument_name": instrument_name,
            "count": count,
        }
        if start_timestamp:
            params["start_timestamp"] = start_timestamp
        if end_timestamp:
            params["end_timestamp"] = end_timestamp

        response = await self._request("GET", "/public/get_last_trades_by_instrument", params)

        trades = []
        for item in response.get("result", {}).get("trades", []):
            trades.append(
                TradeData(
                    trade_seq=item["trade_seq"],
                    trade_id=item["trade_id"],
                    timestamp=datetime.fromtimestamp(
                        item["timestamp"] / 1000, tz=timezone.utc
                    ),
                    instrument_name=item["instrument_name"],
                    direction=item["direction"],
                    price=item["price"],
                    amount=item["amount"],
                    trade_volume_usd=item.get("trade_volume_usd", 0.0),
                    trade_index_price=item.get("trade_index_price", 0.0),
                    inventory_index=item.get("inventory_index", 0.0),
                    volatility=item.get("volatility"),
                    interest_rate=item.get("interest_rate"),
                    mark_price=item.get("mark_price"),
                    index_price=item.get("index_price"),
                )
            )
        return trades

    async def get_mark_price(
        self,
        instrument_name: str,
    ) -> MarkPriceData:
        """Get mark price for an instrument."""
        params = {"instrument_name": instrument_name}
        response = await self._request("GET", "/public/get_mark_price", params)
        result = response["result"][0]

        return MarkPriceData(
            timestamp=datetime.fromtimestamp(
                result["timestamp"] / 1000, tz=timezone.utc
            ),
            instrument_name=result["instrument_name"],
            mark_price=result["mark_price"],
            index_price=result["index_price"],
            settlement_price=result["settlement_price"],
            underlying_price=result["underlying_price"],
        )

    async def get_settlement_price(
        self,
        instrument_name: str,
    ) -> SettlementPriceData:
        """Get settlement price for an instrument."""
        params = {"instrument_name": instrument_name}
        response = await self._request(
            "GET", "/public/get_last_settlements_by_instrument", params
        )
        result = response["result"]["settlements"][0]

        return SettlementPriceData(
            timestamp=datetime.fromtimestamp(
                result["timestamp"] / 1000, tz=timezone.utc
            ),
            instrument_name=result["instrument_name"],
            settlement_price=result["settlement_price"],
            delivery_price=result.get("delivery_price", 0.0),
            settlement_type=result["type"],
        )

    async def get_greeks(
        self,
        instrument_name: str,
    ) -> GreeksData:
        """Get Greeks data for an instrument."""
        params = {"instrument_name": instrument_name}
        response = await self._request("GET", "/public/get_greeks", params)
        result = response["result"]

        return GreeksData(
            timestamp=datetime.now(tz=timezone.utc),
            instrument_name=result["instrument_name"],
            underlying_price=result.get("underlying_price", 0.0),
            mark_price=result.get("mark_price", 0.0),
            open_interest=result.get("open_interest", 0.0),
            delta=result.get("delta", 0.0),
            gamma=result.get("gamma", 0.0),
            rho=result.get("rho", 0.0),
            theta=result.get("theta", 0.0),
            vega=result.get("vega", 0.0),
        )

    async def batch_get_tickers(
        self,
        instrument_names: list[str],
    ) -> list[OptionTicker]:
        """Batch get tickers for multiple instruments."""
        tickers = []
        batch_size = int(self._config.rate_limit.requests_per_second)
        batch_delay = self._config.rate_limit.batch_delay_ms / 1000.0

        for i in range(0, len(instrument_names), batch_size):
            batch = instrument_names[i : i + batch_size]
            for name in batch:
                try:
                    ticker = await self.get_ticker(name)
                    tickers.append(ticker)
                except Exception as e:
                    logger.warning(
                        "ticker_fetch_failed",
                        instrument=name,
                        error=str(e),
                    )
            if i + batch_size < len(instrument_names):
                await asyncio.sleep(batch_delay)

        return tickers

    async def batch_get_order_books(
        self,
        instrument_names: list[str],
        depth: int = 20,
    ) -> list[OptionBook]:
        """Batch get order books for multiple instruments."""
        books = []
        batch_size = int(self._config.rate_limit.requests_per_second)
        batch_delay = self._config.rate_limit.batch_delay_ms / 1000.0

        for i in range(0, len(instrument_names), batch_size):
            batch = instrument_names[i : i + batch_size]
            for name in batch:
                try:
                    book = await self.get_order_book(name, depth)
                    books.append(book)
                except Exception as e:
                    logger.warning(
                        "orderbook_fetch_failed",
                        instrument=name,
                        error=str(e),
                    )
            if i + batch_size < len(instrument_names):
                await asyncio.sleep(batch_delay)

        return books
