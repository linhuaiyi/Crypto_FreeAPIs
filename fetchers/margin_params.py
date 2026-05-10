"""
Margin parameters fetcher for Deribit and Binance perpetual contracts.

Collects initial/maintenance margin rates, leverage, and order constraints.
Falls back to a static margin table when the Deribit API is unreachable.
"""
from dataclasses import dataclass
from typing import List
import time

import requests

from utils import get_logger

logger = get_logger("MarginParamsFetcher")

DERIBIT_BASE = "https://www.deribit.com/api/v2"
BINANCE_BASE = "https://fapi.binance.com/fapi/v1"

# Fallback margin tiers when Deribit API is unreachable.
# Notional bands: (upper_bound, initial_margin_rate, maintenance_margin_rate)
_FALLBACK_TIERS: dict[str, list[tuple[float, float, float]]] = {
    "BTC": [
        (50_000, 0.008, 0.004),
        (250_000, 0.015, 0.007),
        (1_000_000, 0.025, 0.012),
        (5_000_000, 0.040, 0.020),
    ],
    "ETH": [
        (25_000, 0.008, 0.004),
        (125_000, 0.015, 0.007),
        (500_000, 0.025, 0.012),
        (2_500_000, 0.040, 0.020),
    ],
}


@dataclass(frozen=True)
class MarginParams:
    """Margin and trading parameters for a single instrument."""

    timestamp: int
    instrument_name: str
    exchange: str
    instrument_type: str
    initial_margin_rate: float
    maintenance_margin_rate: float
    max_leverage: float
    contract_size: float
    tick_size: float
    min_order_size: float

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "instrument_name": self.instrument_name,
            "exchange": self.exchange,
            "instrument_type": self.instrument_type,
            "initial_margin_rate": self.initial_margin_rate,
            "maintenance_margin_rate": self.maintenance_margin_rate,
            "max_leverage": self.max_leverage,
            "contract_size": self.contract_size,
            "tick_size": self.tick_size,
            "min_order_size": self.min_order_size,
        }


class MarginParamsFetcher:
    """Fetch margin parameters from Deribit and Binance."""

    def __init__(self) -> None:
        self.session = requests.Session()

    # ── Deribit ──

    def fetch_deribit_instruments(
        self, currency: str = "BTC"
    ) -> List[MarginParams]:
        """Fetch margin params for options and futures on Deribit."""
        ts = int(time.time() * 1000)
        results: List[MarginParams] = []

        for kind in ("option", "future"):
            try:
                resp = self.session.get(
                    f"{DERIBIT_BASE}/public/get_instruments",
                    params={"currency": currency, "kind": kind},
                    timeout=15,
                )
                resp.raise_for_status()
                body = resp.json()

                instruments = body.get("result", [])
                for item in instruments:
                    results.append(self._parse_deribit_instrument(item, ts))

                logger.info(
                    f"Deribit {currency}/{kind}: fetched {len(instruments)} instruments"
                )

            except Exception as exc:
                logger.warning(
                    f"Deribit instruments error for {currency}/{kind}: {exc}"
                )
                results.extend(
                    self._fallback_deribit(currency, kind, ts)
                )

        return results

    def _parse_deribit_instrument(
        self, item: dict, ts: int
    ) -> MarginParams:
        """Convert a Deribit instrument JSON object into MarginParams."""
        kind = item.get("kind", "future")
        instrument_type = "option" if kind == "option" else "perp"

        contract_size = float(item.get("contract_size", 0.0))
        tick_size = float(item.get("tick_size", 0.0))
        min_size = float(
            item.get("min_trade_amount", 0.0)
        )

        # Deribit returns margin fields when available.
        im_rate = float(item.get("initial_margin", 0.0))
        mm_rate = float(item.get("maintenance_margin", 0.0))

        # If margin fields are missing, derive from leverage.
        max_leverage = 0.0
        if im_rate > 0:
            max_leverage = min(1.0 / im_rate, 100.0)
        else:
            max_leverage = 50.0
            im_rate = 1.0 / max_leverage

        if mm_rate <= 0:
            mm_rate = im_rate * 0.5

        return MarginParams(
            timestamp=ts,
            instrument_name=item.get("instrument_name", ""),
            exchange="deribit",
            instrument_type=instrument_type,
            initial_margin_rate=im_rate,
            maintenance_margin_rate=mm_rate,
            max_leverage=max_leverage,
            contract_size=contract_size,
            tick_size=tick_size,
            min_order_size=min_size,
        )

    def _fallback_deribit(
        self, currency: str, kind: str, ts: int
    ) -> List[MarginParams]:
        """Return static margin entries when the API is unreachable."""
        tiers = _FALLBACK_TIERS.get(currency, _FALLBACK_TIERS["BTC"])
        first_tier = tiers[0]
        results: List[MarginParams] = []

        instrument_type = "option" if kind == "option" else "perp"
        results.append(
            MarginParams(
                timestamp=ts,
                instrument_name=f"{currency}-{instrument_type}-fallback",
                exchange="deribit",
                instrument_type=instrument_type,
                initial_margin_rate=first_tier[1],
                maintenance_margin_rate=first_tier[2],
                max_leverage=1.0 / first_tier[1] if first_tier[1] > 0 else 50.0,
                contract_size=1.0,
                tick_size=0.0005 if currency == "BTC" else 0.001,
                min_order_size=0.1 if currency == "BTC" else 0.01,
            )
        )
        logger.info(f"Using fallback margin params for {currency}/{kind}")
        return results

    # ── Binance ──

    def fetch_binance_exchange_info(self) -> List[MarginParams]:
        """Fetch margin params for all Binance USDT-M perpetual symbols."""
        ts = int(time.time() * 1000)
        try:
            resp = self.session.get(
                f"{BINANCE_BASE}/exchangeInfo",
                timeout=15,
            )
            resp.raise_for_status()
            body = resp.json()

        except Exception as exc:
            logger.warning(f"Binance exchangeInfo error: {exc}")
            return []

        symbols = body.get("symbols", [])
        results: List[MarginParams] = []

        for symbol_info in symbols:
            if symbol_info.get("contractType") not in ("PERPETUAL",):
                continue
            if symbol_info.get("status") != "TRADING":
                continue

            results.append(self._parse_binance_symbol(symbol_info, ts))

        logger.info(f"Binance: fetched margin params for {len(results)} symbols")
        return results

    def _parse_binance_symbol(
        self, symbol_info: dict, ts: int
    ) -> MarginParams:
        """Parse a Binance symbol entry into MarginParams."""
        filters = symbol_info.get("filters", [])
        filters_by_type: dict[str, dict] = {
            f["filterType"]: f for f in filters
        }

        # LOT_SIZE filter
        lot = filters_by_type.get("LOT_SIZE", {})
        min_qty = float(lot.get("minQty", 0.0))

        # PRICE_FILTER
        price = filters_by_type.get("PRICE_FILTER", {})
        tick_size = float(price.get("tickSize", 0.0))

        # Leverage brackets for margin rates
        brackets_raw: list[dict] = symbol_info.get("brackets", [])
        im_rate = 0.01
        mm_rate = 0.005
        max_leverage = 20.0

        if brackets_raw:
            first_bracket = brackets_raw[0]
            max_leverage = float(first_bracket.get("leverage", 20))
            if max_leverage > 0:
                im_rate = 1.0 / max_leverage
            mm_rate = float(first_bracket.get("maintMarginRatio", 0.005))

        contract_size = 1.0
        contract_type = symbol_info.get("contractType", "PERPETUAL")
        instrument_type = "perp" if contract_type == "PERPETUAL" else "future"

        return MarginParams(
            timestamp=ts,
            instrument_name=symbol_info.get("symbol", ""),
            exchange="binance",
            instrument_type=instrument_type,
            initial_margin_rate=im_rate,
            maintenance_margin_rate=mm_rate,
            max_leverage=max_leverage,
            contract_size=contract_size,
            tick_size=tick_size,
            min_order_size=min_qty,
        )