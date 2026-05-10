"""Main contract mapper: identifies the most liquid option contracts per expiry on Deribit."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

import requests

from utils import get_logger

logger = get_logger(__name__)

DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"


@dataclass(frozen=True)
class ContractMapping:
    """Top-N option contracts for a single expiry date."""

    expiry: str
    calls: List[str]
    puts: List[str]
    atm_strike: float
    total_oi: float


class MainContractMapper:
    """Identifies the most liquid (highest OI) option contracts per expiry."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_instruments(self, currency: str = "BTC") -> List[dict]:
        """Fetch all active option instruments for *currency* from Deribit.

        Calls ``GET /public/get_instruments?currency={currency}&kind=option``.
        """
        params: Dict[str, str] = {
            "currency": currency,
            "kind": "option",
            "expired": "false",
        }
        resp = self._session.get(
            f"{DERIBIT_BASE_URL}/public/get_instruments",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        instruments = body.get("result", [])
        logger.info(
            "Fetched %d active %s option instruments", len(instruments), currency
        )
        return instruments

    def get_underlying_price(self, instrument_name: str = "BTC-PERPETUAL") -> float:
        """Return the last price of a Deribit perpetual future.

        Calls ``GET /public/ticker?instrument_name={instrument_name}``.
        """
        resp = self._session.get(
            f"{DERIBIT_BASE_URL}/public/ticker",
            params={"instrument_name": instrument_name},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        price = float(body["result"]["last_price"])
        logger.debug("Underlying price from %s: %.2f", instrument_name, price)
        return price

    def get_atm_strike(self, instruments: List[dict], underlying_price: float) -> float:
        """Find the strike closest to *underlying_price* among *instruments*."""
        strikes = sorted({inst["strike"] for inst in instruments if "strike" in inst})
        if not strikes:
            return 0.0
        return min(strikes, key=lambda s: abs(s - underlying_price))

    def map_main_contracts(
        self,
        currency: str = "BTC",
        top_n: int = 5,
    ) -> List[ContractMapping]:
        """Build a :class:`ContractMapping` per expiry, keeping the top-N by OI.

        1. Fetch all active option instruments.
        2. Get the current underlying price.
        3. Group instruments by expiry.
        4. For each group, sort by ``open_interest`` (desc), keep top *top_n*.
        5. Split into calls / puts and return a frozen :class:`ContractMapping`.
        """
        instruments = self.fetch_instruments(currency)
        if not instruments:
            logger.warning("No instruments returned for %s", currency)
            return []

        underlying_price = self.get_underlying_price(
            f"{currency}-PERPETUAL"
        )

        # Group by expiry (ISO date string from millisecond timestamp)
        by_expiry: Dict[str, List[dict]] = {}
        for inst in instruments:
            exp_ts = inst.get("expiration_timestamp")
            if exp_ts is None:
                continue
            expiry_str = datetime.fromtimestamp(
                exp_ts / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            by_expiry.setdefault(expiry_str, []).append(inst)

        mappings: List[ContractMapping] = []
        for expiry_str, group in sorted(by_expiry.items()):
            sorted_group = sorted(
                group,
                key=lambda i: i.get("open_interest", 0),
                reverse=True,
            )
            top = sorted_group[:top_n]

            calls: List[str] = []
            puts: List[str] = []
            total_oi = 0.0
            for inst in top:
                name = inst["instrument_name"]
                oi = float(inst.get("open_interest", 0))
                total_oi += oi
                opt_type = inst.get("option_type", "")
                if opt_type == "call":
                    calls.append(name)
                else:
                    puts.append(name)

            atm = self.get_atm_strike(group, underlying_price)
            mappings.append(
                ContractMapping(
                    expiry=expiry_str,
                    calls=calls,
                    puts=puts,
                    atm_strike=atm,
                    total_oi=total_oi,
                )
            )

        logger.info(
            "Mapped %d expiry groups for %s (top_n=%d)",
            len(mappings),
            currency,
            top_n,
        )
        return mappings
