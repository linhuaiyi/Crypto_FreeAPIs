"""Deribit options WebSocket engine — two-pool subscription manager.

Deribit enforces a 32 KB per-message server-side cap (close code 1009).
Subscribing all ~1548 BTC+ETH option ``quote.{instrument}`` channels on a
single connection produces a batched initial snapshot of ~43 KB and gets
rejected. Empirical threshold (see ``tests/ws_stability/``): ≤1100
``quote`` channels per connection is safe; 1200+ fails.

Two subscription tiers
----------------------
The engine now runs **two** dedicated pools:

1. ``quote`` pool (default 6 engines): subscribes the *full* option universe
   to ``quote.{instrument}``. Lightweight messages (prices + amounts only) —
   used to enrich every strike with top-of-book ``bid_size``/``ask_size``.

2. ``ticker`` pool (default 5 engines): subscribes the **near-term ATM
   subset** (≤ ``near_term_dte`` days to expiry AND within ``atm_pct`` of the
   underlying) to ``ticker.{instrument}.100ms``. The ``ticker`` payload is
   ~10× larger (it carries ``mark_iv``/``bid_iv``/``ask_iv`` + greeks), so
   fewer channels per engine keep batches under the 32 KB cap. This is the
   only source of per-strike bid/ask IV — Deribit's batch REST endpoint
   (``get_book_summary_by_currency``) returns ``mark_iv`` only.

``get_sizes()`` fans out across both pools (both carry prices/amounts).
``get_ivs()`` fans out across both but only the ``ticker`` pool yields data.

Refresh strategy: every hour, re-discover live instruments from REST,
classify each as near-term-ATM or not (using a freshly-fetched underlying
price), and hash-partition the two groups across their pools. Each engine's
``add_subscriptions_threadsafe`` no-ops channels it already has, so refresh
only costs network for genuinely new listings.
"""
from __future__ import annotations

import asyncio
import threading
import time
import zlib
from datetime import datetime, timezone
from statistics import median
from typing import Dict, List, Optional, Tuple

import requests

from fetchers.ws_orderbook import WSOrderbookEngine
from utils import get_logger

logger = get_logger("OptionsQuoteEngine")

_SUBSCRIBE_BATCH = 100        # Deribit public/subscribe caps at 100 channels
_REFRESH_INTERVAL_SEC = 3600  # hourly instrument discovery
_DEFAULT_QUOTE_POOL = 6       # full universe (quote channels) ~3800 / 6 ≈ 633/engine
_DEFAULT_TICKER_POOL = 5      # near-term ATM (ticker channels) ~370 / 5 ≈ 74/engine
_MAX_PER_QUOTE_ENGINE = 1000  # quote messages are small; ≤1100 safe
_MAX_PER_TICKER_ENGINE = 120  # ticker messages are ~10× larger; keep batches < 32 KB
_DEFAULT_NEAR_TERM_DTE = 7    # 0-7 DTE strategy window
_DEFAULT_ATM_PCT = 0.15       # ±15% of underlying covers iron-fly wings
# ticker (IV) subscriptions target the strategy's USDC market only — inverse
# BTC/ETH options aren't traded and would otherwise ~triple the ticker load.
_DEFAULT_TICKER_MARKETS = frozenset({"BTC_USDC", "ETH_USDC", "SOL_USDC"})


class DeribitOptionsQuoteEngine:
    """Two-pool WS subscription manager for the full options universe."""

    def __init__(
        self,
        quote_pool_size: int = _DEFAULT_QUOTE_POOL,
        ticker_pool_size: int = _DEFAULT_TICKER_POOL,
        near_term_dte: int = _DEFAULT_NEAR_TERM_DTE,
        atm_pct: float = _DEFAULT_ATM_PCT,
        ticker_markets: frozenset = _DEFAULT_TICKER_MARKETS,
    ) -> None:
        self._quote_pool_size = max(1, quote_pool_size)
        self._ticker_pool_size = max(0, ticker_pool_size)
        self._total = self._quote_pool_size + self._ticker_pool_size
        self._near_term_dte = near_term_dte
        self._atm_pct = atm_pct
        self._ticker_markets = frozenset(ticker_markets)

        # Engines [0:quote] = quote pool, [quote:total] = ticker pool.
        self._engines: List[WSOrderbookEngine] = [
            WSOrderbookEngine("deribit", max_instruments=_MAX_PER_QUOTE_ENGINE + 200)
            for _ in range(self._total)
        ]
        self._threads: List[Optional[threading.Thread]] = [None] * self._total
        self._loops: List[Optional[asyncio.AbstractEventLoop]] = [None] * self._total
        self._last_refresh: float = 0.0
        self._subscribed_count: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        for i in range(self._total):
            t = threading.Thread(
                target=self._run_loop,
                args=(i,),
                daemon=True,
                name=f"OptionsQuoteWS-{i}",
            )
            self._threads[i] = t
            t.start()

    def stop(self) -> None:
        for engine in self._engines:
            engine.stop()
        for i, loop in enumerate(self._loops):
            if loop and loop.is_running():
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass
        for t in self._threads:
            if t:
                t.join(timeout=10)

    def maybe_refresh(self, force: bool = False) -> None:
        """Refresh subscriptions if the interval has elapsed. Called by the
        greeks polling thread — cheap no-op otherwise."""
        if not force and time.monotonic() - self._last_refresh < _REFRESH_INTERVAL_SEC:
            return
        self._refresh_subscriptions()

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_sizes(
        self,
        instrument_names: List[str],
    ) -> Dict[str, Tuple[float, float]]:
        """{(bid_size, ask_size)} across both pools."""
        out: Dict[str, Tuple[float, float]] = {}
        for engine in self._engines:
            partial = engine.get_sizes(instrument_names)
            if partial:
                out.update(partial)
        return out

    def get_ivs(
        self,
        instrument_names: List[str],
    ) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]]:
        """{(bid_iv, ask_iv, mark_iv)}. Only the ``ticker`` pool yields data;
        instruments subscribed via ``quote`` only are absent from the result."""
        out: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}
        for engine in self._engines:
            partial = engine.get_ivs(instrument_names)
            if partial:
                out.update(partial)
        return out

    def get_deribit_greeks(
        self,
        instrument_names: List[str],
    ) -> Dict[str, Tuple[Optional[float], Optional[float], Optional[float],
                         Optional[float], Optional[float]]]:
        """{name: (delta, gamma, vega, theta, rho)} — Deribit-published greeks
        from the ``ticker`` pool. Coexists with our BS-computed greeks."""
        out: Dict[str, Tuple[Optional[float], Optional[float], Optional[float],
                             Optional[float], Optional[float]]] = {}
        for engine in self._engines:
            partial = engine.get_deribit_greeks(instrument_names)
            if partial:
                out.update(partial)
        return out

    @property
    def subscribed_count(self) -> int:
        return self._subscribed_count

    @property
    def pool_size(self) -> int:
        return self._total

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def _refresh_subscriptions(self) -> None:
        self._last_refresh = time.monotonic()
        try:
            instruments = self._discover_option_instruments()
            underlyings = self._fetch_underlyings()

            # ticker IV subscriptions target the strategy's USDC market and
            # only the near-term ATM subset; everything else stays on quote.
            near = [
                n for n in instruments
                if n.split("-", 1)[0] in self._ticker_markets
                and self._is_near_term_atm(n, underlyings)
            ]
            near_set = set(near)
            rest = [n for n in instruments if n not in near_set]

            quote_idx = list(range(0, self._quote_pool_size))
            ticker_idx = list(range(self._quote_pool_size, self._total))

            q_added = self._subscribe_group(
                quote_idx, rest, lambda n: f"quote.{n}", _MAX_PER_QUOTE_ENGINE,
            )
            t_added = 0
            if ticker_idx:
                # 100ms is the only interval Deribit actually delivers for the
                # ticker channel (empirical: ticker.{n}.1000ms yields 0 messages).
                # Cadence is managed by bounding the ticker SUBSET (near-term ATM
                # only), not by coarsening the interval.
                t_added = self._subscribe_group(
                    ticker_idx, near, lambda n: f"ticker.{n}.100ms", _MAX_PER_TICKER_ENGINE,
                )

            self._subscribed_count = sum(len(e._subscriptions) for e in self._engines)
            per_engine = [len(e._subscriptions) for e in self._engines]
            logger.info(
                f"Options subs: quote(rest)={len(rest)} (+{q_added} new) on "
                f"{self._quote_pool_size} engines, "
                f"ticker(near-term ATM ≤{self._near_term_dte}DTE ±{self._atm_pct:.0%})"
                f"={len(near)} (+{t_added} new) on {self._ticker_pool_size} engines; "
                f"per-engine {per_engine}"
            )
        except Exception as e:
            logger.warning(f"Option instrument refresh failed: {e}")

    def _subscribe_group(
        self,
        engine_indices: List[int],
        instruments: List[str],
        channel_for,
        cap_per_engine: int,
    ) -> int:
        """Hash-partition ``instruments`` across the given engines and subscribe.

        ``channel_for(name)`` returns the channel string (e.g. ``quote.{n}``
        or ``ticker.{n}.100ms``). Returns the number of newly added channels.
        """
        if not engine_indices or not instruments:
            return 0
        n = len(engine_indices)
        buckets: List[List[str]] = [[] for _ in range(n)]
        for inst in instruments:
            idx = zlib.crc32(inst.encode("utf-8")) % n
            buckets[idx].append(inst)

        total_added = 0
        for b, eng_idx in enumerate(engine_indices):
            engine = self._engines[eng_idx]
            channels = [channel_for(x) for x in buckets[b]]
            for j in range(0, len(channels), _SUBSCRIBE_BATCH):
                total_added += engine.add_subscriptions_threadsafe(
                    channels[j:j + _SUBSCRIBE_BATCH],
                )
            eng_count = len(engine._subscriptions)
            if eng_count > cap_per_engine:
                logger.warning(
                    f"Engine {eng_idx} at {eng_count} channels (cap {cap_per_engine}); "
                    f"consider increasing pool size",
                )
        return total_added

    # ------------------------------------------------------------------
    # Classification helpers
    # ------------------------------------------------------------------

    def _is_near_term_atm(
        self, name: str, underlyings: Dict[str, float],
    ) -> bool:
        """True if the option expires within ``near_term_dte`` days AND its
        strike is within ``atm_pct`` of the underlying.

        Handles both USDC linear (``BTC_USDC-28AUG26-59000-P``) and inverse
        (``BTC-28AUG26-59000-P``) naming.
        """
        parts = name.split("-")
        if len(parts) < 4:
            return False
        base = parts[0]
        try:
            exp = datetime.strptime(parts[1], "%d%b%y").date()
            strike = float(parts[2])
        except (ValueError, IndexError):
            return False

        today = datetime.now(timezone.utc).date()
        dte = (exp - today).days
        if not (0 <= dte <= self._near_term_dte):
            return False

        u = underlyings.get(base)
        if u is None:
            u = underlyings.get(base.split("_")[0])  # BTC_USDC -> BTC
        if not u or u <= 0:
            return False
        return abs(strike - u) / u <= self._atm_pct

    @staticmethod
    def _discover_option_instruments() -> List[str]:
        """Return all active option instruments (inverse + USDC linear)."""
        out: List[str] = []
        session = requests.Session()
        for currency in ("BTC", "ETH"):
            resp = session.get(
                "https://www.deribit.com/api/v2/public/get_instruments",
                params={"currency": currency, "kind": "option", "expired": "false"},
                timeout=15,
            )
            resp.raise_for_status()
            for inst in resp.json().get("result", []):
                name = inst.get("instrument_name")
                if name:
                    out.append(name)
        resp = session.get(
            "https://www.deribit.com/api/v2/public/get_instruments",
            params={"currency": "USDC", "kind": "option", "expired": "false"},
            timeout=15,
        )
        resp.raise_for_status()
        for inst in resp.json().get("result", []):
            name = inst.get("instrument_name")
            if name:
                out.append(name)
        return out

    @staticmethod
    def _fetch_underlyings() -> Dict[str, float]:
        """{base_or_coin: median_underlying_price} from the USDC option chain.

        Each ``get_book_summary_by_currency`` entry carries an
        ``underlying_price``; we take the median per base (BTC_USDC, ETH_USDC,
        SOL_USDC) and alias the bare coin (BTC/ETH/SOL) for inverse names.
        """
        out: Dict[str, float] = {}
        try:
            resp = requests.get(
                "https://www.deribit.com/api/v2/public/get_book_summary_by_currency",
                params={"currency": "USDC", "kind": "option"},
                timeout=15,
            )
            resp.raise_for_status()
            groups: Dict[str, List[float]] = {}
            for it in resp.json().get("result", []):
                name = it.get("instrument_name", "")
                u = it.get("underlying_price")
                if not name or u in (None, 0, "", "0"):
                    continue
                base = name.split("-", 1)[0]
                try:
                    groups.setdefault(base, []).append(float(u))
                except (TypeError, ValueError):
                    continue
            for base, vals in groups.items():
                out[base] = median(vals)
                coin = base.split("_")[0]
                out.setdefault(coin, out[base])
        except Exception as e:
            logger.warning(f"underlying fetch failed (ticker ATM filter degraded): {e}")
        return out

    # ------------------------------------------------------------------
    # Event loop
    # ------------------------------------------------------------------

    def _run_loop(self, idx: int) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loops[idx] = loop
        try:
            loop.run_until_complete(self._engines[idx].run())
        except Exception as e:
            logger.error(f"Options WS engine {idx} loop exited: {e}")
