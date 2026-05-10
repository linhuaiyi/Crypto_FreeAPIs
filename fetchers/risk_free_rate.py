"""FRED API risk-free rate fetcher with calendar forward-fill and spline interpolation."""
import json
import math
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import requests

from utils import get_logger
from utils.interpolation import interpolate_curve

logger = get_logger("RiskFreeRateFetcher")

# FRED series IDs mapped to tenor in years
FRED_SERIES: Dict[str, float] = {
    "DGS1MO": 1 / 12,
    "DGS3MO": 3 / 12,
    "DGS6MO": 6 / 12,
    "DGS1": 1.0,
    "DGS2": 2.0,
    "DGS5": 5.0,
    "DGS10": 10.0,
    "DGS30": 30.0,
}

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FALLBACK_RATE = 0.05


@dataclass(frozen=True)
class RiskFreeRate:
    """Single risk-free rate observation."""
    date: str
    tenor_years: float
    rate_annual: float
    rate_continuous: float
    is_trading_day: bool


class RiskFreeRateFetcher:
    """Fetches and serves risk-free rates from FRED."""

    def __init__(self, api_key: str, cache_dir: str = "./cache/fred") -> None:
        self._api_key = api_key
        self._cache_dir = cache_dir
        self._session = requests.Session()

    def fetch_series(
        self, series_id: str, start_date: str, end_date: str
    ) -> List[Tuple[str, float]]:
        """Fetch a single FRED series, using local cache when available."""
        cached = self._load_cache(series_id, start_date, end_date)
        if cached is not None:
            return cached

        params = {
            "series_id": series_id,
            "api_key": self._api_key,
            "observation_start": start_date,
            "observation_end": end_date,
            "file_type": "json",
        }

        try:
            resp = self._session.get(FRED_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("FRED fetch failed for %s: %s", series_id, exc)
            return []

        result: List[Tuple[str, float]] = []
        for obs in data.get("observations", []):
            val = obs.get("value", ".")
            if val == ".":
                continue
            try:
                result.append((obs["date"], float(val) / 100.0))
            except (ValueError, KeyError):
                continue

        self._save_cache(series_id, start_date, end_date, result)
        return result

    def build_yield_curve(self, date_str: str) -> List[RiskFreeRate]:
        """Build a full yield curve for *date_str* across all FRED tenors.

        Weekends and holidays inherit the last valid trading-day value via
        calendar forward-fill.
        """
        year = int(date_str[:4])
        calendar = self._generate_calendar(year)
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        raw: Dict[str, List[Tuple[str, float]]] = {
            sid: self.fetch_series(sid, start, end) for sid in FRED_SERIES
        }
        filled = self._ffill_calendar(raw, calendar)

        date_rates = {sid: filled[sid].get(date_str) for sid in FRED_SERIES}
        if not any(v is not None for v in date_rates.values()):
            logger.warning(
                "No FRED data for %s, using fallback %.1f%%",
                date_str, FALLBACK_RATE * 100,
            )
            return self._fallback_curve(date_str)

        is_trading = self._is_trading_day(date_str)
        results: List[RiskFreeRate] = []
        for sid, tenor in FRED_SERIES.items():
            annual = date_rates.get(sid) or FALLBACK_RATE
            results.append(RiskFreeRate(
                date=date_str,
                tenor_years=tenor,
                rate_annual=annual,
                rate_continuous=math.log(1 + annual),
                is_trading_day=is_trading,
            ))
        return results

    def get_rate_for_tenor(self, date_str: str, tenor_years: float) -> float:
        """Return the continuous-compounded rate for an arbitrary tenor."""
        curve = self.build_yield_curve(date_str)
        known_points = [(r.tenor_years, r.rate_annual) for r in curve]
        annual = interpolate_curve(known_points, [tenor_years])[0]
        return math.log(1 + annual)

    # -- Calendar helpers --------------------------------------------------

    def _generate_calendar(self, year: int) -> List[str]:
        """Every date string in *year*."""
        dates: List[str] = []
        current = date(year, 1, 1)
        end = date(year, 12, 31)
        while current <= end:
            dates.append(current.isoformat())
            current += timedelta(days=1)
        return dates

    def _is_us_holiday(self, date_str: str) -> bool:
        """Return True if *date_str* is a NYSE-observed US holiday."""
        d = date.fromisoformat(date_str)
        yr, mo, dy = d.year, d.month, d.day
        wd = d.weekday()  # 0=Mon .. 6=Sun

        # New Year's Day (observed on nearest weekday)
        if mo == 1 and dy == 1 and wd < 5:
            return True
        if mo == 1 and dy == 2 and wd == 0:
            return True
        if mo == 12 and dy == 31 and wd == 4:
            return True
        # MLK Day - 3rd Monday of January
        if mo == 1 and wd == 0 and 15 <= dy <= 21:
            return True
        # Presidents' Day - 3rd Monday of February
        if mo == 2 and wd == 0 and 15 <= dy <= 21:
            return True
        # Good Friday
        if d == self._easter_sunday(yr) - timedelta(days=2):
            return True
        # Memorial Day - last Monday of May
        if mo == 5 and wd == 0 and dy >= 25:
            return True
        # Juneteenth - June 19 (observed on nearest weekday)
        if mo == 6 and dy == 19 and wd < 5:
            return True
        if mo == 6 and dy == 20 and wd == 0:
            return True
        if mo == 6 and dy == 18 and wd == 4:
            return True
        # Independence Day - July 4 (observed on nearest weekday)
        if mo == 7 and dy == 4 and wd < 5:
            return True
        if mo == 7 and dy == 5 and wd == 0:
            return True
        if mo == 7 and dy == 3 and wd == 4:
            return True
        # Labor Day - 1st Monday of September
        if mo == 9 and wd == 0 and dy <= 7:
            return True
        # Thanksgiving - 4th Thursday of November
        if mo == 11 and wd == 3 and 22 <= dy <= 28:
            return True
        # Christmas - December 25 (observed on nearest weekday)
        if mo == 12 and dy == 25 and wd < 5:
            return True
        if mo == 12 and dy == 26 and wd == 0:
            return True
        if mo == 12 and dy == 24 and wd == 4:
            return True
        return False

    def _is_trading_day(self, date_str: str) -> bool:
        """Weekday that is not a US holiday."""
        d = date.fromisoformat(date_str)
        return d.weekday() < 5 and not self._is_us_holiday(date_str)

    def _ffill_calendar(
        self, raw_data: Dict[str, List[Tuple[str, float]]], dates: List[str]
    ) -> Dict[str, Dict[str, float]]:
        """Forward-fill each series across the full calendar."""
        filled: Dict[str, Dict[str, float]] = {}
        for sid in FRED_SERIES:
            lookup = dict(raw_data.get(sid, []))
            series_filled: Dict[str, float] = {}
            last_val: Optional[float] = None
            for d in dates:
                if d in lookup:
                    last_val = lookup[d]
                if last_val is not None:
                    series_filled[d] = last_val
            filled[sid] = series_filled
        return filled

    # -- Helpers -----------------------------------------------------------

    @staticmethod
    def _easter_sunday(year: int) -> date:
        """Compute Easter Sunday (Anonymous Gregorian algorithm)."""
        a = year % 19
        b, c = divmod(year, 100)
        d, e = divmod(b, 4)
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i, k = divmod(c, 4)
        l_val = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l_val) // 451
        month = (h + l_val - 7 * m + 114) // 31
        day = ((h + l_val - 7 * m + 114) % 31) + 1
        return date(year, month, day)

    def _fallback_curve(self, date_str: str) -> List[RiskFreeRate]:
        """Build a flat curve at the fallback rate."""
        continuous = math.log(1 + FALLBACK_RATE)
        is_trading = self._is_trading_day(date_str)
        return [
            RiskFreeRate(
                date=date_str,
                tenor_years=tenor,
                rate_annual=FALLBACK_RATE,
                rate_continuous=continuous,
                is_trading_day=is_trading,
            )
            for tenor in FRED_SERIES.values()
        ]

    # -- Cache I/O ---------------------------------------------------------

    def _cache_path(self, series_id: str, start_date: str, end_date: str) -> str:
        os.makedirs(self._cache_dir, exist_ok=True)
        return os.path.join(
            self._cache_dir, f"{series_id}_{start_date}_{end_date}.json"
        )

    def _load_cache(
        self, series_id: str, start_date: str, end_date: str
    ) -> Optional[List[Tuple[str, float]]]:
        path = self._cache_path(series_id, start_date, end_date)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r") as fh:
                data = json.load(fh)
            return [(item[0], item[1]) for item in data]
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Cache read failed for %s: %s", series_id, exc)
            return None

    def _save_cache(
        self, series_id: str, start_date: str, end_date: str,
        data: List[Tuple[str, float]],
    ) -> None:
        path = self._cache_path(series_id, start_date, end_date)
        try:
            with open(path, "w") as fh:
                json.dump(data, fh)
        except OSError as exc:
            logger.warning("Cache write failed for %s: %s", series_id, exc)
