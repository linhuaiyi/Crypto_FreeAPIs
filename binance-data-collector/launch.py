"""
binance-data-collector — bulk historical downloader for Binance public data.

Phase 1+2 scope (per docs/binance/STRATEGY_DATA_REQUIREMENTS_BINANCE.md):
  - spot_klines  (1m)   /data/spot/daily/klines/...
  - perp_klines  (1m)   /data/futures/um/daily/klines/...
  - mark_klines  (1m)   /data/futures/um/daily/markPriceKlines/...
  - index_klines (1m)   /data/futures/um/daily/indexPriceKlines/...
  - funding_rate (8h)   /data/futures/um/monthly/fundingRate/...

Modes:
  backfill 一次性拉 N 天历史 (default N=90 from config)
  daily     补 T-1 (data.binance.vision 在 UTC 凌晨发布前一天的 zip)
  verify    扫描 data/ 报告每个 stream 的行数、时间范围、缺口

Output path convention (matches deribit collector):
  ./binance-data-collector/data/binance/{data_type}/{SYMBOL}_{YYYY-MM-DD}.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
import time as _time
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

import pandas as pd
import yaml

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SUBPROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

os.environ.setdefault("LOG_DIR", os.path.join(_SUBPROJECT_DIR, "logs"))

from fetchers import binance_archive as arch  # noqa: E402
from storage.chunked_buffer import ChunkedBuffer  # noqa: E402
from utils import get_logger  # noqa: E402

logger = get_logger("BinanceCollector")

_KLINE_TYPES = ("spot_klines", "perp_klines", "mark_klines", "index_klines")
_ALL_TYPES = _KLINE_TYPES + ("funding_rate",)


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _iter_days(start: date, end: date) -> Iterable[date]:
    """Inclusive [start, end]."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _iter_months(start: date, end: date) -> Iterable[date]:
    """First-of-month dates overlapping [start, end] inclusive."""
    cur = start.replace(day=1)
    while cur <= end:
        yield cur
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)


def _add_meta(df: pd.DataFrame, symbol: str, interval: str) -> pd.DataFrame:
    """Attach exchange / symbol / timeframe columns to a kline DataFrame."""
    df = df.copy()
    df["exchange"] = "binance"
    df["symbol"] = symbol
    df["timeframe"] = interval
    return df


class BinanceArchivePipeline:
    def __init__(self, config_path: str) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)

        g = self.cfg.get("global", {})
        # Resolve data_dir relative to project root (config path is from project root POV)
        rel = g.get("data_dir", "./binance-data-collector/data")
        if os.path.isabs(rel):
            self.data_dir = rel
        else:
            self.data_dir = os.path.abspath(os.path.join(_PROJECT_ROOT, rel))

        self.symbols: list[str] = g.get("symbols", ["BTCUSDT", "ETHUSDT"])
        self.interval: str = g.get("interval", "1m")
        self.history_days: int = int(g.get("history_days", 90))
        self.inter_delay_ms: int = int(
            self.cfg.get("api", {}).get("inter_request_delay_ms", 50)
        )

        dt_cfg = self.cfg.get("data_types", {})
        self.enabled_types = [t for t in _ALL_TYPES if dt_cfg.get(t, {}).get("enabled", False)]

        buf_cfg = self.cfg.get("storage", {}).get("chunked_buffer", {})
        self.buffer = ChunkedBuffer(
            data_dir=self.data_dir,
            max_rows=int(buf_cfg.get("max_rows", 200_000)),
            max_memory_mb=int(buf_cfg.get("max_memory_mb", 400)),
            flush_interval_sec=int(buf_cfg.get("flush_interval_sec", 60)),
        )

        self._stats: dict[str, dict[str, int]] = {}

    # ── backfill ────────────────────────────────────────────────────────

    def run_backfill(
        self,
        days: Optional[int] = None,
        symbols: Optional[list[str]] = None,
        types: Optional[list[str]] = None,
    ) -> None:
        days = days if days is not None else self.history_days
        syms = symbols if symbols is not None else self.symbols
        tys = types if types is not None else self.enabled_types

        today = _utc_today()
        start = today - timedelta(days=days)
        end = today - timedelta(days=1)  # vision publishes T-1 by UTC morning

        logger.info(
            f"=== BACKFAIL start={start} end={end} ({days}d) "
            f"symbols={syms} types={tys} ==="
        )

        kline_types = [t for t in tys if t in _KLINE_TYPES]
        if kline_types:
            for day in _iter_days(start, end):
                for sym in syms:
                    for dt_type in kline_types:
                        self._fetch_one_kline_day(dt_type, sym, self.interval, day)
                        self._sleep_polite()

        if "funding_rate" in tys:
            for month in _iter_months(start, end):
                for sym in syms:
                    self._fetch_funding_month(sym, month)
                    self._sleep_polite()

        self.buffer.flush_all()
        # 新版 ChunkedBuffer 为 append-only 写入器, 必须收尾 footer,
        # 否则退出后当日文件缺 Parquet footer 不可读
        self.buffer.close_all_writers()
        self._summarize()

    # ── daily ───────────────────────────────────────────────────────────

    def run_daily(
        self,
        symbols: Optional[list[str]] = None,
        types: Optional[list[str]] = None,
    ) -> None:
        syms = symbols if symbols is not None else self.symbols
        tys = types if types is not None else self.enabled_types

        today = _utc_today()
        yesterday = today - timedelta(days=1)

        logger.info(f"=== DAILY target={yesterday} symbols={syms} types={tys} ===")

        kline_types = [t for t in tys if t in _KLINE_TYPES]
        for sym in syms:
            for dt_type in kline_types:
                self._fetch_one_kline_day(dt_type, sym, self.interval, yesterday)
                self._sleep_polite()

        if "funding_rate" in tys:
            for sym in syms:
                self._fetch_funding_month(sym, yesterday)
                self._sleep_polite()

        self.buffer.flush_all()
        self.buffer.close_all_writers()  # 收尾 parquet footer (append-only 写入器)
        self._summarize()

    # ── verify ──────────────────────────────────────────────────────────

    def run_verify(self) -> int:
        """Walk data_dir, report per-stream stats + gap analysis. Returns exit code."""
        logger.info(f"=== VERIFY data_dir={self.data_dir} ===")
        if not os.path.isdir(self.data_dir):
            logger.error(f"data_dir does not exist: {self.data_dir}")
            return 1

        vcfg = self.cfg.get("verify", {})
        expected_per_day = int(vcfg.get("expected_points_per_day_1m", 1440))
        max_gap_min = int(vcfg.get("max_gap_minutes", 5))
        min_ratio = float(vcfg.get("min_completeness_ratio", 0.90))

        binance_dir = os.path.join(self.data_dir, "binance")
        if not os.path.isdir(binance_dir):
            logger.error(f"no binance/ subdir in {self.data_dir}")
            return 1

        any_fail = False
        rows_report: list[str] = []
        for data_type in sorted(os.listdir(binance_dir)):
            dt_dir = os.path.join(binance_dir, data_type)
            if not os.path.isdir(dt_dir):
                continue
            for fname in sorted(os.listdir(dt_dir)):
                if not fname.endswith(".parquet"):
                    continue
                fpath = os.path.join(dt_dir, fname)
                try:
                    df = pd.read_parquet(fpath)
                except Exception as e:
                    logger.error(f"corrupted: {fpath}: {e}")
                    any_fail = True
                    continue

                ts = df["timestamp"].astype("int64").sort_values().reset_index(drop=True)
                n = len(ts)
                if n == 0:
                    rows_report.append(f"  {data_type}/{fname}: EMPTY")
                    any_fail = True
                    continue

                ts_min = int(ts.iloc[0])
                ts_max = int(ts.iloc[-1])
                d_min = datetime.fromtimestamp(ts_min / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                d_max = datetime.fromtimestamp(ts_max / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

                # Gap analysis only meaningful for 1m klines
                gaps_info = ""
                ratio = 1.0
                if data_type in _KLINE_TYPES:
                    diffs = ts.diff().dropna()
                    if len(diffs) > 0:
                        max_gap_ms = int(diffs.max())
                        median_gap_ms = int(diffs.median())
                        # Count distinct UTC days actually present in data
                        utc_dates = (
                            pd.to_datetime(ts, unit="ms", utc=True)
                            .dt.strftime("%Y-%m-%d")
                            .nunique()
                        )
                        span_days = max(1, utc_dates)
                        expected_total = span_days * expected_per_day
                        ratio = n / expected_total
                        gaps_info = (
                            f" median_gap={median_gap_ms}ms max_gap={max_gap_ms}ms"
                            f" span_days={span_days} expected~{expected_total} ratio={ratio:.3f}"
                        )
                        if max_gap_ms > max_gap_min * 60_000:
                            gaps_info += f" [GAP>{max_gap_min}min]"
                            any_fail = True
                    else:
                        gaps_info = " single-row"
                else:
                    gaps_info = " (funding rows)"

                status = "OK" if ratio >= min_ratio else "LOW"
                if ratio < min_ratio:
                    any_fail = True
                rows_report.append(
                    f"  [{status}] {data_type}/{fname}: n={n} "
                    f"range={d_min}..{d_max}{gaps_info}"
                )

        print()
        print("=" * 72)
        print("VERIFY REPORT")
        print("=" * 72)
        for line in rows_report:
            print(line)
        print()
        if any_fail:
            print(f"RESULT: FAIL (see [LOW] / [GAP] entries above)")
        else:
            print(f"RESULT: PASS")
        return 1 if any_fail else 0

    # ── helpers ─────────────────────────────────────────────────────────

    def _fetch_one_kline_day(
        self, data_type: str, symbol: str, interval: str, day: date
    ) -> None:
        key = f"{data_type}/{symbol}"
        s = self._stats.setdefault(key, {"attempted": 0, "succeeded": 0, "rows": 0})
        s["attempted"] += 1
        try:
            if data_type == "spot_klines":
                df = arch.download_daily_klines(symbol, "spot", interval, day)
            elif data_type == "perp_klines":
                df = arch.download_daily_klines(symbol, "um", interval, day)
            elif data_type == "mark_klines":
                df = arch.download_daily_mark_klines(symbol, interval, day)
            elif data_type == "index_klines":
                df = arch.download_daily_index_klines(symbol, interval, day)
            else:
                return
        except Exception as e:
            logger.error(f"[{key}] {day}: exception {e}")
            return

        if df is None or df.empty:
            logger.warning(f"[{key}] {day}: no data (404 or empty)")
            return

        df = _add_meta(df, symbol, interval)
        self.buffer.append("binance", data_type, symbol, df)
        s["succeeded"] += 1
        s["rows"] += len(df)
        logger.info(f"[{key}] {day}: +{len(df)} rows")

    def _fetch_funding_month(self, symbol: str, month: date) -> None:
        key = f"funding_rate/{symbol}"
        s = self._stats.setdefault(key, {"attempted": 0, "succeeded": 0, "rows": 0})
        s["attempted"] += 1
        try:
            df = arch.download_monthly_funding(symbol, month)
        except Exception as e:
            logger.error(f"[{key}] {month.strftime('%Y-%m')}: exception {e}")
            return

        if df is None or df.empty:
            logger.warning(f"[{key}] {month.strftime('%Y-%m')}: no data")
            return

        df = df.copy()
        df["exchange"] = "binance"
        df["symbol"] = symbol
        self.buffer.append("binance", "funding_rate", symbol, df)
        s["succeeded"] += 1
        s["rows"] += len(df)
        logger.info(f"[{key}] {month.strftime('%Y-%m')}: +{len(df)} rows")

    def _sleep_polite(self) -> None:
        if self.inter_delay_ms > 0:
            _time.sleep(self.inter_delay_ms / 1000.0)

    def _summarize(self) -> None:
        if not self._stats:
            return
        print()
        print("=" * 72)
        print("BACKFILL SUMMARY")
        print("=" * 72)
        total_rows = 0
        for key in sorted(self._stats):
            s = self._stats[key]
            total_rows += s["rows"]
            print(
                f"  {key}: attempted={s['attempted']} "
                f"succeeded={s['succeeded']} rows={s['rows']}"
            )
        print(f"\nTotal rows buffered/written: {total_rows}")


def _parse_csv_list(s: Optional[str]) -> Optional[list[str]]:
    if not s:
        return None
    return [x.strip() for x in s.split(",") if x.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="binance-data-collector")
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily", "verify"],
        required=True,
        help="backfill=一次性拉 N 天; daily=补 T-1; verify=扫描+校验",
    )
    parser.add_argument("--days", type=int, default=None, help="覆盖 config history_days")
    parser.add_argument("--symbols", default=None, help="逗号分隔，覆盖 config symbols")
    parser.add_argument("--types", default=None, help="逗号分隔，覆盖 config data_types")
    parser.add_argument(
        "--config",
        default=os.path.join(_SUBPROJECT_DIR, "config.yaml"),
        help="配置文件路径",
    )
    args = parser.parse_args()

    pipeline = BinanceArchivePipeline(args.config)

    if args.mode == "backfill":
        pipeline.run_backfill(
            days=args.days,
            symbols=_parse_csv_list(args.symbols),
            types=_parse_csv_list(args.types),
        )
        return 0
    if args.mode == "daily":
        pipeline.run_daily(
            symbols=_parse_csv_list(args.symbols),
            types=_parse_csv_list(args.types),
        )
        return 0
    if args.mode == "verify":
        return pipeline.run_verify()
    return 2


if __name__ == "__main__":
    sys.exit(main())
