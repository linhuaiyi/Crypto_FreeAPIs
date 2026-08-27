#!/usr/bin/env python3
"""
panel-collector — 统一 REST 批处理采集器
（2026-08-26 合并 ohlcv-collector + binance-data-collector 而成）

三模式：
  daily     日频防过期（REST 近 rest_window_days 天 + vision T-1 归档）
  backfill  深历史回补（binance 系走 data.binance.vision 归档；OI/情绪硬窗 30 天不可深回）
  verify    自校验（每流 span/行数/新鲜度表；退出码非 0 = 有 MISSING/STALE，可接 cron 告警）

数据类型（data_type）：
  ohlcv      K 线（多周期）；binance 系 source=auto：1m 的 daily 与深历史走 vision 官方终稿
  funding    资金费率（复用共享 fetchers/funding_rate.py：binance/deribit/hyperliquid 三所）
  oi         持仓量 openInterestHist（⚠ 仅 ~30 天保留，过期即焚）
  sentiment  多空情绪 topLongShortAccountRatio + takerlongshortRatio（⚠ 同 30 天窗）

去重规则（与湖的关系，详见 README）：
  deribit funding 不采 → 湖真源 raw.deribit.funding_rate（v2）
  binance BTC/ETH funding 与 v2 freeapis_v2 段重叠 → 入湖时以 v2 为准，本采集器照采供横截面

落盘：panel-collector/data/{exchange_dir}/{SYMBOL}_{slot}.parquet
  slot = 周期(ohlcv) | funding | oi | sentiment；store 原子写 + timestamp 去重 keep='last'
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
import yaml

_SUBDIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SUBDIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fetchers import (  # noqa: E402
    BinanceSpotFetcher,
    BinanceUSDMFetcher,
    DeribitFetcher,
    HyperliquidFetcher,
)
from fetchers import binance_archive as vision  # noqa: E402
from fetchers.funding_rate import FundingRateFetcher  # noqa: E402
from models.ohlcv import OHLCV  # noqa: E402
from storage import ParquetStore  # noqa: E402
from utils import RateLimiter, get_logger  # noqa: E402

logger = get_logger("PanelCollector")

DATA_TYPES = ("ohlcv", "funding", "oi", "sentiment")
OHLCV_FETCHER_CLASSES = {
    "binance_spot": BinanceSpotFetcher,
    "binance_usdm": BinanceUSDMFetcher,
    "deribit": DeribitFetcher,
    "hyperliquid": HyperliquidFetcher,
}


# ── 指标行模型（oi/sentiment；字段含 timestamp/exchange/symbol 以复用 store 去重逻辑） ──

@dataclass(frozen=True)
class OIRow:
    timestamp: int
    exchange: str
    symbol: str
    open_interest: float          # 合约张数（base 计）
    notional_usd: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "open_interest": self.open_interest,
            "notional_usd": self.notional_usd,
        }


@dataclass(frozen=True)
class SentimentRow:
    timestamp: int
    exchange: str
    symbol: str
    long_short_account_ratio: Optional[float]   # 账户多空人数比
    taker_long_short_ratio: Optional[float]     # 主动买卖比（taker 成交量）

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "long_short_account_ratio": self.long_short_account_ratio,
            "taker_long_short_ratio": self.taker_long_short_ratio,
        }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


class PanelCollector:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        data_dir = os.path.join(_SUBDIR, "data")
        self.store = ParquetStore(data_dir)
        g = self.config.get("global", {})
        self.rest_window_days = int(g.get("rest_window_days", 3))
        self.default_history_days = int(g.get("history_days", 365))

        self.funding_fetcher = FundingRateFetcher()
        self.ohlcv_fetchers: Dict[str, object] = {}
        self._init_ohlcv_fetchers()

        self.http = requests.Session()
        # 交易所裸 curl 会被 403/451 误伤（实测教训），一律带 UA
        self.http.headers["User-Agent"] = "Mozilla/5.0 (X11; Linux x86_64) panel-collector/1.0"

        mcfg = self.config.get("metrics_api", {})
        self.metrics_base = mcfg.get("base", "https://fapi.binance.com/futures/data")
        self.metrics_timeout = int(mcfg.get("http_timeout_sec", 30))

    # ── 初始化（复用 oc 的四所 OHLCV fetcher） ──

    def _init_ohlcv_fetchers(self):
        ex_cfg = self.config.get("exchanges", {})
        for ex_name, cls in OHLCV_FETCHER_CLASSES.items():
            cfg = ex_cfg.get(ex_name, {})
            if not cfg.get("enabled", False):
                continue
            dt = cfg.get("data_types", {}).get("ohlcv", {})
            if not dt.get("enabled", False):
                continue
            rl = cfg.get("rate_limit", {})
            rpm = rl.get("requests_per_minute") or (rl.get("requests_per_second", 10) * 60)
            self.ohlcv_fetchers[ex_name] = cls(cfg, RateLimiter(int(rpm), ex_name))
            logger.info(f"[{ex_name}] OHLCV fetcher 就绪 (限速 {rpm} req/min)")

    # ── 配置解析辅助 ──

    def _symbols_map(self, ex_name: str, only: Optional[List[str]]) -> Dict[str, str]:
        m = self.config["exchanges"][ex_name].get("symbols", {})
        if only:
            m = {k: v for k, v in m.items() if k in only}
        return m

    def _types_enabled(self, ex_name: str, only: Optional[List[str]]) -> List[str]:
        dts = self.config["exchanges"][ex_name].get("data_types", {})
        out = []
        for t in DATA_TYPES:
            if dts.get(t, {}).get("enabled", False) and (only is None or t in only):
                out.append(t)
        return out

    def _enabled_exchanges(self, only: Optional[List[str]]) -> List[str]:
        exs = [e for e in self.config.get("exchanges", {}) if e in self.ohlcv_fetchers]
        return [e for e in exs if only is None or e in only]

    # ── OHLCV ──

    def _save_ohlcv_df(self, ex_name: str, unified: str, ex_sym: str, tf: str, df: pd.DataFrame) -> int:
        """vision 归档 DataFrame → OHLCV 行落盘（keep='last' 自动修正未收周期）。"""
        if df is None or df.empty:
            return 0
        rows = [
            OHLCV(
                timestamp=int(r["timestamp"]),
                open=float(r["open"]), high=float(r["high"]),
                low=float(r["low"]), close=float(r["close"]),
                volume=float(r.get("volume", 0.0)),
                quote_volume=float(r.get("quote_volume", 0.0)),
                exchange=ex_name, symbol=ex_sym, timeframe=tf,
                trades=int(r["trades"]) if "trades" in r and pd.notna(r["trades"]) else None,
            )
            for _, r in df.iterrows()
        ]
        return self.store.save(ex_name, unified, tf, rows)

    def _vision_daily_1m(self, ex_name: str, unified: str, ex_sym: str, day: date) -> int:
        kind = "spot" if ex_name == "binance_spot" else "um"
        df = vision.download_daily_klines(ex_sym, kind, "1m", day)
        return self._save_ohlcv_df(ex_name, unified, ex_sym, "1m", df)

    def _rest_gapfill(self, ex_name: str, unified: str, ex_sym: str, tf: str,
                      window_days: Optional[int] = None) -> int:
        """REST 增量：从 store 最后时间戳续到 now；窗口外的深洞留给 backfill。"""
        fetcher = self.ohlcv_fetchers[ex_name]
        now = _now_ms()
        start = now - int(window_days or self.rest_window_days) * 86400_000
        last = self.store.get_last_timestamp(ex_name, unified, tf)
        if last:
            start = max(start, last + 1)
        if start >= now:
            return 0
        try:
            records = fetcher.fetch_with_backoff(ex_sym, tf, start, now)
        except Exception as e:  # 单流失败不拖垮面板
            logger.error(f"[{ex_name}] {unified} {tf} REST 失败: {e}")
            return 0
        return self.store.save(ex_name, unified, tf, records) if records else 0

    def _run_ohlcv_daily(self, only_exs: Optional[List[str]], only_syms: Optional[List[str]]):
        today = _utc_today()
        t1 = today - timedelta(days=1)   # vision 在 UTC 凌晨发布 T-1（实测 03:14 UTC 时尚未出，cron 须排在 ≥05:00 UTC）
        for ex_name in self._enabled_exchanges(only_exs):
            tfs = self.config["exchanges"][ex_name]["data_types"]["ohlcv"].get("timeframes", ["1d"])
            src = self.config["exchanges"][ex_name]["data_types"]["ohlcv"].get("source", "api")
            for unified, ex_sym in self._symbols_map(ex_name, only_syms).items():
                for tf in tfs:
                    # binance 系 source=auto：全周期走 vision T-1 官方终稿日档
                    # （实测教训 2026-08-27：fapi klines REST 对本机 403，vision CDN 无区域限制；
                    #   现货 REST 虽通也统一走归档，保证官方终稿口径）
                    if src == "auto" and ex_name.startswith("binance"):
                        try:
                            kind = "spot" if ex_name == "binance_spot" else "um"
                            df = vision.download_daily_klines(ex_sym, kind, tf, t1)
                            n = self._save_ohlcv_df(ex_name, unified, ex_sym, tf, df) if df is not None else 0
                            if n == 0:
                                self._rest_gapfill(ex_name, unified, ex_sym, tf)
                        except Exception as e:
                            logger.warning(f"[{ex_name}] {unified} {tf} vision T-1 未得手({e})，走 REST")
                            self._rest_gapfill(ex_name, unified, ex_sym, tf)
                    else:
                        self._rest_gapfill(ex_name, unified, ex_sym, tf)

    def _covered_dates(self, ex_name: str, unified: str, slot: str) -> set:
        """读现有 parquet 的日期覆盖集（backfill 跳过已覆盖日，避免重拉存量）。"""
        path = self.store._get_file_path(ex_name, unified, slot)
        if not os.path.exists(path):
            return set()
        try:
            df = pd.read_parquet(path, columns=["timestamp"])
            if df.empty:
                return set()
            return set(pd.to_datetime(df["timestamp"], unit="ms").dt.date)
        except Exception:
            return set()

    def _run_ohlcv_backfill(self, days: int, only_exs: Optional[List[str]], only_syms: Optional[List[str]]):
        start_day = _utc_today() - timedelta(days=days)
        end_day = _utc_today() - timedelta(days=2)   # 已发布归档的最后一日
        for ex_name in self._enabled_exchanges(only_exs):
            dt = self.config["exchanges"][ex_name]["data_types"]["ohlcv"]
            tfs = dt.get("timeframes", ["1d"])
            src = dt.get("source", "api")
            for unified, ex_sym in self._symbols_map(ex_name, only_syms).items():
                # binance 系：vision 日档逐日回（官方终稿；每周期一次性加载已覆盖日期集，跳过存量日）
                if src == "auto" and ex_name.startswith("binance"):
                    kind = "spot" if ex_name == "binance_spot" else "um"
                    for tf in tfs:
                        covered = self._covered_dates(ex_name, unified, tf)
                        n_skip = 0
                        d = start_day
                        while d <= end_day:
                            if d in covered:
                                n_skip += 1; d += timedelta(days=1); continue
                            try:
                                df = vision.download_daily_klines(ex_sym, kind, tf, d)
                                if df is not None:
                                    self._save_ohlcv_df(ex_name, unified, ex_sym, tf, df)
                            except Exception as e:
                                logger.warning(f"[{ex_name}] {unified} {tf} {d} vision 失败: {e}")
                            d += timedelta(days=1)
                        logger.info(f"[{ex_name}] {unified} {tf} 回补窗口 {start_day}→{end_day}（跳过已有 {n_skip} 天）")
                # 其余所：REST 从 store 续拉（窗口有限，尽力而为）
                else:
                    for tf in tfs:
                        last = self.store.get_last_timestamp(ex_name, unified, tf)
                        want = _now_ms() - days * 86400_000
                        if last is None or last > want:
                            self._rest_gapfill(ex_name, unified, ex_sym, tf, window_days=days)
                        else:
                            logger.warning(
                                f"[{ex_name}] {unified} {tf} 深洞 {datetime.fromtimestamp(last/1000, tz=timezone.utc):%F}"
                                f" 前无归档可回，REST 窗口外缺口保留")

    # ── funding ──

    def _save_funding_rows(self, ex_name: str, unified: str, rows: List) -> int:
        return self.store.save(ex_name, unified, "funding", rows) if rows else 0

    def _run_funding_daily(self, only_syms: Optional[List[str]]):
        now = _now_ms()
        start = now - self.rest_window_days * 86400_000
        for ex_name in self._enabled_exchanges(None):
            if "funding" not in self._types_enabled(ex_name, None):
                continue
            for unified, ex_sym in self._symbols_map(ex_name, only_syms).items():
                try:
                    if ex_name == "binance_usdm":
                        rows = self.funding_fetcher.fetch_binance(ex_sym, start, now)
                        self._save_funding_rows(ex_name, unified, rows)
                    elif ex_name == "hyperliquid":
                        rows = self.funding_fetcher.fetch_hyperliquid(ex_sym, start, now)
                        self._save_funding_rows(ex_name, unified, rows)
                except Exception as e:
                    logger.error(f"[{ex_name}] {unified} funding 失败: {e}")

    def _run_funding_backfill(self, days: int, only_syms: Optional[List[str]]):
        # binance：vision 月档逐月回 + REST 补当月尾巴
        if "binance_usdm" in self.ohlcv_fetchers and "funding" in self._types_enabled("binance_usdm", None):
            start_month = (_utc_today() - timedelta(days=days)).replace(day=1)
            this_month = _utc_today().replace(day=1)
            for unified, ex_sym in self._symbols_map("binance_usdm", only_syms).items():
                m = start_month
                while m <= this_month:
                    try:
                        df = vision.download_monthly_funding(ex_sym, m)
                        if df is not None and not df.empty:
                            from fetchers.funding_rate import FundingRate
                            rows = [
                                FundingRate(timestamp=int(r["timestamp"]), exchange="binance_usdm",
                                            symbol=ex_sym, funding_rate=float(r["funding_rate"]))
                                for _, r in df.iterrows()
                            ]
                            self._save_funding_rows("binance_usdm", unified, rows)
                            logger.info(f"[binance_usdm] {unified} funding 月档 {m:%Y-%m}: {len(rows)} 行")
                    except Exception as e:
                        logger.warning(f"[binance_usdm] {unified} funding 月档 {m:%Y-%m} 失败: {e}")
                    m = (m + timedelta(days=32)).replace(day=1)
                # 当月尾巴用 REST 收口
                self._run_funding_daily(only_syms)
                return
        # 其他所：REST 尽力（窗口有限）
        self._run_funding_daily(only_syms)

    # ── oi / sentiment（binance_usdm 指标族，30 天硬窗） ──

    def _fetch_metric_json(self, path: str, ex_sym: str, days: int, period: str) -> List[dict]:
        end = _now_ms()
        start = end - days * 86400_000
        params = {"symbol": ex_sym, "period": period, "limit": 500,
                  "startTime": start, "endTime": end}
        r = self.http.get(f"{self.metrics_base}/{path}", params=params, timeout=self.metrics_timeout)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

    def _run_oi(self, only_syms: Optional[List[str]], days: int):
        if "binance_usdm" not in self.ohlcv_fetchers or "oi" not in self._types_enabled("binance_usdm", None):
            return
        period = self.config["exchanges"]["binance_usdm"]["data_types"]["oi"].get("period", "1d")
        days = min(days, 29)   # ⚠ 硬窗：openInterestHist 仅 ~30 天
        for unified, ex_sym in self._symbols_map("binance_usdm", only_syms).items():
            try:
                data = self._fetch_metric_json("openInterestHist", ex_sym, days, period)
                rows = [
                    OIRow(timestamp=int(d["timestamp"]), exchange="binance_usdm", symbol=ex_sym,
                          open_interest=float(d["sumOpenInterest"]),
                          notional_usd=float(d.get("sumOpenInterestValue") or 0) or None)
                    for d in data
                ]
                if rows:
                    self.store.save("binance_usdm", unified, "oi", rows)
                    logger.info(f"[binance_usdm] {unified} oi: {len(rows)} 行")
            except Exception as e:
                logger.error(f"[binance_usdm] {unified} oi 失败: {e}")

    def _run_sentiment(self, only_syms: Optional[List[str]], days: int):
        if "binance_usdm" not in self.ohlcv_fetchers or "sentiment" not in self._types_enabled("binance_usdm", None):
            return
        period = self.config["exchanges"]["binance_usdm"]["data_types"]["sentiment"].get("period", "1d")
        days = min(days, 29)
        for unified, ex_sym in self._symbols_map("binance_usdm", only_syms).items():
            try:
                ls = {int(d["timestamp"]): d for d in
                      self._fetch_metric_json("topLongShortAccountRatio", ex_sym, days, period)}
                tk = {int(d["timestamp"]): d for d in
                      self._fetch_metric_json("takerlongshortRatio", ex_sym, days, period)}
                ts = sorted(set(ls) | set(tk))
                rows = [
                    SentimentRow(
                        timestamp=t, exchange="binance_usdm", symbol=ex_sym,
                        long_short_account_ratio=float(ls[t]["longShortRatio"]) if t in ls else None,
                        taker_long_short_ratio=float(tk[t]["buySellRatio"]) if t in tk else None,
                    )
                    for t in ts
                ]
                if rows:
                    self.store.save("binance_usdm", unified, "sentiment", rows)
                    logger.info(f"[binance_usdm] {unified} sentiment: {len(rows)} 行")
            except Exception as e:
                logger.error(f"[binance_usdm] {unified} sentiment 失败: {e}")

    # ── 模式入口 ──

    def run_daily(self, exchanges=None, symbols=None, types=None):
        logger.info(f"=== DAILY 目标=近{self.rest_window_days}天+T-1归档 symbols={symbols or '全'} types={types or '全'} ===")
        if types is None or "ohlcv" in types:
            self._run_ohlcv_daily(exchanges, symbols)
        if types is None or "funding" in types:
            self._run_funding_daily(symbols)
        if types is None or "oi" in types:
            self._run_oi(symbols, self.rest_window_days)
        if types is None or "sentiment" in types:
            self._run_sentiment(symbols, self.rest_window_days)

    def run_backfill(self, days: int, exchanges=None, symbols=None, types=None):
        logger.info(f"=== BACKfill {days}天 symbols={symbols or '全'} types={types or '全'} ===")
        if types is None or "ohlcv" in types:
            self._run_ohlcv_backfill(days, exchanges, symbols)
        if types is None or "funding" in types:
            self._run_funding_backfill(days, symbols)
        if types is None or "oi" in types:
            self._run_oi(symbols, days)
        if types is None or "sentiment" in types:
            self._run_sentiment(symbols, days)

    def run_verify(self) -> int:
        """每流 span/行数/新鲜度；退出码 1 = 有 MISSING/STALE（cron 告警接此）。"""
        now = _now_ms()
        max_age = self.config.get("verify", {}).get("max_age_hours", {})
        bad = 0
        rows_report = []
        for ex_name in self._enabled_exchanges(None):
            for t in self._types_enabled(ex_name, None):
                if t == "ohlcv":
                    tfs = self.config["exchanges"][ex_name]["data_types"]["ohlcv"].get("timeframes", ["1d"])
                    for unified in self._symbols_map(ex_name, None):
                        for tf in tfs:
                            bad += self._verify_one(ex_name, unified, tf, t, now, max_age, rows_report)
                else:
                    for unified in self._symbols_map(ex_name, None):
                        bad += self._verify_one(ex_name, unified, t, t, now, max_age, rows_report)
        print(f"\n{'流':52s} {'行数':>8s} {'最后时间(UTC)':>20s} 状态")
        for r in rows_report:
            print(r)
        print(f"\n结果: {'✅ 全部新鲜' if bad == 0 else f'⚠ {bad} 流 MISSING/STALE'}")
        return 0 if bad == 0 else 1

    def _verify_one(self, ex_name, unified, slot, dtype, now, max_age, report) -> int:
        path = self.store._get_file_path(ex_name, unified, slot)
        if not os.path.exists(path):
            report.append(f"{ex_name}/{unified}/{slot:8s} {'0':>8s} {'-':>20s} ❌ MISSING")
            return 1
        df = pd.read_parquet(path)
        if df.empty:
            report.append(f"{ex_name}/{unified}/{slot:8s} {'0':>8s} {'-':>20s} ❌ EMPTY")
            return 1
        last = int(df["timestamp"].max())
        age_h = (now - last) / 3600_000
        limit = float(max_age.get(dtype, 48))
        status = "✅" if age_h <= limit else f"⚠ STALE({age_h:.0f}h)"
        ts = datetime.fromtimestamp(last / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        report.append(f"{ex_name}/{unified}/{slot:8s} {len(df):>8d} {ts:>20s} {status}")
        return 0 if age_h <= limit else 1


def _csv_list(v: Optional[str]) -> Optional[List[str]]:
    if not v:
        return None
    return [x.strip() for x in v.split(",") if x.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description="panel-collector 统一 REST 采集器")
    sub = ap.add_subparsers(dest="mode", required=True)

    d = sub.add_parser("daily", help="日频防过期采集")
    for p in (d,):
        p.add_argument("--exchanges", help="逗号分隔：binance_spot,binance_usdm,deribit,hyperliquid")
        p.add_argument("--symbols", help="逗号分隔统一符号：BTC,ETH")
        p.add_argument("--types", help="逗号分隔：ohlcv,funding,oi,sentiment")

    b = sub.add_parser("backfill", help="深历史回补")
    b.add_argument("--days", type=int, default=None, help="回补天数（默认 config history_days）")
    b.add_argument("--exchanges"); b.add_argument("--symbols"); b.add_argument("--types")

    v = sub.add_parser("verify", help="自校验（退出码=健康）")
    ap.add_argument("--config", default=os.path.join(_SUBDIR, "config.yaml"))

    args = ap.parse_args()
    pc = PanelCollector(args.config)

    if args.mode == "daily":
        pc.run_daily(_csv_list(args.exchanges), _csv_list(args.symbols), _csv_list(args.types))
        return pc.run_verify() if not any([args.exchanges, args.symbols, args.types]) else 0
    if args.mode == "backfill":
        pc.run_backfill(args.days or pc.default_history_days, _csv_list(args.exchanges),
                        _csv_list(args.symbols), _csv_list(args.types))
        return 0
    if args.mode == "verify":
        return pc.run_verify()
    return 2


if __name__ == "__main__":
    sys.exit(main())
