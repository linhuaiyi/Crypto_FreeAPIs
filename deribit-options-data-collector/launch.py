"""
V3.0 期权+合约策略数据采集系统 — 统一启动入口

单进程、多线程架构:
  - WS Bridge: QuoteFetcher WebSocket → ChunkedBuffer
  - REST Pollers: FundingRate / MarkPrice / MarginParams / RiskFreeRate
  - Signal Activation: SpotPrice / Greeks / BasisVol
  - Monitor: 线程健康检查 + 内存哨兵
  - 信号处理: SIGINT/SIGTERM 优雅退出

用法:
  python launch.py --mode live          # 实盘持续采集
  python launch.py --mode test          # 运行 60 秒验证后退出
  python launch.py --strategies P0      # 仅启动 P0 级策略
"""

from __future__ import annotations

import argparse
import calendar
import gc
import os
import signal
import sys
import time
import threading

import requests
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd

# ── sys.path 预处理: 导入父目录的 V3.0 模块 ──
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SUBPROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ── .env 加载 ──
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(_env_path):
        _env_path = os.path.join(_PROJECT_ROOT, ".env")
    load_dotenv(_env_path, override=False)
except ImportError:
    pass

from storage.chunked_buffer import ChunkedBuffer
from fetchers.quote_fetcher import QuoteFetcher
from fetchers.funding_rate import FundingRateFetcher
from fetchers.mark_price import MarkPriceFetcher
from fetchers.risk_free_rate import RiskFreeRateFetcher
from fetchers.margin_params import MarginParamsFetcher
from collections import deque

from fetchers.deribit_market_data import DeribitMarketDataFetcher, IndexPriceSnapshot
from fetchers.deribit_options_ws import DeribitOptionsQuoteEngine
from processors.greeks_processor import GreeksProcessor, DeribitOptionsChainFetcher
from processors.basis_calculator import BasisCalculator, BasisPoint
from processors.vol_surface import VolatilitySurfaceBuilder
from processors.iv_rank import IVRankTracker
from processors import validators as V
from pipeline.strategy_configs import get_all_strategies, StrategyConfig, DataRequirement
from utils import get_logger
from utils.config_loader import ConfigLoader

# LOG_DIR 环境变量 — RotatingFileHandler 会自动使用
os.environ.setdefault("LOG_DIR", os.path.join(_SUBPROJECT_DIR, "logs"))

try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False

logger = get_logger("Launch")


# ── 常量 ──
MEMORY_LIMIT_MB = 6 * 1024          # 6 GB 内存警戒线
MONITOR_INTERVAL_SEC = 15            # 监控检查间隔
TEST_MODE_DURATION_SEC = 60          # test 模式运行时长
SHUTDOWN_JOIN_TIMEOUT = 10           # 线程 join 超时

# WS → Buffer 桥接间隔
WS_POLL_INTERVAL_SEC = 1.0

# REST 轮询间隔
MARK_PRICE_INTERVAL_SEC = 30
FUNDING_RATE_INTERVAL_SEC = 8 * 3600  # 8h
MARGIN_PARAMS_INTERVAL_SEC = 24 * 3600  # 24h
RISK_FREE_RATE_INTERVAL_SEC = 24 * 3600  # 24h

# Daily prune settings
PRUNE_HOUR = 3                          # 3am local time trigger
DEFAULT_PRUNE_KEEP_DAYS = 14            # retain 14 days of data

# Deribit WS 频道模板 — USDC-settled perpetuals
DERIBIT_WS_CHANNELS = [
    "ticker.BTC_USDC-PERPETUAL.100ms",
    "ticker.ETH_USDC-PERPETUAL.100ms",
    "ticker.SOL_USDC-PERPETUAL.100ms",
]

# Signal Activation intervals
SPOT_PRICE_INTERVAL_SEC = 1
GREEKS_INTERVAL_SEC = 5
BASIS_VOL_INTERVAL_SEC = 10
SPOT_SYMBOLS = ["BTC_USDC", "ETH_USDC", "SOL_USDC"]  # Deribit USDC spot tickers

# Deribit market data intervals
DVOL_INTERVAL_SEC = 30          # DVOL index (slow-moving, 30s is plenty)
INDEX_PRICE_INTERVAL_SEC = 10   # Real settlement index price


@dataclass
class CollectorStatus:
    """线程运行状态追踪。"""
    name: str
    priority: str
    thread: threading.Thread
    alive: bool = True
    error_count: int = 0
    last_success_ts: float = 0.0


class CollectorThread(threading.Thread):
    """可监控的 daemon 采集线程基类。"""

    def __init__(self, name: str, priority: str, shutdown_event: threading.Event) -> None:
        super().__init__(name=name, daemon=True)
        self._shutdown = shutdown_event
        self._priority = priority
        self._error_count = 0
        self._last_success = 0.0

    @property
    def priority(self) -> str:
        return self._priority

    @property
    def error_count(self) -> int:
        return self._error_count

    @property
    def last_success(self) -> float:
        return self._last_success

    def mark_success(self) -> None:
        self._last_success = time.monotonic()

    def stop(self) -> None:
        """信号子线程退出。子类可覆写。"""
        pass


class WSBridgeCollector(CollectorThread):
    """WS 快照 → ChunkedBuffer 桥接线程。

    每秒从 QuoteFetcher 采集一次 WS 快照，
    转换为 DataFrame 后写入 ChunkedBuffer。
    """

    def __init__(
        self,
        quote_fetcher: QuoteFetcher,
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
        shared_state: Optional[Dict] = None,
        state_lock: Optional[threading.Lock] = None,
    ) -> None:
        super().__init__("WS-Bridge", "P0", shutdown_event)
        self._fetcher = quote_fetcher
        self._buffer = buffer
        self._shared_state = shared_state
        self._state_lock = state_lock

    def stop(self) -> None:
        self._fetcher.stop_ws()

    def run(self) -> None:
        logger.info("[WS-Bridge] 启动，开始采集 WS 快照")
        while not self._shutdown.is_set():
            try:
                snapshots = self._fetcher.collect_ws_snapshots()
                if snapshots:
                    rows = [s.to_dict() for s in snapshots]
                    df = pd.DataFrame(rows)
                    now_ms = int(time.time() * 1000)

                    # 按 instrument 分组写入，便于 Hive 分区
                    for inst_name, group in df.groupby("instrument_name"):
                        self._buffer.append(
                            exchange=self._fetcher.exchange,
                            data_type="options_ticker",
                            symbol=inst_name,
                            df=group,
                        )
                        # 更新共享 perp price (供 BasisVol 线程使用)
                        if self._state_lock and "PERPETUAL" in inst_name:
                            mid = float(group["mid_price"].iloc[0])
                            currency = inst_name.split("-")[0]
                            with self._state_lock:
                                self._shared_state["perp_prices"][currency] = mid
                    self.mark_success()
                    logger.debug(f"[WS-Bridge] {len(snapshots)} snapshots → buffer")
                else:
                    logger.debug("[WS-Bridge] 无 WS 快照")

            except Exception as e:
                self._error_count += 1
                logger.warning(f"[WS-Bridge] 采集异常: {e}")

            self._shutdown.wait(WS_POLL_INTERVAL_SEC)

        logger.info("[WS-Bridge] 退出")


class RESTPollerCollector(CollectorThread):
    """通用 REST 轮询采集线程。"""

    def __init__(
        self,
        name: str,
        priority: str,
        poll_interval_sec: float,
        fetch_fn: Callable[[], int],
        shutdown_event: threading.Event,
    ) -> None:
        super().__init__(name, priority, shutdown_event)
        self._poll_interval = poll_interval_sec
        self._fetch_fn = fetch_fn

    def run(self) -> None:
        logger.info(f"[{self.name}] 启动，轮询间隔 {self._poll_interval}s")

        # ── Cold start: fetch once immediately before entering the wait loop ──
        try:
            rows = self._fetch_fn()
            if rows > 0:
                self.mark_success()
                logger.info(f"[{self.name}] 初始拉取成功: {rows} rows")
            else:
                logger.info(f"[{self.name}] 初始拉取: 暂无数据 (将在 {self._poll_interval}s 后重试)")
        except Exception as e:
            self._error_count += 1
            logger.warning(f"[{self.name}] 初始拉取异常: {e}")

        while not self._shutdown.is_set():
            self._shutdown.wait(self._poll_interval)
            if self._shutdown.is_set():
                break
            try:
                rows = self._fetch_fn()
                if rows > 0:
                    self.mark_success()
                    logger.debug(f"[{self.name}] {rows} rows fetched")
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[{self.name}] 轮询异常: {e}")

        logger.info(f"[{self.name}] 退出")


class GreeksProcessorThread(CollectorThread):
    """Greeks 计算线程：REST 获取期权链 → 向量化 BS 计算 → ChunkedBuffer。"""

    def __init__(
        self,
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
        shared_state: Dict,
        state_lock: threading.Lock,
        options_ws: Optional[DeribitOptionsQuoteEngine] = None,
    ) -> None:
        super().__init__("GreeksProcessor", "P0", shutdown_event)
        self._buffer = buffer
        self._shared_state = shared_state
        self._state_lock = state_lock
        self._options_ws = options_ws
        self._chain_fetcher = DeribitOptionsChainFetcher(ws_engine=options_ws)
        self._greeks_proc = GreeksProcessor()

    def run(self) -> None:
        logger.info("[GreeksProcessor] 启动，轮询间隔 5s")
        while not self._shutdown.is_set():
            if self._options_ws:
                try:
                    self._options_ws.maybe_refresh()
                except Exception as e:
                    logger.warning(f"[GreeksProcessor] options WS refresh: {e}")
            try:
                with self._state_lock:
                    rfr = self._shared_state.get("risk_free_rate", 0.05)

                # Linear (USDC-settled) options: BTC_USDC, ETH_USDC, SOL_USDC
                usdc_chains = self._chain_fetcher.fetch_usdc_option_chains()
                for base, chain in usdc_chains.items():
                    if not chain:
                        continue
                    result_df = self._greeks_proc.compute_batch(chain, risk_free_rate=rfr)
                    if not result_df.empty:
                        result_df = V.validate_options_greeks(result_df)
                    if not result_df.empty:
                        self._buffer.append(
                            "deribit", "options_greeks", base, result_df,
                        )
                        with self._state_lock:
                            self._shared_state["latest_greeks"][base] = result_df
                        self.mark_success()
                        del result_df

            except Exception as e:
                self._error_count += 1
                logger.warning(f"[GreeksProcessor] 计算异常: {e}")

            self._shutdown.wait(GREEKS_INTERVAL_SEC)

        logger.info("[GreeksProcessor] 退出")


class BasisVolProcessorThread(CollectorThread):
    """基差 + 波动率曲面计算线程。"""

    def __init__(
        self,
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
        shared_state: Dict,
        state_lock: threading.Lock,
        data_dir: str,
    ) -> None:
        super().__init__("BasisVol", "P1", shutdown_event)
        self._buffer = buffer
        self._shared_state = shared_state
        self._state_lock = state_lock
        self._basis_calc = BasisCalculator()
        self._vol_builder = VolatilitySurfaceBuilder()

        # Per-currency IV rank trackers, bootstrapped from existing parquet
        # so the first tick after restart already has a real baseline.
        # A state file outside data/ survives pull_data.sh cleanups and
        # gives the tracker a baseline even after remote-restart wipes.
        abs_data_dir = os.path.abspath(data_dir)
        state_dir = os.path.join(_SUBPROJECT_DIR, "state")
        self._iv_trackers: Dict[str, IVRankTracker] = {
            sym: IVRankTracker(
                sym,
                state_file=os.path.join(state_dir, f"iv_rank_{sym}.json"),
            )
            for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC")
        }
        for tracker in self._iv_trackers.values():
            tracker.bootstrap_from_parquet(abs_data_dir)

    def run(self) -> None:
        logger.info("[BasisVol] 启动，计算间隔 10s")
        while not self._shutdown.is_set():
            try:
                self._compute_basis()
                self._compute_vol_surface()
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[BasisVol] 计算异常: {e}")

            self._shutdown.wait(BASIS_VOL_INTERVAL_SEC)

        # Persist IV rank state so the next process restart can bootstrap
        # without losing the in-memory baseline.
        for tracker in self._iv_trackers.values():
            try:
                tracker.save_state()
            except Exception as e:
                logger.warning(f"[BasisVol] save IV rank state failed: {e}")

        logger.info("[BasisVol] 退出")

    def _compute_basis(self) -> None:
        with self._state_lock:
            spot = dict(self._shared_state.get("spot_prices", {}))
            perp = dict(self._shared_state.get("perp_prices", {}))

        now_ms = int(time.time() * 1000)
        for sym in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
            spot_price = spot.get(sym)
            perp_price = perp.get(sym)
            if not spot_price or not perp_price or spot_price <= 0:
                continue

            basis = perp_price - spot_price
            basis_pct = basis / spot_price * 100.0 if spot_price != 0 else 0.0
            annualized = basis_pct * (365.0 / 365)  # perp = 365d notional

            bp = BasisPoint(
                timestamp=now_ms,
                symbol=sym,
                basis_type="spot_perp",
                spot_price=spot_price,
                perp_price=perp_price,
                basis=basis,
                basis_pct=basis_pct,
                annualized_basis=annualized,
                days_to_expiry=365,
            )

            row = bp.to_dict()
            for k, v in row.items():
                if isinstance(v, float):
                    row[k] = np.float32(v)

            self._buffer.append(
                "deribit", "basis", sym,
                pd.DataFrame([row]),
            )
            self.mark_success()

    def _compute_vol_surface(self) -> None:
        # Read latest greeks per currency from shared state
        with self._state_lock:
            greeks_by_currency = dict(self._shared_state.get("latest_greeks", {}))

        for currency in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
            subset = greeks_by_currency.get(currency)
            if subset is None or subset.empty:
                continue

            try:
                # Build vol surface from greeks data
                options_df = pd.DataFrame({
                    "strike": subset["strike"],
                    "iv": subset["iv"],
                    "delta": subset["delta"],
                    "expiry": subset["expiry"],
                    "underlying_price": subset["underlying_price"],
                    "timestamp": subset["timestamp"],
                })
                underlying = float(subset["underlying_price"].iloc[0])

                tracker = self._iv_trackers[currency]
                historical_ivs = tracker.historical_ivs()

                point = self._vol_builder.build_surface(
                    options_df, underlying,
                    historical_ivs=historical_ivs,
                    symbol=currency,
                )

                # Feed this tick's atm_iv into the tracker so future days
                # include it in their baseline. Called AFTER rank computation
                # so the current day never ranks against itself.
                tracker.update(point.timestamp, point.atm_iv)

                row = {
                    "timestamp": point.timestamp,
                    "symbol": currency,
                    "atm_iv": np.float32(point.atm_iv),
                    "skew_25d": np.float32(point.skew_25d),
                    "butterfly_25d": np.float32(point.butterfly_25d),
                    "iv_rank": np.float32(point.iv_rank),
                    "term_structure": str(point.term_structure),
                    "quality": point.quality,
                }
                self._buffer.append(
                    "deribit", "vol_surface", currency,
                    pd.DataFrame([row]),
                )
                self.mark_success()

            except Exception as e:
                logger.warning(f"[BasisVol] Vol surface error for {currency}: {e}")


class DvolCollectorThread(CollectorThread):
    """DVOL (Deribit Volatility Index) collector — T2.

    Polls public/get_volatility_index_data for BTC and ETH and writes to
    deribit/dvol/{BTC,ETH}_*.parquet. The 0-1DTE strategy uses this for
    VIX-rank-style position scaling instead of proxying atm_iv.
    """

    def __init__(
        self,
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
    ) -> None:
        super().__init__("DvolCollector", "P1", shutdown_event)
        self._buffer = buffer
        self._fetcher = DeribitMarketDataFetcher()

    def run(self) -> None:
        logger.info(f"[DvolCollector] 启动，轮询间隔 {DVOL_INTERVAL_SEC}s")
        poll_count = 0
        while not self._shutdown.is_set():
            try:
                poll_count += 1
                for currency in ("BTC", "ETH"):
                    snap = self._fetcher.fetch_dvol(currency)
                    if snap is None or snap.dvol <= 0:
                        logger.warning(
                            f"[DvolCollector] poll={poll_count} {currency} "
                            f"no data (snap={snap})"
                        )
                        continue
                    df = V.validate_dvol(
                        pd.DataFrame([snap.to_dict()])
                    )
                    if df.empty:
                        logger.warning(
                            f"[DvolCollector] poll={poll_count} {currency} "
                            f"validate_dvol dropped row (dvol={snap.dvol})"
                        )
                        continue
                    self._buffer.append(
                        "deribit", "dvol", currency, df,
                    )
                    self.mark_success()
                    if poll_count % 4 == 0:
                        logger.info(
                            f"[DvolCollector] poll={poll_count} {currency} "
                            f"dvol={snap.dvol:.2f} buffered"
                        )
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[DvolCollector] 异常: {e}")

            self._shutdown.wait(DVOL_INTERVAL_SEC)

        logger.info("[DvolCollector] 退出")


class IndexPriceCollectorThread(CollectorThread):
    """Real Deribit index price collector — T3.

    Polls public/ticker on {BTC,ETH,SOL}_USDC-PERPETUAL for ``index_price`` —
    the settlement index used for option delivery. This is NOT the perpetual
    mark price (which carries basis). Writes to
    deribit/index_price/{BTC_USDC,ETH_USDC,SOL_USDC}_*.parquet.

    EDP (estimated_delivery_price) is computed as a rolling 30-minute
    TWAP of the index, matching Deribit's option settlement mechanism
    (07:30–08:00 UTC TWAP). The REST API ``public/ticker`` returns
    EDP = index_price for all instruments, so local computation is
    required.
    """

    _EDP_WINDOW_SEC = 1800       # 30 minutes
    _EDP_MAX_SAMPLES = 200       # at 10s interval ≈ 30 min + buffer

    def __init__(
        self,
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
        shared_state: Dict,
        state_lock: threading.Lock,
    ) -> None:
        super().__init__("IndexPrice", "P0", shutdown_event)
        self._buffer = buffer
        self._shared_state = shared_state
        self._state_lock = state_lock
        self._fetcher = DeribitMarketDataFetcher()
        self._index_history: Dict[str, deque] = {
            "BTC_USDC": deque(maxlen=self._EDP_MAX_SAMPLES),
            "ETH_USDC": deque(maxlen=self._EDP_MAX_SAMPLES),
            "SOL_USDC": deque(maxlen=self._EDP_MAX_SAMPLES),
        }

    def _compute_edp(self, currency: str) -> float:
        """Compute rolling 30-min TWAP of index price as EDP."""
        history = self._index_history[currency]
        if not history:
            return 0.0
        now_ms = int(time.time() * 1000)
        window_ms = self._EDP_WINDOW_SEC * 1000
        cutoff_ms = now_ms - window_ms
        windowed = [ip for ts, ip in history if ts >= cutoff_ms]
        if not windowed:
            return history[-1][1] if history else 0.0
        return sum(windowed) / len(windowed)

    def run(self) -> None:
        logger.info(f"[IndexPrice] 启动，轮询间隔 {INDEX_PRICE_INTERVAL_SEC}s")
        while not self._shutdown.is_set():
            try:
                for currency in ("BTC_USDC", "ETH_USDC", "SOL_USDC"):
                    snap = self._fetcher.fetch_index_price(currency)
                    if snap is None or snap.index_price <= 0:
                        continue

                    # Maintain rolling history for TWAP computation
                    self._index_history[currency].append(
                        (snap.timestamp, snap.index_price)
                    )

                    # Replace REST EDP (always == index_price) with
                    # locally-computed rolling 30-min TWAP
                    edp = self._compute_edp(currency)
                    fixed_snap = IndexPriceSnapshot(
                        timestamp=snap.timestamp,
                        symbol=snap.symbol,
                        index_price=snap.index_price,
                        mark_price=snap.mark_price,
                        estimated_delivery_price=edp,
                    )

                    df = V.validate_index_price(
                        pd.DataFrame([fixed_snap.to_dict()])
                    )
                    if not df.empty:
                        self._buffer.append(
                            "deribit", "index_price", currency, df,
                        )
                        with self._state_lock:
                            self._shared_state["index_prices"][currency] = snap.index_price
                        self.mark_success()
            except Exception as e:
                self._error_count += 1
                logger.warning(f"[IndexPrice] 异常: {e}")

            self._shutdown.wait(INDEX_PRICE_INTERVAL_SEC)

        logger.info("[IndexPrice] 退出")


class ResourceMonitor:
    """资源监控哨兵线程。"""

    def __init__(
        self,
        collectors: List[CollectorThread],
        buffer: ChunkedBuffer,
        shutdown_event: threading.Event,
        data_dir: str = "./data",
        prune_keep_days: int = DEFAULT_PRUNE_KEEP_DAYS,
    ) -> None:
        self._collectors = collectors
        self._buffer = buffer
        self._shutdown = shutdown_event
        self._thread: Optional[threading.Thread] = None
        self._process = psutil.Process() if _HAS_PSUTIL else None
        self._data_dir = data_dir
        self._prune_keep_days = prune_keep_days
        self._last_prune_date: Optional[str] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="Monitor")
        self._thread.start()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread:
            self._thread.join(timeout)

    def _run(self) -> None:
        logger.info("[Monitor] 启动")
        while not self._shutdown.is_set():
            self._check()
            self._shutdown.wait(MONITOR_INTERVAL_SEC)

    def _check(self) -> None:
        # 线程存活检查
        for c in self._collectors:
            if not c.is_alive():
                logger.warning(
                    f"[Monitor] 线程 {c.name} ({c.priority}) 已死亡, "
                    f"errors={c.error_count}"
                )

        if not self._process:
            return

        # 内存检查
        try:
            rss_mb = self._process.memory_info().rss / (1024**2)
            if rss_mb > MEMORY_LIMIT_MB:
                logger.warning(
                    f"[Monitor] RSS {rss_mb:.0f} MB 超过 {MEMORY_LIMIT_MB} MB 警戒线, "
                    f"触发紧急 flush"
                )
                try:
                    self._buffer.flush_all()
                except Exception as e:
                    logger.error(f"[Monitor] 紧急 flush 失败: {e}")

                gc.collect()
        except Exception:
            pass

        # Daily prune trigger (default 3am)
        now = time.localtime()
        today_str = time.strftime("%Y-%m-%d")
        if now.tm_hour == PRUNE_HOUR and self._last_prune_date != today_str:
            self._last_prune_date = today_str
            self._run_daily_prune()

    def _run_daily_prune(self) -> None:
        """Trigger daily data pruning via scripts/prune_data.py."""
        try:
            _scripts = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "scripts",
            )
            if _scripts not in sys.path:
                sys.path.insert(0, _scripts)
            from prune_data import prune_data as _prune_fn

            stats = _prune_fn(self._data_dir, self._prune_keep_days, execute=True)
            deleted = stats.get("deleted_dirs", 0)
            freed_mb = stats.get("freed_bytes", 0) / 1024 / 1024
            logger.info(
                f"[Monitor] Daily prune: {deleted} dirs removed, "
                f"{freed_mb:.1f} MB freed"
            )
        except Exception as e:
            logger.warning(f"[Monitor] Daily prune failed: {e}")


class SystemLauncher:
    """V3.0 数据采集系统总调度器。"""

    def __init__(self, args: argparse.Namespace) -> None:
        self._mode = args.mode
        self._strategies_filter = args.strategies
        self._shutdown_event = threading.Event()
        self._collectors: List[CollectorThread] = []
        self._ws_fetcher: Optional[QuoteFetcher] = None
        self._buffer: Optional[ChunkedBuffer] = None
        self._monitor: Optional[ResourceMonitor] = None
        self._start_time = 0.0
        self._data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "deribit-options-data-collector", "data"
        )
        self._fred_mode = "Fallback (5%)"  # updated during _start_p2_collectors

        # Signal Activation shared state (cross-thread data)
        self._shared_state: Dict = {
            "spot_prices": {},       # {"BTC_USDC": 103000.0, ...}
            "perp_prices": {},       # {"BTC_USDC": 103050.0, ...}
            "index_prices": {},      # {"BTC_USDC": 102980.0, ...} — real Deribit index
            "risk_free_rate": 0.05,
            "latest_greeks": {},     # {"BTC_USDC": df, ...} — per-currency
        }
        self._state_lock = threading.Lock()

    def run(self) -> None:
        self._start_time = time.monotonic()
        self._print_banner()

        # 注册信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._init_storage()
            self._start_p0_collectors()
            time.sleep(2)
            self._start_p1_collectors()
            time.sleep(1)
            self._start_p2_collectors()

            self._start_monitor()

            if self._mode == "test":
                self._run_test_mode()
            else:
                self._run_live_mode()
        except Exception as e:
            logger.error(f"系统异常: {e}")
        finally:
            self._shutdown()

    def _print_banner(self) -> None:
        config = ConfigLoader.get(
            os.path.join(_SUBPROJECT_DIR, "config_strategy.yaml")
        )
        data_dir = config.get_value("global", "data_dir", default="./data")
        fred_key = os.environ.get("FRED_API_KEY", "")
        fallback = config.get_value("risk_free_rate", "fallback_rate", default=0.05)
        fred_status = (
            "已配置" if fred_key
            else f"未配置 -- 降级模式 ({fallback * 100:.1f}%)"
        )

        print("=" * 72)
        print("  V3.0 期权+合约策略数据采集系统")
        print("=" * 72)
        print(f"  模式:     {self._mode}")
        print(f"  策略:     {self._strategies_filter}")
        print(f"  数据目录: {os.path.abspath(data_dir)}")
        print(f"  FRED Key: {fred_status}")
        print(f"  Python:   {sys.version.split()[0]}")
        print(f"  线程数:   {threading.active_count()}")
        print("=" * 72)
        logger.info(f"FRED_API_KEY 加载状态: {fred_status}")

    def _init_storage(self) -> None:
        """Phase 0: 初始化 ChunkedBuffer 并启动定时 flush。"""
        config = ConfigLoader.get(
            os.path.join(_SUBPROJECT_DIR, "config_strategy.yaml")
        )
        data_dir = config.get_value("global", "data_dir", default="./data")
        max_rows = config.get_value("storage", "chunked_buffer", "max_rows", default=100_000)
        max_mem = config.get_value("storage", "chunked_buffer", "max_memory_mb", default=200)
        flush_sec = config.get_value("storage", "chunked_buffer", "flush_interval_sec", default=300)

        self._data_dir = data_dir
        self._buffer = ChunkedBuffer(
            data_dir=data_dir,
            max_rows=max_rows,
            max_memory_mb=max_mem,
            flush_interval_sec=flush_sec,
        )
        self._buffer.start_periodic_flush()
        logger.info(f"ChunkedBuffer 初始化完成 (data_dir={data_dir})")

    def _should_run(self, priority: str) -> bool:
        """检查指定优先级是否应启动。"""
        filt = self._strategies_filter
        if filt == "all":
            return True
        return priority in filt.split(",")

    def _start_p0_collectors(self) -> None:
        """Phase 1: P0 级数据源 — WS 报价 + 标记价格。"""
        if not self._should_run("P0"):
            logger.info("跳过 P0 采集器 (策略过滤)")
            return

        logger.info("── 启动 P0 级采集器 ──")

        # WS Bridge: Deribit L1 报价
        self._ws_fetcher = QuoteFetcher("deribit")
        self._ws_fetcher.add_ws_channels(DERIBIT_WS_CHANNELS)
        self._ws_fetcher.start_ws()

        # Options quote WS engine (quote.{instrument}) — provides
        # bid_size/ask_size coverage for the full options chain on every
        # greeks tick. Hourly refresh picks up new expiries.
        self._options_ws = DeribitOptionsQuoteEngine()
        self._options_ws.start()
        time.sleep(2)  # let the WS connection establish before subscribing
        try:
            self._options_ws.maybe_refresh(force=True)
        except Exception as e:
            logger.warning(f"Options quote WS 初始订阅失败: {e}")

        bridge = WSBridgeCollector(
            quote_fetcher=self._ws_fetcher,
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
            shared_state=self._shared_state,
            state_lock=self._state_lock,
        )
        bridge.start()
        self._collectors.append(bridge)

        # MarkPrice REST 轮询
        mark_fetcher = MarkPriceFetcher()

        def _poll_mark_price() -> int:
            total = 0
            now_ms = int(time.time() * 1000)
            # Binance klines 最小粒度 1m, 需要至少几分钟的窗口
            start_ms = now_ms - max(MARK_PRICE_INTERVAL_SEC, 300) * 1000

            for symbol in ["BTCUSDT", "ETHUSDT"]:
                try:
                    records = mark_fetcher.fetch_binance(symbol, start_ms, now_ms)
                    if records:
                        rows = [r.to_dict() for r in records]
                        # Fill index_price from premiumIndex (Binance
                        # markPriceKlines only returns mark OHLC, not index).
                        try:
                            pi = requests.get(
                                "https://fapi.binance.com/fapi/v1/premiumIndex",
                                params={"symbol": symbol},
                                timeout=10,
                            ).json()
                            ip = float(pi.get("indexPrice", 0))
                            mp = float(pi.get("markPrice", 0))
                            if ip > 0:
                                for row in rows:
                                    row["index_price"] = ip
                                    row["basis"] = row.get("mark_price", mp) - ip
                        except Exception:
                            pass
                        self._buffer.append("binance", "mark_price", symbol, pd.DataFrame(rows))
                        total += len(records)
                except Exception as e:
                    logger.warning(f"MarkPrice Binance {symbol}: {e}")

            for symbol in ["BTC_USDC-PERPETUAL", "ETH_USDC-PERPETUAL", "SOL_USDC-PERPETUAL"]:
                try:
                    records = mark_fetcher.fetch_deribit(symbol, start_ms, now_ms)
                    if records:
                        rows = [r.to_dict() for r in records]
                        # T3: fill index_price from the real Deribit settlement
                        # index. fetch_deribit only returns mark klines, so we
                        # pull the current index_price via ticker and attach it
                        # to each row. This is approximate for historical rows
                        # in the batch but ensures the column is non-NULL going
                        # forward. The dedicated index_price stream provides
                        # the precise sub-minute series.
                        currency = symbol.split("-")[0]
                        try:
                            mdf = DeribitMarketDataFetcher()
                            ip_snap = mdf.fetch_index_price(currency)
                            if ip_snap and ip_snap.index_price > 0:
                                for row in rows:
                                    row["index_price"] = ip_snap.index_price
                                    row["basis"] = row["mark_price"] - ip_snap.index_price
                        except Exception as e:
                            logger.debug(f"MarkPrice index fill {symbol}: {e}")
                        df = pd.DataFrame(rows)
                        df = V.validate_mark_price(df)
                        if not df.empty:
                            self._buffer.append("deribit", "mark_price", symbol, df)
                            total += len(df)
                except Exception as e:
                    logger.warning(f"MarkPrice Deribit {symbol}: {e}")

            return total

        mark_poller = RESTPollerCollector(
            name="MarkPrice",
            priority="P0",
            poll_interval_sec=MARK_PRICE_INTERVAL_SEC,
            fetch_fn=_poll_mark_price,
            shutdown_event=self._shutdown_event,
        )
        mark_poller.start()
        self._collectors.append(mark_poller)

        # ── SpotPrice Poller (P0) — Deribit USDC spot tickers ──
        spot_fetcher = DeribitMarketDataFetcher()

        def _poll_spot_price() -> int:
            total = 0
            for symbol in SPOT_SYMBOLS:
                try:
                    sp = spot_fetcher.fetch_spot_ticker(symbol)
                    if sp is None:
                        continue
                    self._buffer.append(
                        "deribit", "spot_price", sp.symbol,
                        pd.DataFrame([sp.to_dict()]),
                    )
                    total += 1
                    with self._state_lock:
                        self._shared_state["spot_prices"][sp.symbol] = sp.price
                except Exception as e:
                    logger.warning(f"SpotPrice {symbol}: {e}")
            return total

        spot_poller = RESTPollerCollector(
            name="SpotPrice",
            priority="P0",
            poll_interval_sec=SPOT_PRICE_INTERVAL_SEC,
            fetch_fn=_poll_spot_price,
            shutdown_event=self._shutdown_event,
        )
        spot_poller.start()
        self._collectors.append(spot_poller)

        # ── Greeks Processor Thread (P0) ──
        greeks_thread = GreeksProcessorThread(
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
            shared_state=self._shared_state,
            state_lock=self._state_lock,
            options_ws=self._options_ws,
        )
        greeks_thread.start()
        self._collectors.append(greeks_thread)

        # ── Index Price Collector (P0) — real Deribit settlement index (T3) ──
        index_thread = IndexPriceCollectorThread(
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
            shared_state=self._shared_state,
            state_lock=self._state_lock,
        )
        index_thread.start()
        self._collectors.append(index_thread)

    def _start_p1_collectors(self) -> None:
        """Phase 2: P1 级数据源 — 资金费率 + 保证金参数。"""
        if not self._should_run("P1"):
            logger.info("跳过 P1 采集器 (策略过滤)")
            return

        logger.info("── 启动 P1 级采集器 ──")

        # FundingRate
        funding_fetcher = FundingRateFetcher()

        def _binance_funding_with_snapshot(symbol: str, start_ms: int, end_ms: int):
            """Historical events (authoritative funding_rate) enriched with
            current mark_price/index_price from the realtime endpoint.

            The /fundingRate history endpoint doesn't return mark/index; the
            /premiumIndex realtime endpoint does but only gives the most
            recent funding rate. Combine: take all historical events in the
            poll window, then overlay current mark/index onto the latest one.
            Older events keep mark/index=None (Binance doesn't expose historical
            mark/index at the funding instant).
            """
            from dataclasses import replace as _replace
            records = list(funding_fetcher.fetch_binance(symbol, start_ms, end_ms))
            rt = funding_fetcher.fetch_binance_realtime(symbol)
            if rt is None:
                return records
            if not records:
                return [rt]
            records[-1] = _replace(
                records[-1],
                mark_price=rt.mark_price,
                index_price=rt.index_price,
            )
            return records

        def _poll_funding_rate() -> int:
            total = 0
            now_ms = int(time.time() * 1000)
            start_ms = now_ms - FUNDING_RATE_INTERVAL_SEC * 1000

            def _deribit_funding_with_snapshot(instrument: str, start_ms: int, end_ms: int):
                """Deribit funding history doesn't return mark_price.
                Supplement with current mark_price from public/ticker."""
                records = list(funding_fetcher.fetch_deribit(instrument, start_ms, end_ms))
                if not records:
                    return records
                try:
                    currency = instrument.split("-")[0]
                    mdf = DeribitMarketDataFetcher()
                    ip_snap = mdf.fetch_index_price(currency)
                    if ip_snap and ip_snap.mark_price > 0:
                        from dataclasses import replace as _replace
                        records[-1] = _replace(
                            records[-1],
                            mark_price=ip_snap.mark_price,
                        )
                except Exception:
                    pass
                return records

            tasks = [
                ("binance", "BTCUSDT", lambda: _binance_funding_with_snapshot("BTCUSDT", start_ms, now_ms)),
                ("binance", "ETHUSDT", lambda: _binance_funding_with_snapshot("ETHUSDT", start_ms, now_ms)),
                ("binance", "SOLUSDT", lambda: _binance_funding_with_snapshot("SOLUSDT", start_ms, now_ms)),
                ("deribit", "BTC_USDC-PERPETUAL", lambda: _deribit_funding_with_snapshot("BTC_USDC-PERPETUAL", start_ms, now_ms)),
                ("deribit", "ETH_USDC-PERPETUAL", lambda: _deribit_funding_with_snapshot("ETH_USDC-PERPETUAL", start_ms, now_ms)),
                ("deribit", "SOL_USDC-PERPETUAL", lambda: _deribit_funding_with_snapshot("SOL_USDC-PERPETUAL", start_ms, now_ms)),
                ("hyperliquid", "BTC", lambda: funding_fetcher.fetch_hyperliquid("BTC", start_ts=start_ms, end_ts=now_ms)),
                ("hyperliquid", "ETH", lambda: funding_fetcher.fetch_hyperliquid("ETH", start_ts=start_ms, end_ts=now_ms)),
                ("hyperliquid", "SOL", lambda: funding_fetcher.fetch_hyperliquid("SOL", start_ts=start_ms, end_ts=now_ms)),
            ]
            for exchange, symbol, fn in tasks:
                try:
                    records = fn()
                    if records:
                        rows = [r.to_dict() for r in records]
                        self._buffer.append(exchange, "funding_rate", symbol, pd.DataFrame(rows))
                        total += len(records)
                except Exception as e:
                    logger.warning(f"FundingRate {exchange}/{symbol}: {e}")
            return total

        funding_poller = RESTPollerCollector(
            name="FundingRate",
            priority="P1",
            poll_interval_sec=FUNDING_RATE_INTERVAL_SEC,
            fetch_fn=_poll_funding_rate,
            shutdown_event=self._shutdown_event,
        )
        funding_poller.start()
        self._collectors.append(funding_poller)

        # MarginParams
        margin_fetcher = MarginParamsFetcher()

        def _poll_margin_params() -> int:
            total = 0
            for currency in ["BTC", "ETH"]:
                try:
                    records = margin_fetcher.fetch_deribit_instruments(currency)
                    if records:
                        rows = [r.to_dict() for r in records]
                        self._buffer.append("deribit", "margin_params", currency, pd.DataFrame(rows))
                        total += len(records)
                except Exception as e:
                    logger.warning(f"MarginParams {currency}: {e}")
            return total

        margin_poller = RESTPollerCollector(
            name="MarginParams",
            priority="P1",
            poll_interval_sec=MARGIN_PARAMS_INTERVAL_SEC,
            fetch_fn=_poll_margin_params,
            shutdown_event=self._shutdown_event,
        )
        margin_poller.start()
        self._collectors.append(margin_poller)

        # ── Basis/Vol Surface Processor (P1) ──
        basis_vol_thread = BasisVolProcessorThread(
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
            shared_state=self._shared_state,
            state_lock=self._state_lock,
            data_dir=self._data_dir,
        )
        basis_vol_thread.start()
        self._collectors.append(basis_vol_thread)

        # ── DVOL Collector (P1) — Deribit volatility index (T2) ──
        dvol_thread = DvolCollectorThread(
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
        )
        dvol_thread.start()
        self._collectors.append(dvol_thread)

    def _start_p2_collectors(self) -> None:
        """Phase 3: P2 级数据源 — 无风险利率。"""
        if not self._should_run("P2"):
            logger.info("跳过 P2 采集器 (策略过滤)")
            return

        fred_key = os.environ.get("FRED_API_KEY", "")
        config = ConfigLoader.get(
            os.path.join(_SUBPROJECT_DIR, "config_strategy.yaml")
        )
        fallback = config.get_value("risk_free_rate", "fallback_rate", default=0.05)

        if not fred_key:
            logger.warning(
                "FRED_API_KEY 未设置，RiskFreeRate 进入降级模式 (使用 fallback rate)"
            )
            self._fred_mode = f"Fallback ({fallback * 100:.1f}%)"
        else:
            self._fred_mode = "Live (FRED API)"

        cache_dir = config.get_value("api", "fred", "cache_dir", default="./cache/fred")
        # 相对路径锚定到子项目目录, 避免随运行 CWD 漂移产生双份缓存
        if not os.path.isabs(cache_dir):
            cache_dir = os.path.join(_SUBPROJECT_DIR, cache_dir)

        risk_fetcher = RiskFreeRateFetcher(
            api_key=fred_key or "DEGRADED",
            cache_dir=cache_dir,
        )

        def _poll_risk_free_rate() -> int:
            today = time.strftime("%Y-%m-%d")
            try:
                curve = risk_fetcher.build_yield_curve(today)
                if curve:
                    rows = []
                    for r in curve:
                        d = r.to_dict() if hasattr(r, 'to_dict') else vars(r)
                        d["timestamp"] = int(
                            calendar.timegm(time.strptime(d.get("date", today), "%Y-%m-%d"))
                        ) * 1000
                        rows.append(d)
                    self._buffer.append("fred", "risk_free_rate", "USD", pd.DataFrame(rows))
                    # 更新共享 RFR (供 Greeks 线程使用)
                    for r in curve:
                        rd = r.to_dict() if hasattr(r, 'to_dict') else vars(r)
                        if rd.get("tenor_years") == 0.25:  # 3M rate
                            rate = rd.get("rate_continuous", 0.05)
                            with self._state_lock:
                                self._shared_state["risk_free_rate"] = rate
                            break
                    return len(curve)
            except Exception as e:
                logger.warning(f"RiskFreeRate: {e}")
            return 0

        risk_poller = RESTPollerCollector(
            name="RiskFreeRate",
            priority="P2",
            poll_interval_sec=RISK_FREE_RATE_INTERVAL_SEC,
            fetch_fn=_poll_risk_free_rate,
            shutdown_event=self._shutdown_event,
        )
        risk_poller.start()
        self._collectors.append(risk_poller)

    def _start_monitor(self) -> None:
        """启动资源监控哨兵。"""
        keep_days = DEFAULT_PRUNE_KEEP_DAYS
        config = ConfigLoader.get(
            os.path.join(_SUBPROJECT_DIR, "config_strategy.yaml")
        )
        keep_days = config.get_value(
            "storage", "prune_keep_days", default=DEFAULT_PRUNE_KEEP_DAYS
        )
        self._monitor = ResourceMonitor(
            collectors=self._collectors,
            buffer=self._buffer,
            shutdown_event=self._shutdown_event,
            data_dir=self._data_dir,
            prune_keep_days=keep_days,
        )
        self._monitor.start()

    def _run_live_mode(self) -> None:
        """实盘模式: 阻塞主线程直到收到退出信号。"""
        logger.info("系统启动完成，进入实盘采集模式 (Ctrl+C 退出)")
        self._shutdown_event.wait()

    def _run_test_mode(self) -> None:
        """测试模式: 运行指定时长后退出。"""
        logger.info(f"测试模式: 运行 {TEST_MODE_DURATION_SEC} 秒后退出")
        self._shutdown_event.wait(timeout=TEST_MODE_DURATION_SEC)
        if not self._shutdown_event.is_set():
            logger.info("测试时间到，触发退出")
            self._shutdown_event.set()

    def _signal_handler(self, signum: int, frame: object) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(f"收到信号 {sig_name}，开始优雅退出...")
        self._shutdown_event.set()

    def _shutdown(self) -> None:
        """优雅退出序列。"""
        elapsed = time.monotonic() - self._start_time
        logger.info("── 开始优雅退出 ──")

        # 1. 设置 shutdown event (可能已经设置了)
        self._shutdown_event.set()

        # 1a. 立即取消周期 flush 定时器，防止它与 close_all_writers 抢 _lock
        #     导致优雅退出超时被强杀、丢当天数据。
        try:
            t = getattr(self._buffer, "_timer", None)
            if t is not None:
                t.cancel()
                self._buffer._timer = None
        except Exception:
            pass

        # 2. 停止 WS Bridge (关闭 WebSocket 连接)
        for c in self._collectors:
            try:
                c.stop()
            except Exception as e:
                logger.warning(f"停止 {c.name} 失败: {e}")

        # 3. join 所有采集线程
        for c in self._collectors:
            try:
                c.join(timeout=SHUTDOWN_JOIN_TIMEOUT)
                status = "alive" if c.is_alive() else "stopped"
                logger.info(f"  {c.name} ({c.priority}): {status}")
            except Exception:
                pass

        # 3.5 停止 Options quote WS engine (在 greeks 线程退出之后)
        options_ws = getattr(self, "_options_ws", None)
        if options_ws is not None:
            try:
                options_ws.stop()
                logger.info("Options quote WS engine 已停止")
            except Exception as e:
                logger.warning(f"停止 Options quote WS 失败: {e}")

        # 4. 最终 flush — 确保内存中的数据全部落盘
        flushed_rows = 0
        if self._buffer:
            try:
                flushed_rows = self._buffer.flush_all()
                logger.info(f"最终 flush: {flushed_rows} rows")
            except Exception as e:
                logger.error(f"最终 flush 失败: {e}")
            # Finalize append-only Parquet writers (write footers). Writers
            # stay open between flushes for memory efficiency; without this
            # close, the day-files lack a footer and are unreadable on restart
            # (the restart-merge would quarantine them, losing the day's data).
            try:
                self._buffer.close_all_writers()
                logger.info("parquet writers finalized (footers written)")
            except Exception as e:
                logger.error(f"close_all_writers 失败: {e}")

        # 5. 停止 Monitor
        if self._monitor:
            self._monitor.join(timeout=5)

        # 6. 显式 GC
        gc.collect()

        # 7. 数据完整性审计 —— 关停时跳过：它会 pd.read_parquet 读取全部 parquet
        #    文件 (GB 级, 分钟级耗时)，拖慢退出导致超时强杀、丢当天 open-writer 数据。
        #    需要审计时用 verify-data skill 按需运行。
        # if self._mode == "live" and elapsed > 300:
        #     self._run_data_audit()

        # 8. 打印运行摘要
        self._print_summary(elapsed, flushed_rows)

        logger.info("── 退出完成 ──")

    def _run_data_audit(self) -> None:
        """在 flush 完成后对落盘数据做完整性审计。"""
        config = ConfigLoader.get(
            os.path.join(_SUBPROJECT_DIR, "config_strategy.yaml")
        )
        data_dir = config.get_value("global", "data_dir", default="./data")

        if not os.path.isdir(data_dir):
            logger.warning("数据审计跳过: 数据目录不存在")
            return

        audit_lines: List[str] = []
        audit_lines.append(f"# 数据完整性审计报告")
        audit_lines.append(f"")
        audit_lines.append(f"> 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        audit_lines.append(f"> 数据目录: `{data_dir}`")
        audit_lines.append(f"")
        audit_lines.append(f"---")
        audit_lines.append(f"")

        total_files = 0
        total_rows = 0
        issues: List[str] = []

        audit_lines.append(f"| 路径 | 文件数 | 总行数 | 大小 |")
        audit_lines.append(f"|------|--------|--------|------|")

        for root, dirs, files in os.walk(data_dir):
            parquet_files = [f for f in files if f.endswith(".parquet")]
            if not parquet_files:
                continue
            total_files += len(parquet_files)

            dir_rows = 0
            dir_bytes = 0
            for pf in parquet_files:
                fp = os.path.join(root, pf)
                dir_bytes += os.path.getsize(fp)
                try:
                    df = pd.read_parquet(fp, columns=["timestamp"])
                    dir_rows += len(df)
                except Exception:
                    dir_rows += 0

            total_rows += dir_rows
            rel_path = os.path.relpath(root, data_dir)
            size_str = f"{dir_bytes / 1024 / 1024:.1f} MB"
            audit_lines.append(f"| `{rel_path}` | {len(parquet_files)} | {dir_rows:,} | {size_str} |")

        audit_lines.append(f"")
        audit_lines.append(f"**汇总**: {total_files} 文件, {total_rows:,} 行")
        audit_lines.append(f"")

        if issues:
            audit_lines.append(f"### 问题")
            for issue in issues:
                audit_lines.append(f"- {issue}")
        else:
            audit_lines.append(f"**状态**: 全部通过")

        # Print audit summary to console
        print("\n" + "=" * 60)
        print("  Data Integrity Audit")
        print("=" * 60)
        for line in audit_lines:
            if line.startswith("|") or line.startswith("**") or line.startswith("---"):
                print(f"  {line}")
        print("=" * 60)

        # Persist to logs/last_audit.md
        log_dir = os.environ.get("LOG_DIR", os.path.join(_SUBPROJECT_DIR, "logs"))
        os.makedirs(log_dir, exist_ok=True)
        audit_path = os.path.join(log_dir, "last_audit.md")

        try:
            with open(audit_path, "w", encoding="utf-8") as f:
                f.write("\n".join(audit_lines) + "\n")
            logger.info(f"数据审计完成: {total_files} files, {total_rows:,} rows -> {audit_path}")
        except OSError as e:
            logger.warning(f"审计报告写入失败: {e}")

    def _print_summary(self, elapsed: float, flushed_rows: int = 0) -> None:
        alive = [c.name for c in self._collectors if c.is_alive()]
        stopped = [c.name for c in self._collectors if not c.is_alive()]

        print("\n" + "=" * 72)
        print("  V3.0 运行摘要")
        print("=" * 72)
        print(f"  运行时长:     {elapsed:.1f} 秒")
        print(f"  启动线程:     {len(self._collectors)}")
        print(f"  已停止:       {', '.join(stopped) if stopped else '无'}")
        print(f"  仍在运行:     {', '.join(alive) if alive else '无'}")
        print(f"  最终 flush:   {flushed_rows} rows")
        print(f"  Rate Mode:    {self._fred_mode}")
        for c in self._collectors:
            print(f"    {c.name} ({c.priority}): errors={c.error_count}")
        print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="V3.0 期权+合约策略数据采集系统"
    )
    parser.add_argument(
        "--mode",
        choices=["live", "test"],
        default="live",
        help="运行模式: live=实盘持续, test=60秒验证",
    )
    parser.add_argument(
        "--strategies",
        default="all",
        help="策略优先级过滤: all, P0, P0,P1 等",
    )
    args = parser.parse_args()

    # Memory diagnostics: tracemalloc snapshots every 10 min -> logs/mem_trace.log.
    # Lets us distinguish a Python-level leak (tracked size climbs with RSS) from
    # allocator retention (tracked flat, RSS climbs). Safe no-op if unavailable.
    try:
        from utils.mem_trace import start as _start_mem_trace
        _start_mem_trace(interval_sec=600)
    except Exception as _e:
        logger.warning(f"mem_trace disabled: {_e}")

    launcher = SystemLauncher(args)
    launcher.run()


if __name__ == "__main__":
    main()
