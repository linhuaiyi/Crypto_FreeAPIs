"""
V3.0 期权+合约数据采集管线 — 本地端到端极限压测

硬件约束: i7-6700HQ (4C/8T), 24GB RAM, 40GB SSD 可用
防爆盘机制: 总写入 ≤ 5GB 或运行 ≤ 60s，结束后自动清理

压测链路:
  阶段一 (前40s): NumPy 批量生成 → ChunkedBuffer → ZSTD Parquet (磁盘I/O)
  阶段二 (后20s): TimeAligner merge_asof 多源对齐 (CPU)
  贯穿全程: psutil 资源监控哨兵 (rows/s, MB/s, CPU%, disk GB, RSS MB)
"""

from __future__ import annotations

import os
import shutil
import signal
import sys
import time
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import psutil

from storage.chunked_buffer import ChunkedBuffer
from processors.time_aligner import TimeAligner

# ── 硬限制 ──
MAX_DISK_BYTES = 5 * 1024**3        # 5 GB
MAX_DURATION_SEC = 60                # 总运行上限 60 秒
PHASE1_BUDGET_SEC = 40               # 阶段一时间预算
MONITOR_INTERVAL_SEC = 1.0          # 监控采样间隔
DISK_MIN_FREE_GB = 10               # 磁盘剩余低于此值立刻停止

# ── 数据生成参数 ──
BATCH_SIZE = 10_000                  # 每批行数
TARGET_ROWS_PER_SEC = 30_000        # 目标生成速率
NUM_OPTION_INSTRUMENTS = 50         # 模拟期权合约数
NUM_PERP_INSTRUMENTS = 3            # 模拟永续合约数

# ── ChunkedBuffer 参数 ──
BUFFER_MAX_ROWS = 100_000           # 触发行数 flush
BUFFER_MAX_MEMORY_MB = 200          # 触发内存 flush

# ── 压测数据目录 ──
STRESS_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_stress_test")


@dataclass
class StressMetrics:
    """压测运行指标累积器。"""
    total_rows_generated: int = 0
    total_rows_flushed: int = 0
    total_bytes_on_disk: int = 0
    total_flushes: int = 0
    peak_rss_mb: float = 0.0
    start_time: float = 0.0
    stop_reason: str = ""
    merge_asof_total_rows: int = 0
    merge_asof_total_sec: float = 0.0
    merge_asof_rounds: int = 0

    current_rows_per_sec: float = 0.0
    current_write_mb_per_sec: float = 0.0
    current_cpu_pct: float = 0.0
    current_phase: str = "init"


class DataGenerator:
    """NumPy 向量化批量 Mock 数据生成器。"""

    def __init__(self, seed: int = 42) -> None:
        self._rng = np.random.default_rng(seed)
        self._base_ts = int(time.time() * 1000)

        strikes = np.arange(20000, 70000, 1000)
        expiries = ["28MAR26", "27JUN26", "26SEP26"]
        self._option_names: List[str] = []
        for exp in expiries:
            for strike in strikes:
                if len(self._option_names) >= NUM_OPTION_INSTRUMENTS:
                    break
                self._option_names.append(f"BTC-{exp}-{strike}-C")
            if len(self._option_names) >= NUM_OPTION_INSTRUMENTS:
                break

        self._perp_names = ["BTC-PERPETUAL", "ETH-PERPETUAL", "SOL-PERPETUAL"][
            :NUM_PERP_INSTRUMENTS
        ]

    def generate_option_batch(self, batch_idx: int) -> pd.DataFrame:
        n = BATCH_SIZE
        rng = self._rng

        timestamps = self._base_ts + batch_idx * n + np.arange(n, dtype=np.int64)
        name_indices = rng.integers(0, len(self._option_names), size=n)
        instrument_names = np.array(self._option_names)[name_indices]

        base_prices = rng.uniform(2.0, 8.0, size=n)
        spread_pct = rng.uniform(0.001, 0.01, size=n)

        bid_prices = np.round(base_prices * (1 - spread_pct / 2), 4)
        ask_prices = np.round(base_prices * (1 + spread_pct / 2), 4)
        mid_prices = np.round((bid_prices + ask_prices) / 2, 4)
        bid_sizes = np.round(rng.uniform(0.1, 5.0, size=n), 3)
        ask_sizes = np.round(rng.uniform(0.1, 5.0, size=n), 3)

        return pd.DataFrame({
            "timestamp": timestamps,
            "instrument_name": instrument_names,
            "exchange": "deribit",
            "symbol": "BTC",
            "bid_price": bid_prices,
            "ask_price": ask_prices,
            "mid_price": mid_prices,
            "bid_size": bid_sizes,
            "ask_size": ask_sizes,
        })

    def generate_perp_batch(self, batch_idx: int) -> pd.DataFrame:
        n = BATCH_SIZE
        rng = self._rng

        timestamps = self._base_ts + batch_idx * n + np.arange(n, dtype=np.int64)
        name_indices = rng.integers(0, len(self._perp_names), size=n)
        instrument_names = np.array(self._perp_names)[name_indices]

        base_prices = rng.uniform(25000.0, 105000.0, size=n)
        spread_pct = rng.uniform(0.0001, 0.001, size=n)

        bid_prices = np.round(base_prices * (1 - spread_pct / 2), 2)
        ask_prices = np.round(base_prices * (1 + spread_pct / 2), 2)
        mid_prices = np.round((bid_prices + ask_prices) / 2, 2)
        bid_sizes = np.round(rng.uniform(0.01, 2.0, size=n), 4)
        ask_sizes = np.round(rng.uniform(0.01, 2.0, size=n), 4)

        return pd.DataFrame({
            "timestamp": timestamps,
            "instrument_name": instrument_names,
            "exchange": "deribit",
            "symbol": instrument_names,
            "bid_price": bid_prices,
            "ask_price": ask_prices,
            "mid_price": mid_prices,
            "bid_size": bid_sizes,
            "ask_size": ask_sizes,
        })


class ResourceMonitor:
    """独立线程资源监控哨兵。"""

    def __init__(self, metrics: StressMetrics, stop_event: threading.Event) -> None:
        self._metrics = metrics
        self._stop_event = stop_event
        self._process = psutil.Process()
        self._thread: Optional[threading.Thread] = None
        self._prev_rows = 0
        self._prev_bytes = 0
        self._prev_time = 0.0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread:
            self._thread.join(timeout)

    def _run(self) -> None:
        self._prev_time = time.monotonic()
        # Warm up cpu_percent (first call returns 0.0)
        self._process.cpu_percent(interval=None)
        while not self._stop_event.is_set():
            self._stop_event.wait(MONITOR_INTERVAL_SEC)
            if not self._stop_event.is_set():
                self._sample()

    def _sample(self) -> None:
        now = time.monotonic()
        elapsed = now - self._prev_time
        if elapsed < 0.01:
            return

        m = self._metrics

        row_delta = m.total_rows_generated - self._prev_rows
        m.current_rows_per_sec = row_delta / elapsed
        self._prev_rows = m.total_rows_generated

        byte_delta = m.total_bytes_on_disk - self._prev_bytes
        m.current_write_mb_per_sec = (byte_delta / (1024**2)) / elapsed
        self._prev_bytes = m.total_bytes_on_disk

        try:
            m.current_cpu_pct = self._process.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            m.current_cpu_pct = 0.0

        try:
            rss_mb = self._process.memory_info().rss / (1024**2)
            if rss_mb > m.peak_rss_mb:
                m.peak_rss_mb = rss_mb
        except psutil.NoSuchProcess:
            pass

        disk_free_gb = self._get_disk_free_gb()
        elapsed_total = now - m.start_time

        print(
            f"[{elapsed_total:5.1f}s] "
            f"[{m.current_phase:8s}] "
            f"gen={m.current_rows_per_sec:>7.0f} r/s | "
            f"write={m.current_write_mb_per_sec:>5.1f} MB/s | "
            f"CPU={m.current_cpu_pct:>5.1f}% | "
            f"RSS={m.peak_rss_mb:>6.0f} MB | "
            f"disk_free={disk_free_gb:>5.1f} GB | "
            f"data={m.total_bytes_on_disk / 1024**2:>6.1f} MB"
        )

        self._prev_time = now

        if disk_free_gb < DISK_MIN_FREE_GB:
            print(f"\n!!! 磁盘剩余 {disk_free_gb:.1f} GB 低于安全阈值 {DISK_MIN_FREE_GB} GB !!!")
            m.stop_reason = f"磁盘空间不足 (剩余 {disk_free_gb:.1f} GB)"
            self._stop_event.set()

    @staticmethod
    def _get_disk_free_gb() -> float:
        try:
            usage = psutil.disk_usage(os.path.dirname(os.path.abspath(__file__)))
            return usage.free / (1024**3)
        except Exception:
            return 999.0


def _get_parquet_total_size(directory: str) -> int:
    """递归计算目录下所有 Parquet 文件总大小。"""
    if not os.path.exists(directory):
        return 0
    total = 0
    for root, _dirs, files in os.walk(directory):
        for f in files:
            if f.endswith(".parquet"):
                total += os.path.getsize(os.path.join(root, f))
    return total


def _count_files(directory: str) -> int:
    if not os.path.exists(directory):
        return 0
    count = 0
    for _, _, files in os.walk(directory):
        count += sum(1 for f in files if f.endswith(".parquet"))
    return count


def _cleanup_test_data(directory: str) -> None:
    """删除压测生成的数据目录。"""
    if os.path.exists(directory):
        print(f"\n正在清理压测数据: {directory}")
        shutil.rmtree(directory, ignore_errors=True)
        if not os.path.exists(directory):
            print("清理完成。")
        else:
            print("警告: 部分文件可能仍在占用，请手动删除。")


def _print_final_report(m: StressMetrics) -> None:
    """打印最终压测报告。"""
    elapsed = time.monotonic() - m.start_time
    phase1_time = min(elapsed, PHASE1_BUDGET_SEC)
    phase2_time = max(0, elapsed - PHASE1_BUDGET_SEC)

    disk_gb = m.total_bytes_on_disk / 1024**3

    print("\n" + "=" * 72)
    print("  V3.0 管线极限压测 — 最终报告")
    print("=" * 72)
    print(f"  停止原因:         {m.stop_reason}")
    print(f"  总运行时间:       {elapsed:.1f} 秒")
    print()
    print(f"  ── 阶段一: ChunkedBuffer I/O ({phase1_time:.0f}s) ──")
    print(f"  生成行数:         {m.total_rows_generated:,}")
    print(f"  Flush 次数:       {m.total_flushes}")
    print(f"  Flush 行数:       {m.total_rows_flushed:,}")
    print(f"  Parquet 文件大小: {disk_gb:.3f} GB ({m.total_bytes_on_disk / 1024**2:.1f} MB)")
    print(f"  平均生成速率:     {m.total_rows_generated / max(phase1_time, 0.01):,.0f} rows/s")
    print(f"  平均写入速率:     {(m.total_bytes_on_disk / 1024**2) / max(phase1_time, 0.01):.1f} MB/s")
    if m.total_rows_generated > 0:
        compression_ratio = m.total_rows_flushed * 8 * 9 / max(m.total_bytes_on_disk, 1)
        print(f"  估算压缩比:       ~{compression_ratio:.1f}x (ZSTD)")
    print()
    print(f"  ── 阶段二: merge_asof CPU ({phase2_time:.0f}s) ──")
    print(f"  对齐轮次:         {m.merge_asof_rounds}")
    print(f"  对齐总行数:       {m.merge_asof_total_rows:,}")
    print(f"  对齐总耗时:       {m.merge_asof_total_sec:.3f} 秒")
    if m.merge_asof_total_sec > 0:
        print(f"  对齐速率:         {m.merge_asof_total_rows / m.merge_asof_total_sec:,.0f} rows/s")
    print()
    print(f"  ── 资源峰值 ──")
    print(f"  峰值 RSS:         {m.peak_rss_mb:.0f} MB")
    print("=" * 72)


def _run_chunked_buffer_stress(
    metrics: StressMetrics,
    stop_event: threading.Event,
) -> None:
    """阶段一: ChunkedBuffer 磁盘 I/O 压测 (时间预算 PHASE1_BUDGET_SEC)。"""
    print(f"\n{'─' * 72}")
    print(f"  阶段一: ChunkedBuffer 磁盘 I/O 压测 (预算 {PHASE1_BUDGET_SEC}s)")
    print(f"{'─' * 72}\n")

    metrics.current_phase = "CB-I/O"

    buffer = ChunkedBuffer(
        data_dir=STRESS_DATA_DIR,
        max_rows=BUFFER_MAX_ROWS,
        max_memory_mb=BUFFER_MAX_MEMORY_MB,
        flush_interval_sec=300,
    )

    generator = DataGenerator()
    batch_idx = 0
    sleep_per_batch = BATCH_SIZE / TARGET_ROWS_PER_SEC
    prev_disk_bytes = 0

    while not stop_event.is_set():
        # 时间预算
        elapsed = time.monotonic() - metrics.start_time
        if elapsed >= PHASE1_BUDGET_SEC:
            metrics.stop_reason = f"阶段一时间预算 {PHASE1_BUDGET_SEC}s 已用完"
            break

        # 磁盘限制 (每 10 批检查一次，减少 os.walk 开销)
        if batch_idx % 10 == 0:
            current_size = _get_parquet_total_size(STRESS_DATA_DIR)
            metrics.total_bytes_on_disk = current_size
            if current_size >= MAX_DISK_BYTES:
                metrics.stop_reason = f"数据量达到 {MAX_DISK_BYTES / 1024**3:.1f} GB 上限"
                stop_event.set()
                break

        # 交替生成期权 (75%) / 永续 (25%) 数据
        if batch_idx % 4 == 0:
            df = generator.generate_perp_batch(batch_idx)
            key = ("deribit", "perp_ticker", "BTC-PERPETUAL")
        else:
            df = generator.generate_option_batch(batch_idx)
            key = ("deribit", "options_ticker", "BTC")

        metrics.total_rows_generated += len(df)

        result = buffer.append(*key, df)

        # 当返回值 >= max_rows，说明触发了 flush
        if result >= BUFFER_MAX_ROWS:
            metrics.total_flushes += 1
            # 追踪磁盘增量来估算实际 flush 的数据量
            new_disk_bytes = _get_parquet_total_size(STRESS_DATA_DIR)
            delta = new_disk_bytes - prev_disk_bytes
            # 增量可能为负 (ZSTD重压缩)，取绝对值
            metrics.total_rows_flushed += BUFFER_MAX_ROWS
            prev_disk_bytes = new_disk_bytes
            metrics.total_bytes_on_disk = new_disk_bytes

        batch_idx += 1

        if sleep_per_batch > 0:
            time.sleep(sleep_per_batch)

    # 最终 flush
    disk_before = _get_parquet_total_size(STRESS_DATA_DIR)
    final_rows = buffer.flush_all()
    disk_after = _get_parquet_total_size(STRESS_DATA_DIR)

    if final_rows > 0:
        metrics.total_flushes += 1
        metrics.total_rows_flushed += final_rows
    metrics.total_bytes_on_disk = disk_after

    print(f"\n  阶段一结束: {metrics.total_rows_generated:,} rows generated, "
          f"{metrics.total_flushes} flushes, "
          f"{disk_after / 1024**2:.1f} MB on disk")


def _run_merge_asof_stress(
    metrics: StressMetrics,
    stop_event: threading.Event,
) -> None:
    """阶段二: TimeAligner merge_asof CPU 压测。"""
    remaining = MAX_DURATION_SEC - (time.monotonic() - metrics.start_time)
    if remaining <= 2:
        return

    print(f"\n{'─' * 72}")
    print(f"  阶段二: TimeAligner merge_asof CPU 压测 (剩余 {remaining:.0f}s)")
    print(f"{'─' * 72}\n")

    metrics.current_phase = "merge_asof"

    rng = np.random.default_rng(123)
    aligner = TimeAligner()

    # 3 个数据源，模拟真实策略对齐
    base_size = 100_000
    source_configs = [
        ("options_ticker", 80_000, 5),
        ("mark_price", 60_000, 3),
        ("funding_rate", 10_000, 1),
    ]

    total_aligned = 0
    total_sec = 0.0
    round_idx = 0

    while not stop_event.is_set():
        elapsed = time.monotonic() - metrics.start_time
        if elapsed >= MAX_DURATION_SEC:
            break

        # 生成基础时间线
        base_ts = pd.Series(
            np.sort(rng.integers(1700000000000, 1700086400000, size=base_size))
        )

        data_sources: Dict[str, pd.DataFrame] = {}
        for name, size, n_cols in source_configs:
            ts = np.sort(rng.integers(1700000000000, 1700086400000, size=size))
            data = {"timestamp": ts}
            for c in range(n_cols):
                data[f"{name}_col{c}"] = rng.uniform(1.0, 100.0, size=size)
            data_sources[name] = pd.DataFrame(data)

        t0 = time.monotonic()
        result = aligner.build_strategy_slice(base_ts, data_sources)
        dt = time.monotonic() - t0

        total_aligned += len(result)
        total_sec += dt
        round_idx += 1

        if round_idx % 3 == 0:
            print(
                f"  round {round_idx}: {len(result):,} rows x {len(result.columns)} cols "
                f"in {dt:.3f}s ({len(result) / max(dt, 0.001):,.0f} rows/s)"
            )

    metrics.merge_asof_total_rows = total_aligned
    metrics.merge_asof_total_sec = total_sec
    metrics.merge_asof_rounds = round_idx

    print(f"\n  阶段二结束: {round_idx} rounds, {total_aligned:,} rows aligned, "
          f"{total_sec:.3f}s total")


def main() -> None:
    print("=" * 72)
    print("  V3.0 期权+合约数据采集管线 — 本地极限压测")
    print("=" * 72)
    print(f"  数据目录:     {STRESS_DATA_DIR}")
    print(f"  磁盘上限:     {MAX_DISK_BYTES / 1024**3:.0f} GB")
    print(f"  总时间上限:   {MAX_DURATION_SEC} 秒 (阶段一 {PHASE1_BUDGET_SEC}s + 阶段二 {MAX_DURATION_SEC - PHASE1_BUDGET_SEC}s)")
    print(f"  磁盘安全线:   {DISK_MIN_FREE_GB} GB 剩余")
    print(f"  目标速率:     {TARGET_ROWS_PER_SEC:,} rows/s")
    print(f"  Batch 大小:   {BATCH_SIZE:,} rows")
    print(f"  Flush 阈值:   {BUFFER_MAX_ROWS:,} rows / {BUFFER_MAX_MEMORY_MB} MB")
    print(f"  CPU:          {psutil.cpu_count(logical=True)} threads, "
          f"{psutil.cpu_freq().current:.0f} MHz")
    print(f"  RAM:          {psutil.virtual_memory().total / 1024**3:.1f} GB")
    disk = psutil.disk_usage(os.path.dirname(os.path.abspath(__file__)))
    print(f"  Disk free:    {disk.free / 1024**3:.1f} GB")
    print("=" * 72)

    _cleanup_test_data(STRESS_DATA_DIR)

    metrics = StressMetrics()
    metrics.start_time = time.monotonic()

    stop_event = threading.Event()

    def _signal_handler(signum: int, frame: object) -> None:
        print("\n\n收到中断信号，正在优雅退出...")
        metrics.stop_reason = "用户中断 (Ctrl+C)"
        stop_event.set()

    signal.signal(signal.SIGINT, _signal_handler)

    monitor = ResourceMonitor(metrics, stop_event)
    monitor.start()

    try:
        _run_chunked_buffer_stress(metrics, stop_event)

        elapsed = time.monotonic() - metrics.start_time
        if elapsed < MAX_DURATION_SEC and not stop_event.is_set():
            _run_merge_asof_stress(metrics, stop_event)

        if not metrics.stop_reason:
            metrics.stop_reason = "正常完成"
    except Exception as e:
        print(f"\n压测异常: {e}")
        metrics.stop_reason = f"异常: {e}"
    finally:
        stop_event.set()
        monitor.join(timeout=5.0)
        _print_final_report(metrics)
        _cleanup_test_data(STRESS_DATA_DIR)
        print("\n压测结束。")


if __name__ == "__main__":
    main()
