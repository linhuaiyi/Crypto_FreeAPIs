"""
Chunked buffer for batched Parquet writes with date-based file naming.

Memory-bounded accumulation with triple flush triggers:
  - Row count threshold (default: 100,000 rows)
  - Memory size threshold (default: 200 MB)
  - Time interval threshold (default: 5 minutes)

Output path convention:
  data/{exchange}/{data_type}/{symbol}_{date}.parquet
"""

import os
import sys
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils import get_logger

logger = get_logger("ChunkedBuffer")

# ZSTD compression balances ratio and CPU cost on NVMe
_PARQUET_COMPRESSION = "zstd"


class ChunkedBuffer:
    """Memory-bounded buffered writer with flat date-named Parquet output."""

    def __init__(
        self,
        data_dir: str = "./data",
        max_rows: int = 100_000,
        max_memory_mb: int = 200,
        flush_interval_sec: int = 300,
    ) -> None:
        self.data_dir = data_dir
        self.max_rows = max_rows
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.flush_interval_sec = flush_interval_sec

        self._buffers: Dict[str, pd.DataFrame] = {}
        self._last_flush_time: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None

        os.makedirs(data_dir, exist_ok=True)

    def append(
        self,
        exchange: str,
        data_type: str,
        symbol: str,
        df: pd.DataFrame,
    ) -> int:
        """Append rows to buffer. Returns number of rows buffered (after dedup)."""
        if df.empty:
            return 0

        key = f"{exchange}/{data_type}/{symbol}"

        with self._lock:
            if key in self._buffers:
                self._buffers[key] = pd.concat(
                    [self._buffers[key], df], ignore_index=True
                )
            else:
                self._buffers[key] = df.copy()

            current = self._buffers[key]

            if self._should_flush(key, current):
                flushed = self.flush(key)
                return flushed

            return len(current)

    def flush(self, key: Optional[str] = None) -> int:
        """Flush buffer(s) to Parquet. Returns total rows written."""
        total = 0

        with self._lock:
            keys_to_flush = [key] if key else list(self._buffers.keys())

            for k in keys_to_flush:
                if k not in self._buffers:
                    continue
                df = self._buffers.pop(k)
                self._last_flush_time.pop(k, None)
                total += self._write_parquet(k, df)

        return total

    def flush_all(self) -> int:
        """Flush all buffered data. Returns total rows written."""
        return self.flush()

    def get_buffer_stats(self) -> Dict[str, Dict[str, int]]:
        """Return current buffer sizes for monitoring."""
        with self._lock:
            stats = {}
            for key, df in self._buffers.items():
                mem = df.memory_usage(deep=True).sum()
                stats[key] = {
                    "rows": len(df),
                    "memory_mb": round(mem / (1024 * 1024), 2),
                }
            return stats

    def start_periodic_flush(self) -> None:
        """Start a background timer for periodic flush."""
        self._schedule_timer()

    def stop_periodic_flush(self) -> None:
        """Stop the periodic flush timer and flush remaining data."""
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self.flush_all()

    def _should_flush(self, key: str, df: pd.DataFrame) -> bool:
        if len(df) >= self.max_rows:
            logger.info(f"[{key}] Flush triggered: row count {len(df)} >= {self.max_rows}")
            return True

        mem = df.memory_usage(deep=True).sum()
        if mem >= self.max_memory_bytes:
            logger.info(f"[{key}] Flush triggered: memory {mem / 1024 / 1024:.1f} MB >= {self.max_memory_bytes / 1024 / 1024:.0f} MB")
            return True

        last = self._last_flush_time.get(key, 0)
        if last > 0 and (time.time() - last) >= self.flush_interval_sec:
            logger.info(f"[{key}] Flush triggered: interval >= {self.flush_interval_sec}s")
            return True

        return False

    def _write_parquet(self, key: str, df: pd.DataFrame) -> int:
        """Write DataFrame to flat date-named Parquet files.

        Output: data_dir/{exchange}/{data_type}/{symbol}_{date}.parquet
        """
        if df.empty:
            return 0

        if "timestamp" not in df.columns:
            logger.warning(f"[{key}] No timestamp column, cannot partition by date")
            return 0

        df = df.copy()
        df["_date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime("%Y-%m-%d")

        total_written = 0
        for date_str, group in df.groupby("_date"):
            group = group.drop(columns=["_date"])

            if group.empty:
                continue

            parts = key.split("/")
            # parts = [exchange, data_type, symbol]
            # Flat path: data_dir/exchange/data_type/symbol_YYYY-MM-DD.parquet
            symbol = parts[-1]
            base_parts = parts[:-1]  # [exchange, data_type]
            dir_path = os.path.join(self.data_dir, *base_parts)
            os.makedirs(dir_path, exist_ok=True)

            file_path = os.path.join(dir_path, f"{symbol}_{date_str}.parquet")

            if os.path.exists(file_path):
                existing = pd.read_parquet(file_path)
                group = pd.concat([existing, group], ignore_index=True)
                dedup_cols = ["timestamp"]
                for col in ["exchange", "symbol", "instrument_name"]:
                    if col in group.columns:
                        dedup_cols.append(col)
                group.drop_duplicates(subset=dedup_cols, keep="last", inplace=True)

            group.sort_values("timestamp", inplace=True)
            group.reset_index(drop=True, inplace=True)

            table = pa.Table.from_pandas(group, preserve_index=False)
            pq.write_table(
                table,
                file_path,
                compression=_PARQUET_COMPRESSION,
            )

            total_written += len(group)
            logger.info(
                f"[{key}] {date_str}: wrote {len(group)} rows to {file_path}"
            )

        self._last_flush_time[key] = time.time()
        return total_written

    def _schedule_timer(self) -> None:
        """Schedule periodic flush timer."""
        if self._timer:
            self._timer.cancel()

        self._timer = threading.Timer(self.flush_interval_sec, self._on_timer)
        self._timer.daemon = True
        self._timer.start()

    def _on_timer(self) -> None:
        """Timer callback: flush stale buffers and reschedule."""
        now = time.time()
        with self._lock:
            stale_keys = []
            for key, last_time in self._last_flush_time.items():
                if key in self._buffers and (now - last_time) >= self.flush_interval_sec:
                    stale_keys.append(key)

            # Also flush buffers that have never been flushed but have data
            for key in self._buffers:
                if key not in self._last_flush_time:
                    stale_keys.append(key)

        for key in stale_keys:
            self.flush(key)

        self._schedule_timer()
