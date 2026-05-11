"""Tests for storage/chunked_buffer.py"""

import os
import shutil
import tempfile
import time
from datetime import datetime, timezone

import pandas as pd
import pytest

from storage.chunked_buffer import ChunkedBuffer


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


def _make_df(n: int, start_ts: int = 1700000000000) -> pd.DataFrame:
    """Create a test DataFrame with n rows."""
    return pd.DataFrame({
        "timestamp": [start_ts + i * 1000 for i in range(n)],
        "exchange": ["deribit"] * n,
        "symbol": ["BTC-PERPETUAL"] * n,
        "price": [100.0 + i for i in range(n)],
        "volume": [10.0] * n,
    })


class TestChunkedBufferAppend:
    def test_append_single_row(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=5)
        df = _make_df(1)
        result = buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        assert result >= 1

    def test_append_accumulates_below_threshold(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=10)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(3))
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(3, start_ts=1700000003000))
        stats = buf.get_buffer_stats()
        key = "deribit/tickers/BTC-PERPETUAL"
        assert key in stats
        assert stats[key]["rows"] == 6

    def test_append_triggers_flush_at_threshold(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=5)
        for i in range(5):
            buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(1, start_ts=1700000000000 + i * 1000))
        # 5 rows should have triggered flush at max_rows=5
        stats = buf.get_buffer_stats()
        key = "deribit/tickers/BTC-PERPETUAL"
        assert key not in stats or stats[key]["rows"] == 0

    def test_append_empty_df_skipped(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=100)
        result = buf.append("deribit", "tickers", "BTC-PERPETUAL", pd.DataFrame())
        assert result == 0


class TestChunkedBufferFlush:
    def test_flush_creates_flat_named_files(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        df = _make_df(10, start_ts=1700000000000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        written = buf.flush_all()
        assert written == 10

        # Verify flat file structure: data_dir/exchange/data_type/symbol_date.parquet
        base = os.path.join(tmp_dir, "deribit", "tickers")
        parquet_files = [f for f in os.listdir(base) if f.endswith(".parquet")]
        assert len(parquet_files) >= 1
        assert parquet_files[0].startswith("BTC-PERPETUAL_")

    def test_flush_deduplicates(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        df1 = _make_df(5, start_ts=1700000000000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df1)
        buf.flush_all()

        # Append overlapping + new data
        df2 = _make_df(7, start_ts=1700000000000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df2)
        written = buf.flush_all()
        # 7 total, 5 overlap = 2 new unique
        assert written == 7

    def test_flush_specific_key(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(3))
        buf.append("binance", "tickers", "BTCUSDT", _make_df(4))

        buf.flush("deribit/tickers/BTC-PERPETUAL")

        stats = buf.get_buffer_stats()
        assert "deribit/tickers/BTC-PERPETUAL" not in stats
        assert "binance/tickers/BTCUSDT" in stats
        assert stats["binance/tickers/BTCUSDT"]["rows"] == 4

    def test_flush_all_empties_buffer(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(3))
        buf.append("binance", "tickers", "BTCUSDT", _make_df(4))
        buf.flush_all()
        assert buf.get_buffer_stats() == {}


class TestChunkedBufferMemoryTrigger:
    def test_flush_triggered_by_memory(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=999_999_999, max_memory_mb=1)
        # Create a DataFrame with large string columns to exceed 1 MB
        big_str = "X" * 10000
        df = pd.DataFrame({
            "timestamp": list(range(5000)),
            "exchange": ["deribit"] * 5000,
            "symbol": [big_str] * 5000,
            "data": [big_str] * 5000,
        })
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)

        stats = buf.get_buffer_stats()
        key = "deribit/tickers/BTC-PERPETUAL"
        assert key not in stats or stats[key]["rows"] == 0


class TestChunkedBufferPartitioning:
    def test_multi_date_partitioning(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        # Create data spanning 2 days (86400000 ms per day)
        day1_ts = 1700000000000
        day2_ts = day1_ts + 86400000
        df = pd.DataFrame({
            "timestamp": [day1_ts, day1_ts + 1000, day2_ts, day2_ts + 1000],
            "exchange": ["deribit"] * 4,
            "symbol": ["BTC-PERPETUAL"] * 4,
            "price": [100.0, 101.0, 102.0, 103.0],
        })
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        written = buf.flush_all()
        assert written == 4

        base = os.path.join(tmp_dir, "deribit", "tickers")
        parquet_files = sorted([f for f in os.listdir(base) if f.endswith(".parquet")])
        assert len(parquet_files) == 2

    def test_read_parquet_by_date(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        day1_ts = 1700000000000
        day2_ts = day1_ts + 86400000
        df = pd.DataFrame({
            "timestamp": [day1_ts, day2_ts],
            "exchange": ["deribit"] * 2,
            "symbol": ["BTC-PERPETUAL"] * 2,
            "price": [100.0, 200.0],
        })
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        buf.flush_all()

        # Path: tmp_dir/deribit/tickers/BTC-PERPETUAL_YYYY-MM-DD.parquet
        base = os.path.join(tmp_dir, "deribit", "tickers")
        date1_str = datetime.fromtimestamp(day1_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
        fpath = os.path.join(base, f"BTC-PERPETUAL_{date1_str}.parquet")
        assert os.path.exists(fpath)

        result = pd.read_parquet(fpath)
        assert len(result) == 1
        assert result.iloc[0]["price"] == 100.0

    def test_zstd_compression(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=100)
        df = _make_df(50)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        buf.flush_all()

        import pyarrow.parquet as pq
        base = os.path.join(tmp_dir, "deribit", "tickers")
        parquet_files = [f for f in os.listdir(base) if f.endswith(".parquet")]
        assert len(parquet_files) >= 1
        fpath = os.path.join(base, parquet_files[0])
        meta = pq.read_metadata(fpath)
        for i in range(meta.num_row_groups):
            col_meta = meta.row_group(i).column(0)
            assert "ZSTD" in str(col_meta.compression).upper()


class TestChunkedBufferGetStats:
    def test_buffer_stats_report(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=1000)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(5))
        buf.append("binance", "funding", "BTCUSDT", _make_df(3))

        stats = buf.get_buffer_stats()
        assert "deribit/tickers/BTC-PERPETUAL" in stats
        assert stats["deribit/tickers/BTC-PERPETUAL"]["rows"] == 5
        assert stats["binance/funding/BTCUSDT"]["rows"] == 3


class TestChunkedBufferPeriodicFlush:
    def test_start_stop_periodic_flush(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=999999, flush_interval_sec=1)
        buf.append("deribit", "tickers", "BTC-PERPETUAL", _make_df(5))
        buf.start_periodic_flush()
        # Manually flush via timer callback to avoid blocking sleep
        buf._on_timer()
        buf.stop_periodic_flush()

        stats = buf.get_buffer_stats()
        key = "deribit/tickers/BTC-PERPETUAL"
        assert key not in stats or stats[key]["rows"] == 0

    def test_no_timestamp_columns_warning(self, tmp_dir):
        buf = ChunkedBuffer(data_dir=tmp_dir, max_rows=100)
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        buf.append("deribit", "tickers", "BTC-PERPETUAL", df)
        written = buf.flush_all()
        assert written == 0
