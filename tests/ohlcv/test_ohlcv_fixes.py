import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fetchers.deribit import resample_ohlcv
from models import OHLCV
from storage.parquet_store import ParquetStore


def _mk(ts, o, h, l, c, v, tf):
    return OHLCV(ts, o, h, l, c, v, 0.0, "deribit", "BTC", tf, 1)


def test_resample_4h_aggregates():
    base = 0  # 1970-01-01 00:00 UTC
    records = [
        _mk(base + 0 * 3600000, 10, 12, 9, 11, 100, "1h"),
        _mk(base + 1 * 3600000, 11, 15, 10, 14, 200, "1h"),
        _mk(base + 2 * 3600000, 14, 14, 8, 9, 150, "1h"),
        _mk(base + 3 * 3600000, 9, 13, 9, 12, 50, "1h"),
        _mk(base + 4 * 3600000, 12, 16, 12, 13, 70, "1h"),
        _mk(base + 5 * 3600000, 13, 14, 11, 11, 30, "1h"),
    ]
    out = resample_ohlcv(records, "4h")
    assert len(out) == 2
    assert out[0].timestamp == base
    assert out[0].open == 10 and out[0].close == 12
    assert out[0].high == 15 and out[0].low == 8
    assert out[0].volume == 500 and out[0].trades == 4
    assert out[1].timestamp == base + 4 * 3600000
    assert out[1].open == 12 and out[1].close == 11
    assert out[1].high == 16 and out[1].low == 11
    assert out[1].volume == 100 and out[1].trades == 2
    assert all(r.timeframe == "4h" for r in out)


def test_resample_4h_skips_empty_bucket():
    base = 0
    records = (
        [_mk(base + i * 3600000, 1, 2, 0.5, 1.5, 10, "1h") for i in range(4)]
        + [_mk(base + (8 + i) * 3600000, 1, 2, 0.5, 1.5, 10, "1h") for i in range(4)]
    )
    out = resample_ohlcv(records, "4h")
    assert [r.timestamp for r in out] == [base, base + 8 * 3600000]


def test_resample_1w_monday_anchor():
    base = 0  # 1970-01-01 是周四
    records = [_mk(base + i * 86400000, 1, 2, 0.5, 1.5, 10, "1d") for i in range(10)]
    out = resample_ohlcv(records, "1w")
    assert len(out) == 2
    assert out[0].timestamp == base - 3 * 86400000  # 1969-12-29 周一 (周四往前 3 天)
    assert out[0].volume == 40  # 周四~周日共 4 天
    assert out[1].timestamp == base + 4 * 86400000  # 1970-01-05 周一
    assert out[1].volume == 60


def test_resample_1M_month_start():
    jan1 = 1704067200000  # 2024-01-01 UTC
    feb1 = 1706745600000  # 2024-02-01 UTC
    records = [
        _mk(jan1, 10, 11, 9, 10.5, 100, "1d"),
        _mk(jan1 + 86400000, 10.5, 12, 10, 11, 200, "1d"),
        _mk(feb1, 20, 21, 19, 20.5, 300, "1d"),
        _mk(feb1 + 86400000, 20.5, 22, 20, 21, 400, "1d"),
    ]
    out = resample_ohlcv(records, "1M")
    assert [r.timestamp for r in out] == [jan1, feb1]
    assert out[0].open == 10 and out[0].close == 11 and out[0].volume == 300
    assert out[1].volume == 700


def test_resample_empty():
    assert resample_ohlcv([], "4h") == []


def test_parquet_filename_1m_1M_distinct():
    with tempfile.TemporaryDirectory() as d:
        store = ParquetStore(d)
        p_1m = store._get_file_path("deribit", "BTC", "1m")
        p_1M = store._get_file_path("deribit", "BTC", "1M")
        assert os.path.basename(p_1m) == "BTC_1m.parquet"
        assert os.path.basename(p_1M) == "BTC_1mon.parquet"
        # 大小写不敏感文件系统上两个路径必须不同
        assert os.path.normcase(p_1m) != os.path.normcase(p_1M)


def test_resample_dedup_boundary_candle():
    """分页边界蜡烛可能重复返回, 聚合前必须去重否则成交量双计。"""
    base = 0
    day = [  # 4 根日线
        _mk(base + i * 86400000, 1, 2, 0.5, 1.5, 10, "1d") for i in range(4)
    ]
    out = resample_ohlcv(day + day, "1w", now_ms=base + 14 * 86400000)
    assert len(out) == 1
    assert out[0].volume == 40  # 不是 80


def test_resample_drops_unclosed_bucket():
    base = 0
    records = [_mk(base + i * 3600000, 1, 2, 0.5, 1.5, 10, "1h") for i in range(6)]  # 0..5h
    # now=5h: 第二个 4h 桶 (4h~8h) 未走完, 必须丢弃
    out = resample_ohlcv(records, "4h", now_ms=base + 5 * 3600000)
    assert [r.timestamp for r in out] == [base]
    # now=8h: 两个桶都已闭合, 保留 (第二桶数据不全但周期已结束)
    out_closed = resample_ohlcv(records, "4h", now_ms=base + 8 * 3600000)
    assert len(out_closed) == 2


def test_resample_drops_unclosed_month():
    jan1 = 1704067200000  # 2024-01-01
    feb1 = 1706745600000  # 2024-02-01
    records = [
        _mk(jan1, 10, 11, 9, 10.5, 100, "1d"),
        _mk(feb1, 20, 21, 19, 20.5, 300, "1d"),
        _mk(feb1 + 86400000, 20.5, 22, 20, 21, 400, "1d"),
    ]
    # now = 2024-02-10: 2 月未走完, 只保留 1 月
    out = resample_ohlcv(records, "1M", now_ms=feb1 + 9 * 86400000)
    assert [r.timestamp for r in out] == [jan1]
    # now = 2024-03-01: 两月均已闭合
    out_full = resample_ohlcv(records, "1M", now_ms=1709251200000)
    assert [r.timestamp for r in out_full] == [jan1, feb1]


def test_parquet_save_new_data_overwrites_same_timestamp(tmp_path):
    """同戳重拉时新数据必须覆盖旧数据, 否则半周期数据被永久冻结。"""
    import pandas as pd
    store = ParquetStore(str(tmp_path))
    old = [OHLCV(86400000, 1.0, 2.0, 0.5, 1.5, 10, 0.0, "ex", "S", "1d", 1)]
    corrected = [OHLCV(86400000, 1.0, 3.0, 0.5, 2.0, 99, 0.0, "ex", "S", "1d", 5)]
    store.save("ex", "S", "1d", old)
    store.save("ex", "S", "1d", corrected)
    df = pd.read_parquet(tmp_path / "ex" / "S_1d.parquet")
    assert len(df) == 1
    assert df.iloc[0]["volume"] == 99
    assert df.iloc[0]["trades"] == 5


class _NoWaitLimiter:
    def wait(self):
        pass


def test_hyperliquid_pagination_terminates_and_no_duplicate(monkeypatch):
    """含端点 API 语义下, 分页必须终止且边界蜡烛恰好拉取一次。"""
    from fetchers.hyperliquid import HyperliquidFetcher

    fetcher = HyperliquidFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    day_ms = 86400000
    candles = [
        (d * day_ms, 1.0, 2.0, 0.5, 1.5, 10) for d in range(100, 105)
    ]
    calls = []

    def fake_batch(coin, unified, interval, tf, batch_start, batch_end):
        calls.append((batch_start, batch_end))
        if len(calls) > 20:
            raise RuntimeError("分页未终止")
        # 模拟 Hyperliquid 实测语义: startTime/endTime 均含端点
        return [
            OHLCV(t, o, h, l, c, v, 0.0, "Hyperliquid", unified, tf, 1)
            for (t, o, h, l, c, v) in candles
            if batch_start <= t <= batch_end
        ]

    monkeypatch.setattr(fetcher, "_fetch_one_batch", fake_batch)
    monkeypatch.setattr(fetcher, "_is_symbol_available", lambda coin: True)

    out = fetcher._do_fetch("BTC", "1d", 50 * day_ms, 200 * day_ms)

    ts = [r.timestamp for r in out]
    assert ts == sorted(set(ts)), "存在重复蜡烛"
    assert len(ts) == 5
    assert len(calls) <= 20, "分页调用次数异常"


def test_hyperliquid_1w_resampled_from_daily(monkeypatch):
    """HL 原生周线为周四锚定, 必须改走 1d 基础周期重采样对齐周一。"""
    from fetchers.hyperliquid import HyperliquidFetcher

    fetcher = HyperliquidFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    seen = {}

    def fake_batch(coin, unified, interval, tf, batch_start, batch_end):
        seen["interval"] = interval
        day = 86400000
        mon = 1704067200000  # 2024-01-01 周一
        return [
            OHLCV(mon, 1, 2, 0.5, 1.5, 10, 0.0, "Hyperliquid", unified, tf, 1),
            OHLCV(mon + day, 1, 2, 0.5, 1.5, 10, 0.0, "Hyperliquid", unified, tf, 1),
        ]

    monkeypatch.setattr(fetcher, "_fetch_one_batch", fake_batch)
    monkeypatch.setattr(fetcher, "_is_symbol_available", lambda coin: True)

    out = fetcher._do_fetch("BTC", "1w", 1704067200000, 1704153600000)

    assert seen["interval"] == "1d", "周线必须以日线为基础周期拉取"
    assert len(out) == 1 and out[0].timestamp == 1704067200000
    assert out[0].volume == 20


def test_hyperliquid_native_drops_unclosed_candle(monkeypatch):
    from fetchers.hyperliquid import HyperliquidFetcher

    fetcher = HyperliquidFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    hour = 3_600_000
    now = int(time.time() * 1000)
    closed_ts = (now // hour) * hour - hour
    open_ts = closed_ts + hour  # 结束于 closed_ts+2h > now, 未走完

    def fake_batch(coin, unified, interval, tf, batch_start, batch_end):
        return [
            OHLCV(closed_ts, 1, 2, 0.5, 1.5, 10, 0.0, "Hyperliquid", unified, tf, 1),
            OHLCV(open_ts, 1, 2, 0.5, 1.5, 10, 0.0, "Hyperliquid", unified, tf, 1),
        ]

    monkeypatch.setattr(fetcher, "_fetch_one_batch", fake_batch)
    monkeypatch.setattr(fetcher, "_is_symbol_available", lambda coin: True)

    out = fetcher._do_fetch("BTC", "1h", closed_ts, now + hour)
    assert [r.timestamp for r in out] == [closed_ts], "未走完的蜡烛必须被丢弃"


def test_deribit_native_drops_unclosed_candle(monkeypatch):
    from fetchers.deribit import DeribitFetcher

    fetcher = DeribitFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    hour = 3_600_000
    now = int(time.time() * 1000)
    closed_ts = (now // hour) * hour - hour
    open_ts = closed_ts + hour

    monkeypatch.setattr(
        fetcher,
        "_fetch_chunk",
        lambda *a, **k: [
            OHLCV(closed_ts, 1, 2, 0.5, 1.5, 10, 0.0, "Deribit", "BTC", "1h"),
            OHLCV(open_ts, 1, 2, 0.5, 1.5, 10, 0.0, "Deribit", "BTC", "1h"),
        ],
    )
    out = fetcher._do_fetch("BTC-PERPETUAL", "1h", closed_ts, now + hour)
    assert [r.timestamp for r in out] == [closed_ts], "未走完的蜡烛必须被丢弃"


def test_deribit_resample_raises_on_chunk_failure(monkeypatch):
    """重采样周期缺任一分块必须整段失败, 不得静默聚合错误周期值。"""
    import pytest
    from fetchers.deribit import DeribitFetcher

    fetcher = DeribitFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    day = 86_400_000
    t0 = 1_690_000_000_000

    def fake_chunk(exchange_symbol, timeframe, resolution, s, e):
        if s != t0:
            raise RuntimeError("simulated chunk failure")
        return [OHLCV(t0, 1, 2, 0.5, 1.5, 10, 0.0, "Deribit", "BTC", timeframe)]

    monkeypatch.setattr(fetcher, "_fetch_chunk", fake_chunk)

    # 1M 以 1D 为基础周期, chunk=365d, 700d 范围产生 2+ 个分块
    with pytest.raises(RuntimeError):
        fetcher._do_fetch("BTC-PERPETUAL", "1M", t0, t0 + 700 * day)


def test_deribit_native_returns_partial_on_chunk_failure(monkeypatch):
    """原生周期分块失败只跳过该块, 已获取数据正常返回 (单次不重试)。"""
    from fetchers.deribit import DeribitFetcher

    fetcher = DeribitFetcher(
        {"base_url": "http://x", "symbols": {"BTC": "BTC"}}, _NoWaitLimiter()
    )
    day = 86_400_000
    t0 = 1_690_000_000_000

    def fake_chunk(exchange_symbol, timeframe, resolution, s, e):
        if s != t0:
            raise RuntimeError("simulated chunk failure")
        return [OHLCV(t0, 1, 2, 0.5, 1.5, 10, 0.0, "Deribit", "BTC", timeframe)]

    monkeypatch.setattr(fetcher, "_fetch_chunk", fake_chunk)

    # 1h 分块 ~187d, 700d 范围产生 4 个分块, 仅第 1 块成功
    out = fetcher._do_fetch("BTC-PERPETUAL", "1h", t0, t0 + 700 * day)
    assert [r.timestamp for r in out] == [t0]
