"""OHLCV 存量数据外科清理 (一次性维护脚本, 2026-08-16)。

只删除两类可证明有问题的行, 其余全部保留:

1. 尾部半周期行: 某行 K 线桶的结束时间晚于文件最后写入时间 (mtime),
   说明写入时该周期尚未走完, 数值是被冻结的半周期值; 回填尾部续拉从
   last_ts+周期 开始, 永远不会覆盖它, 必须删除让回填重拉。
2. 内洞超过 gapfill 上限 (50) 的文件: 截断到首个内洞为止, 保留开头
   连续段; 被截断部分落在保留段之后, 由回填尾部整体重拉。

另: Hyperliquid 原生周线为周四锚定 (与 Binance/Deribit 周一锚定不一致),
取数器已改为从日线重采样, 旧周四数据整体作废删除 — 日线文件完整,
全部历史可由重采样重建。
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "ohlcv-collector", "data",
)

GAP_CAP = 50  # 与 ohlcv-collector/launch.py MAX_GAPS_PER_FILE 一致

TIMEFRAME_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000, "1w": 604_800_000,
}


def bucket_end_ms(ts: int, timeframe: str) -> int:
    """该时间戳所在 K 线桶的结束时间 (毫秒)。"""
    if timeframe in ("1M", "1mon"):
        end = pd.Timestamp(ts, unit="ms", tz="UTC") + pd.tseries.offsets.MonthBegin(1)
    else:
        end = pd.Timestamp(ts, unit="ms", tz="UTC") + pd.Timedelta(
            milliseconds=TIMEFRAME_MS[timeframe]
        )
    return int(end.value // 10**6)


def clean_file(path: str) -> None:
    name = os.path.relpath(path, DATA_ROOT)
    timeframe = os.path.basename(path)[:-8].rsplit("_", 1)[1]
    tf_key = "1M" if timeframe == "1mon" else timeframe
    mtime_ms = int(os.path.getmtime(path) * 1000)

    df = pd.read_parquet(path)
    n0 = len(df)
    ts = df["timestamp"].tolist()

    # 1) 尾部半周期行: 桶结束 > 文件写入时间
    drop_tail = 0
    while ts and bucket_end_ms(ts[-1], tf_key) > mtime_ms:
        ts.pop()
        drop_tail += 1

    # 2) 内洞超限: 截断到首个内洞 (仅规则周期)
    cut = None
    iv = TIMEFRAME_MS.get(tf_key)
    if iv is not None and len(ts) >= 2:
        gaps = [i for i in range(1, len(ts)) if ts[i] - ts[i - 1] > iv]
        if len(gaps) > GAP_CAP:
            cut = gaps[0]

    if drop_tail == 0 and cut is None:
        return

    keep = len(ts) if cut is None else cut
    kept = df.iloc[:keep]
    dropped = df.iloc[keep:]
    kept.to_parquet(path, index=False)

    parts = [f"{name}: {n0} -> {len(kept)} 行"]
    if drop_tail:
        parts.append(f"删尾部半周期 {drop_tail} 行 (最后一行桶结束 > mtime)")
    if cut is not None:
        first = pd.Timestamp(int(dropped["timestamp"].iloc[0]), unit="ms", tz="UTC")
        parts.append(
            f"内洞超 {GAP_CAP} 截断至首个内洞 (保留至 "
            f"{pd.Timestamp(int(kept['timestamp'].iloc[-1]), unit='ms', tz='UTC'):%Y-%m-%d %H:%M}, "
            f"丢弃 {len(dropped)} 行起于 {first:%Y-%m-%d %H:%M}, 由回填重拉)"
        )
    print("  " + "; ".join(parts), flush=True)


def main() -> None:
    exchanges = sorted(
        d for d in os.listdir(DATA_ROOT)
        if os.path.isdir(os.path.join(DATA_ROOT, d))
    )
    print(f"交易所目录: {exchanges}")

    # HL 旧周线 (周四锚定) 整体作废, 由日线重采样重建
    hl_1w_dir = os.path.join(DATA_ROOT, "hyperliquid")
    removed = 0
    for f in sorted(os.listdir(hl_1w_dir)):
        if f.endswith("_1w.parquet"):
            os.remove(os.path.join(hl_1w_dir, f))
            removed += 1
    print(f"hyperliquid: 删除周四锚定旧周线 {removed} 个文件 (改由日线重采样重建)")

    for exch in exchanges:
        print(f"[{exch}]")
        exch_dir = os.path.join(DATA_ROOT, exch)
        for f in sorted(os.listdir(exch_dir)):
            if f.endswith(".parquet"):
                clean_file(os.path.join(exch_dir, f))
    print("完成")


if __name__ == "__main__":
    main()
