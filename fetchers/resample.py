from typing import List, Optional

import pandas as pd

from models import OHLCV


# 重采样对齐: 4h 对齐 UTC 0/4/8..., 周线以周一为起点, 月线以自然月为起点
RESAMPLE_RULE = {"4h": "4h", "1w": "W-MON", "1M": "MS"}

# 各周期的桶跨度, 用于判定末尾未走完的周期
BUCKET_END_OFFSET = {
    "4h": pd.Timedelta(hours=4),
    "1w": pd.tseries.offsets.Week(weekday=0),
    "1M": pd.tseries.offsets.MonthBegin(1),
}


def resample_ohlcv(
    records: List[OHLCV],
    target_timeframe: str,
    now_ms: Optional[int] = None,
) -> List[OHLCV]:
    """将基础周期 OHLCV 聚合为目标周期 (4h/1w/1M)。

    - 空桶不生成记录
    - 相同时间戳去重 (分页边界可能重复返回同一根)
    - now_ms 提供时丢弃尚未走完的末尾周期, 避免半周期数据被
      落盘后因去重 keep 规则永久冻结
    """
    if not records:
        return []

    rule = RESAMPLE_RULE.get(target_timeframe)
    if rule is None:
        raise ValueError(f"不支持的重采样目标周期: {target_timeframe}")

    df = pd.DataFrame([r.to_dict() for r in records])
    df = df.drop_duplicates(subset=['timestamp'], keep='last')
    df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df = df.set_index('dt')
    df['trades'] = df['trades'].fillna(0)

    agg = df.resample(rule, label='left', closed='left').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'quote_volume': 'sum', 'trades': 'sum',
    }).dropna(subset=['open'])

    if now_ms is not None:
        bucket_end = agg.index + BUCKET_END_OFFSET[target_timeframe]
        agg = agg[bucket_end <= pd.Timestamp(now_ms, unit='ms', tz='UTC')]

    first = records[0]
    return [
        OHLCV(
            timestamp=int(dt.value // 10**6),
            open=float(row['open']),
            high=float(row['high']),
            low=float(row['low']),
            close=float(row['close']),
            volume=float(row['volume']),
            quote_volume=float(row['quote_volume']),
            exchange=first.exchange,
            symbol=first.symbol,
            timeframe=target_timeframe,
            trades=int(row['trades']),
        )
        for dt, row in agg.iterrows()
    ]
