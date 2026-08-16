"""Inspect all deribit sample parquet files."""
import datetime as dt
import glob
import os

import pandas as pd

ROOT = "deribit-options-data-collector/data/_validation_sample/deribit"


def fmt_ts(ms):
    if ms is None or pd.isna(ms):
        return "None"
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


for stream_dir in sorted(glob.glob(f"{ROOT}/*")):
    stream = os.path.basename(stream_dir)
    print(f"\n############### deribit/{stream} ###############")
    for f in sorted(glob.glob(f"{stream_dir}/*.parquet")):
        df = pd.read_parquet(f)
        sym = os.path.basename(f).removesuffix(".parquet")
        print(f"\n--- {sym} | rows={len(df)} cols={len(df.columns)} ---")
        print(f"columns: {list(df.columns)}")
        # For wide tables, show just key columns + last 2 rows
        if "timestamp" in df.columns:
            df2 = df.tail(2).copy()
            df2["_ts_h"] = df2["timestamp"].apply(fmt_ts)
            # Keep timestamp visualization + at most 8 other cols
            key_cols = [c for c in df2.columns if c in (
                "instrument_name", "funding_rate", "index_price", "mark_price",
                "bid_price", "ask_price", "bid_size", "ask_size",
                "mark_iv", "open_interest", "volume",
                "tenor_years", "rate_annual",
                "dvol", "oi", "volume_usd",
            )]
            cols_to_show = ["_ts_h"] + key_cols if key_cols else list(df2.columns)[:9]
            print(df2[cols_to_show].to_string(index=False))
