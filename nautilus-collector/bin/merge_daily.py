#!/usr/bin/env python3
"""
merge_daily.py — 按日合并采集小块为归档大文件（流式版，VPS 8G 内存安全）

输入: /app/data/production/<exch>/<type>/  小块 <EXCH>_<type>_raw_YYYYMMDD-HHMM_YYYYMMDD-HHMM.parquet
输出: /app/data/archive/<exch>/<type>/      <EXCH>_<type>_YYYYMMDD.parquet

v2 修复（2026-08-21）: v1 全量载入+python排序 OOM(rc=137)。改为:
  - ParquetWriter 流式逐块写 row group（内存常数级，单块 ≤50k 行）
  - 不排序: 源块按文件名序(=时间序)且块内数据按采集顺序写入 → 天然全局有序;
    严格排序由湖侧 hive 化(telegram_to_hive.py)负责(WSL 内存充裕)
  - 行数校验: 写完重读 metadata 与累计行数核对
  - 幂等: 同日重跑覆盖; 仅写出成功后删源块

用法(宿主 cron 02:15 CEST = 00:15 UTC):
  docker run --rm -v $PWD/data:/app/data crypto-nc:base python /app/merge_daily.py
"""
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("merge_daily")

PROD = Path("/app/data/production")
ARCH = Path("/app/data/archive")
EXCHANGES = ["binance", "hyperliquid"]
FNAME_RE = re.compile(r"_(\d{8})[-_]\d{4}_\d{8}[-_]\d{4}\.parquet$")
RESERVED_KEEP_DAYS = 7
# 单文件 row group 大小（流式写出粒度）
ROW_GROUP_ROWS = 500_000


def merge_day(exch: str, dtype: str, day_compact: str, files: list) -> bool:
    day_iso = f"{day_compact[:4]}-{day_compact[4:6]}-{day_compact[6:]}"
    out_dir = ARCH / exch / dtype
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{exch.upper()}_{dtype}_{day_compact}.parquet"
    tmp_path = out_path.with_suffix(".parquet.tmp")

    total_rows = 0
    writer = None
    try:
        for f in files:  # files 已按文件名排序 = 时间序
            t = pq.read_table(f)
            total_rows += t.num_rows
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, t.schema,
                                          compression="snappy")
            writer.write_table(t, row_group_size=ROW_GROUP_ROWS)
        if writer is None:
            return False
        writer.close()
        writer = None

        # 校验
        meta = pq.ParquetFile(tmp_path).metadata
        if meta.num_rows != total_rows:
            log.error(f"[FAIL] {exch}/{dtype} {day_iso}: 行数不符 {meta.num_rows} != {total_rows}")
            tmp_path.unlink(missing_ok=True)
            return False

        tmp_path.replace(out_path)
        sz = out_path.stat().st_size
        for f in files:
            f.unlink(missing_ok=True)
        log.info(f"[OK] {exch}/{dtype} {day_iso}: {len(files)} 小块 → {out_path.name} "
                 f"({sz/1024/1024:.1f}MB, {total_rows:,} rows)")
        return True
    except Exception as e:
        log.error(f"[FAIL] {exch}/{dtype} {day_iso}: {e}")
        if writer is not None:
            writer.close()
        tmp_path.unlink(missing_ok=True)
        return False


def main():
    today_utc = datetime.now(timezone.utc).strftime("%Y%m%d")
    any_merged = False

    for exch in EXCHANGES:
        base = PROD / exch
        if not base.exists():
            continue
        for dtype_dir in sorted(p for p in base.iterdir() if p.is_dir()):
            dtype = dtype_dir.name
            by_day = defaultdict(list)
            for f in dtype_dir.glob("*.parquet"):
                m = FNAME_RE.search(f.name)
                if m:
                    by_day[m.group(1)].append(f)
            for day in sorted(by_day):
                if day >= today_utc:
                    continue  # 当日进行中
                files = sorted(by_day[day], key=lambda p: p.name)
                any_merged |= merge_day(exch, dtype, day, files)

            # 兜底清理：超期残留小块（合并失败堆积的）
            cutoff = time.time() - RESERVED_KEEP_DAYS * 86400
            for f in dtype_dir.glob("*.parquet"):
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    log.warning(f"[GC] 清理过期小块 {f.name}")

    for exch in EXCHANGES:
        arch = ARCH / exch
        if arch.exists():
            for dtype_dir in sorted(p for p in arch.iterdir() if p.is_dir()):
                days = sorted(p.name for p in dtype_dir.glob("*.parquet"))
                if days:
                    log.info(f"[summary] archive/{exch}/{dtype_dir.name}: {len(days)} 日文件 "
                             f"({days[0]} → {days[-1]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
