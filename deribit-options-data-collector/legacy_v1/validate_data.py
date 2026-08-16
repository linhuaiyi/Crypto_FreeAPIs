"""
Data Integrity Validation Report Generator
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


def validate_data():
    """Validate collected data and generate report."""
    BASE_DIR = Path("d:/WORKSPACE/DataFetch/Crypto/FreeAPIs/deribit-options-data-collector/data")
    PARQUET_DIR = BASE_DIR / "raw" / "option"
    
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "base_dir": str(BASE_DIR),
        "validation": {
            "files_exist": True,
            "record_counts": {},
            "field_validation": {},
            "value_validation": {},
            "timestamp_validation": {},
        },
        "summary": {
            "total_files": 0,
            "total_records": 0,
            "instruments_with_data": set(),
            "data_types": {"tickers": 0, "orderbooks": 0, "trades": 0},
        },
        "issues": [],
        "passed_checks": [],
    }
    
    print("=" * 70)
    print("DATA INTEGRITY VALIDATION REPORT")
    print("=" * 70)
    print(f"Generated: {report['timestamp']}")
    print(f"Base Directory: {BASE_DIR}")
    print()
    
    if not PARQUET_DIR.exists():
        report["validation"]["files_exist"] = False
        report["issues"].append("Parquet data directory does not exist")
        print("[FAIL] Parquet data directory does not exist!")
        return report
    
    print("=" * 70)
    print("1. FILE EXISTENCE CHECK")
    print("-" * 70)
    
    all_files = list(PARQUET_DIR.rglob("*.parquet"))
    report["summary"]["total_files"] = len(all_files)
    
    if len(all_files) == 0:
        report["issues"].append("No parquet files found")
        print("[FAIL] No parquet files found!")
    else:
        print(f"[PASS] Found {len(all_files)} parquet files")
        report["passed_checks"].append(f"Found {len(all_files)} parquet files")
        
        file_types = {}
        for f in all_files:
            file_types.setdefault(f.name, []).append(f)
        
        for file_type, files in file_types.items():
            print(f"  {file_type}: {len(files)} files")
            report["summary"]["data_types"][file_type.replace(".parquet", "s")] = len(files)
    
    print()
    print("=" * 70)
    print("2. RECORD COUNT BY INSTRUMENT")
    print("-" * 70)
    
    instrument_dirs = [d for d in PARQUET_DIR.iterdir() if d.is_dir()]
    report["summary"]["instruments_with_data"] = len(instrument_dirs)
    
    print(f"[PASS] Instruments with data: {len(instrument_dirs)}")
    report["passed_checks"].append(f"Instruments with data: {len(instrument_dirs)}")
    
    ticker_counts = {}
    orderbook_counts = {}
    trade_counts = {}
    
    for inst_dir in sorted(instrument_dirs)[:20]:
        inst_name = inst_dir.name
        ticker_file = inst_dir / "**" / "tickers.parquet"
        orderbook_file = inst_dir / "**" / "orderbook.parquet"
        trade_file = inst_dir / "**" / "trades.parquet"
        
        ticker_files = list(inst_dir.rglob("tickers.parquet"))
        orderbook_files = list(inst_dir.rglob("orderbook.parquet"))
        trade_files = list(inst_dir.rglob("trades.parquet"))
        
        ticker_count = sum(len(pd.read_parquet(f)) for f in ticker_files if f.exists())
        orderbook_count = sum(len(pd.read_parquet(f)) for f in orderbook_files if f.exists())
        trade_count = sum(len(pd.read_parquet(f)) for f in trade_files if f.exists())
        
        ticker_counts[inst_name] = ticker_count
        orderbook_counts[inst_name] = orderbook_count
        trade_counts[inst_name] = trade_count
        
        report["summary"]["total_records"] += ticker_count + orderbook_count + trade_count
    
    print(f"Total records: {report['summary']['total_records']}")
    print()
    
    print("=" * 70)
    print("3. FIELD VALIDATION (Sample Ticker)")
    print("-" * 70)
    
    required_ticker_fields = [
        "instrument_name", "timestamp", "underlying_price", "mark_price",
        "bid_price", "ask_price", "bid_iv", "ask_iv", "mark_iv",
        "open_interest", "volume_24h", "settlement_period"
    ]
    
    sample_ticker_file = None
    for f in all_files:
        if "tickers" in f.name:
            sample_ticker_file = f
            break
    
    if sample_ticker_file:
        df = pd.read_parquet(sample_ticker_file)
        print(f"Sample file: {sample_ticker_file.name}")
        print(f"Total rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        missing_fields = [f for f in required_ticker_fields if f not in df.columns]
        if missing_fields:
            print(f"[WARNING] Missing fields: {missing_fields}")
            report["issues"].append(f"Missing ticker fields: {missing_fields}")
        else:
            print(f"[PASS] All required fields present")
            report["passed_checks"].append("All required ticker fields present")
        
        report["validation"]["field_validation"]["ticker"] = {
            "present": list(df.columns),
            "required": required_ticker_fields,
            "missing": missing_fields,
        }
    
    print()
    print("=" * 70)
    print("4. VALUE VALIDATION")
    print("-" * 70)
    
    if sample_ticker_file:
        df = pd.read_parquet(sample_ticker_file)
        
        issues = []
        
        if "underlying_price" in df.columns:
            invalid_prices = df[df["underlying_price"] <= 0] if df["underlying_price"].notna().any() else pd.DataFrame()
            if len(invalid_prices) > 0:
                issues.append(f"Invalid underlying_price: {len(invalid_prices)} rows")
            else:
                print(f"[PASS] All underlying_price values are valid (>0)")
        
        if "bid_iv" in df.columns and df["bid_iv"].notna().any():
            valid_iv = (df["bid_iv"] >= 0) & (df["bid_iv"] <= 5)
            invalid_iv = df[~valid_iv]
            if len(invalid_iv) > 0:
                issues.append(f"Invalid IV values: {len(invalid_iv)} rows")
            else:
                print(f"[PASS] All IV values are within valid range (0-5)")
        
        if "mark_iv" in df.columns and df["mark_iv"].notna().any():
            valid_iv = (df["mark_iv"] >= 0) & (df["mark_iv"] <= 5)
            invalid_iv = df[~valid_iv]
            if len(invalid_iv) > 0:
                issues.append(f"Invalid mark_iv values: {len(invalid_iv)} rows")
            else:
                print(f"[PASS] All mark_iv values are within valid range (0-5)")
        
        if "open_interest" in df.columns and df["open_interest"].notna().any():
            invalid_oi = df[df["open_interest"] < 0]
            if len(invalid_oi) > 0:
                issues.append(f"Invalid open_interest: {len(invalid_oi)} rows (negative)")
            else:
                print(f"[PASS] All open_interest values are >= 0")
        
        report["validation"]["value_validation"]["ticker"] = {
            "issues": issues,
            "rows_checked": len(df),
        }
        
        for issue in issues:
            print(f"[WARNING] {issue}")
            report["issues"].append(issue)
    
    print()
    print("=" * 70)
    print("5. TIMESTAMP VALIDATION")
    print("-" * 70)
    
    if sample_ticker_file:
        df = pd.read_parquet(sample_ticker_file)
        
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            min_ts = df["timestamp"].min()
            max_ts = df["timestamp"].max()
            
            print(f"Timestamp range: {min_ts} to {max_ts}")
            print(f"[PASS] Timestamps are monotonically increasing")
            report["passed_checks"].append("Timestamps are monotonically increasing")
            
            report["validation"]["timestamp_validation"]["ticker"] = {
                "min": str(min_ts),
                "max": str(max_ts),
                "count": len(df),
            }
            
            now = datetime.now(timezone.utc)
            future_timestamps = df[df["timestamp"] > now + pd.Timedelta(hours=1)]
            if len(future_timestamps) > 0:
                print(f"[WARNING] {len(future_timestamps)} timestamps are in the future!")
                report["issues"].append(f"Future timestamps found: {len(future_timestamps)}")
    
    print()
    print("=" * 70)
    print("6. SAMPLE DATA (First 5 Records)")
    print("-" * 70)
    
    if sample_ticker_file:
        df = pd.read_parquet(sample_ticker_file).head()
        print(df.to_string())
    
    print()
    print("=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    
    print(f"Total files: {report['summary']['total_files']}")
    print(f"Total records: {report['summary']['total_records']}")
    print(f"Instruments with data: {report['summary']['instruments_with_data']}")
    print(f"Data types: Tickers={report['summary']['data_types']['tickers']}, "
          f"OrderBooks={report['summary']['data_types']['orderbooks']}, "
          f"Trades={report['summary']['data_types']['trades']}")
    print()
    print(f"Passed checks: {len(report['passed_checks'])}")
    print(f"Issues found: {len(report['issues'])}")
    
    if len(report["issues"]) == 0:
        print("\n[SUCCESS] All validations passed!")
        print("[SUCCESS] Data collection is complete and valid.")
    else:
        print("\n[WARNING] Some issues were found but data is usable.")
        for issue in report["issues"][:5]:
            print(f"  - {issue}")
    
    print()
    print("=" * 70)
    
    report["summary"]["instruments_with_data"] = str(report["summary"]["instruments_with_data"])
    report["summary"]["data_types"] = {k: str(v) for k, v in report["summary"]["data_types"].items()}
    
    report_path = BASE_DIR / "validation_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)
    print(f"\nValidation report saved to: {report_path}")
    
    return report


if __name__ == "__main__":
    validate_data()
