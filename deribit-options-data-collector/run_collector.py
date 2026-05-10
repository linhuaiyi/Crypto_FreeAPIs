"""
Deribit Options Data Collector - 15 Minute Data Collection Run
"""

import asyncio
import sys
import time
import signal
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd
import pyarrow.parquet as pq
from tabulate import tabulate

BASE_URL = "https://www.deribit.com/api/v2"
DATA_DIR = Path("d:/WORKSPACE/DataFetch/Crypto/FreeAPIs/deribit-options-data-collector/data")
SQLITE_PATH = DATA_DIR / "deribit_options.db"
PARQUET_PATH = DATA_DIR / "raw" / "option"

print("=" * 70)
print("Deribit Options Data Collector - 15 Minute Run")
print("=" * 70)
print(f"Start time: {datetime.now(timezone.utc).isoformat()}")
print(f"Data directory: {DATA_DIR}")
print()

DATA_DIR.mkdir(parents=True, exist_ok=True)
(PARQUET_PATH).mkdir(parents=True, exist_ok=True)


class DataCollector:
    def __init__(self):
        self.instruments = {"BTC": [], "ETH": []}
        self.collected = {
            "tickers": [],
            "orderbooks": [],
            "trades": [],
        }
        self.errors = []
        self.running = True
        self.start_time = time.time()
        self.collection_count = 0

    async def fetch_instruments(self):
        """Fetch all available instruments."""
        print("Fetching instruments...")
        for currency in ["BTC", "ETH"]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{BASE_URL}/public/get_instruments",
                        params={"currency": currency, "kind": "option"},
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        data = await resp.json()
                        if "result" in data:
                            self.instruments[currency] = [
                                i for i in data["result"] if i.get("is_active", False)
                            ]
                            print(f"  {currency}: {len(self.instruments[currency])} active contracts")
            except Exception as e:
                self.errors.append(f"Fetch instruments {currency}: {e}")
                print(f"  [ERROR] {currency}: {e}")

    async def collect_ticker(self, session, instrument_name):
        """Collect ticker data for a single instrument."""
        try:
            async with session.get(
                f"{BASE_URL}/public/ticker",
                params={"instrument_name": instrument_name},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if "result" in data:
                    ticker = data["result"]
                    self.collected["tickers"].append({
                        "instrument_name": ticker.get("instrument_name"),
                        "timestamp": datetime.now(timezone.utc),
                        "underlying_price": ticker.get("underlying_price"),
                        "mark_price": ticker.get("mark_price"),
                        "bid_price": ticker.get("bid_price"),
                        "ask_price": ticker.get("ask_price"),
                        "bid_iv": ticker.get("bid_iv"),
                        "ask_iv": ticker.get("ask_iv"),
                        "mark_iv": ticker.get("mark_iv"),
                        "open_interest": ticker.get("open_interest"),
                        "volume_24h": ticker.get("stats", {}).get("volume"),
                        "settlement_period": ticker.get("settlement_period"),
                    })
        except Exception as e:
            self.errors.append(f"Ticker {instrument_name}: {e}")

    async def collect_orderbook(self, session, instrument_name):
        """Collect orderbook data for a single instrument."""
        try:
            async with session.get(
                f"{BASE_URL}/public/get_order_book",
                params={"instrument_name": instrument_name, "depth": 20},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if "result" in data:
                    book = data["result"]
                    self.collected["orderbooks"].append({
                        "instrument_name": book.get("instrument_name"),
                        "timestamp": datetime.now(timezone.utc),
                        "underlying_price": book.get("underlying_price"),
                        "settlement_price": book.get("settlement_price"),
                        "bid_levels": len(book.get("bids", [])),
                        "ask_levels": len(book.get("asks", [])),
                        "best_bid": book["bids"][0][0] if book.get("bids") else None,
                        "best_ask": book["asks"][0][0] if book.get("asks") else None,
                    })
        except Exception as e:
            self.errors.append(f"Orderbook {instrument_name}: {e}")

    async def collect_trades(self, session, instrument_name):
        """Collect trades for a single instrument."""
        try:
            async with session.get(
                f"{BASE_URL}/public/get_last_trades_by_instrument",
                params={"instrument_name": instrument_name, "count": 10},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json()
                if "result" in data:
                    trades = data["result"].get("trades", [])
                    for trade in trades:
                        self.collected["trades"].append({
                            "trade_id": trade.get("trade_id"),
                            "instrument_name": trade.get("instrument_name"),
                            "timestamp": datetime.fromtimestamp(
                                trade.get("timestamp", 0) / 1000, tz=timezone.utc
                            ),
                            "direction": trade.get("direction"),
                            "price": trade.get("price"),
                            "amount": trade.get("amount"),
                        })
        except Exception as e:
            self.errors.append(f"Trades {instrument_name}: {e}")

    async def collection_cycle(self):
        """Perform one collection cycle."""
        self.collection_count += 1
        elapsed = time.time() - self.start_time
        print(f"\n[{elapsed:.0f}s] Collection cycle #{self.collection_count}")
        
        all_instruments = self.instruments["BTC"] + self.instruments["ETH"]
        instrument_names = [i["instrument_name"] for i in all_instruments[:50]]
        
        print(f"  Fetching data for {len(instrument_names)} instruments...")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for name in instrument_names:
                tasks.append(self.collect_ticker(session, name))
                tasks.append(self.collect_orderbook(session, name))
                tasks.append(self.collect_trades(session, name))
            
            await asyncio.gather(*tasks, return_exceptions=True)
        
        print(f"  Tickers: {len(self.collected['tickers'])}, "
              f"OrderBooks: {len(self.collected['orderbooks'])}, "
              f"Trades: {len(self.collected['trades'])}")

    def save_data(self):
        """Save collected data to Parquet and SQLite."""
        print("\nSaving data...")
        
        if self.collected["tickers"]:
            df = pd.DataFrame(self.collected["tickers"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            for instrument in df["instrument_name"].unique():
                inst_df = df[df["instrument_name"] == instrument]
                safe_name = instrument.replace("/", "-")
                out_dir = PARQUET_PATH / safe_name / today
                out_dir.mkdir(parents=True, exist_ok=True)
                path = out_dir / "tickers.parquet"
                
                if path.exists():
                    existing = pd.read_parquet(path)
                    combined = pd.concat([existing, inst_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["instrument_name", "timestamp"], keep="last")
                else:
                    combined = inst_df
                
                combined.to_parquet(path, compression="snappy", index=False)
            
            print(f"  Saved {len(df)} ticker records")

        if self.collected["orderbooks"]:
            df = pd.DataFrame(self.collected["orderbooks"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            for instrument in df["instrument_name"].unique():
                inst_df = df[df["instrument_name"] == instrument]
                safe_name = instrument.replace("/", "-")
                out_dir = PARQUET_PATH / safe_name / today
                out_dir.mkdir(parents=True, exist_ok=True)
                path = out_dir / "orderbook.parquet"
                
                if path.exists():
                    existing = pd.read_parquet(path)
                    combined = pd.concat([existing, inst_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["instrument_name", "timestamp"], keep="last")
                else:
                    combined = inst_df
                
                combined.to_parquet(path, compression="snappy", index=False)
            
            print(f"  Saved {len(df)} orderbook records")

        if self.collected["trades"]:
            df = pd.DataFrame(self.collected["trades"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            
            for instrument in df["instrument_name"].unique():
                inst_df = df[df["instrument_name"] == instrument]
                safe_name = instrument.replace("/", "-")
                out_dir = PARQUET_PATH / safe_name / today
                out_dir.mkdir(parents=True, exist_ok=True)
                path = out_dir / "trades.parquet"
                
                if path.exists():
                    existing = pd.read_parquet(path)
                    combined = pd.concat([existing, inst_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["trade_id"], keep="last")
                else:
                    combined = inst_df
                
                combined.to_parquet(path, compression="snappy", index=False)
            
            print(f"  Saved {len(df)} trade records")

        self.collected = {"tickers": [], "orderbooks": [], "trades": []}

    def stop(self):
        """Stop the collector."""
        self.running = False


async def main():
    collector = DataCollector()
    
    def signal_handler(sig, frame):
        print("\nReceived shutdown signal, saving data...")
        collector.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Step 1: Fetching available instruments...")
    await collector.fetch_instruments()
    
    total_instruments = len(collector.instruments["BTC"]) + len(collector.instruments["ETH"])
    print(f"\nTotal active instruments: {total_instruments}")
    
    print("\nStep 2: Starting data collection (15 minutes)...")
    print("-" * 70)
    
    start_time = time.time()
    duration = 15 * 60
    
    while collector.running and (time.time() - start_time) < duration:
        await collector.collection_cycle()
        await asyncio.sleep(1)
        
        if collector.collection_count % 10 == 0:
            collector.save_data()
    
    print("\n" + "=" * 70)
    print("Final data save...")
    collector.save_data()
    
    elapsed = time.time() - start_time
    print(f"\nCollection completed!")
    print(f"Total time: {elapsed:.0f} seconds")
    print(f"Total collection cycles: {collector.collection_count}")
    print(f"Total errors: {len(collector.errors)}")
    
    if collector.errors:
        print("\nErrors encountered:")
        for err in collector.errors[:10]:
            print(f"  - {err}")
        if len(collector.errors) > 10:
            print(f"  ... and {len(collector.errors) - 10} more")
    
    print(f"\nData saved to: {DATA_DIR}")
    return collector


if __name__ == "__main__":
    collector = asyncio.run(main())
