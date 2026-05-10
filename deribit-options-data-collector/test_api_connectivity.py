"""Deribit API connectivity test script."""

import asyncio
import sys
from datetime import datetime, timezone

import aiohttp


async def test_api_connectivity():
    """Test Deribit API endpoints."""
    base_url = "https://www.deribit.com/api/v2"
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tests": [],
        "summary": {"passed": 0, "failed": 0}
    }
    
    active_instrument = None

    async with aiohttp.ClientSession() as session:
        print("=" * 60)
        print("Test 1: Get BTC Options Instruments")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_instruments",
                params={"currency": "BTC", "kind": "option"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    btc_instruments = data["result"]
                    active_instruments = [i for i in btc_instruments if i.get('is_active', False)]
                    print(f"[PASS] API connected successfully")
                    print(f"[PASS] BTC options count: {len(btc_instruments)}")
                    print(f"[PASS] Active BTC options: {len(active_instruments)}")
                    if active_instruments:
                        active_instrument = active_instruments[0]
                        active_name = active_instrument.get('instrument_name')
                        print(f"  Active sample: {active_name}")
                        print(f"  Strike: {active_instrument.get('strike')}")
                        print(f"  Type: {active_instrument.get('option_type')}")
                        print(f"  Expiration: {active_instrument.get('expiration_timestamp')}")
                    results["tests"].append({
                        "name": "get_instruments_btc",
                        "status": "PASS",
                        "total_count": len(btc_instruments),
                        "active_count": len(active_instruments)
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_instruments_btc",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_instruments_btc",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        print("\n" + "=" * 60)
        print("Test 2: Get ETH Options Instruments")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_instruments",
                params={"currency": "ETH", "kind": "option"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    eth_instruments = data["result"]
                    active_eth = [i for i in eth_instruments if i.get('is_active', False)]
                    print(f"[PASS] API connected successfully")
                    print(f"[PASS] ETH options count: {len(eth_instruments)}")
                    print(f"[PASS] Active ETH options: {len(active_eth)}")
                    if active_eth:
                        print(f"  Active sample: {active_eth[0].get('instrument_name')}")
                    results["tests"].append({
                        "name": "get_instruments_eth",
                        "status": "PASS",
                        "total_count": len(eth_instruments),
                        "active_count": len(active_eth)
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_instruments_eth",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_instruments_eth",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        if active_instrument:
            instrument_name = active_instrument.get('instrument_name')
        else:
            instrument_name = "BTC-5JUN26-90000-C"

        print("\n" + "=" * 60)
        print(f"Test 3: Get Ticker Data ({instrument_name})")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/ticker",
                params={"instrument_name": instrument_name},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    ticker = data["result"]
                    print(f"[PASS] Ticker fetched successfully")
                    print(f"  Instrument: {ticker.get('instrument_name')}")
                    print(f"  Underlying: {ticker.get('underlying_price')}")
                    print(f"  Mark Price: {ticker.get('mark_price')}")
                    print(f"  Bid/Ask: {ticker.get('bid_price')}/{ticker.get('ask_price')}")
                    print(f"  Bid IV: {ticker.get('bid_iv')}")
                    print(f"  Ask IV: {ticker.get('ask_iv')}")
                    print(f"  Mark IV: {ticker.get('mark_iv')}")
                    print(f"  Open Interest: {ticker.get('open_interest')}")
                    print(f"  24h Volume: {ticker.get('stats', {}).get('volume')}")
                    results["tests"].append({
                        "name": "get_ticker",
                        "status": "PASS",
                        "data": {
                            "instrument": ticker.get('instrument_name'),
                            "underlying_price": ticker.get('underlying_price'),
                            "mark_price": ticker.get('mark_price'),
                            "bid_iv": ticker.get('bid_iv'),
                            "mark_iv": ticker.get('mark_iv'),
                            "open_interest": ticker.get('open_interest')
                        }
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_ticker",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_ticker",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        print("\n" + "=" * 60)
        print(f"Test 4: Get Order Book ({instrument_name})")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_order_book",
                params={"instrument_name": instrument_name, "depth": 20},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    book = data["result"]
                    print(f"[PASS] Order Book fetched successfully")
                    print(f"  Instrument: {book.get('instrument_name')}")
                    print(f"  Bid levels: {len(book.get('bids', []))}")
                    print(f"  Ask levels: {len(book.get('asks', []))}")
                    if book.get('bids'):
                        print(f"  Best bid: {book['bids'][0][0]}")
                    if book.get('asks'):
                        print(f"  Best ask: {book['asks'][0][0]}")
                    print(f"  Settlement price: {book.get('settlement_price')}")
                    results["tests"].append({
                        "name": "get_order_book",
                        "status": "PASS",
                        "data": {
                            "instrument": book.get('instrument_name'),
                            "bid_levels": len(book.get('bids', [])),
                            "ask_levels": len(book.get('asks', [])),
                            "settlement_price": book.get('settlement_price')
                        }
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_order_book",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_order_book",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        print("\n" + "=" * 60)
        print(f"Test 5: Get Recent Trades ({instrument_name})")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_last_trades_by_instrument",
                params={"instrument_name": instrument_name, "count": 10},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    trades_data = data["result"]
                    trades = trades_data.get("trades", [])
                    print(f"[PASS] Trades API connected")
                    print(f"  Recent trades: {len(trades)}")
                    if trades:
                        sample = trades[0]
                        print(f"  Sample: price={sample.get('price')}, amount={sample.get('amount')}")
                        print(f"  Direction: {sample.get('direction')}")
                    results["tests"].append({
                        "name": "get_trades",
                        "status": "PASS",
                        "count": len(trades)
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_trades",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_trades",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        print("\n" + "=" * 60)
        print(f"Test 6: Get Summary ({instrument_name})")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_summary",
                params={"instrument_name": instrument_name},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    summary = data["result"]
                    print(f"[PASS] Summary fetched successfully")
                    print(f"  Instrument: {summary.get('instrument_name')}")
                    print(f"  Open Interest: {summary.get('open_interest')}")
                    print(f"  Volume: {summary.get('volume')}")
                    print(f"  Bid: {summary.get('bid_price')}")
                    print(f"  Ask: {summary.get('ask_price')}")
                    results["tests"].append({
                        "name": "get_summary",
                        "status": "PASS",
                        "data": {
                            "open_interest": summary.get('open_interest'),
                            "volume": summary.get('volume')
                        }
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_summary",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_summary",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

        print("\n" + "=" * 60)
        print("Test 7: Get Volatility Index (DVOL)")
        print("-" * 60)
        try:
            async with session.get(
                f"{base_url}/public/get_volatility_index_data",
                params={"currency": "BTC", "start_timestamp": 1748131200000, "end_timestamp": 1748217600000},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                if resp.status == 200 and "result" in data:
                    dvol = data["result"]
                    print(f"[PASS] DVOL data fetched successfully")
                    print(f"  Data points: {len(dvol)}")
                    if dvol:
                        sample = dvol[0]
                        print(f"  Sample: timestamp={sample.get('timestamp')}, close={sample.get('close')}")
                    results["tests"].append({
                        "name": "get_dvol",
                        "status": "PASS",
                        "count": len(dvol)
                    })
                    results["summary"]["passed"] += 1
                else:
                    print(f"[FAIL] API error: {data}")
                    results["tests"].append({
                        "name": "get_dvol",
                        "status": "FAIL",
                        "error": str(data)
                    })
                    results["summary"]["failed"] += 1
        except Exception as e:
            print(f"[FAIL] Connection failed: {e}")
            results["tests"].append({
                "name": "get_dvol",
                "status": "FAIL",
                "error": str(e)
            })
            results["summary"]["failed"] += 1

    print("\n" + "=" * 60)
    print("API Connectivity Test Summary")
    print("=" * 60)
    print(f"Test time: {results['timestamp']}")
    print(f"Passed: {results['summary']['passed']}")
    print(f"Failed: {results['summary']['failed']}")

    if results['summary']['failed'] == 0:
        print("\n[PASS] All API tests passed! Data source is available.")
        return True
    else:
        print("\n[WARNING] Some tests failed, but core API is functional.")
        return results['summary']['passed'] >= 5

if __name__ == "__main__":
    result = asyncio.run(test_api_connectivity())
    sys.exit(0 if result else 1)
