# binance-data-collector

Bulk historical downloader for Binance public data, per
[STRATEGY_DATA_REQUIREMENTS_BINANCE.md](../docs/binance/STRATEGY_DATA_REQUIREMENTS_BINANCE.md) §4.1.

Phase 1+2 scope: spot + perp + mark + index 1m klines + funding rate for BTCUSDT/ETHUSDT.
Binance Options (Phase 3, `vapi.binance.com`) is intentionally deferred — see strategy doc §5.

## Data source

[data.binance.vision](https://data.binance.vision) — static CDN of daily/monthly zipped CSVs.
No API key, no rate limit (but we add a 50ms politeness delay).

## Quick start

```bash
# 90-day backfill for all enabled types × both symbols
python launch.py --mode backfill

# Smaller smoke test
python launch.py --mode backfill --days 3 --symbols BTCUSDT --types spot_klines,perp_klines

# Daily incremental (run after UTC 05:00 to fetch yesterday's complete zip)
python launch.py --mode daily

# Verify what's on disk
python launch.py --mode verify
```

## Output layout

```
data/binance/{data_type}/{SYMBOL}_{YYYY-MM-DD}.parquet
```

Kline files (`spot_klines`, `perp_klines`, `mark_klines`, `index_klines`) carry the unified
schema `timestamp, exchange, symbol, timeframe, open, high, low, close, [, volume, quote_volume,
trades], close_time`. The volume/trades columns exist only on spot/perp (Binance mark/index
klines don't carry volume).

Funding files carry `timestamp, exchange, symbol, funding_rate`.

## Config

See [config.yaml](config.yaml). Key knobs:
- `global.symbols` — default `[BTCUSDT, ETHUSDT]`
- `global.interval` — default `1m` (GK RV原料)
- `global.history_days` — default `90`
- `data_types.<name>.enabled` — toggle individual streams

## Dependencies

Reuses parent project modules — no isolated virtualenv:
- `fetchers/binance_archive.py` — vision CDN bulk downloader
- `fetchers/binance_index_klines.py` — REST `/indexPriceKlines` (reserved for future REST-fallback mode)
- `storage/chunked_buffer.py` — atomic write, date-sharded zstd parquet

## Out of scope (deferred)

- Binance Options REST (`vapi.binance.com`)
- Live WS daemon (Binance historical is batch in nature)
- Remote deployment (217.76.63.39) — local Windows first
- Docker / Prometheus / SQLite
