# Server Management Guide

## Server Info

| Item | Value |
|------|-------|
| IP | 217.76.63.39 |
| OS | Ubuntu 24.04 LTS |
| CPU | 4 vCPU |
| RAM | 8 GB |
| Disk | 72 GB NVMe |
| Python | 3.12.3 |

## SSH Access

```bash
ssh -i ~/.ssh/id_rsa root@217.76.63.39
```

## Project Layout

```
/opt/Crypto_FreeAPIs/       # Project root
├── venv/                   # Python virtual environment
├── data/                   # Parquet output (gitignored)
├── logs/                   # Log files (gitignored)
├── .env                    # Secrets (gitignored)
├── deribit-options-data-collector/
│   └── launch.py           # Main entrypoint
├── fetchers/               # Data fetcher modules
├── processors/             # Signal processing modules
├── storage/                # Buffer and write layer
├── pipeline/               # Strategy pipeline
└── tests/                  # Test suite
```

## Service Management

### Start (tmux)

```bash
tmux new-session -d -s crypto \
  'cd /opt/Crypto_FreeAPIs && source venv/bin/activate && \
   python deribit-options-data-collector/launch.py --mode live 2>&1 | \
   tee -a logs/launch_$(date +%Y%m%d).log'
```

### Stop

```bash
tmux kill-session -t crypto
```

### Restart

```bash
tmux kill-session -t crypto 2>/dev/null
tmux new-session -d -s crypto \
  'cd /opt/Crypto_FreeAPIs && source venv/bin/activate && \
   python deribit-options-data-collector/launch.py --mode live 2>&1 | \
   tee -a logs/launch_$(date +%Y%m%d).log'
```

### View Live Output

```bash
tmux attach -t crypto
# Detach: Ctrl+B then D
```

### Check Status (without attaching)

```bash
tmux capture-pane -t crypto -p | tail -20
```

### Test Mode (60s smoke test)

```bash
cd /opt/Crypto_FreeAPIs && source venv/bin/activate
python deribit-options-data-collector/launch.py --mode test
```

## Deployment (Code Update)

```bash
# 1. Stop the service
tmux kill-session -t crypto 2>/dev/null

# 2. Pull latest code
cd /opt/Crypto_FreeAPIs
git pull origin master

# 3. Update dependencies (if requirements.txt changed)
source venv/bin/activate
pip install -r requirements.txt -q

# 4. Restart
tmux new-session -d -s crypto \
  'cd /opt/Crypto_FreeAPIs && source venv/bin/activate && \
   python deribit-options-data-collector/launch.py --mode live 2>&1 | \
   tee -a logs/launch_$(date +%Y%m%d).log'
```

## Data Management

### Data Directory Structure

```
data/{exchange}/{data_type}/{symbol}_{YYYY-MM-DD}.parquet
```

Examples:
```
data/deribit/options_greeks/BTC_2026-05-10.parquet
data/deribit/mark_price/BTC-PERPETUAL_2026-05-10.parquet
data/binance/spot_price/BTCUSDT_2026-05-10.parquet
data/fred/risk_free_rate/USD_2026-05-09.parquet
```

### Check Data Output

```bash
# List all parquet files
find /opt/Crypto_FreeAPIs/data -name '*.parquet' | sort

# Disk usage
du -sh /opt/Crypto_FreeAPIs/data/

# Check a specific file
source venv/bin/activate
python -c "
import pandas as pd
df = pd.read_parquet('data/deribit/options_greeks/BTC_2026-05-10.parquet')
print(f'Rows: {len(df)}, Columns: {list(df.columns)}')
print(df.head(2))
"
```

### Clean Old Data

```bash
# Remove data older than N days
find /opt/Crypto_FreeAPIs/data -name '*.parquet' -mtime +30 -delete

# Remove empty directories
find /opt/Crypto_FreeAPIs/data -type d -empty -delete
```

## Monitoring

### System Resources

```bash
# CPU and memory
htop

# Disk usage
df -h /

# Memory detail
free -h
```

### Log Inspection

```bash
# Latest log
tail -50 /opt/Crypto_FreeAPIs/logs/launch_$(date +%Y%m%d).log

# Search for errors
grep -i error /opt/Crypto_FreeAPIs/logs/*.log

# Live tail
tail -f /opt/Crypto_FreeAPIs/logs/launch_$(date +%Y%m%d).log
```

## Troubleshooting

### Service won't start

```bash
# Check if port/session already exists
tmux ls

# Check Python imports
cd /opt/Crypto_FreeAPIs && source venv/bin/activate
python -c "from storage import ChunkedBuffer; print('OK')"

# Check .env
cat /opt/Crypto_FreeAPIs/.env
```

### High memory usage

```bash
# Check buffer stats
tmux capture-pane -t crypto -p | grep "Flush triggered"

# Restart to clear buffers
tmux kill-session -t crypto
# ... then start again
```

### Disk full

```bash
# Check data sizes
du -sh /opt/Crypto_FreeAPIs/data/*/*

# Clean old data (keep last 7 days)
find /opt/Crypto_FreeAPIs/data -name '*.parquet' -mtime +7 -delete
```
