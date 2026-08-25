# nautilus-collector — Binance/Hyperliquid 采集（落盘版）

2026-08-20 自 /opt/nautilus_ares 移植归口 FreeAPIs；Telegram 发送链路退役。

## 数据流
```
交易所 → NautilusTrader(nc-binance/nc-hyperliquid 容器)
  → 小块 parquet(2min) data/production/<exch>/<type>/
  → cron 00:15 UTC: merge_daily.py 合并昨日小块 → data/archive/<exch>/<type>/<EXCH>_<type>_YYYYMMDD.parquet
  → WSL lake_pull 每日 09:07(北京) 拉取 archive/ → ~/datalake/raw/nautilus_prod/
```

## 镜像
`crypto-nc:base` FROM `nautilus_ares-binance:latest`（现有生产镜像，勿删！依赖全量复用）

## 运维
```bash
docker compose ps / logs -f nc-binance
docker run --rm -v $PWD/data:/app/data crypto-nc:base python /app/merge_daily.py  # 手动合并
# VPS cron: 15 2 * * * 上述 docker run（00:15 UTC）
```

## 宿主 cron（VPS root）
```
15 2 * * * docker run --rm -v /opt/Crypto_FreeAPIs/nautilus-collector/data:/app/data crypto-nc:base python /app/merge_daily.py >> /opt/Crypto_FreeAPIs/nautilus-collector/logs/merge_daily.log 2>&1
```
