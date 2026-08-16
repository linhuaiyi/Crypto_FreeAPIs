# Deribit Options Data Collector (V3.0)

期权 + 永续合约策略数据采集系统。本目录是 **V3.0 生产版本**，运行入口为 `launch.py`，
复用仓库根目录的共享模块（`fetchers/`、`processors/`、`storage/`、`pipeline/`）。

> ⚠️ `legacy_v1/` 内是已被 V3.0 取代的旧版实现（Poetry + aiohttp 架构，SQLite 双写），
> 仅作历史存档，**不要使用**。详见 `legacy_v1/README.md`。

## 架构（单进程多线程）

```
launch.py --mode live
├── WS Bridge        QuoteFetcher (Binance fstream) → ChunkedBuffer
├── Deribit WS       期权链行情引擎 (options_ticker → greeks → vol_surface → IV rank)
├── REST Pollers     funding_rate (8h) / mark_price (30s) / margin_params (24h) / risk_free_rate FRED (24h)
├── Signal Activation  spot_price / basis / dvol 派生计算
├── Monitor          线程健康检查 + 内存哨兵 (6GB 警戒线)
└── 信号处理          SIGINT/SIGTERM 优雅退出
```

## 运行

```bash
# 本地 / 云端 (tmux, 详见 docs/SERVER_MANAGEMENT.md)
python launch.py --mode live            # 实盘持续采集
python launch.py --mode test            # 运行 60 秒验证后退出
python launch.py --strategies P0        # 仅启动 P0 级策略数据
python launch.py --strategies all
```

依赖根目录 `.env`（`FRED_API_KEY` 等）与父级共享模块，无独立虚拟环境。

## 配置

`config_strategy.yaml` — 唯一配置文件：
- `api.*` 各交易所 endpoint / 限速
- `risk_free_rate.*` FRED 序列与 fallback
- `strategies.*` 各策略（short_strangle / synthetic_covered_call / ...）的数据需求声明
- `storage.chunked_buffer` 写缓冲；`prune_*` 云端数据生命周期

## 输出数据（Parquet, zstd）

```
data/
├── deribit/      options_ticker, options_greeks, mark_price, index_price,
│                 spot_price, funding_rate, basis, dvol, vol_surface, margin_params
├── binance/      spot_price, mark_price, funding_rate, basis
├── hyperliquid/  funding_rate
└── fred/         risk_free_rate
```

状态文件：`state/iv_rank_{BTC,ETH}.json`（IV rank 滚动状态，跨重启持久化，
可由 `scripts/export_iv_rank_state.sh` / `push_iv_rank_state.sh` 迁移）。

## 部署拓扑

- **云端节点** 217.76.63.39（4C/8G/75G NVMe）：tmux 常驻 `launch.py --mode live`，
  数据落在 `/opt/Crypto_FreeAPIs/deribit-options-data-collector/data`
- **回传**：每日 `scripts/pull_data.sh`（rsync 按日拉取）或 Syncthing 同步到本地
- **Docker**：仓库根目录 `Dockerfile` / `docker-compose.yml` 即本子项目的容器化部署
  （注意与 tmux 方案的卷路径不同，二选一）

## 相关文档

- `docs/PLAN_OPTIONS_PERP_DATA_SYSTEM.md` — 系统设计文档
- `docs/SERVER_MANAGEMENT.md` — 服务器运维手册
- `docs/dev_report/` — 开发与验收报告
