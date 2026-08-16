# FreeAPIs — 加密货币数据采集 Monorepo

> 最后更新: 2026-08-16（重组后）

## 一、仓库结构

本仓库是 **共享 Python 库 + 3 个采集子项目** 的 monorepo。
所有子项目通过 `sys.path` 复用根目录的共享模块，共用根目录 `venv`。

```
FreeAPIs/
├── fetchers/      共享采集器: binance / deribit / hyperliquid OHLCV,
│                  funding_rate / mark_price / risk_free_rate(FRED) /
│                  margin_params / ws_orderbook / binance_archive 等
├── processors/    信号处理: greeks / basis / vol_surface / iv_rank /
│                  outlier_filter / gap_detector / time_aligner / validators
├── storage/       parquet_store + chunked_buffer (zstd 原子分块写)
├── pipeline/      策略数据需求注册表 + 编排
├── models/ utils/ OHLCV 模型、限速器、日志、配置加载
├── tests/         共享模块测试 (20+ 文件)
│
├── ohlcv-collector/               [子项目] 多交易所 OHLCV K线批采集
├── binance-data-collector/        [子项目] Binance vision CDN 历史批量下载
├── deribit-options-data-collector/[子项目] V3.0 期权+永续实时采集 (生产主力)
│
├── scripts/       运维脚本 (拉数据/裁剪/校验/IV rank 回填)
├── docs/          文档 (见下)
├── Dockerfile / docker-compose.yml  deribit 子项目的容器化部署
└── .env           密钥 (gitignored)
```

## 二、三个子项目

| 子项目 | 用途 | 模式 | 输出 |
|---|---|---|---|
| **ohlcv-collector** | Binance 现货/USDM + Deribit + Hyperliquid，14 币 × 9 周期 K 线 | `backfill` / `daily` / `single` / `gapfill` | `ohlcv-collector/data/{exchange}/{SYMBOL}_{tf}.parquet` |
| **binance-data-collector** | data.binance.vision CDN 批量下载 BTC/ETH 的 spot/perp/mark/index 1m K线 + funding | `backfill` / `daily` / `verify` | `binance-data-collector/data/binance/{type}/{SYMBOL}_{DATE}.parquet` |
| **deribit-options-data-collector** | V3.0 策略数据系统：WS 行情 + REST 轮询 + Greeks/基差/波动率曲面/IV rank | `live` / `test` | 见其 README |

各子项目 README 有详细说明。`deribit-options-data-collector/legacy_v1/` 是已废弃的旧版实现，仅存档。

## 三、部署拓扑

```
云端 217.76.63.39 (4C/8G)          本地 Windows
┌─────────────────────┐   rsync/   ┌──────────────────────────┐
│ tmux: deribit       │  Syncthing │ pull_data.sh 拉回归档      │
│  launch.py --mode   │ ─────────→ │ ohlcv/binance collector  │
│  live               │  每日      │  补历史 + gapfill         │
│ 云端仅保留 14 天     │           │                          │
└─────────────────────┘           └──────────────────────────┘
```

- 云端运维: `docs/SERVER_MANAGEMENT.md`
- 每日回传: `scripts/pull_data.sh`（或 Syncthing）
- 云端裁剪: `scripts/prune_cloud_data.sh`（同步后删除 >14 天数据）

## 四、常用操作

```bash
# OHLCV 每日增量 / 回填 / 空洞修补
python ohlcv-collector/launch.py --mode daily --timeframes "1m,15m,30m,1h,4h,1d,1w,1M"
python ohlcv-collector/launch.py --mode backfill --days 2000 --timeframes ...
python ohlcv-collector/launch.py --mode gapfill --timeframes 1m

# Binance 历史批量
python binance-data-collector/launch.py --mode backfill [--days N] [--symbols BTCUSDT]
python binance-data-collector/launch.py --mode verify

# 期权+永续实盘（云端）
python deribit-options-data-collector/launch.py --mode live

# 数据校验
python scripts/verify_collected_data.py
python scripts/verify_pulled_data.py
```

## 五、文档索引

| 文档 | 内容 |
|---|---|
| `docs/SERVER_MANAGEMENT.md` | 云端服务器运维（SSH/tmux/部署） |
| `docs/PLAN_OPTIONS_PERP_DATA_SYSTEM.md` | V3.0 系统设计 |
| `docs/binance/STRATEGY_DATA_REQUIREMENTS_BINANCE.md` | Binance 采集范围依据 |
| `docs/deribit/` | Deribit 基础设施评估、runbooks |
| `docs/dev_report/` | 开发/验收/技术债报告（历史） |
| `docs/archive/` | 早期规划文档归档（原 .trae） |

## 六、注意事项

1. 各子项目数据目录均 gitignored；历史大归档已移出仓库
   （`../_archive/`，含 23GB 的 data_archive_20260715）
2. 第三方参考代码已移至 `../../_reference/FreeAPIs_external/`（nautilus_trader 等）
3. Hyperliquid 的 `kPEPE/kBONK/kSHIB/kFLOKI` 是 1000x 单位，跨所对齐时注意量纲
4. 密钥统一在根目录 `.env`，勿提交
