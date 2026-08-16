# 零中断并行部署 runbook — V3 采集器 Docker 化 + 升级到 GitHub HEAD

> 日期: 2026-08-16 | 状态: Phase A 执行中
> 硬约束: **数据采集不可中断 (一秒都不行)**

## 1. VPS 现状侦查结论 (2026-08-16)

### 1.1 采集架构 — 无按标的分容器

| 组件 | 运行形态 | 说明 |
|---|---|---|
| **Deribit 期权+永续采集器** (升级对象) | **tmux `crypto` 单进程** `/opt/Crypto_FreeAPIs/venv/bin/python launch.py --mode live` (PID 见 pgrep) | 单进程多线程覆盖全部标的: BTC/ETH/SOL USDC 期权链 + *_USDC-PERPETUAL + binance WS bridge + FRED。RSS ~756MB, 已跑 16 天 |
| nautilus-binance / nautilus-hyperliquid 容器 | docker, `scripts/production_collector.py`, /opt/nautilus_ares | **另一系统** (Donchian 策略 K 线采集), 与本次升级无关, 不动 |
| mt5-terminal 容器 | docker (wine) | MT5 实盘, 无关 |

### 1.2 代码版本核对 (全文件规范化 md5 对比)

- VPS git 停在 `0f6b53d` (6-29), 工作区含 7 月热修 (未提交)
- **VPS 工作区 == 7月末快照 ⊂ GitHub HEAD `df2cd3c`** (严格子集, 本地无缺失)
- VPS 独有仅: `deribit-options-data-collector/run_verify.sh` (cron 包装, 待回流) + state 备份 tar
- 活跃 iv_rank 状态: `iv_rank_{BTC,ETH,SOL}_USDC.json` (每日 02:00 UTC 写)

### 1.3 侦查发现的生产问题 (升级动机)

| # | 问题 | 证据 | 本地 HEAD 是否已修 |
|---|---|---|---|
| P1 | parquet **非原子写**, 读端可撞见损坏文件 | 08-16 ETH_USDC options_greeks 读出 "magic bytes not found" | ✅ `.tmp`+rename |
| P2 | WSOrderbookEngine **1009 无限重连** (单连接 1400+ 通道初始快照洪水 >16MiB) | console log 累计 70 万次断线, ~3s 一次 | ❌ (上游同版; sizes 数据由 ticker 通道 100% 覆盖, 不阻塞) |
| P3 | keep='first' 去重冻结半周期 K 线 | code review | ✅ keep='last' |
| P4 | cache_dir 随 CWD 漂移 | code review | ✅ 锚定子项目 |
| P5 | OHLCV gapfill 空洞饥饿 | commit 4a64f60 | ✅ |

## 2. 零中断策略 — 并行双跑 + 整日边界切换

```
时间轴:  ──deploy(T0)────验证≥48h────cut(T1=某日00:00 UTC)──────>
旧(tmux):    ■■■■■■■■■■■■■■■■■■■■■■□ (SIGINT→flush_all, 补齐最后几秒)
新(容器):              ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■▶ (从 T0 起完整覆盖)
数据归属:   ← 旧文件 (整日, ≤D-1) | D-1 尾秒由旧 flush_all 补齐 | 新文件 (整日, ≥D) →
```

**关键设计**: 切换点选整日 00:00 UTC, 归属按"整日文件"粒度二选一 (旧/新), **不做行级缝合** → 无缺口、无重复、无需去重。

- 旧容器在 T1 SIGINT → `flush_all` 把 D-1 最后几秒写入 D-1 文件 (行按数据时间戳落文件)
- 新容器从 T0 起独立采集, D 当天文件完整
- 重叠期 (T0..D-1) 的新文件**弃用** (保留作对照), 旧文件为准

## 3. Phase A — 并行部署 (不动旧进程, 全部为增量操作)

```bash
# A1. 代码上 VPS (独立目录, 不碰 /opt/Crypto_FreeAPIs)
rsync -az --exclude='.git/' --exclude='venv/' --exclude='**/data/' \
  --exclude='**/logs/' --exclude='**/cache/' --exclude='**/state/' \
  --exclude='**/__pycache__/' --exclude='.claude/' --exclude='.env' \
  ./FreeAPIs/ 217.76.63.39:/opt/Crypto_FreeAPIs_v2/
# CRLF 归一 (本地 Windows 检出)
ssh 217.76.63.39 "find /opt/Crypto_FreeAPIs_v2 -type f \( -name '*.py' -o -name '*.sh' -o -name '*.yaml' -o -name '*.toml' \) -exec sed -i 's/\r\$//' {} +"

# A2. 卷目录 + 状态种子 + env
ssh 217.76.63.39 '
mkdir -p /opt/crypto-v2-data /opt/crypto-v2-state
cp /opt/Crypto_FreeAPIs/deribit-options-data-collector/state/iv_rank_*.json /opt/crypto-v2-state/
cp /opt/Crypto_FreeAPIs/.env /opt/Crypto_FreeAPIs_v2/.env   # FRED_API_KEY
'

# A3. 构建镜像 (上下文已被 .dockerignore 裁剪: venv/data/state/.git 等不入镜像)
ssh 217.76.63.39 'cd /opt/Crypto_FreeAPIs_v2 && docker build -t crypto-collector:v2.0-20260816 .'

# A4. 启动并行容器 (资源限制保守: mem 2g / cpu 1.5; 旧进程 756MB 不受影响)
ssh 217.76.63.39 '
UID_C=$(docker run --rm --entrypoint id crypto-collector:v2.0-20260816 -u collector)
chown -R $UID_C /opt/crypto-v2-data /opt/crypto-v2-state
docker run -d --name crypto-collector-v2 --restart unless-stopped \
  --memory 2g --memory-swap 2g --cpus 1.5 \
  --env-file /opt/Crypto_FreeAPIs_v2/.env \
  -v /opt/crypto-v2-data:/app/data \
  -v /opt/crypto-v2-state:/app/deribit-options-data-collector/state \
  -v /opt/Crypto_FreeAPIs/.env:/app/.env:ro \
  crypto-collector:v2.0-20260816
'
```

## 4. Phase A 验证清单 (并行期 ≥48h)

1. **启动健康** (T0+5min): `docker logs crypto-collector-v2` — greeks 引擎 ~2524 instruments/kept 1466, WS 订阅成功, 无 crash loop
2. **首批落盘** (T0+15min): `/opt/crypto-v2-data` 出现 deribit/ options_greeks 等 parquet, 行数非零
3. **小时级对齐比对** (T0+1h 起): 同一 UTC 小时窗口, 旧 vs 新逐流 (stream×symbol) 行数量级一致, nonnull 率 (iv/bid_size/ask_size) 一致
4. **整日校验** (次日): 旧 venv 跑 `verify_fields.py <D-1> --data-dir /opt/crypto-v2-data` 全绿
5. **日线事件**: 观察到 ≥1 次 02:00 UTC iv_rank state 写入 + 08:00 UTC 期权到期链切换
6. **资源**: 新容器 RSS < 1.5g, 宿主 available > 1.5g, 磁盘增速正常 (~200MB/天/份)
7. **1009 对等性**: 新容器 orderbook 引擎同样 1009 循环 = 与旧一致 (P2 不因升级恶化; 修复另立项)

## 5. Phase B — 切换 (验证全绿后, 用户批准执行)

1. **T1 = 目标日 00:00-00:05 UTC**: `tmux send-keys -t crypto C-c` → 等 flush_all + 进程退出 (≤60s)
2. **数据归一**: 一次性把旧 dir 历史整日文件 (< D) 拷入 `/opt/crypto-v2-data`; 重叠期新文件 rename 到 `_parallel_overlap/` 留档
3. **切换下游**: cron `run_verify.sh` 的 `--data-dir` → `/opt/crypto-v2-data`; 本地 `pull_data.sh` REMOTE_DIR 同步改
4. **旧进程退役**: tmux `crypto` 会话保留壳但不再拉起; `/opt/Crypto_FreeAPIs` 整目录冻结归档 (含 git 工作区, 作为 7 月生产态快照)
5. **回滚预案**: 若切换后发现问题 → 停新容器, 按整日边界把归属切回旧文件, tmux 重启旧进程 (回滚窗口内新数据保留在 v2 dir, 无损)

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| Deribit/Binance 对同 IP 双连接限速 | 公共行情无 per-IP 连接上限; A2 后盯 1h 日志确认订阅成功率 |
| 宿主内存 (7.8G, 现 available 4.9G) | 新容器 hard limit 2g; 旧 756MB; 监控 available ≥1.5g |
| 双跑数据盘翻倍 (~200MB/天×2) | 39G 余量, 重叠期文件切换后归档/清理 |
| 新容器 crash loop | restart unless-stopped + 健康检查; 旧进程独立, 不受任何影响 |
| FRED key 共享限流 | 每日 2 次调用, 无风险 |
