# 零中断并行部署 runbook — V3 采集器 Docker 化 + 升级到 GitHub HEAD

> 日期: 2026-08-16 | 状态: Phase A 已部署 (v2 容器运行中, 08-17 00:00 UTC 后做整天比对)
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
| P1 | ~~parquet 非原子写~~ **误诊**: ChunkedBuffer 为长驻 ParquetWriter 追加写, 当日文件日中无 footer 属正常设计; 日切/优雅退出才闭合。真实风险 = 硬杀进程 → 当日文件无 footer → 重启时被 quarantine (行数据滞留难恢复) | 08-16 文件日中读必报 magic bytes; 08-15 文件读取完好 | N/A (设计); 对策: 任何停止用 SIGINT/SIGTERM (容器 `docker stop -t 30`, 禁 `docker kill`) |
| P2 | WSOrderbookEngine **1009 无限重连** (~3s 一次, 累计 70 万次) — **根因是旧进程陈旧**: tmux 进程 7-19 启动, 早于 7-25 落盘的分片热修 (rsync 了代码但从未重启); 新代码 11 引擎分片 (单引擎≤630通道), v2 实测 **0 次 1009** | 旧 console log vs v2 docker logs | ✅ (代码已在 HEAD, 旧进程未重启而已) |
| P3 | keep='first' 去重冻结半周期 K 线 | code review | ✅ keep='last' |
| P4 | cache_dir 随 CWD 漂移 | code review | ✅ 锚定子项目 |
| P5 | OHLCV gapfill 空洞饥饿 | commit 4a64f60 | ✅ |

> 教训: VPS 存在 "磁盘代码 ≠ 运行代码" 漂移 (rsync 热修未重启进程), 全文件 md5 对比只能验证磁盘态。

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

> ⚠️ 当日文件为长驻 ParquetWriter 追加写, 日中无 footer 不可直读 — 行数验证只针对**日切后已闭合的整天文件**; 日中验证靠日志吞吐 (ChunkedBuffer `opened/appended N rows`) 与监控。

1. ✅ **启动健康** (T0+5min, 2026-08-16 14:20 UTC 完成): 全引擎拉起, greeks 2524 instruments kept 1466 (与旧一致), IVRank 从 state 种子引导 28 天, FRED 拉取成功, 无异常
2. ✅ **WS 分片对等** (T0+10min): quote(rest)=3546 on 6 engines + ticker(近月 ATM ≤7DTE ±15%)=480 on 5 engines, 单引擎 ≤630 通道, **0 次 1009** (旧进程 ~3s 一次)
3. ✅ **首批落盘** (T0+15min): options_ticker/dvol/basis/index_price/mark_price/funding_rate/margin_params 等陆续 opened writer
4. ⏳ **整天文件比对** (08-17 00:00 UTC 后): 旧 vs 新 08-16 整天文件逐流 (stream×symbol) 行数量级一致, nonnull 率 (iv/bid_size/ask_size) 一致 — 用 A5 的 compare 脚本
5. ⏳ **字段级校验** (08-17): 旧 venv 跑 `verify_fields.py 2026-08-16 --data-dir /opt/crypto-v2-data` 全绿
6. ⏳ **日线事件** (08-17): 观察到 00:00 UTC 日切 writer 闭合 + 02:00 iv_rank state 写入; 08-19 前后观察 08:00 期权到期链切换
7. ⏳ **资源**: 新容器 RSS < 1.5g (当前 210MB 起), 宿主 available > 1.5g, 磁盘增速正常
8. ⏳ **48h 稳定性**: 无 crash/restart (docker events)

## 5. Phase B — 切换 (验证全绿后, 用户批准执行)

1. **T1 = 目标日 00:00-00:05 UTC** (建议 08-19 或之后, 需整天比对+字段校验全绿): `tmux send-keys -t crypto C-c` → 等进程优雅退出 (flush_all + close_all_writers + state 保存, ≤60s); 新容器**全程不停**, 无缝接管
2. **数据归一**: 一次性把旧 dir 历史整日文件 (< D) 拷入 `/opt/crypto-v2-data`; 重叠期新文件 rename 到 `_parallel_overlap/` 留档
3. **切换下游**: cron `run_verify.sh` 的 `--data-dir` → `/opt/crypto-v2-data`; 本地 `pull_data.sh` REMOTE_DIR 同步改
4. **旧进程退役**: tmux `crypto` 会话保留壳但不再拉起; `/opt/Crypto_FreeAPIs` 整目录冻结归档 (含 git 工作区, 作为 7 月生产态快照)
5. **回滚预案**: 若切换后发现问题 → `docker stop -t 30 crypto-collector-v2` (优雅退出, 禁 kill), 按整日边界把归属切回旧文件, tmux 重启旧进程 (回滚窗口内新数据保留在 v2 dir, 无损)

> 容器生命周期铁律: 任何停止/重启用 `docker stop -t 30` / `docker restart -t 30` (launch.py 处理 SIGTERM→flush+闭合 writer); **绝不 `docker kill` / `rm -f`** (当日文件无 footer → 重启被 quarantine → 当日数据滞留)。

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| Deribit/Binance 对同 IP 双连接限速 | 公共行情无 per-IP 连接上限; A2 后盯 1h 日志确认订阅成功率 |
| 宿主内存 (7.8G, 现 available 4.9G) | 新容器 hard limit 2g; 旧 756MB; 监控 available ≥1.5g |
| 双跑数据盘翻倍 (~200MB/天×2) | 39G 余量, 重叠期文件切换后归档/清理 |
| 新容器 crash loop | restart unless-stopped + 健康检查; 旧进程独立, 不受任何影响 |
| FRED key 共享限流 | 每日 2 次调用, 无风险 |
