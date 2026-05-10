# V3.0 实盘启动脚本开发完成报告

> 日期: 2026-05-08
> 基于: V3.0 期权+合约策略数据采集系统
> 前置文档: `docs/dev_report/TECH_DEBT_CLEANUP_REPORT.md`

---

## 一、任务背景

V3.0 的全部模块（6 个 fetcher、5 个 processor、ChunkedBuffer、6 策略管线）已开发并通过 184 项测试，但缺少一个统一的实盘启动入口。旧的 `run_collector.py` 仅支持单次 15 分钟采集，且架构与 V3.0 不兼容（使用 aiohttp + SQLite，而非 threading + ChunkedBuffer）。

本次任务为 V3.0 系统编写了一个基于「单进程、多线程」架构的生产级启动管理脚本 `launch.py`，替代旧的 `run_collector.py`。

---

## 二、设计决策

### 2.1 为什么选择单进程多线程（而非多进程）

| 方案 | 优势 | 劣势 |
|------|------|------|
| **subprocess / multiprocessing** | 进程隔离、崩溃不传染 | 无法共享 ChunkedBuffer 实例（需 IPC），内存开销翻倍，架构与现有代码不兼容 |
| **threading (选定)** | 共享 ChunkedBuffer、低内存开销、与现有代码一致 | GIL 限制 CPU 并行（但 I/O 密集型不受影响） |

V3.0 系统的瓶颈是网络 I/O（WS 等待、REST 轮询）和磁盘 I/O（Parquet 写入），而非 CPU 计算。`threading` 完全满足需求，且 ChunkedBuffer 已使用 `RLock` 保证线程安全。

### 2.2 线程架构

```
main thread (orchestrator)
|
|-- Phase 0: ChunkedBuffer + start_periodic_flush()
|
|-- Phase 1 (P0):
|   |-- [daemon] WS Bridge Thread
|   |   +-- QuoteFetcher.start_ws() -> 每秒 collect_ws_snapshots() -> buffer.append()
|   +-- [daemon] MarkPrice REST Poller (30s 间隔)
|
|-- Phase 2 (P1): 延迟 2s 启动
|   |-- [daemon] FundingRate REST Poller (8h 间隔)
|   +-- [daemon] MarginParams REST Poller (24h 间隔)
|
|-- Phase 3 (P2): 延迟 3s 启动
|   +-- [daemon] RiskFreeRate REST Poller (24h 间隔)
|
|-- [daemon] Monitor Thread (15s 间隔)
|   |-- 线程存活检查
|   |-- RSS 内存检查 (>6GB -> 紧急 flush)
|   |-- 每日 3:00 自动清理过期数据分区 (调用 prune_data.py)
|   +-- gc.collect() 释放碎片
|
|-- RotatingFileHandler (logger.py)
|   |-- maxBytes=50MB, backupCount=5, encoding=utf-8
|   +-- LOG_DIR 不可写时自动降级 (仅 console)
|
+-- Signal Handler (SIGINT/SIGTERM)
    +-- 停止线程 -> flush_all() -> 数据审计 -> stop_ws() -> gc.collect()
```

### 2.3 WS -> ChunkedBuffer 桥接

`QuoteFetcher.collect_ws_snapshots()` 只返回数据，不自动写入存储。因此设计了 `WSBridgeCollector` 线程：

```
每秒循环:
  snapshots = quote_fetcher.collect_ws_snapshots()
  df = pd.DataFrame([s.to_dict() for s in snapshots])
  for instrument, group in df.groupby("instrument_name"):
      buffer.append(exchange, "options_ticker", instrument, group)
```

这是 prompt 原始需求中未提及但必不可少的关键桥接层。

### 2.4 退出序列

```
1. shutdown_event.set()          -> 所有 daemon 线程退出 while 循环
2. 各 CollectorThread.stop()     -> WS Bridge 关闭 WebSocket 连接
3. 各线程 join(timeout=10)       -> 等待线程退出
4. buffer.flush_all()            -> 最终落盘 (返回 flushed_rows)
5. 数据审计 _run_data_audit()   -> 扫描落盘数据，生成 last_audit.md (仅 live>5min)
6. gc.collect()                  -> 释放内存
7. 打印运行摘要 (含 flushed_rows + FRED 模式)
```

---

## 三、交付清单

### 3.1 新建文件

| 文件 | 行数 | 用途 |
|------|------|------|
| `deribit-options-data-collector/launch.py` | 796 | 统一启动入口（完整替代 `run_collector.py`） |
| `deribit-options-data-collector/entrypoint.sh` | 61 | Docker/本地一键启动脚本（含自动重启） |
| `scripts/prune_data.py` | 176 | 跨平台 Hive 分区数据清理（替代 Bash 版本） |
| `.env` | 3 | 环境变量（FRED_API_KEY 等） |
| `.gitignore` | 7 | 防止 .env 等敏感文件意外提交 |

### 3.2 修改文件

| 文件 | 变更 |
|------|------|
| `requirements.txt` | 新增 `python-dotenv>=1.0.0` |
| `fetchers/ws_orderbook.py` | 修复 websockets 16.x `ws.closed` 兼容性问题（详见第六章） |
| `Dockerfile` | ENTRYPOINT 改为 `...launch.py --mode live`；CMD 简化为 `--strategies all`；新增 `ENV LOG_DIR` |
| `docker-compose.yml` | 新增 `.env` 文件挂载 (`:ro`)；新增 `LOG_DIR` 环境变量 |
| `utils/logger.py` | 重写为 `RotatingFileHandler` 方案（详见 7.2 节） |

### 3.3 launch.py 核心类

| 类 | 职责 |
|----|------|
| `CollectorThread(Thread)` | 可监控 daemon 线程基类，含 error_count / last_success 追踪 |
| `WSBridgeCollector` | WS 快照 -> ChunkedBuffer 桥接（P0, 1s 轮询） |
| `RESTPollerCollector` | 通用 REST 轮询线程（可配置间隔和 fetch 函数） |
| `ResourceMonitor` | 每 15s 检查线程存活 + 内存，>6GB 触发紧急 flush；每日 3:00 自动清理过期分区 |
| `SystemLauncher` | 总调度器，管理启动序列、信号处理、优雅退出、数据审计 |

### 3.4 CLI 接口

```bash
# 实盘持续采集
python deribit-options-data-collector/launch.py --mode live

# 60 秒验证模式
python deribit-options-data-collector/launch.py --mode test

# 仅启动 P0 级数据源
python deribit-options-data-collector/launch.py --strategies P0

# P0 + P1
python deribit-options-data-collector/launch.py --strategies P0,P1
```

---

## 四、功能验证结果

### 4.1 实盘测试（--mode test, 60 秒）

```
========================================================================
  V3.0 运行摘要
========================================================================
  运行时长:     63.0 秒
  启动线程:     5
  已停止:       WS-Bridge, MarkPrice, FundingRate, MarginParams, RiskFreeRate
  仍在运行:     无
  最终 flush:   1841 rows
  Rate Mode:    Live (FRED API)
    WS-Bridge (P0): errors=0
    MarkPrice (P0): errors=0
    FundingRate (P1): errors=0
    MarginParams (P1): errors=0
    RiskFreeRate (P2): errors=0
========================================================================
```

### 4.2 数据写入验证

ChunkedBuffer 成功生成 Hive 分区 Parquet 文件：

| 数据路径 | 行数 |
|---------|------|
| `data/deribit/options_ticker/date=2026-05-08/BTC-PERPETUAL.parquet` | 61 |
| `data/deribit/options_ticker/date=2026-05-08/ETH-PERPETUAL.parquet` | 61 |
| `data/binance/funding_rate/date=2026-05-08/BTCUSDT.parquet` | 1 |
| `data/deribit/margin_params/date=2026-05-08/BTC.parquet` | 949 |
| `data/deribit/margin_params/date=2026-05-08/ETH.parquet` | 761 |
| `data/fred/risk_free_rate/date=2026-05-08/USD.parquet` | 8 |

- WS Bridge 以 ~1 次/秒 频率采集 Deribit 永续合约 L1 报价
- MarginParams 首次轮询即获取 936 BTC 期权 + 13 永续 + 748 ETH 期权 + 13 永续合约参数
- RiskFreeRate 通过 FRED API 获取 8 条收益率曲线数据
- 优雅退出时 `flush_all()` 正确将内存中 1841 行全部落盘

### 4.3 降级模式验证

未设置 `FRED_API_KEY` 时，系统自动打印警告并使用 config 中的 fallback rate：

```
FRED_API_KEY 未设置，RiskFreeRate 进入降级模式 (使用 fallback rate)
```

Banner 显示实际 fallback 值（从 `config_strategy.yaml` 读取）：

```
  FRED Key: 未配置 -- 降级模式 (5.0%)
```

摘要行显示：

```
  Rate Mode: Fallback (5.0%)
```

已配置 FRED_API_KEY 后：

```
  FRED Key: 已配置
  Rate Mode: Live (FRED API)
```

### 4.4 信号处理验证

Ctrl+C 触发优雅退出序列，所有线程正常停止：

```
1. shutdown_event.set()          -> 所有 daemon 线程退出 while 循环
2. WSBridgeCollector.stop()      -> QuoteFetcher.stop_ws() 关闭 WebSocket
3. 各线程 join(timeout=10)       -> 等待线程退出
4. buffer.flush_all()            -> 最终落盘 1841 rows
5. 数据审计                      -> 生成 logs/last_audit.md (仅 live 模式 >5min)
6. gc.collect()                  -> 释放内存
```

### 4.5 回归测试

```
182 passed, 2 xpassed, 0 failed, 1 warning in 28.70s
```

launch.py 及所有起航前微调未引入任何回归。websockets 相关 DeprecationWarning 已从 5 条降至 1 条（剩余 1 条来自测试文件中的 `asyncio.get_event_loop()` 用法，不影响生产代码）。

---

## 五、REST 轮询间隔设计

| 采集器 | 优先级 | 轮询间隔 | 依据 |
|--------|--------|---------|------|
| WS Bridge (options_ticker) | P0 | 1s | strategy_configs 中 P0 策略要求 1s 频率 |
| MarkPrice | P0 | 30s | WS 负责实时价格，REST 仅作补充校准 |
| FundingRate | P1 | 8h | 与 Binance 资金费率结算周期一致 |
| MarginParams | P1 | 24h | Deribit 保证金参数日级别变动 |
| RiskFreeRate | P2 | 24h | FRED 每日发布 |

---

## 六、websockets 16.x 兼容性修复（高优先级）

### 6.1 问题

`ws_orderbook.py` 中使用 `ws.closed` 属性检查 WebSocket 连接状态，但 websockets 14+ 将 `WebSocketClientProtocol` 重构为 `ClientConnection`，移除了 `.closed` 属性。退出清理时出现：

```
AttributeError: 'ClientConnection' object has no attribute 'closed'
```

此错误发生在 `_cleanup()` 和 `_app_heartbeat_loop()` 中，可能导致优雅退出序列中断，使 `buffer.flush_all()` 无法执行或执行不完整。

### 6.2 修复

在 `ws_orderbook.py` 中新增兼容函数 `_ws_is_closed()`，同时支持新旧版本 API：

```python
def _ws_is_closed(ws) -> bool:
    if hasattr(ws, "closed"):           # websockets < 14
        return ws.closed
    if hasattr(ws, "state"):            # websockets 14+
        from websockets.protocol import State
        return ws.state >= State.CLOSING
    return True
```

替换了 2 处 `ws.closed` 调用：
- `_cleanup()` 中：`if self._ws and not self._ws.closed` -> `if self._ws and not _ws_is_closed(self._ws)`
- `_app_heartbeat_loop()` 中：`while self._running and not ws.closed` -> `while self._running and not _ws_is_closed(ws)`

同时将 3 处类型标注从 `websockets.WebSocketClientProtocol`（已 deprecated）改为 `object`，消除 3 条 DeprecationWarning。

### 6.3 验证

修复后全量测试通过，且退出时不再出现 `AttributeError`：

```
19 passed (test_ws_orderbook.py) -- 全量通过
182 passed, 2 xpassed, 1 warning -- 全量套件通过
```

---

## 七、环境变量

| 变量 | 必需 | 缺失行为 |
|------|------|---------|
| `FRED_API_KEY` | 否 | 降级模式，使用 `config_strategy.yaml` 中的 `fallback_rate: 0.05` |
| `DATA_DIR` | 否 | 默认 `./data`（由 config_strategy.yaml 覆盖） |
| `LOG_DIR` | 否 | 默认 `{project_root}/logs`；Docker 中为 `/opt/crypto-data/logs` |
| `LOG_LEVEL` | 否 | 默认 `INFO` |

`.env` 文件已配置：

```env
FRED_API_KEY=<已配置>
DATA_DIR=./data
LOG_LEVEL=INFO
```

`.gitignore` 已添加 `.env`，防止密钥意外提交。

### 7.1 Docker 入口 (Dockerfile)

```dockerfile
ENV LOG_DIR=/opt/crypto-data/logs

ENTRYPOINT ["python", "deribit-options-data-collector/launch.py", "--mode", "live"]
CMD ["--strategies", "all"]
```

### 7.2 日志轮转 (utils/logger.py)

`get_logger()` 使用 `logging.handlers.RotatingFileHandler` 替代了简单的 FileHandler：

- 单文件上限 50MB，保留 5 个轮转副本
- 编码强制 `utf-8`，避免 Windows GBK 编码错误
- `LOG_DIR` 不可写时（如 Docker read-only mount）自动降级为仅 console 输出
- Docker 环境中日志随数据卷持久化到 `/opt/crypto-data/logs/`

### 7.3 docker-compose.yml 更新

新增 `.env` 文件只读挂载和 `LOG_DIR` 环境变量：

```yaml
volumes:
  - ./.env:/app/.env:ro
environment:
  - LOG_DIR=/opt/crypto-data/logs
```

---

## 八、数据完整性审计

### 8.1 实现方案

`SystemLauncher._run_data_audit()` 方法在退出时对落盘数据做完整性扫描：

- 遍历 `data_dir` 下所有 `.parquet` 文件
- 按目录统计文件数、行数、磁盘占用
- 生成 Markdown 表格写入 `logs/last_audit.md`
- 同时将审计摘要打印到控制台（表格 + 汇总行）
- 仅在 `live` 模式且运行超过 5 分钟时触发（避免 test 模式无谓开销）

审计报告示例 (`logs/last_audit.md`)：

```markdown
# 数据完整性审计报告

> 生成时间: 2026-05-08 20:30:00
> 数据目录: `./data`

---

| 路径 | 文件数 | 总行数 | 大小 |
|------|--------|--------|------|
| `deribit/options_ticker/date=2026-05-08` | 2 | 122 | 0.1 MB |
| `binance/funding_rate/date=2026-05-08` | 1 | 1 | 0.0 MB |
| `deribit/margin_params/date=2026-05-08` | 2 | 1,710 | 0.3 MB |
| `fred/risk_free_rate/date=2026-05-08` | 1 | 8 | 0.0 MB |

**汇总**: 6 文件, 1,841 行

**状态**: 全部通过
```

### 8.2 跨平台数据清理 (scripts/prune_data.py)

使用 `pathlib` 实现的跨平台 Hive 分区清理脚本，替代仅限 Linux 的 `prune_cloud_data.sh`：

- 扫描 `DATA_DIR` 下所有 `date=YYYY-MM-DD` 格式的分区目录
- 自动清理超过保留天数的旧分区（默认 14 天）
- 清理完成后移除空的父目录
- 支持 dry-run 预览模式（默认）和 `--execute` 实际删除

```bash
# Dry-run 预览
python scripts/prune_data.py --data-dir ./data

# 实际删除 14 天前的数据
python scripts/prune_data.py --data-dir ./data --execute

# 自定义保留天数
python scripts/prune_data.py --keep-days 7 --execute
```

ResourceMonitor 每日凌晨 3 点自动触发清理，保留天数可通过 `config_strategy.yaml` 的 `storage.prune_keep_days` 配置。

验证结果（dry-run）：

```
=== Data Prune (DRY-RUN) ===
Data dir:   ./data
Keep days:  14
Cutoff:     2026-04-24
Scanned:    4 date partitions
Skipped:    4 (within retention)

(Dry-run: no files deleted. Use --execute to apply.)
```

### 8.3 entrypoint.sh 自动重启

`entrypoint.sh` 在 live 模式下内置异常退出自动重启逻辑：

- 正常退出（exit 0，如 SIGINT/SIGTERM 触发的优雅退出）：不重启
- 异常退出（exit code != 0，如未捕获异常）：自动重启
- 支持 `MAX_RETRIES`（默认 0 = 无限）和 `RETRY_DELAY`（默认 30s）环境变量
- 兼容 Linux / macOS / Windows Git Bash

```bash
# Docker 中自定义重启参数
docker run -e MAX_RETRIES=5 -e RETRY_DELAY=60 crypto-data-collector
```

---

## 九、已知遗留问题与运维建议

### 9.1 Binance markPriceKlines 时间窗口

Binance 的 `markPriceKlines` 接口对短时间内（< 1 分钟）的查询返回 400 错误。当前已调整为 300s 窗口，但实时采集场景下仍可能无数据。

**影响**: 不影响主路径（Deribit WS 负责 1s 级价格），Binance mark_price 仅作补充。

---

## 十、与旧系统的对比

| 维度 | 旧 `run_collector.py` | 新 `launch.py` |
|------|----------------------|----------------|
| 架构 | asyncio 单线程 | threading 多线程 |
| 存储 | SQLite + Snappy Parquet | ChunkedBuffer + ZSTD Parquet |
| 数据源 | 仅 Deribit REST | Deribit WS + REST, Binance, Hyperliquid, FRED |
| 运行模式 | 固定 15 分钟 | 持续运行 / 60s 测试 |
| 信号处理 | 基础 SIGINT | SIGINT + SIGTERM 优雅退出 |
| 资源监控 | 无 | 15s 线程健康 + 内存哨兵 |
| 优先级 | 无 | P0 -> P1 -> P2 分级启动 |
| 降级处理 | 无 | FRED_API_KEY 缺失自动降级（显示实际 fallback 值） |
| 配置管理 | 硬编码 | config_strategy.yaml + .env |
| WS 兼容性 | 不涉及 | 已修复 websockets 16.x 兼容 |
| 日志轮转 | 无 | RotatingFileHandler 50MB x5 |
| 数据审计 | 无 | 退出时扫描落盘数据，同时输出控制台 + `last_audit.md` |
| 数据清理 | 无 | 跨平台 prune_data.py + Monitor 每日 3:00 自动触发 |
| 崩溃重启 | 无 | entrypoint.sh 异常退出自动重启 (可配置 MAX_RETRIES) |
| Docker 入口 | 旧 run_strategy_data.py | launch.py --mode live + entrypoint.sh |
| FRED 状态 | 不涉及 | Banner + 摘要显示 Live/Fallback (X.X%) 模式 |
