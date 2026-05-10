# Deribit Options Data Collector

实时采集 Deribit 交易所 BTC/ETH 期权合约的全量行情数据，支持 REST API 和 WebSocket 双通道采集。

## 功能特性

- **双通道采集**: REST API 轮询 + WebSocket 实时订阅
- **增量采集**: 每秒采集 ticker、book、trades、markprice、greeks 数据
- **每日快照**: UTC 08:00 自动触发全量 orderbook 深度 20 档快照
- **双重存储**: Parquet 文件 + SQLite 数据库
- **监控告警**: Prometheus 指标 + PagerDuty 告警
- **优雅退出**: SIGTERM 信号捕获，确保数据零丢失

## 快速启动

### Docker Compose (推荐)

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f deribit-collector

# 停止
docker-compose down
```

### 本地运行

```bash
# 安装依赖
poetry install

# 启动采集器
poetry run python -m deribit_options_collector --config config/collector.yaml

# 或直接运行
python -m deribit_options_collector
```

## 配置说明

所有配置项在 `config/collector.yaml` 中定义，支持环境变量覆盖。

### 重要配置项

| 配置项 | 环境变量 | 默认值 | 说明 |
|--------|----------|--------|------|
| `deribit.api_key` | `DERIBIT_API_KEY` | 空 | API Key (可选，公共 API 不需要) |
| `deribit.api_secret` | `DERIBIT_API_SECRET` | 空 | API Secret |
| `collection.currencies` | `COLLECTION_CURRENCIES` | BTC,ETH | 采集品种 |
| `collection.incremental_interval_seconds` | `COLLECTION_INTERVAL` | 1 | 增量采集间隔(秒) |
| `collection.snapshot_cron` | `SNAPSHOT_CRON` | 0 8 * * * | 快照触发 cron |
| `storage.parquet.base_path` | `PARQUET_BASE_PATH` | data/raw/option | Parquet 存储路径 |
| `storage.sqlite.path` | `SQLITE_PATH` | db/deribit_options.db | SQLite 数据库路径 |
| `metrics.port` | `METRICS_PORT` | 9090 | Prometheus 指标端口 |
| `metrics.health_port` | `HEALTH_PORT` | 8080 | 健康检查端口 |
| `logging.level` | `LOG_LEVEL` | INFO | 日志级别 |
| `alerts.pagerduty.enabled` | `PAGERDUTY_ENABLED` | false | 是否启用 PagerDuty |
| `alerts.pagerduty.routing_key` | `PAGERDUTY_ROUTING_KEY` | 空 | PagerDuty Routing Key |

### 环境变量覆盖示例

```bash
export DERIBIT_API_KEY=your_api_key
export DERIBIT_API_SECRET=your_api_secret
export COLLECTION_CURRENCIES=BTC,ETH
export LOG_LEVEL=DEBUG
export PAGERDUTY_ENABLED=true
export PAGERDUTY_ROUTING_KEY=your_routing_key

docker-compose up -d
```

## 目录结构

```
deribit-options-data-collector/
├── config/
│   └── collector.yaml           # 主配置文件
├── src/
│   └── deribit_options_collector/
│       ├── __init__.py
│       ├── __main__.py         # 入口文件
│       ├── config.py           # 配置管理
│       ├── models.py           # 数据模型
│       ├── pipeline.py         # 主管道编排
│       ├── api/
│       │   ├── rest_client.py       # REST API 客户端
│       │   └── websocket_client.py  # WebSocket 客户端
│       ├── collectors/
│       │   ├── base.py               # 采集器基类
│       │   ├── incremental_collector.py  # 增量采集器
│       │   └── snapshot_collector.py  # 快照采集器
│       ├── storage/
│       │   ├── parquet_store.py      # Parquet 存储
│       │   └── sqlite_store.py        # SQLite 存储
│       └── metrics/
│           └── prometheus.py          # Prometheus 指标
├── data/
│   └── raw/option/                    # Parquet 数据目录
│       └── <instrument_name>/<yyyy-mm-dd>/
├── db/
│   └── deribit_options.db             # SQLite 数据库
├── tests/                              # 单元测试
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

## 数据存储

### Parquet 文件

路径格式: `data/raw/option/<instrument_name>/<yyyy-mm-dd>/`

| 数据类型 | 文件名 | 字段 |
|----------|--------|------|
| Ticker | `tickers.parquet` | instrument_name, timestamp, underlying_price, mark_price, bid/ask_price, bid/ask_iv, mark_iv, open_interest, volume_24h, ... |
| OrderBook | `orderbook.parquet` | instrument_name, timestamp, underlying_price, settlement_price, bids, asks, ... |
| Trades | `trades.parquet` | trade_id, timestamp, instrument_name, direction, price, amount, ... |
| Greeks | `greeks.parquet` | timestamp, instrument_name, delta, gamma, rho, theta, vega, ... |
| MarkPrice | `markprice.parquet` | timestamp, instrument_name, mark_price, index_price, settlement_price, ... |

### SQLite 表结构

- `option_tickers` - Ticker 数据 (主键: instrument_name, timestamp)
- `order_books` - 订单簿数据 (主键: instrument_name, timestamp)
- `trades` - 成交数据 (主键: trade_id)
- `greeks` - 希腊值数据 (主键: instrument_name, timestamp)
- `mark_prices` - 标记价格 (主键: instrument_name, timestamp)
- `settlement_prices` - 结算价格 (主键: instrument_name, timestamp)
- `instruments` - 合约元数据 (主键: instrument_name)

## 监控指标

### Prometheus 端点

- 指标端口: `http://localhost:9090/metrics`
- 健康检查: `http://localhost:8080/health`
- 就绪检查: `http://localhost:8080/ready`

### 关键指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `deribit_options_messages_lag_seconds` | Gauge | WebSocket 消息延迟 |
| `deribit_options_write_errors_total` | Counter | 写入错误总数 |
| `deribit_options_last_snapshot_timestamp` | Gauge | 上次快照时间戳 |
| `deribit_options_tickers_collected_total` | Counter | 已采集 ticker 总数 |
| `deribit_options_ws_connected` | Gauge | WebSocket 连接状态 |
| `deribit_options_data_flush_size` | Histogram | 每批次 flush 大小 |

### 告警规则

- WebSocket 断线超过 30 秒 → PagerDuty Critical
- 连续写入失败 5 次 → PagerDuty Critical

## 测试

```bash
# 运行所有测试
poetry run pytest

# 带覆盖率报告
poetry run pytest --cov=deribit_options_collector --cov-report=html

# 单个测试文件
poetry run pytest tests/test_api.py -v

# 单个测试用例
poetry run pytest tests/test_storage.py::TestSQLiteStore::test_save_ticker -v
```

## 故障排查

### 常见问题

1. **WebSocket 连接失败**
   - 检查网络连接
   - 确认 Deribit API 可访问
   - 查看日志中的详细错误信息

2. **数据写入失败**
   - 检查磁盘空间
   - 确认目录权限
   - 查看 SQLite 数据库是否被锁定

3. **告警频繁触发**
   - 检查网络稳定性
   - 调整 `ws_disconnect_threshold_seconds`
   - 调整 `write_failure_threshold`

### 日志级别调整

```bash
# 通过环境变量
export LOG_LEVEL=DEBUG

# 通过配置文件
logging:
  level: DEBUG
```

## 开发

### 代码规范

```bash
# 代码格式化
poetry run ruff format

# 代码检查
poetry run ruff check

# 类型检查
poetry run mypy src/

# 运行 pre-commit
poetry run pre-commit run --all-files
```

### 添加新功能

1. 在 `models.py` 中定义新的数据模型
2. 在 `collectors/` 中实现采集逻辑
3. 在 `storage/` 中实现存储逻辑
4. 添加对应的单元测试
5. 更新文档

## 依赖列表

| 依赖 | 版本 | 用途 |
|------|------|------|
| Python | 3.11+ | 运行环境 |
| aiohttp | >=3.9.0 | 异步 HTTP 客户端 |
| websockets | >=12.0 | WebSocket 客户端 |
| pandas | >=2.1.0 | 数据处理 |
| pyarrow | >=15.0.0 | Parquet 文件格式 |
| pydantic | >=2.5.0 | 数据验证 |
| prometheus-client | >=0.19.0 | Prometheus 指标 |
| structlog | >=24.1.0 | 结构化日志 |
| tenacity | >=8.2.0 | 重试机制 |
| apscheduler | >=3.10.0 | 定时任务 |
| orjson | >=3.9.0 | 高性能 JSON |

## License

MIT
