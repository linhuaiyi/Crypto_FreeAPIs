# 加密货币 OHLCV 数据采集系统

> 最后更新: 2026-05-06

## 一、系统概述

本系统用于从多个加密货币交易所自动采集 OHLCV（开高低收量）K线数据，支持历史回填和每日增量更新。

### 技术环境
- **运行环境**: Windows + Python 3.10+
- **虚拟环境**: venv
- **代理模式**: TUN 模式直连（无需代理）
- **数据格式**: Parquet 压缩存储

---

## 二、数据源覆盖

### 2.1 交易所列表

| 交易所 | 市场类型 | 标的数 | 周期数 |
|--------|----------|--------|--------|
| **Binance Spot** | 现货 | 14 | 7 |
| **Binance USDT-M** | 永续合约 | 10 | 7 |
| **Deribit** | 永续合约 | 3 | 5 |
| **Hyperliquid** | 永续合约 | 13 | 7 |
| **Deribit Options** | 期权 | 2 | 5 |
| **合计** | - | **42** | - |

### 2.2 采集的标的

| 标的 | Binance Spot | Binance USDT-M | Deribit | Hyperliquid | Deribit Options |
|------|:---:|:---:|:---:|:---:|:---:|
| BTC  | ✅ | ✅ | ✅ | ✅ | ✅ |
| ETH  | ✅ | ✅ | ✅ | ✅ | ✅ |
| SOL  | ✅ | ✅ | ✅ | ✅ | ❌ |
| BNB  | ✅ | ✅ | ❌ | ❌ | ❌ |
| XRP  | ✅ | ✅ | ❌ | ✅ | ❌ |
| ADA  | ✅ | ✅ | ❌ | ✅ | ❌ |
| DOGE | ✅ | ✅ | ❌ | ✅ | ❌ |
| SHIB | ✅ | ❌ | ❌ | ✅ | ❌ |
| PEPE | ✅ | ❌ | ❌ | ✅ | ❌ |
| WIF  | ✅ | ✅ | ❌ | ✅ | ❌ |
| BONK | ✅ | ❌ | ❌ | ✅ | ❌ |
| FLOKI| ✅ | ❌ | ❌ | ✅ | ❌ |
| LINK | ✅ | ✅ | ❌ | ✅ | ❌ |
| AVAX | ✅ | ✅ | ❌ | ✅ | ❌ |

### 2.3 支持的时间周期

```
1m   - 1分钟
15m  - 15分钟
30m  - 30分钟
1h   - 1小时
4h   - 4小时
1d   - 日线
1w   - 周线
1M   - 月线
```

---

## 三、数据统计

### 3.1 文件统计

| 交易所 | 文件数 | 标的 | 周期 |
|--------|--------|------|------|
| binance_spot | 98 | ADA, AVAX, BNB, BONK, BTC, DOGE, ETH, FLOKI, LINK, PEPE, SHIB, SOL, WIF, XRP | 15m, 1d, 1h, 1m, 1w, 30m, 4h |
| binance_usdm | 70 | ADA, AVAX, BNB, BTC, DOGE, ETH, LINK, SOL, WIF, XRP | 15m, 1d, 1h, 1m, 1w, 30m, 4h |
| deribit | 15 | BTC, ETH, SOL | 15m, 1d, 1h, 1m, 30m |
| deribit_options | 2 | BTC, ETH | 1m, 15m, 30m, 1h, 1d |
| hyperliquid | 91 | ADA, AVAX, BONK, BTC, DOGE, ETH, FLOKI, LINK, PEPE, SHIB, SOL, WIF, XRP | 15m, 1d, 1h, 1m, 1w, 30m, 4h |
| **总计** | **276** | - | - |

### 3.2 存储大小

- **总大小**: 约 2.07 GB
- **总文件数**: 276 个 Parquet 文件

---

## 四、项目结构

```
FreeAPIs/
├── venv/                      # Python 虚拟环境
├── config.yaml                # 配置文件（现货/期货）
├── config_options.yaml        # 配置文件（期权）
├── requirements.txt           # 依赖列表（现货/期货）
├── requirements-options.txt   # 依赖列表（期权）
├── run.py                     # 入口脚本（现货/期货）
├── run_options.py             # 入口脚本（期权独立运行）
├── options_collector.py      # 期权数据采集脚本（独立）
├── stats.py                   # 统计脚本
├── fetchers/                  # 数据采集器
│   ├── __init__.py
│   ├── base.py                # 基类
│   ├── binance.py             # Binance 采集器
│   ├── deribit.py             # Deribit 采集器
│   ├── deribit_options.py     # Deribit 期权采集器
│   └── hyperliquid.py         # Hyperliquid 采集器
├── models/                    # 数据模型
│   └── ohlcv.py               # OHLCV 数据结构
├── storage/                   # 存储管理
│   └── parquet_store.py        # Parquet 存储
├── utils/                     # 工具模块
│   ├── logger.py              # 日志工具
│   └── rate_limiter.py        # 速率限制器
├── data/                      # 数据输出目录
│   ├── binance_spot/          # Binance 现货数据
│   ├── binance_usdm/          # Binance USDT-M 合约数据
│   ├── deribit/               # Deribit 永续合约数据
│   ├── deribit_options/       # Deribit 期权数据
│   └── hyperliquid/            # Hyperliquid 数据
├── tests/                     # 测试目录
│   ├── test_deribit_options.py
│   └── test_options_collector.py
├── docs/                      # 文档目录
│   └── README.md              # 本文档
└── .trae/                    # 系统文件
    └── documents/             # 规划文档
```

---

## 五、使用方法

### 5.1 激活虚拟环境

```bash
.\venv\Scripts\python.exe run.py [参数]
```

### 5.2 运行模式（现货/期货）

#### 每日增量更新（推荐）

```bash
.\venv\Scripts\python.exe run.py --mode daily --timeframes "1m,15m,30m,1h,4h,1d,1w,1M"
```

#### 历史回填

```bash
.\venv\Scripts\python.exe run.py --mode backfill --timeframes "1m,15m,30m,1h,4h,1d,1w,1M" --days 2000
```

#### 单标的测试

```bash
.\venv\Scripts\python.exe run.py --mode single --exchange binance_spot --symbol BTC --days 7
```

---

## 六、期权数据独立采集

### 6.1 概述

期权数据采集已从现货/期货脚本中剥离，形成独立的 `options_collector.py` 脚本。该脚本提供完整的期权数据采集功能，包括：

- **期权链解析**: 自动解析期权合约名称，提取行权价、到期日、期权类型
- **希腊值计算**: 使用 Black-Scholes 模型计算 Delta、Gamma、Vega、Theta、Rho
- **波动率曲面**: 生成隐含波动率曲面数据
- **多周期支持**: 支持 1m、5m、15m、30m、1h、4h、1d、1w 周期
- **异常重试**: 内置指数退避重试机制

### 6.2 依赖安装

期权模块需要额外的科学计算依赖：

```bash
pip install -r requirements-options.txt
```

**主要依赖**:
- `scipy` - 用于希腊值和波动率计算
- `pandas` - 数据处理
- `numpy` - 数值计算
- `requests` - HTTP 请求

### 6.3 命令行参数

```
python options_collector.py --help

必选参数:
  --symbol          标的货币 (BTC, ETH, SOL)
  --date            采集日期 (YYYY-MM-DD格式)
  --expiry          期权到期日筛选天数 (默认: 30)

可选参数:
  --mode            运行模式: daily(默认), backfill
  --timeframe       K线周期 (默认: 1d)
  --days            回填天数 (仅backfill模式)
  --output-path     数据输出路径 (默认: ./data)
  --config          配置文件路径
  --no-greeks       不计算希腊值
  --no-volatility   不计算隐含波动率
  --debug           启用调试模式
```

### 6.4 使用示例

#### 采集 BTC 期权数据（30天内到期）

```bash
python options_collector.py --date 2026-05-06 --symbol BTC --expiry 30
```

#### 采集 ETH 期权数据（7天内到期）

```bash
python options_collector.py --date 2026-05-06 --symbol ETH --expiry 7 --timeframe 1d
```

#### 自定义输出路径

```bash
python options_collector.py --date 2026-05-06 --symbol BTC --output-path /data/options
```

#### 回填历史数据

```bash
python options_collector.py --mode backfill --symbol BTC --days 365 --expiry 30
```

#### 使用自定义配置

```bash
python options_collector.py --config config_options.yaml --symbol BTC
```

### 6.5 数据库表配置

期权数据使用独立的表前缀 `options_ohlcv`，存储路径为 `data/deribit_options/`。

**表命名规则**:
```
{标的}_options_{周期}.parquet
```

例如:
- `BTC_options_1d.parquet` - BTC 期权日线数据
- `ETH_options_1d.parquet` - ETH 期权日线数据

### 6.6 Crontab 定时任务配置

建议每天 18:05 执行期权数据采集任务（比现货数据晚5分钟）：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（每天18:05执行）
5 18 * * * cd /path/to/FreeAPIs && ./venv/bin/python options_collector.py --date $(date +\%Y-\%m-\%d) --symbol BTC --expiry 30 >> /var/log/options_btc.log 2>&1
5 18 * * * cd /path/to/FreeAPIs && ./venv/bin/python options_collector.py --date $(date +\%Y-\%m-\%d) --symbol ETH --expiry 30 >> /var/log/options_eth.log 2>&1
```

**说明**:
- `--date $(date +\%Y-\%m-\%d)` 自动获取当天日期
- 日志输出到 `/var/log/options_*.log`
- `2>&1` 将错误输出重定向到标准输出

### 6.7 配置说明

期权配置文件 `config_options.yaml`:

```yaml
deribit_options:
  enabled: true
  base_url: "https://www.deribit.com/api/v2"
  rate_limit:
    requests_per_second: 15
  max_retries: 3
  retry_delay_seconds: 2
  option_symbols:
    BTC: "BTC"
    ETH: "ETH"
    SOL: "SOL"

storage:
  data_dir: "./data/deribit_options"
  table_prefix: "options_ohlcv"
  format: "parquet"

options:
  default_timeframe: "1d"
  default_expiry_days: 30
  include_expired: false
  calculate_greeks: true
  calculate_volatility: true

greeks:
  risk_free_rate: 0.05
  volatility_model: "black_scholes"

logging:
  level: "INFO"
  format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

scheduler:
  daily_run_time: "18:05"
  backfill_enabled: true
  max_concurrent_requests: 10
```

---

## 七、数据格式

### 7.1 Parquet 文件命名规则（现货/期货）

```
{交易所}/{标的}_{周期}.parquet
```

例如:
- `binance_spot/BTC_1d.parquet` - BTC 日线数据
- `hyperliquid/ETH_1h.parquet` - ETH 小时线数据

### 7.2 Parquet 文件命名规则（期权）

```
deribit_options/{标的}_options_{周期}.parquet
```

例如:
- `deribit_options/BTC_options_1d.parquet` - BTC 期权日线数据

### 7.3 数据字段（现货/期货）

| 字段 | 类型 | 说明 |
|------|------|------|
| timestamp | int | 开盘时间 (毫秒时间戳) |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | float | 成交量 |
| quote_volume | float | 成交额 |
| exchange | string | 交易所名称 |
| symbol | string | 标的符号 |
| timeframe | string | K线周期 |
| trades | int (可选) | 成交笔数 |

### 7.4 数据字段（期权）

| 字段 | 类型 | 说明 |
|------|------|------|
| timestamp | int | 开盘时间 (毫秒时间戳) |
| instrument_name | string | 期权合约名称 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | float | 成交量 |
| underlying_price | float | 标的资产价格 |
| mark_price | float | 标记价格 |
| bid_price | float | 买一价 |
| ask_price | float | 卖一价 |
| open_interest | float | 未平仓合约数 |
| delta | float (可选) | Delta 希腊值 |
| gamma | float (可选) | Gamma 希腊值 |
| vega | float (可选) | Vega 希腊值 |
| theta | float (可选) | Theta 希腊值 |
| rho | float (可选) | Rho 希腊值 |
| implied_volatility | float (可选) | 隐含波动率 |

---

## 八、单元测试

### 8.1 运行期权模块测试

```bash
python tests/test_options_collector.py
```

**测试覆盖**:
- ✅ 期权链解析测试 (9个)
- ✅ 波动率计算测试 (8个，需scipy)
- ✅ 数据存储测试 (6个)
- ✅ 数据模型测试 (2个)

### 8.2 运行Deribit期权API测试

```bash
python tests/test_deribit_options.py
```

---

## 九、注意事项

1. **代理设置**: 系统当前使用 TUN 模式直连，无需代理配置
2. **速率限制**: 各交易所有不同的 API 速率限制，系统内置自动重试和速率限制器
3. **数据去重**: 写入时会自动基于 timestamp 去重
4. **增量更新**: daily 模式只获取最新的缺失数据，不会重复获取已有数据
5. **期权依赖**: 期权模块需要额外安装 `scipy` 依赖才能计算希腊值
6. **独立运行**: 期权数据采集可以独立于现货/期货脚本运行

---

## 十、依赖

### 现货/期货依赖 (requirements.txt)

```
requests>=2.31.0
pandas>=2.1.0
pyarrow>=14.0.0
pyyaml>=6.0
```

### 期权依赖 (requirements-options.txt)

```
requests>=2.28.0
pandas>=1.5.0
numpy>=1.23.0
pyarrow>=12.0.0
scipy>=1.10.0
pyyaml>=6.0
python-dateutil>=2.8.0
```
