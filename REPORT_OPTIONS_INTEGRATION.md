# Deribit 期权历史数据集成报告

## 项目概述

本项目成功完成了Deribit交易所期权历史数据接口的调研、测试与集成工作，将期权数据采集无缝集成到现有的每日OHLCV数据批处理脚本中。

## 调研结果

### Deribit API 可用性

**结论：Deribit API 提供期权历史数据访问权限，且接口完全可用。**

#### 可获取的数据类型

1. **期权合约列表** (`public/get_instruments`)
   - 支持的标的：BTC, ETH
   - 数据字段：合约名称、行权价(strike)、期权类型(call/put)、到期时间、状态等
   - BTC期权数量：930个活跃合约
   - ETH期权数量：744个活跃合约

2. **期权K线数据** (`public/get_tradingview_chart_data`)
   - 数据字段：时间戳、开仓价、最高价、最低价、收盘价、成交量
   - 支持的周期：1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 1d, 1w, 1M
   - 适用性：完全适用于期权合约的历史数据获取

3. **已到期期权数据**
   - 可以获取已到期期权的历史数据用于回测
   - 已到期BTC期权数量：56个

#### 关键API参数

- `currency`: 标的货币 (BTC, ETH)
- `kind`: 合约类型 (option)
- `expired`: 是否已到期 ('true'/'false'，注意必须是字符串)
- `instrument_name`: 合约名称
- `start_timestamp`: 开始时间戳（毫秒）
- `end_timestamp`: 结束时间戳（毫秒）
- `resolution`: K线周期

## 实现详情

### 1. 创建的文件

#### 1.1 Deribit期权采集器
**文件**: `fetchers/deribit_options.py`

核心类：`DeribitOptionsFetcher`

主要功能：
- `get_all_option_instruments()`: 获取指定货币的所有期权合约
- `_do_fetch()`: 基础K线数据获取
- `fetch_options_for_currency()`: 批量获取指定货币的所有期权数据

#### 1.2 集成批处理脚本
**文件**: `run_options.py`

扩展的功能：
- 支持期权数据采集模式
- 统一的配置管理
- 增量更新支持
- 日志记录与错误重试

#### 1.3 单元测试
**文件**: `tests/test_deribit_options.py`

测试覆盖：
- **初始化测试** (3个)
  - 采集器初始化
  - 符号映射
  - 统一符号转换

- **API测试** (3个)
  - BTC期权合约列表获取
  - ETH期权合约列表获取
  - 期权K线数据获取

- **数据集成测试** (4个)
  - BTC期权日频历史数据获取
  - ETH期权日频历史数据获取
  - 数据完整性验证
  - 时间序列正确性检查

- **异常处理测试** (3个)
  - 无效合约处理
  - 过期期权处理
  - 限流处理

- **鉴权测试** (2个)
  - 公共端点无需鉴权
  - 私有端点需要鉴权

### 2. 更新的文件

#### 2.1 配置更新
**文件**: `config.yaml`

新增配置项：
```yaml
deribit_options:
  enabled: true
  base_url: "https://www.deribit.com/api/v2"
  rate_limit:
    requests_per_second: 15
  option_symbols:
    BTC: "BTC"
    ETH: "ETH"
```

#### 2.2 模块导出更新
**文件**: `fetchers/__init__.py`

新增导出：`DeribitOptionsFetcher`

## 测试结果

### 测试执行摘要

```
总测试数: 15
通过: 15
失败: 0
错误: 0
总耗时: 15.421秒
```

### 详细测试结果

#### BTC期权数据获取

- 活跃BTC期权数量：930个
- 成功获取数据的期权数：3个（测试样例）
- 数据记录数：每个期权约3-4条日频数据
- 数据时间范围：2026-05-02 ~ 2026-05-05

#### ETH期权数据获取

- 活跃ETH期权数量：744个
- 成功获取数据的期权数：2个（测试样例）
- 数据记录数：每个期权约3-4条日频数据
- 数据时间范围：2026-05-02 ~ 2026-05-05

#### 数据质量验证

1. **数据完整性** ✓
   - 所有必需字段（timestamp, open, high, low, close, volume）均存在
   - 字段长度一致性验证通过

2. **OHLC逻辑验证** ✓
   - High >= Open ✓
   - High >= Close ✓
   - Low <= Open ✓
   - Low <= Close ✓
   - High >= Low ✓

3. **时间序列正确性** ✓
   - 时间戳递增验证通过
   - 时间范围准确

## 使用方法

### 基本用法

```bash
# 运行每日增量更新（包含期权数据）
python run_options.py --mode daily

# 排除期权数据采集
python run_options.py --mode daily --no-options

# 单标的测试
python run_options.py --mode single --exchange deribit_options --symbol BTC

# 历史回填
python run_options.py --mode backfill --days 30 --timeframe 1d
```

### 配置说明

在 `config.yaml` 中配置：

```yaml
deribit_options:
  enabled: true           # 启用期权采集
  base_url: "..."        # API地址
  rate_limit:
    requests_per_second: 15  # 限速设置
  option_symbols:
    BTC: "BTC"           # BTC期权
    ETH: "ETH"           # ETH期权
```

## 技术特性

### 1. 统一的数据格式

所有采集的期权数据使用与现货/期货相同的 `OHLCV` 数据模型：
- `timestamp`: 时间戳（毫秒）
- `open`, `high`, `low`, `close`: 价格数据
- `volume`: 成交量
- `exchange`: 交易所名称
- `symbol`: 标的符号（如 BTC-OPTIONS）
- `timeframe`: K线周期

### 2. 统一的存储路径

```
data/
├── deribit/              # 永续合约
│   ├── BTC_1d.parquet
│   └── ETH_1d.parquet
└── deribit_options/      # 期权数据
    ├── BTC-OPTIONS_1d.parquet
    └── ETH-OPTIONS_1d.parquet
```

### 3. 统一的调度时间

期权数据与现货数据使用相同的调度逻辑：
- 每日增量更新（默认）
- 支持自定义回填周期
- 自动跳过已更新数据

### 4. 日志记录

完善的日志系统：
- INFO: 正常操作日志
- WARNING: 异常情况（如限流、数据缺失）
- ERROR: 请求失败

### 5. 错误重试机制

内置重试机制：
- 最大重试次数：3次
- 指数退避：2秒 * 2^attempt
- 自动跳过无效数据

### 6. 限流处理

- 限速：15请求/秒
- 自动等待机制
- 429状态码特殊处理

## 已知限制

1. **数据延迟**
   - 部分期权可能刚上市，暂无历史数据
   - 建议使用已运行一段时间的期权进行回测

2. **数据量**
   - 单个货币可能有数百个期权合约
   - 完整采集可能需要较长时间

3. **Greeks数据**
   - 当前API不直接提供Greeks（delta, gamma, vega, theta, rho）
   - 如需可使用第三方数据源（如CoinMetrics）

## 扩展建议

1. **支持更多标的**
   - 当前支持BTC、ETH
   - 可扩展至SOL等其他主流币种

2. **数据增强**
   - 添加Greeks数据（需额外数据源）
   - 添加波动率曲面数据

3. **性能优化**
   - 并发采集多个期权
   - 增量更新优化

## 结论

✅ Deribit期权历史数据接口完全可用
✅ 成功实现BTC、ETH期权日频历史数据获取
✅ 所有单元测试通过（15/15）
✅ 与现有批处理系统无缝集成
✅ 数据格式统一、存储统一、调度统一

## 参考资料

- Deribit API文档: https://docs.deribit.com/
- Deribit JSON-RPC协议: https://docs.deribit.com/articles/json-rpc-overview
- 单元测试代码: `tests/test_deribit_options.py`
- 集成脚本: `run_options.py`
- 采集器代码: `fetchers/deribit_options.py`

---

生成时间: 2026-05-06
测试状态: 全部通过 ✓
