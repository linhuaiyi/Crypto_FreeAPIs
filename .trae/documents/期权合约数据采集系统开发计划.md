# 期权+合约类策略数据采集系统开发计划

> 项目: Deribit 期权数据采集系统
> 制定日期: 2026-05-07
> 计划周期: 4-6周

---

## 一、项目背景与目标

### 1.1 项目目标
构建完整的加密货币期权+永续合约量化交易数据采集系统，支持 6 类组合策略的回测与实盘运行。

### 1.2 策略数据依赖矩阵

| 策略 | 频率 | 核心数据依赖 | 优先级 |
|------|------|--------------|--------|
| Short Strangle + 永续对冲 | 日级 | 期权bid/ask、Delta、保证金率 | P0 |
| 合成备兑看涨 | 周级 | Call权利金、永续价格，资金费率 | P0 |
| 领口策略 | 月级 | Call/Put权利金差、Delta | P0 |
| 资金费率套利 + 期权保护 | 日级 | 资金费率、基差、Put权利金 | P1 |
| Gamma Scalping | 日级 | 实时Greeks、永续bid/ask | P1 |
| 波动率期限结构套利 | 周级 | 近远端IV，两组Greeks | P2 |

---

## 二、当前系统状态评估

### 2.1 已完成模块 (70%)

| 模块 | 状态 | 文件位置 |
|------|------|----------|
| 配置管理 | ✅ 完成 | `config.py` |
| 数据模型 | ✅ 完成 | `models.py` |
| REST客户端 | ✅ 完成 | `api/rest_client.py` |
| WebSocket客户端 | ✅ 完成 | `api/websocket_client.py` |
| Parquet存储 | ✅ 完成 | `storage/parquet_store.py` |
| SQLite存储 | ✅ 完成 | `storage/sqlite_store.py` |
| 增量采集器 | ✅ 完成 | `collectors/incremental_collector.py` |
| 快照采集器 | ✅ 完成 | `collectors/snapshot_collector.py` |
| Prometheus监控 | ✅ 完成 | `metrics/prometheus.py` |
| Docker部署 | ✅ 完成 | `Dockerfile`, `docker-compose.yml` |

### 2.2 待解决问题

#### P0 - 必须解决

| 问题 | 描述 | 影响 |
|------|------|------|
| Greeks计算 | ~30%合约Greeks为空值，需Black-Scholes补算 | 策略回测不准确 |
| 无风险利率曲线 | 缺少US Treasury收益率曲线 | Greeks计算不完整 |
| 主力合约映射 | 缺少自动识别主力/次主力逻辑 | 无法确定交易标的 |
| 单元测试覆盖 | 当前70%，目标>=90% | 代码质量风险 |

#### P1 - 建议解决

| 问题 | 描述 |
|------|------|
| Greeks字段优化 | 实时Greeks采集完善 |
| 数据间隙检测 | 检测>60s间隙 |
| 异常值过滤 | Z-Score>5过滤 |
| 波动率曲面 | ATM IV、Skew计算 |

---

## 三、详细开发计划

### Sprint 1: 核心功能完善 (5天)

#### Day 1-2: Greeks计算模块

**任务 T-101: Black-Scholes Greeks计算引擎**

```
目标: 实现期权Greeks自动计算，补全约30%的缺失数据

实现内容:
1. 创建 `greeks_calculator.py` 模块
   - Black-Scholes定价模型
   - 计算Delta、Gamma、Theta、Vega、Rho
   - 支持欧式期权(看涨/看跌)

2. 数据模型扩展
   - GreeksCalculationResult 数据类
   - 计算参数配置

3. 与现有系统集成
   - 在数据处理管道中添加补算逻辑
   - 配置项控制是否启用自动补算

数学公式:
- d1 = (ln(S/K) + (r + σ²/2)T) / (σ√T)
- d2 = d1 - σ√T
- Delta(C) = N(d1), Delta(P) = N(d1) - 1
- Gamma = N'(d1) / (S × σ × √T)
- Theta(C) = -S × N'(d1) × σ / (2√T) - r × K × e^(-rT) × N(d2)
- Vega = S × √T × N'(d1)

技术要求:
- 精度: 计算结果与Deribit Greeks误差<1%
- 性能: 单合约计算<1ms
- 缓存: 使用numpy向量化加速

输出文件:
- `src/deribit_options_collector/greeks_calculator.py`
- `tests/test_greeks_calculator.py`
```

**任务 T-102: FRED API无风险利率对接**

```
目标: 获取US Treasury收益率曲线作为无风险利率输入

实现内容:
1. 创建 `risk_free_rate.py` 模块
   - FRED API客户端
   - 支持下载多种期限国债收益率
   - 本地缓存机制

2. 插值计算
   - 线性插值/三次样条插值
   - 计算任意期限的精确利率
   - 连续复利转换

3. 配置扩展
   - 添加 FRED_API_Key 配置
   - 缓存时间配置

API端点:
- https://api.stlouisfed.org/fred/series/observations
- 系列: DTB3 (3个月), DTB6 (6个月), DGS10 (10年)等

技术约束:
- API限速: 120请求/小时
- 本地缓存: 至少24小时
- 备用方案: 使用固定利率0.05 (5%)

输出文件:
- `src/deribit_options_collector/risk_free_rate.py`
- `tests/test_risk_free_rate.py`
```

#### Day 3-4: 主力合约识别

**任务 T-103: 主力合约自动映射**

```
目标: 基于OI自动识别主力/次主力合约

实现内容:
1. 创建 `main_contract_identifier.py` 模块
   - 按币种分组 (BTC, ETH, SOL)
   - 按到期日分组 (周, 月, 季度)
   - 按OI排序
   - ATM/OTM分类

2. 映射表生成
   - 主力合约 (OI最高)
   - 次主力合约 (OI第二)
   - 活跃合约 (OI>阈值)
   - 到期日映射

3. 定时更新
   - 每日更新主力映射
   - 存储到SQLite

数据结构:
```python
MainContractMapping:
    currency: str  # BTC, ETH, SOL
    expiration_date: date
    main_contract: str  # 主力合约名
    secondary_contract: str  # 次主力合约名
    active_contracts: list[str]
    atm_strike: float  # ATM执行价
    atm_call: str
    atm_put: str
    generated_at: datetime
```

输出文件:
- `src/deribit_options_collector/main_contract_identifier.py`
- `tests/test_main_contract_identifier.py`
```

#### Day 5: 测试覆盖率提升

**任务 T-104: 单元测试覆盖率提升至90%**

```
目标: 补全测试用例，覆盖率从70%提升至90%

需要补充的测试:
1. greeks_calculator.py 测试
   - BS_CALL/BS_PUT公式验证
   - 边界条件测试 (深度虚值/实值)
   - 精度测试

2. risk_free_rate.py 测试
   - 缓存机制测试
   - 插值算法测试
   - 降级方案测试

3. main_contract_identifier.py 测试
   - OI排序测试
   - ATM识别测试
   - 多币种测试

4. 集成测试补充
   - 端到端数据采集测试
   - 异常处理测试

执行命令:
pytest tests/ -v --cov=src --cov-report=html --cov-fail-under=90
```

---

### Sprint 2: 数据质量增强 (5天)

#### Day 1-2: 数据间隙检测

**任务 T-201: 数据间隙检测器**

```
目标: 检测并记录>60s的数据采集间隙

实现内容:
1. 创建 `gap_detector.py` 模块
   - 时间序列间隙检测
   - 间隙长度计算
   - 间隙原因分类

2. 告警机制
   - Prometheus指标: gap_detected_total
   - 日志记录
   - 可选: 邮件/短信通知

3. 间隙修复
   - 自动触发REST补采
   - 标记数据来源

数据结构:
```python
DataGap:
    instrument_name: str
    data_type: str  # ticker, orderbook, trades
    gap_start: datetime
    gap_end: datetime
    gap_duration_seconds: float
    recovery_action: str
```

输出文件:
- `src/deribit_options_collector/gap_detector.py`
- `tests/test_gap_detector.py`
```

**任务 T-202: 异常值过滤器**

```
目标: 使用Z-Score过滤异常数据点

实现内容:
1. 创建 `outlier_filter.py` 模块
   - Z-Score计算
   - 可配置阈值 (默认5σ)
   - 滑动窗口机制

2. 过滤规则
   - 价格异常: bid/ask spread过宽
   - IV异常: bid_iv < 0 或 > 500%
   - OI异常: 负值
   - Greeks异常: 超出理论范围

3. 标记vs过滤
   - 标记: 保留数据但标记为可疑
   - 过滤: 丢弃数据

配置示例:
```yaml
data_quality:
  outlier_filter:
    enabled: true
    zscore_threshold: 5.0
    window_size: 100
    action: "flag"  # flag or filter
```

输出文件:
- `src/deribit_options_collector/outlier_filter.py`
- `tests/test_outlier_filter.py`
```

#### Day 3-4: 波动率曲面

**任务 T-203: 波动率曲面构建**

```
目标: 计算ATM IV、Skew、波动率曲面

实现内容:
1. 创建 `volatility_surface.py` 模块
   - ATM IV计算
   - Skew计算 (25Δ, 10Δ)
   - IV Rank计算
   - IV Percentile计算

2. 曲面数据
   - 按strike和expiry组织
   - 插值填充空值
   - 时间序列存储

3. 指标计算
```python
VolatilityMetrics:
    instrument_name: str
    timestamp: datetime
    atm_iv: float
    rr_25d: float  # 25Δ Risk Reversal
    rr_10d: float   # 10Δ Risk Reversal
    bf_25d: float   # 25Δ Butterfly
    bf_10d: float   # 10Δ Butterfly
    iv_rank: float
    iv_percentile: float
```

输出文件:
- `src/deribit_options_collector/volatility_surface.py`
- `tests/test_volatility_surface.py`
```

#### Day 5: 数据完整性验证

**任务 T-204: 数据验证脚本**

```
目标: 创建数据完整性验证工具

实现内容:
1. 创建 `validate_data.py` 脚本
   - 文件完整性检查
   - 字段完整性检查
   - 数据范围检查
   - 重复记录检查

2. 验证报告
   - JSON格式报告
   - 问题汇总
   - 修复建议

3. 自动修复 (可选)
   - 修复重复记录
   - 填充空值

执行命令:
python validate_data.py --date 2026-05-07 --report validation_report.json
```

---

### Sprint 3: 高级功能 (5天)

#### Day 1-2: 自动展期机制

**任务 T-301: 合约自动展期**

```
目标: 到期前自动切换到新主力合约

实现内容:
1. 创建 `rollover_manager.py` 模块
   - 到期日监控
   - 展期触发 (T-5天)
   - 旧合约数据归档

2. 展期策略
   - 选择下一个主力合约
   - 通知下游系统
   - 更新订阅列表

3. 回滚机制
   - 展期失败告警
   - 手动回滚选项
```

#### Day 3-4: 数据血缘追踪

**任务 T-302: 数据血缘追踪**

```
目标: 记录数据采集的完整血缘关系

实现内容:
1. 创建 `data_manifest.py` 模块
   - Manifest生成
   - SHA256校验
   - 血缘关系记录

2. Manifest结构:
```json
{
    "manifest_version": "1.0",
    "generated_at": "2026-05-07T10:00:00Z",
    "files": [
        {
            "path": "BTC-28MAR26-80000-C/2026-05-07/tickers.parquet",
            "sha256": "abc123...",
            "record_count": 86400,
            "source": "deribit_api"
        }
    ],
    "collection_stats": {
        "total_records": 1000000,
        "gaps": [],
        "errors": []
    }
}
```

#### Day 5: 性能优化

**任务 T-303: 性能调优**

```
目标: 优化系统性能指标

优化内容:
1. 并发优化
   - 异步IO优化
   - 批量处理优化

2. 内存优化
   - 缓冲区大小调优
   - 减少内存碎片

3. 存储优化
   - Parquet压缩优化
   - 索引优化

目标KPI:
- 吞吐量: >500条/秒
- 采集延迟: <1秒
- 存储写入: <100ms
```

---

### Sprint 4: 上线准备 (5天)

#### Day 1-2: 完整测试

**任务 T-401: 端到端测试**

```
测试内容:
1. 15分钟连续采集测试
2. 故障恢复测试
3. 数据质量验证
4. 性能基准测试

验收标准:
- 无错误日志
- 记录数>12000条 (50合约 × 240次采集)
- Greeks有效率>95%
```

#### Day 3-4: 部署文档

**任务 T-402: 部署文档完善**

```
文档内容:
1. 部署手册
2. 运维手册
3. 故障排查指南
4. API文档
```

#### Day 5: 上线验收

**任务 T-403: 生产环境部署**

```
上线检查清单:
- [ ] 所有单元测试通过
- [ ] 集成测试通过
- [ ] Docker镜像构建成功
- [ ] 健康检查通过
- [ ] Prometheus指标正常
- [ ] 数据质量达标
```

---

## 四、里程碑时间线

```
Sprint 1 (5 days)                    Sprint 2 (5 days)
├── Day 1-2: Greeks 计算              ├── Day 1-2: 间隙检测
│   ├── Black-Scholes 模型实现        │   ├── 间隙检测器
│   ├── 波动率插值                   │   ├── 异常值过滤器
│   └── 单元测试                    │   └── 日志记录
├── Day 3-4: 利率曲线               ├── Day 3-4: 波动率曲面
│   ├── FRED API 对接               │   ├── ATM IV 计算
│   ├── 样条插值                    │   ├── Skew 计算
│   └── 连续复利转换                │   └── IV Rank 计算
└── Day 5: 主力映射 & 测试覆盖      └── Day 5: 数据验证
    ├── OI 排序                    └── 验证脚本
    └── 测试>=90%

Sprint 3 (5 days)                   Sprint 4 (5 days)
├── Day 1-2: 自动展期               ├── Day 1-2: 完整测试
│   ├── 到期监控                   │   ├── 端到端测试
│   ├── 展期触发                   │   └── 故障恢复测试
│   └── 归档机制                   ├── Day 3-4: 部署文档
├── Day 3-4: 数据血缘               └── Day 5: 上线验收
│   ├── Manifest 生成
│   └── SHA256 校验
└── Day 5: 性能调优
    └── 性能基准测试
```

---

## 五、技术架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           数据采集系统架构                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐    │
│  │   Deribit API    │     │   FRED API       │     │  其他数据源      │    │
│  │  (期权+永续)      │     │  (利率曲线)       │     │  (Binance/HL)   │    │
│  └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘    │
│           │                        │                        │               │
│           ▼                        ▼                        ▼               │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                         数据处理层 (Processing)                          ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ ││
│  │  │ Greeks计算器 │  │ 利率曲线获取 │  │ 主力合约识别 │  │ 波动率曲面   │ ││
│  │  │ (Black-Scholes│  │ (FRED API)   │  │ (OI排序)     │  │ (ATM IV)     │ ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ ││
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   ││
│  │  │ 间隙检测    │  │ 异常值过滤   │  │ 数据验证     │                   ││
│  │  │ (>60s)      │  │ (Z-Score>5)  │  │ (完整性)     │                   ││
│  │  └──────────────┘  └──────────────┘  └──────────────┘                   ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                          存储层 (Storage)                                ││
│  │  ┌─────────────────────┐              ┌─────────────────────┐          ││
│  │  │   Parquet           │              │   SQLite            │          ││
│  │  │   (原始数据归档)     │              │   (元数据/缓存)      │          ││
│  │  │   按日期/合约分区    │              │   主力合约映射       │          ││
│  │  └─────────────────────┘              └─────────────────────┘          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                          监控层 (Monitoring)                            ││
│  │  ┌─────────────────────┐  ┌─────────────────────┐  ┌────────────────┐  ││
│  │  │ Prometheus         │  │ Health Check        │  │ Alert Manager  │  ││
│  │  │ /metrics           │  │ /health, /ready     │  │ (PagerDuty)    │  ││
│  │  └─────────────────────┘  └─────────────────────┘  └────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 六、关键文件清单

### Sprint 1 新增文件

```
src/deribit_options_collector/
├── greeks_calculator.py      # Black-Scholes Greeks计算
├── risk_free_rate.py         # FRED API利率曲线
└── main_contract_identifier.py # 主力合约识别

tests/
├── test_greeks_calculator.py
├── test_risk_free_rate.py
└── test_main_contract_identifier.py
```

### Sprint 2 新增文件

```
src/deribit_options_collector/
├── gap_detector.py            # 数据间隙检测
├── outlier_filter.py         # 异常值过滤
├── volatility_surface.py     # 波动率曲面
└── validate_data.py          # 数据验证脚本

tests/
├── test_gap_detector.py
├── test_outlier_filter.py
└── test_volatility_surface.py
```

### Sprint 3 新增文件

```
src/deribit_options_collector/
├── rollover_manager.py       # 自动展期管理
└── data_manifest.py          # 数据血缘追踪
```

---

## 七、验收标准

### Sprint 1 验收

| 验收项 | 完成标准 | 测试方法 |
|--------|----------|----------|
| V1 | Greeks计算误差<1% | 与Deribit Greeks交叉验证 |
| V2 | 利率曲线可用 | 查询任意期限利率 |
| V3 | 主力映射准确率>95% | OI排序验证 |
| V4 | 测试覆盖率>=90% | pytest --cov |

### Sprint 2 验收

| 验收项 | 完成标准 | 测试方法 |
|--------|----------|----------|
| V5 | 间隙检测工作 | 日志记录间隙事件 |
| V6 | 异常过滤工作 | 标记/过滤异常数据 |
| V7 | 波动率曲面可用 | ATM IV计算正确 |

### 最终验收

| 验收项 | 完成标准 |
|--------|----------|
| M1 | BTC/ETH全量期权合约可采集 |
| M2 | Greeks有效率>=95% |
| M3 | 数据完整率>=99% |
| M4 | 系统可稳定运行24小时 |
| M5 | 所有P0问题已解决 |

---

## 八、风险与缓解措施

| 风险 | 描述 | 缓解措施 |
|------|------|----------|
| R1 | FRED API限流/不可用 | 本地缓存+固定利率备用 |
| R2 | Black-Scholes计算误差 | 与Deribit Greeks交叉验证 |
| R3 | WebSocket频道超限 | 优先级队列+分页订阅 |
| R4 | 数据质量不达标 | 每日报告+告警 |
| R5 | 主力识别逻辑不准确 | OI阈值动态调整 |

---

## 九、决策点

| 决策点 | 需要确认 |
|--------|----------|
| D1 | Greeks补算是启用还是可选? |
| D2 | 异常数据是标记还是过滤? |
| D3 | 波动率曲面存储频率? |
| D4 | 展期提前天数? |

---

*计划版本: v1.0*
*最后更新: 2026-05-07*
