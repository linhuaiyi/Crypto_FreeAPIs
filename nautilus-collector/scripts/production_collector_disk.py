#!/usr/bin/env python3
"""
生产环境数据采集服务（落盘版 disk edition）

Telegram 通道已退役（2026-08-20）：数据只落盘 /app/data/production/<exch>/<type>/，
按日合并归档由独立脚本 merge_daily.py（cron）完成，湖侧 lake_pull 定期拉取归档。

架构：每个交易所独立容器（避免 NautilusTrader 多 DataClient 互斥）。
     本文件 = 原版去掉 Telegram 发送链路的最小改动版。
"""
import signal
import logging
import os
import sys
from pathlib import Path
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 加载环境变量
from dotenv import load_dotenv
load_dotenv()

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.enums import BookType
from nautilus_trader.test_kit.strategies.tester_data import DataTester
from nautilus_trader.test_kit.strategies.tester_data import DataTesterConfig

from ares.data.writers import ParquetWriter

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


# ─── Exchange-specific Actor subclasses ──────────────────────────
# NautilusTrader 基于**类名**生成 Actor ID，所以每个交易所需要独立子类。

class BinanceDataActor(DataTester):
    """Binance 数据采集 Actor"""
    pass


class HyperliquidDataActor(DataTester):
    """Hyperliquid 数据采集 Actor"""
    pass


# ─── 共享的数据处理 mixin ────────────────────────────────────────

class ProductionDataActorMixin:
    """
    数据处理混入类 — Actor 数据回调的共用逻辑。
    子类需设置 self.writer 和 self.exchange_name。
    """
    writer: ParquetWriter
    exchange_name: str
    _stats: dict
    _start_time: float
    log: logging.Logger

    def _init_stats(self):
        self._stats = {
            "total_trades": 0,
            "total_bars": 0,
            "total_quotes": 0,
            "total_order_book_updates": 0,
        }
        self._start_time = time.time()
        logger.info(f"[{self.exchange_name}] Buffer size: {self.writer._buffer_size}")

    def on_trade_tick(self, tick):
        try:
            symbol = tick.instrument_id.symbol.value
            timestamp_ms = tick.ts_event // 1_000_000

            # TradeTick 用 aggressor_side（BUYER/SELLER）
            # NautilusTrader Cython 枚举用 .name 获取字符串，str() 返回整数
            try:
                side = tick.aggressor_side.name
            except AttributeError:
                side = str(tick.aggressor_side)
            trade_id = str(tick.trade_id) if hasattr(tick, 'trade_id') else ""

            record = {
                "timestamp": timestamp_ms,
                "price": float(tick.price),
                "size": float(tick.size),
                "side": side,
                "trade_id": trade_id,
                "symbol": symbol,
            }

            buffer_key = "trades_1m"
            self.writer.buffers[buffer_key].append(record)

            if len(self.writer.buffers[buffer_key]) >= self.writer._buffer_size:
                self.writer._flush_buffer(buffer_key, "trades", "1m")

            self._stats["total_trades"] += 1

        except Exception as e:
            self.log.error(f"[{self.exchange_name}] 处理 trade tick 失败: {e}")

    def on_bar(self, bar):
        try:
            if hasattr(bar, 'bar_type'):
                symbol = bar.bar_type.instrument_id.symbol.value
            else:
                return

            timestamp_ms = bar.ts_event // 1_000_000

            record = {
                "timestamp": timestamp_ms,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
                "symbol": symbol,
            }

            # 只存 1m 窗口（NautilusTrader 订阅的就是 1-MINUTE bar）
            buffer_key = "ohlcv_1m"
            self.writer.buffers[buffer_key].append(record)

            if len(self.writer.buffers[buffer_key]) >= self.writer._buffer_size:
                self.writer._flush_buffer(buffer_key, "ohlcv", "1m")

            self._stats["total_bars"] += 1

        except Exception as e:
            self.log.error(f"[{self.exchange_name}] 处理 bar 失败: {e}")

    def on_quote_tick(self, tick):
        pass

    def on_order_book_deltas(self, order_book_deltas):
        try:
            symbol = order_book_deltas.instrument_id.symbol.value
            timestamp_ms = order_book_deltas.ts_event // 1_000_000

            # 遍历所有 delta（每个 delta 对应一档变更）
            deltas = order_book_deltas.deltas if order_book_deltas.deltas else []
            for idx, delta in enumerate(deltas):
                if not hasattr(delta, 'order') or delta.order is None:
                    continue

                order = delta.order
                # NautilusTrader 枚举用 .name 获取字符串
                try:
                    side = order.side.name
                except AttributeError:
                    side = str(order.side)
                price = float(order.price) if hasattr(order, 'price') and order.price else 0.0
                size = float(order.size) if hasattr(order, 'size') and order.size else 0.0

                record = {
                    "timestamp": timestamp_ms,
                    "price": price,
                    "size": size,
                    "side": side,
                    "level": idx + 1,
                    "symbol": symbol,
                }

                buffer_key = "l2_book_1m"
                self.writer.buffers[buffer_key].append(record)

            # 批量检查 flush（而非逐条检查）
            buffer_key = "l2_book_1m"
            if len(self.writer.buffers[buffer_key]) >= self.writer._buffer_size:
                self.writer._flush_buffer(buffer_key, "l2_book", "1m")

            if "total_order_book_updates" not in self._stats:
                self._stats["total_order_book_updates"] = 0
            self._stats["total_order_book_updates"] += 1

        except Exception as e:
            self.log.error(f"[{self.exchange_name}] 处理 order book deltas 失败: {e}")

    def get_statistics(self) -> dict:
        uptime = time.time() - self._start_time
        stats = self._stats.copy()
        stats['uptime_hours'] = uptime / 3600
        return stats


# ─── 具体 Actor（Mixin + 子类 多重继承）─────────────────────────

class BinanceActor(ProductionDataActorMixin, BinanceDataActor):
    def __init__(self, config, writer, exchange_name):
        super().__init__(config)
        self.writer = writer
        self.exchange_name = exchange_name
        self._init_stats()


class HyperliquidActor(ProductionDataActorMixin, HyperliquidDataActor):
    def __init__(self, config, writer, exchange_name):
        super().__init__(config)
        self.writer = writer
        self.exchange_name = exchange_name
        self._init_stats()


# ─── 单交易所采集服务 ────────────────────────────────────────────

class SingleExchangeCollector:
    """
    单交易所数据采集器。

    每个实例连接一个交易所，运行在独立进程中（Docker 容器）。
    通过 EXCHANGE 环境变量选择交易所。
    """

    MAX_FILE_SIZE = 50 * 1024 * 1024
    # 发送间隔（分钟）：默认 240（4小时），可通过 SEND_INTERVAL_MINUTES 环境变量覆盖
    SEND_INTERVAL_MINUTES = float(os.getenv("SEND_INTERVAL_MINUTES", "240"))

    EXCHANGE_CONFIGS = {
        "binance": {
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "instrument_id_fmt": "{}.BINANCE",
            "trader_id": "PROD-BINANCE-001",
        },
        "hyperliquid": {
            "symbols": ["BTC-USD-PERP", "ETH-USD-PERP"],
            "instrument_id_fmt": "{}.HYPERLIQUID",
            "trader_id": "PROD-HYPERLIQUID-001",
        },
    }

    def __init__(self):
        self.exchange = os.getenv("EXCHANGE", "binance").lower()
        if self.exchange not in self.EXCHANGE_CONFIGS:
            raise ValueError(f"Unknown exchange: {self.exchange}. Use 'binance' or 'hyperliquid'.")

        data_path = os.getenv("DATA_PATH", "./data/production")
        data_path = os.path.expandvars(data_path)
        data_path = os.path.expanduser(data_path)
        self.base_dir = Path(data_path)
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # disk edition: Telegram 已退役
        self.node = None
        self.actor = None
        self.writer = None
        self.running = False
        self.last_send_time = None

    def setup(self):
        cfg = self.EXCHANGE_CONFIGS[self.exchange]
        logger.info(f"[{self.exchange}] 初始化单交易所采集器...")

        # 输出目录
        exchange_dir = self.base_dir / self.exchange
        exchange_dir.mkdir(parents=True, exist_ok=True)

        self.writer = ParquetWriter(
            exchange=self.exchange,
            base_path=exchange_dir,
            buffer_size=50000,
        )

        # Instrument IDs
        instrument_ids = [
            InstrumentId.from_str(cfg["instrument_id_fmt"].format(s))
            for s in cfg["symbols"]
        ]
        bar_types = [
            BarType.from_str(f"{iid}-1-MINUTE-LAST-EXTERNAL")
            for iid in instrument_ids
        ]

        # TradingNode 配置（单一 DataClient）
        data_clients = {}
        data_client_factories = {}

        if self.exchange == "binance":
            from nautilus_trader.adapters.binance import BINANCE
            from nautilus_trader.adapters.binance import BinanceAccountType
            from nautilus_trader.adapters.binance import BinanceDataClientConfig
            from nautilus_trader.adapters.binance import BinanceLiveDataClientFactory

            data_clients[BINANCE] = BinanceDataClientConfig(
                api_key=None,
                api_secret=None,
                account_type=BinanceAccountType.SPOT,
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset(instrument_ids)
                ),
            )
            data_client_factories[BINANCE] = BinanceLiveDataClientFactory
            ActorClass = BinanceActor

        elif self.exchange == "hyperliquid":
            from nautilus_trader.adapters.hyperliquid import HYPERLIQUID
            from nautilus_trader.adapters.hyperliquid import HyperliquidDataClientConfig
            from nautilus_trader.adapters.hyperliquid import HyperliquidLiveDataClientFactory
            from nautilus_trader.adapters.hyperliquid.enums import HyperliquidProductType

            data_clients[HYPERLIQUID] = HyperliquidDataClientConfig(
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset(instrument_ids)
                ),
                product_types=(HyperliquidProductType.PERP,),
            )
            data_client_factories[HYPERLIQUID] = HyperliquidLiveDataClientFactory
            ActorClass = HyperliquidActor

        config_node = TradingNodeConfig(
            trader_id=TraderId(cfg["trader_id"]),
            logging=LoggingConfig(log_level="INFO", use_pyo3=True),
            data_clients=data_clients,
            timeout_connection=20.0,
            timeout_disconnection=10.0,
            timeout_post_stop=1.0,
        )

        self.node = TradingNode(config=config_node)

        # Actor
        actor_config = DataTesterConfig(
            instrument_ids=instrument_ids,
            bar_types=bar_types,
            subscribe_instrument=True,
            subscribe_book_at_interval=False,
            subscribe_book_deltas=True,
            book_type=BookType.L2_MBP,
            subscribe_quotes=False,
            subscribe_trades=True,
            subscribe_bars=True,
        )

        self.actor = ActorClass(
            config=actor_config,
            writer=self.writer,
            exchange_name=self.exchange.capitalize(),
        )
        self.node.trader.add_actor(self.actor)

        # 注册 factory 并 build
        for name, factory in data_client_factories.items():
            self.node.add_data_client_factory(name, factory)
        self.node.build()

        logger.info(f"[{self.exchange}] ✅ TradingNode 构建完成")

    # 合并块大小上限（Telegram 50MB 限制，留余量）
    MAX_MERGE_CHUNK_SIZE = 45 * 1024 * 1024

    def check_disk_space(self):
        """磁盘水位检查（纯日志版，无 Telegram 告警）。"""
        try:
            import shutil
            total, used, free = shutil.disk_usage(self.base_dir)
            logger.info(f"[Disk] 剩余: {free / 1024 / 1024 / 1024:.2f}GB")
        except Exception as e:
            logger.error(f"[Disk] 检查失败: {e}")

    def run(self):
        try:
            logger.info("=" * 60)
            logger.info(f"数据采集服务启动 — {self.exchange.upper()}")
            logger.info("=" * 60)

            self.setup()
            logger.info(f"📊 交易对: {', '.join(self.EXCHANGE_CONFIGS[self.exchange]['symbols'])}")

            # 后台线程启动 TradingNode
            def run_node():
                self.node.run()

            node_thread = threading.Thread(target=run_node, daemon=True)
            node_thread.start()
            time.sleep(3)

            self.running = True
            self.last_flush_time = time.time()

            logger.info(f"✅ {self.exchange.upper()} 数据采集服务已启动（disk edition）")
            logger.info(f"📂 数据目录: {self.base_dir}")

            n = 0
            while self.running:
                try:
                    time.sleep(30)
                    n += 1
                    # 周期刷盘（ohlcv 等低流量 buffer 到不了行数阈值，
                    # 必须定时 flush；否则数据滞留内存，重启即丢）
                    if time.time() - self.last_flush_time >= 1800:
                        logger.info("[Flush] 周期性 flush_all()")
                        self.writer.flush_all()
                        self.last_flush_time = time.time()
                    if n % 120 == 0:  # 每小时心跳+磁盘检查
                        logger.info("[Heartbeat] running, disk check follows")
                        self.check_disk_space()
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"[Main] 主循环错误: {e}")

        except Exception as e:
            logger.error(f"[Main] 服务启动失败: {e}")
        finally:
            self.stop()

    def stop(self):
        if not self.running:
            return
        logger.info("正在停止...")
        self.running = False
        if self.node:
            self.node.stop()
        for thread in threading.enumerate():
            if thread.is_alive() and thread != threading.current_thread():
                thread.join(timeout=5)
        if self.writer:
            self.writer.flush_all()
        if self.node:
            self.node.dispose()
        logger.info("✅ 服务已停止")


def signal_handler(sig, frame):
    logger.info("\n收到停止信号")
    if service:
        service.stop()
    sys.exit(0)


if __name__ == "__main__":
    service = None
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        service = SingleExchangeCollector()
        service.run()
    except Exception as e:
        logger.error(f"服务异常退出: {e}")
        sys.exit(1)
