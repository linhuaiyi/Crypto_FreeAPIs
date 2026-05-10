import argparse
import time
import yaml
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fetchers import (
    BinanceSpotFetcher,
    BinanceUSDMFetcher,
    DeribitFetcher,
    HyperliquidFetcher,
)
from storage import ParquetStore
from utils import RateLimiter, get_logger


logger = get_logger("Runner", "INFO")


class DataPipeline:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.data_dir = self.config.get('global', {}).get('data_dir', './data')
        self.default_timeframe = self.config.get('global', {}).get('timeframe', '1d')
        self.default_days = self.config.get('global', {}).get('default_history_days', 365)
        self.max_retries = self.config.get('global', {}).get('max_retries', 3)
        self.retry_delay = self.config.get('global', {}).get('retry_delay_seconds', 2)

        self.store = ParquetStore(self.data_dir)
        self.fetchers: Dict[str, object] = {}
        self._init_fetchers()

    def _init_fetchers(self):
        exch_cfg = self.config.get('exchanges', {})

        if exch_cfg.get('binance_spot', {}).get('enabled', False):
            cfg = exch_cfg['binance_spot']
            rpm = cfg['rate_limit']['requests_per_minute']
            self.fetchers['binance_spot'] = BinanceSpotFetcher(
                cfg, RateLimiter(rpm, 'BinanceSpot')
            )
            logger.info(f"Binance Spot 采集器已初始化 (限速: {rpm} req/min)")

        if exch_cfg.get('binance_usdm', {}).get('enabled', False):
            cfg = exch_cfg['binance_usdm']
            rpm = cfg['rate_limit']['requests_per_minute']
            self.fetchers['binance_usdm'] = BinanceUSDMFetcher(
                cfg, RateLimiter(rpm, 'BinanceUSDM')
            )
            logger.info(f"Binance USDT-M 采集器已初始化 (限速: {rpm} req/min)")

        if exch_cfg.get('deribit', {}).get('enabled', False):
            cfg = exch_cfg['deribit']
            rps = cfg['rate_limit']['requests_per_second']
            self.fetchers['deribit'] = DeribitFetcher(
                cfg, RateLimiter(rps * 60, 'Deribit')
            )
            logger.info(f"Deribit 采集器已初始化 (限速: {rps} req/s)")

        if exch_cfg.get('hyperliquid', {}).get('enabled', False):
            cfg = exch_cfg['hyperliquid']
            rpm = cfg['rate_limit']['requests_per_minute']
            self.fetchers['hyperliquid'] = HyperliquidFetcher(
                cfg, RateLimiter(rpm, 'Hyperliquid')
            )
            logger.info(f"Hyperliquid 采集器已初始化 (限速: {rpm} req/min)")

    def _fetch_symbol(
        self,
        exchange_name: str,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> int:
        fetcher = self.fetchers.get(exchange_name)
        if not fetcher:
            logger.warning(f"未知交易所: {exchange_name}")
            return 0

        exchange_symbol = symbol
        mapping = fetcher.get_symbol_mapping()
        for unified, ex_sym in mapping.items():
            if unified == symbol:
                exchange_symbol = ex_sym
                break

        try:
            records = fetcher.fetch_with_backoff(exchange_symbol, timeframe, start_ts, end_ts)
            if records:
                added = self.store.save(exchange_name, symbol, timeframe, records)
                return added
            else:
                logger.info(f"[{exchange_name}] {symbol}: 无新数据")
                return 0
        except Exception as e:
            logger.error(f"[{exchange_name}] {symbol}: 采集失败 - {e}")
            return 0

    def run_single(
        self,
        exchange_name: str,
        symbol: str,
        days: int,
        timeframe: Optional[str] = None,
    ):
        tf = timeframe or self.default_timeframe
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days * 86400 * 1000

        logger.info(f"=== 单标的测试模式 ===")
        logger.info(f"交易所: {exchange_name}, 标的: {symbol}, 时间范围: {days} 天, 周期: {tf}")

        added = self._fetch_symbol(exchange_name, symbol, tf, start_ms, now_ms)
        logger.info(f"完成。新增 {added} 条记录。")

    def run_backfill(self, days: int, timeframes: list[str] = None):
        if timeframes is None:
            timeframes = [self.default_timeframe]

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days * 86400 * 1000

        logger.info(f"=== 历史回填模式 ===")
        logger.info(f"时间范围: {days} 天 ({datetime.fromtimestamp(start_ms/1000)} -> {datetime.fromtimestamp(now_ms/1000)})")
        logger.info(f"周期: {timeframes}")

        total_added = 0
        for tf in timeframes:
            logger.info(f"\n{'='*50}")
            logger.info(f"采集周期: {tf}")
            logger.info(f"{'='*50}")

            for exchange_name, fetcher in self.fetchers.items():
                mapping = fetcher.get_symbol_mapping()
                logger.info(f"\n--- {exchange_name} ({len(mapping)} 个标的) ---")

                for unified_symbol in mapping.keys():
                    last_ts = self.store.get_last_timestamp(exchange_name, unified_symbol, tf)
                    if last_ts:
                        fetch_start = last_ts + 86400 * 1000
                        logger.info(f"  {unified_symbol}: 已有数据，从 {datetime.fromtimestamp(fetch_start/1000)} 继续")
                    else:
                        fetch_start = start_ms
                        logger.info(f"  {unified_symbol}: 无数据，从 {datetime.fromtimestamp(fetch_start/1000)} 开始")

                    added = self._fetch_symbol(exchange_name, unified_symbol, tf, fetch_start, now_ms)
                    total_added += added

                    stats = self.store.get_stats(exchange_name, unified_symbol, tf)
                    if stats['exists']:
                        logger.info(f"  {unified_symbol}: 共 {stats['count']} 条 [{datetime.fromtimestamp(stats['start_time']/1000).date()} ~ {datetime.fromtimestamp(stats['end_time']/1000).date()}]")

        logger.info(f"\n=== 回填完成 ===")
        logger.info(f"总计新增: {total_added} 条记录")

    def run_daily(self, timeframes: list[str] = None):
        if timeframes is None:
            timeframes = [self.default_timeframe]

        now_ms = int(time.time() * 1000)
        day_ms = 86400 * 1000
        start_ms = now_ms - day_ms

        logger.info(f"=== 每日增量更新模式 ===")
        logger.info(f"时间范围: {datetime.fromtimestamp(start_ms/1000)} -> {datetime.fromtimestamp(now_ms/1000)}")
        logger.info(f"周期: {timeframes}")

        total_added = 0
        for tf in timeframes:
            logger.info(f"\n--- 周期: {tf} ---")

            for exchange_name, fetcher in self.fetchers.items():
                mapping = fetcher.get_symbol_mapping()
                for unified_symbol in mapping.keys():
                    last_ts = self.store.get_last_timestamp(exchange_name, unified_symbol, tf)
                    if last_ts:
                        fetch_start = last_ts + 86400 * 1000
                        if fetch_start >= now_ms:
                            logger.info(f"[{exchange_name}] {unified_symbol}: 数据已是最新，跳过")
                            continue
                    else:
                        fetch_start = start_ms

                    added = self._fetch_symbol(exchange_name, unified_symbol, tf, fetch_start, now_ms)
                    total_added += added

        logger.info(f"\n=== 增量更新完成 ===")
        logger.info(f"总计新增: {total_added} 条记录")


def main():
    parser = argparse.ArgumentParser(description='加密货币 OHLCV 数据采集管线（仅现货/期货）')
    parser.add_argument(
        '--mode',
        choices=['backfill', 'daily', 'single'],
        default='daily',
        help='运行模式: backfill=历史回填, daily=每日增量, single=单标的测试'
    )
    parser.add_argument('--exchange', help='交易所名称 (single模式必需)')
    parser.add_argument('--symbol', help='标的符号 (single模式必需)')
    parser.add_argument('--days', type=int, default=365, help='回填天数')
    parser.add_argument('--timeframe', help='K线周期 (单个)')
    parser.add_argument('--timeframes', help='K线周期 (多个，用逗号分隔，如 1m,15m,30m,1h,4h,1d,1w,1M)')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')

    args = parser.parse_args()

    timeframes = None
    if args.timeframes:
        timeframes = [tf.strip() for tf in args.timeframes.split(',')]
    elif args.timeframe:
        timeframes = [args.timeframe]

    pipeline = DataPipeline(config_path=args.config)

    if args.mode == 'single':
        if not args.exchange or not args.symbol:
            parser.error("single 模式需要 --exchange 和 --symbol 参数")
        tf = args.timeframe or args.timeframes or '1d'
        pipeline.run_single(args.exchange, args.symbol, args.days, tf.split(',')[0] if ',' in str(tf) else tf)
    elif args.mode == 'backfill':
        pipeline.run_backfill(args.days, timeframes)
    elif args.mode == 'daily':
        pipeline.run_daily(timeframes)


if __name__ == '__main__':
    main()
