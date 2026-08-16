import argparse
import json
import time
import yaml
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)

from fetchers import (
    BinanceSpotFetcher,
    BinanceUSDMFetcher,
    DeribitFetcher,
    HyperliquidFetcher,
)
from storage import ParquetStore
from utils import RateLimiter, get_logger

_SUBPROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = get_logger("Runner", "INFO")

# gapfill 内部空洞检测用的周期毫秒数; 1M 为不规则周期不参与
TIMEFRAME_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000, "1w": 604_800_000,
}

# 每个文件最多修补的空洞数, 防止对零成交量缺桶反复无效请求
MAX_GAPS_PER_FILE = 50

# gapfill 挂起状态: 超出单轮上限的空洞记入 state/, 下轮优先处理,
# 避免小空洞因"按大小排序"永远进不了 top-N 而永久饥饿
_STATE_DIR = os.path.join(_SUBPROJECT_DIR, "state")
_GAPFILL_STATE_FILE = os.path.join(_STATE_DIR, "gapfill_pending.json")


def _load_gapfill_state() -> dict:
    try:
        with open(_GAPFILL_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_gapfill_state(state: dict) -> None:
    os.makedirs(_STATE_DIR, exist_ok=True)
    tmp = _GAPFILL_STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, _GAPFILL_STATE_FILE)


class DataPipeline:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(_SUBPROJECT_DIR, "config.yaml")
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
        target_start_ms = now_ms - days * 86400 * 1000

        logger.info(f"=== 历史回填模式 ===")
        logger.info(f"目标范围: {days} 天 ({datetime.fromtimestamp(target_start_ms/1000)} -> {datetime.fromtimestamp(now_ms/1000)})")
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
                    first_ts = self.store.get_first_timestamp(exchange_name, unified_symbol, tf)
                    last_ts = self.store.get_last_timestamp(exchange_name, unified_symbol, tf)

                    ranges_to_fetch = []
                    if first_ts is None and last_ts is None:
                        ranges_to_fetch.append((target_start_ms, now_ms))
                        logger.info(f"  {unified_symbol}: 无数据，采集全量")
                    elif first_ts is None:
                        ranges_to_fetch.append((target_start_ms, last_ts - 1))
                        logger.info(f"  {unified_symbol}: 头部缺失，采集至 {datetime.fromtimestamp(last_ts/1000).date()}")
                    elif last_ts is None:
                        ranges_to_fetch.append((first_ts + 86400 * 1000, now_ms))
                        logger.info(f"  {unified_symbol}: 尾部缺失，采集至 {datetime.fromtimestamp(first_ts/1000).date()}")
                    else:
                        if target_start_ms < first_ts:
                            ranges_to_fetch.append((target_start_ms, first_ts - 1))
                            logger.info(f"  {unified_symbol}: 头部补全 {datetime.fromtimestamp(target_start_ms/1000).date()} ~ {datetime.fromtimestamp(first_ts/1000).date()}")
                        if last_ts < now_ms:
                            # 尾部从下一根 K 线开始, 固定 +1 天会让分钟/小时级
                            # 每次回填漏掉 last_ts 之后约 24h 的数据
                            if tf == "1M":
                                # 月线周期不规则: 从最后一桶起点整月重拉基础
                                # 周期重采样, keep='last' 覆盖旧行; 若从
                                # last_ts+1 天开始会漏掉月初基础 K 线, 用残缺
                                # 桶覆盖原本正确的整月行
                                tail_start = last_ts
                            else:
                                tail_start = last_ts + TIMEFRAME_MS.get(tf, 86400 * 1000)
                            ranges_to_fetch.append((tail_start, now_ms))
                            logger.info(f"  {unified_symbol}: 尾部补全 {datetime.fromtimestamp(last_ts/1000).date()} ~ {datetime.fromtimestamp(now_ms/1000).date()}")

                    session_added = 0
                    for start_ms, end_ms in ranges_to_fetch:
                        added = self._fetch_symbol(exchange_name, unified_symbol, tf, start_ms, end_ms)
                        session_added += added

                    total_added += session_added

                    stats = self.store.get_stats(exchange_name, unified_symbol, tf)
                    if stats['exists']:
                        logger.info(f"    结果: {stats['count']} 条 [{datetime.fromtimestamp(stats['start_time']/1000).date()} ~ {datetime.fromtimestamp(stats['end_time']/1000).date()}]")

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
                        # 月线从最后一桶起点重拉整月, 其余从下一根开始 (同回填)
                        if tf == "1M":
                            fetch_start = last_ts
                        else:
                            fetch_start = last_ts + TIMEFRAME_MS.get(tf, 86400 * 1000)
                        if fetch_start >= now_ms:
                            logger.info(f"[{exchange_name}] {unified_symbol}: 数据已是最新，跳过")
                            continue
                    else:
                        fetch_start = start_ms

                    added = self._fetch_symbol(exchange_name, unified_symbol, tf, fetch_start, now_ms)
                    total_added += added

        logger.info(f"\n=== 增量更新完成 ===")
        logger.info(f"总计新增: {total_added} 条记录")

    def run_gapfill(self, timeframes: list[str] = None, exchanges: list[str] = None):
        """检测已有文件头尾之间的内部空洞并补拉(回填只处理头尾缺口)。

        超出 MAX_GAPS_PER_FILE 的空洞挂起到 state/gapfill_pending.json,
        下一轮优先处理; 已尝试但补不上的(零成交缺桶)视为耗尽不再挂起,
        保证每处空洞最终都会被尝试至少一次, 不会永久饥饿。
        """
        if timeframes is None:
            timeframes = [self.default_timeframe]

        logger.info(f"=== 内部空洞修补模式 ===")
        logger.info(f"周期: {timeframes}")
        if exchanges:
            logger.info(f"仅处理交易所: {exchanges}")

        pending_state = _load_gapfill_state()
        state_dirty = False
        total_added = 0
        for tf in timeframes:
            interval_ms = TIMEFRAME_MS.get(tf)
            if interval_ms is None:
                logger.info(f"周期 {tf}: 不规则周期, 跳过空洞检测")
                continue

            for exchange_name, fetcher in self.fetchers.items():
                if exchanges and exchange_name not in exchanges:
                    continue
                for unified_symbol in fetcher.get_symbol_mapping().keys():
                    ts_list = self.store.get_timestamps(exchange_name, unified_symbol, tf)
                    if len(ts_list) < 2:
                        continue

                    gaps = [
                        (prev + interval_ms, curr - 1)
                        for prev, curr in zip(ts_list, ts_list[1:])
                        if curr - prev > interval_ms
                    ]
                    key = f"{exchange_name}|{unified_symbol}|{tf}"
                    gap_set = set(gaps)
                    prev_pending = [tuple(g) for g in pending_state.get(key, [])]

                    # 上轮挂起且仍存在的空洞优先; 已被填上的自动淘汰
                    deferred = [g for g in prev_pending if g in gap_set]
                    fresh = [g for g in gaps if g not in set(deferred)]
                    deferred.sort(key=lambda g: g[1] - g[0], reverse=True)
                    fresh.sort(key=lambda g: g[1] - g[0], reverse=True)

                    if deferred:
                        logger.info(
                            f"[{exchange_name}] {unified_symbol} ({tf}): "
                            f"优先处理上轮挂起的 {len(deferred)} 处空洞"
                        )

                    # 尝试集 = 挂起优先, 再按大小补足
                    attempt = (deferred + fresh)[:MAX_GAPS_PER_FILE]
                    if not attempt:
                        if key in pending_state:
                            pending_state.pop(key)
                            state_dirty = True
                        continue

                    logger.info(f"[{exchange_name}] {unified_symbol} ({tf}): 修补 {len(attempt)} 处内部空洞")
                    for start_ms, end_ms in attempt:
                        added = self._fetch_symbol(exchange_name, unified_symbol, tf, start_ms, end_ms)
                        total_added += added

                    # 未尝试的挂起到下一轮; 尝试过仍缺的不再挂起(零成交缺桶)
                    attempted_set = set(attempt)
                    not_attempted = [g for g in gaps if g not in attempted_set]
                    if not_attempted:
                        pending_state[key] = [list(g) for g in not_attempted]
                        state_dirty = True
                        logger.info(
                            f"    {len(not_attempted)} 处空洞挂起至下轮 (state/gapfill_pending.json)"
                        )
                    elif key in pending_state:
                        pending_state.pop(key)
                        state_dirty = True

        if state_dirty:
            _save_gapfill_state(pending_state)

        logger.info(f"\n=== 空洞修补完成 ===")
        logger.info(f"总计新增: {total_added} 条记录")


def main():
    parser = argparse.ArgumentParser(description='OHLCV 多级别 K 线数据采集管线')
    parser.add_argument(
        '--mode',
        choices=['backfill', 'daily', 'single', 'gapfill'],
        default='daily',
        help='运行模式: backfill=历史回填, daily=每日增量, single=单标的测试, gapfill=内部空洞修补'
    )
    parser.add_argument('--exchange', help='交易所名称 (single模式必需)')
    parser.add_argument('--symbol', help='标的符号 (single模式必需)')
    parser.add_argument('--days', type=int, default=365, help='回填天数')
    parser.add_argument('--timeframe', help='K线周期 (单个)')
    parser.add_argument('--timeframes', help='K线周期 (多个，用逗号分隔，如 1m,15m,30m,1h,4h,1d,1w,1M)')
    parser.add_argument('--config', default=None, help='配置文件路径 (默认: ohlcv-collector/config.yaml)')

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
    elif args.mode == 'gapfill':
        pipeline.run_gapfill(timeframes, args.exchange.split(',') if args.exchange else None)


if __name__ == '__main__':
    main()
