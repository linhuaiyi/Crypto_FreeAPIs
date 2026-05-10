"""
Deribit Options独立采集脚本

功能：
- 期权链解析和筛选
- 到期日计算
- OHLCV数据采集
- 希腊值计算（Greeks: delta, gamma, vega, theta, rho）
- 波动率曲面生成
- 数据存储（Parquet格式）

使用方法：
    python options_collector.py --date 2026-05-06 --symbol BTC --expiry 30 --output-path ./data
    python options_collector.py --date 2026-05-06 --symbol ETH --expiry 7 --timeframe 1d
    python options_collector.py --mode daily --config config_options.yaml

作者: Deribit Options Collector
日期: 2026-05-06
"""

import argparse
import time
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import logging
import hashlib
from pathlib import Path

import requests
import pandas as pd
import numpy as np

try:
    from scipy.stats import norm
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    norm = None


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('OptionsCollector')


DERIBIT_RESOLUTION_MAP = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "1d": "1D",
    "1w": "1W", "1M": "1M",
}


@dataclass
class OptionContract:
    instrument_name: str
    base_currency: str
    expiry_timestamp: int
    strike: float
    option_type: str
    is_active: bool
    tick_size: float
    contract_size: float


@dataclass
class OptionOHLCV:
    timestamp: int
    instrument_name: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    underlying_price: float
    mark_price: float
    bid_price: float
    ask_price: float
    open_interest: float
    delta: Optional[float] = None
    gamma: Optional[float] = None
    vega: Optional[float] = None
    theta: Optional[float] = None
    rho: Optional[float] = None
    implied_volatility: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp,
            'instrument_name': self.instrument_name,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'underlying_price': self.underlying_price,
            'mark_price': self.mark_price,
            'bid_price': self.bid_price,
            'ask_price': self.ask_price,
            'open_interest': self.open_interest,
            'delta': self.delta,
            'gamma': self.gamma,
            'vega': self.vega,
            'theta': self.theta,
            'rho': self.rho,
            'implied_volatility': self.implied_volatility,
        }


class VolatilityCalculator:
    def __init__(self, risk_free_rate: float = 0.05):
        self.risk_free_rate = risk_free_rate

    def black_scholes_call(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        if not SCIPY_AVAILABLE or T <= 0 or sigma <= 0:
            return 0.0
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)

    def black_scholes_put(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        if not SCIPY_AVAILABLE or T <= 0 or sigma <= 0:
            return 0.0
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    def calculate_greeks(self, S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> Dict[str, float]:
        if not SCIPY_AVAILABLE or T <= 0 or sigma <= 0:
            return {'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0, 'rho': 0.0}

        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)

        if option_type == 'call':
            delta = norm.cdf(d1)
            rho = K * T * math.exp(-r * T) * norm.cdf(d2) / 100
        else:
            delta = norm.cdf(d1) - 1
            rho = -K * T * math.exp(-r * T) * norm.cdf(-d2) / 100

        gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
        vega = S * norm.pdf(d1) * math.sqrt(T) / 100
        theta = (-S * norm.pdf(d1) * sigma / (2 * math.sqrt(T))
                 - r * K * math.exp(-r * T) * (norm.cdf(d2) if option_type == 'call' else norm.cdf(-d2))) / 365

        return {
            'delta': delta,
            'gamma': gamma,
            'vega': vega,
            'theta': theta,
            'rho': rho
        }

    def calculate_implied_volatility(self, market_price: float, S: float, K: float,
                                    T: float, r: float, option_type: str,
                                    tol: float = 1e-6, max_iter: int = 100) -> float:
        if not SCIPY_AVAILABLE or market_price <= 0:
            return 0.0

        sigma = 0.5
        for _ in range(max_iter):
            if option_type == 'call':
                price = self.black_scholes_call(S, K, T, r, sigma)
            else:
                price = self.black_scholes_put(S, K, T, r, sigma)

            diff = market_price - price
            if abs(diff) < tol:
                return sigma

            d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
            vega = S * norm.pdf(d1) * math.sqrt(T)
            if abs(vega) < 1e-10:
                break
            sigma += diff / vega

        return sigma


class DeribitOptionsAPI:
    def __init__(self, base_url: str = "https://www.deribit.com/api/v2",
                 rate_limit: int = 15, max_retries: int = 3, retry_delay: float = 2.0):
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.last_request_time = 0.0
        self.vol_calc = VolatilityCalculator()

    def _rate_limit_wait(self):
        elapsed = time.time() - self.last_request_time
        min_interval = 1.0 / self.rate_limit if self.rate_limit > 0 else 0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()

    def _request_with_retry(self, method: str, endpoint: str, params: dict = None) -> dict:
        for attempt in range(self.max_retries):
            try:
                self._rate_limit_wait()

                url = f"{self.base_url}/{endpoint}"
                response = self.session.request(method, url, params=params, timeout=30)

                if response.status_code == 429:
                    logger.warning("触发限流，等待5秒...")
                    time.sleep(5)
                    continue

                if response.status_code == 400:
                    error_data = response.json()
                    error_msg = error_data.get('error', {}).get('message', 'Unknown error')
                    logger.warning(f"请求参数错误: {error_msg}")
                    return {}

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise

        return {}

    def get_instruments(self, currency: str, kind: str = 'option', expired: str = 'false') -> List[Dict]:
        params = {
            'currency': currency,
            'kind': kind,
            'expired': expired
        }
        result = self._request_with_retry('GET', 'public/get_instruments', params)
        if 'result' in result:
            return result['result']
        return []

    def get_tradingview_chart_data(self, instrument_name: str, start_ts: int,
                                   end_ts: int, resolution: str = '1D') -> Dict:
        params = {
            'instrument_name': instrument_name,
            'start_timestamp': start_ts,
            'end_timestamp': end_ts,
            'resolution': resolution
        }
        return self._request_with_retry('GET', 'public/get_tradingview_chart_data', params)

    def get_book_summary_by_instrument(self, instrument_name: str) -> Dict:
        params = {'instrument_name': instrument_name}
        return self._request_with_retry('GET', 'public/get_book_summary_by_instrument', params)

    def get_ticker(self, instrument_name: str) -> Dict:
        params = {'instrument_name': instrument_name}
        return self._request_with_retry('GET', 'public/ticker', params)


class OptionsChainParser:
    def __init__(self):
        self.expiry_formats = {
            'weekly': '%d%b%y',
            'monthly': '%d%b%y',
        }

    def parse_instrument_name(self, instrument_name: str) -> Optional[Dict]:
        parts = instrument_name.split('-')
        if len(parts) < 4:
            return None

        try:
            base_currency = parts[0]
            expiry_str = parts[1]
            strike = float(parts[2])
            option_type = parts[3]

            expiry_date = datetime.strptime(expiry_str, '%d%b%y')
            expiry_timestamp = int(expiry_date.timestamp() * 1000)

            return {
                'base_currency': base_currency,
                'expiry_str': expiry_str,
                'expiry_date': expiry_date,
                'expiry_timestamp': expiry_timestamp,
                'strike': strike,
                'option_type': option_type,
                'instrument_name': instrument_name
            }
        except (ValueError, IndexError):
            return None

    def filter_by_expiry(self, instruments: List[Dict], days: int) -> List[Dict]:
        now = datetime.now()
        cutoff = now + timedelta(days=days)
        cutoff_ts = int(cutoff.timestamp() * 1000)

        filtered = []
        for inst in instruments:
            exp_ts = inst.get('expiration_timestamp', 0)
            if 0 < exp_ts < cutoff_ts:
                filtered.append(inst)

        return sorted(filtered, key=lambda x: x.get('expiration_timestamp', 0))

    def group_by_expiry(self, instruments: List[Dict]) -> Dict[str, List[Dict]]:
        groups = {}
        for inst in instruments:
            parsed = self.parse_instrument_name(inst.get('instrument_name', ''))
            if parsed:
                expiry_str = parsed['expiry_str']
                if expiry_str not in groups:
                    groups[expiry_str] = []
                groups[expiry_str].append(inst)
        return groups

    def calculate_moneyness(self, instrument: Dict, spot_price: float) -> float:
        strike = instrument.get('strike', 0)
        if strike == 0:
            return 0.0
        return spot_price / strike

    def get_strike_range(self, instruments: List[Dict]) -> Tuple[float, float]:
        strikes = [inst.get('strike', 0) for inst in instruments if inst.get('strike', 0) > 0]
        if not strikes:
            return 0.0, 0.0
        return min(strikes), max(strikes)


class OptionsDataStorage:
    def __init__(self, data_dir: str = "./data", table_prefix: str = "options_ohlcv"):
        self.data_dir = Path(data_dir)
        self.table_prefix = table_prefix
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, symbol: str, timeframe: str) -> Path:
        exchange_dir = self.data_dir / "deribit_options"
        exchange_dir.mkdir(parents=True, exist_ok=True)
        return exchange_dir / f"{symbol}_options_{timeframe}.parquet"

    def save(self, symbol: str, timeframe: str, records: List[OptionOHLCV]) -> int:
        if not records:
            return 0

        file_path = self._get_file_path(symbol, timeframe)

        df_new = pd.DataFrame([r.to_dict() for r in records])

        if file_path.exists():
            df_existing = pd.read_parquet(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=['timestamp', 'instrument_name'], keep='first', inplace=True)
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined) - len(df_existing)
        else:
            df_combined = df_new.copy()
            df_combined.sort_values('timestamp', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            added = len(df_combined)

        df_combined.to_parquet(file_path, index=False)
        logger.info(f"[存储] {symbol}: 写入 {len(df_new)} 条，去重后新增 {added} 条 -> {file_path.name}")
        return added

    def get_last_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        file_path = self._get_file_path(symbol, timeframe)
        if not file_path.exists():
            return None
        df = pd.read_parquet(file_path)
        if df.empty:
            return None
        return int(df['timestamp'].max())

    def get_stats(self, symbol: str, timeframe: str) -> Dict:
        file_path = self._get_file_path(symbol, timeframe)
        if not file_path.exists():
            return {'exists': False, 'count': 0}
        df = pd.read_parquet(file_path)
        return {
            'exists': True,
            'count': len(df),
            'start_time': int(df['timestamp'].min()) if not df.empty else None,
            'end_time': int(df['timestamp'].max()) if not df.empty else None,
        }


class VolatilitySurfaceBuilder:
    def __init__(self, vol_calculator: VolatilityCalculator):
        self.vol_calculator = vol_calculator

    def build_surface(self, options_data: List[OptionOHLCV]) -> pd.DataFrame:
        if not options_data:
            return pd.DataFrame()

        records = []
        for opt in options_data:
            if opt.underlying_price > 0 and opt.strike > 0 and opt.expiry_timestamp > 0:
                T = (opt.expiry_timestamp - opt.timestamp) / (365 * 24 * 3600 * 1000)
                if T > 0:
                    records.append({
                        'timestamp': opt.timestamp,
                        'instrument_name': opt.instrument_name,
                        'strike': opt.strike,
                        'underlying_price': opt.underlying_price,
                        'moneyness': opt.underlying_price / opt.strike,
                        'time_to_expiry': T,
                        'implied_volatility': opt.implied_volatility or 0.0,
                        'delta': opt.delta,
                        'gamma': opt.gamma,
                        'vega': opt.vega,
                        'theta': opt.theta,
                    })

        return pd.DataFrame(records)

    def calculate_smile_skew(self, options_df: pd.DataFrame) -> Dict:
        if options_df.empty:
            return {}

        atm_options = options_df[abs(options_df['moneyness'] - 1.0) < 0.05]
        otm_calls = options_df[options_df['moneyness'] > 1.0]
        otm_puts = options_df[options_df['moneyness'] < 1.0]

        atm_iv = atm_options['implied_volatility'].mean() if not atm_options.empty else 0
        call_iv_25 = otm_calls[otm_calls['moneyness'].between(1.1, 1.3)]['implied_volatility'].mean() if not otm_calls.empty else 0
        put_iv_25 = otm_puts[otm_puts['moneyness'].between(0.7, 0.9)]['implied_volatility'].mean() if not otm_puts.empty else 0

        return {
            'atm_volatility': atm_iv,
            'call_25_delta_iv': call_iv_25,
            'put_25_delta_iv': put_iv_25,
            'skew': put_iv_25 - call_iv_25 if call_iv_25 > 0 and put_iv_25 > 0 else 0,
        }


class OptionsCollector:
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.base_url = self.config.get('base_url', 'https://www.deribit.com/api/v2')
        self.rate_limit = self.config.get('rate_limit', {}).get('requests_per_second', 15)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay = self.config.get('retry_delay_seconds', 2)

        self.api = DeribitOptionsAPI(
            base_url=self.base_url,
            rate_limit=self.rate_limit,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay
        )
        self.chain_parser = OptionsChainParser()
        self.vol_calculator = VolatilityCalculator()
        self.surface_builder = VolatilitySurfaceBuilder(self.vol_calculator)

    def collect_options_chain(
        self,
        currency: str,
        start_ts: int,
        end_ts: int,
        timeframe: str = '1D',
        expiry_days: int = 30,
        include_expired: bool = False,
        calculate_greeks: bool = True,
        calculate_volatility: bool = True,
    ) -> List[OptionOHLCV]:
        logger.info(f"开始采集 {currency} 期权数据...")
        logger.info(f"时间范围: {datetime.fromtimestamp(start_ts/1000)} ~ {datetime.fromtimestamp(end_ts/1000)}")
        logger.info(f"到期日筛选: {expiry_days} 天内")

        all_records = []

        instruments = self.api.get_instruments(currency, 'option', 'false')
        if not instruments:
            logger.warning(f"未找到 {currency} 的期权合约")
            return all_records

        active_instruments = [i for i in instruments if i.get('is_active', False)]
        logger.info(f"活跃期权数量: {len(active_instruments)}")

        if expiry_days > 0:
            filtered_instruments = self.chain_parser.filter_by_expiry(active_instruments, expiry_days)
            logger.info(f"筛选后到期日({expiry_days}天内)期权数量: {len(filtered_instruments)}")
        else:
            filtered_instruments = active_instruments

        resolution = DERIBIT_RESOLUTION_MAP.get(timeframe, "1D")

        for inst in filtered_instruments:
            inst_name = inst['instrument_name']
            try:
                chart_data = self.api.get_tradingview_chart_data(inst_name, start_ts, end_ts, resolution)
                if not chart_data or 'result' not in chart_data:
                    continue

                data = chart_data['result']
                ticks = data.get('ticks', [])
                if not ticks:
                    continue

                book_summary = self.api.get_book_summary_by_instrument(inst_name)
                ticker_data = self.api.get_ticker(inst_name)

                underlying_price = ticker_data.get('result', {}).get('underlying_price', 0)
                mark_price = ticker_data.get('result', {}).get('mark_price', 0)
                bid_price = ticker_data.get('result', {}).get('best_bid_price', 0)
                ask_price = ticker_data.get('result', {}).get('best_ask_price', 0)
                open_interest = book_summary.get('result', {}).get('open_interest', 0) if 'result' in book_summary else 0

                opens = data.get('open', [])
                highs = data.get('high', [])
                lows = data.get('low', [])
                closes = data.get('close', [])
                volumes = data.get('volume', [])

                strike = inst.get('strike', 0)
                option_type = inst.get('option_type', 'call')
                expiry_ts = inst.get('expiration_timestamp', 0)

                for i in range(len(ticks)):
                    record = OptionOHLCV(
                        timestamp=int(ticks[i]),
                        instrument_name=inst_name,
                        open=float(opens[i]) if i < len(opens) else 0.0,
                        high=float(highs[i]) if i < len(highs) else 0.0,
                        low=float(lows[i]) if i < len(lows) else 0.0,
                        close=float(closes[i]) if i < len(closes) else 0.0,
                        volume=float(volumes[i]) if i < len(volumes) else 0.0,
                        underlying_price=underlying_price,
                        mark_price=mark_price,
                        bid_price=bid_price,
                        ask_price=ask_price,
                        open_interest=open_interest,
                    )

                    if calculate_greeks and underlying_price > 0 and strike > 0:
                        T = (expiry_ts - ticks[i]) / (365 * 24 * 3600 * 1000)
                        if T > 0:
                            sigma = mark_price / underlying_price if underlying_price > 0 else 0.5
                            greeks = self.vol_calculator.calculate_greeks(
                                underlying_price, strike, T,
                                self.vol_calculator.risk_free_rate, sigma, option_type
                            )
                            record.delta = greeks['delta']
                            record.gamma = greeks['gamma']
                            record.vega = greeks['vega']
                            record.theta = greeks['theta']
                            record.rho = greeks['rho']

                    if calculate_volatility and mark_price > 0 and underlying_price > 0:
                        T = (expiry_ts - ticks[i]) / (365 * 24 * 3600 * 1000)
                        if T > 0:
                            record.implied_volatility = self.vol_calculator.calculate_implied_volatility(
                                mark_price, underlying_price, strike, T,
                                self.vol_calculator.risk_free_rate, option_type
                            )

                    all_records.append(record)

                logger.debug(f"  {inst_name}: {len(ticks)} 条记录")

            except Exception as e:
                logger.warning(f"采集 {inst_name} 失败: {e}")
                continue

        logger.info(f"采集完成，共 {len(all_records)} 条记录")
        return all_records

    def run_backfill(self, currency: str, days: int, timeframe: str = '1D',
                    expiry_days: int = 30, output_path: str = './data'):
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days * 86400 * 1000

        storage = OptionsDataStorage(output_path)

        last_ts = storage.get_last_timestamp(currency, timeframe)
        if last_ts:
            fetch_start = last_ts + 86400 * 1000
            logger.info(f"已有数据，从 {datetime.fromtimestamp(fetch_start/1000)} 继续")
        else:
            fetch_start = start_ms

        records = self.collect_options_chain(
            currency=currency,
            start_ts=fetch_start,
            end_ts=now_ms,
            timeframe=timeframe,
            expiry_days=expiry_days,
            include_expired=False
        )

        if records:
            added = storage.save(currency, timeframe, records)
            stats = storage.get_stats(currency, timeframe)
            if stats['exists']:
                logger.info(f"数据统计: {stats['count']} 条 "
                          f"[{datetime.fromtimestamp(stats['start_time']/1000).date()} ~ "
                          f"{datetime.fromtimestamp(stats['end_time']/1000).date()}]")
            return added

        return 0

    def run_daily(self, currency: str, timeframe: str = '1D',
                 expiry_days: int = 30, output_path: str = './data'):
        now_ms = int(time.time() * 1000)
        day_ms = 86400 * 1000
        start_ms = now_ms - day_ms

        storage = OptionsDataStorage(output_path)

        last_ts = storage.get_last_timestamp(currency, timeframe)
        if last_ts:
            fetch_start = last_ts + day_ms
            if fetch_start >= now_ms:
                logger.info(f"{currency}: 数据已是最新，跳过")
                return 0
        else:
            fetch_start = start_ms

        records = self.collect_options_chain(
            currency=currency,
            start_ts=fetch_start,
            end_ts=now_ms,
            timeframe=timeframe,
            expiry_days=expiry_days
        )

        if records:
            return storage.save(currency, timeframe, records)

        return 0


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Deribit期权数据独立采集脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 采集BTC期权数据（30天内到期）
  python options_collector.py --date 2026-05-06 --symbol BTC --expiry 30

  # 采集ETH期权数据（7天内到期）
  python options_collector.py --date 2026-05-06 --symbol ETH --expiry 7 --timeframe 1d

  # 自定义输出路径
  python options_collector.py --date 2026-05-06 --symbol BTC --output-path /data/options

  # 回填历史数据
  python options_collector.py --mode backfill --symbol BTC --days 365 --expiry 30

  # 使用自定义配置
  python options_collector.py --config config_options.yaml --symbol BTC
        """
    )

    parser.add_argument(
        '--mode',
        choices=['daily', 'backfill'],
        default='daily',
        help='运行模式: daily=每日增量, backfill=历史回填'
    )

    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='采集日期 (YYYY-MM-DD格式)，默认今天'
    )

    parser.add_argument(
        '--symbol',
        type=str,
        default='BTC',
        choices=['BTC', 'ETH', 'SOL'],
        help='标的货币 (默认: BTC)'
    )

    parser.add_argument(
        '--expiry',
        type=int,
        default=30,
        help='期权到期日筛选天数 (默认: 30天)'
    )

    parser.add_argument(
        '--timeframe',
        type=str,
        default='1d',
        choices=['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w'],
        help='K线周期 (默认: 1d)'
    )

    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='回填天数 (仅backfill模式)'
    )

    parser.add_argument(
        '--output-path',
        type=str,
        default='./data',
        help='数据输出路径 (默认: ./data)'
    )

    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='配置文件路径'
    )

    parser.add_argument(
        '--no-greeks',
        action='store_true',
        help='不计算希腊值'
    )

    parser.add_argument(
        '--no-volatility',
        action='store_true',
        help='不计算隐含波动率'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式'
    )

    return parser.parse_args()


def load_config(config_path: str = None) -> dict:
    if config_path is None:
        return {}

    import yaml
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"配置文件加载失败: {e}")
        return {}


def main():
    args = parse_arguments()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("Deribit Options 独立采集脚本")
    logger.info("=" * 60)

    config = load_config(args.config)

    if 'deribit_options' in config:
        config = config['deribit_options']

    collector = OptionsCollector(config)

    symbols = [args.symbol] if args.symbol else config.get('option_symbols', ['BTC', 'ETH']).keys()

    total_added = 0

    for symbol in symbols:
        logger.info(f"\n处理标的: {symbol}")

        if args.mode == 'backfill':
            added = collector.run_backfill(
                currency=symbol,
                days=args.days,
                timeframe=args.timeframe,
                expiry_days=args.expiry,
                output_path=args.output_path
            )
            total_added += added

        else:
            added = collector.run_daily(
                currency=symbol,
                timeframe=args.timeframe,
                expiry_days=args.expiry,
                output_path=args.output_path
            )
            total_added += added

    logger.info("\n" + "=" * 60)
    logger.info(f"采集完成，总计新增: {total_added} 条记录")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
