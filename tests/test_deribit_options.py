import unittest
import time
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetchers.deribit_options import DeribitOptionsFetcher
from utils import RateLimiter, get_logger


class TestDeribitOptionsFetcher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger("TestDeribitOptionsFetcher")
        cls.config = {
            'base_url': 'https://www.deribit.com/api/v2',
            'rate_limit': {'requests_per_second': 15},
            'option_symbols': {
                'BTC': 'BTC',
                'ETH': 'ETH',
            }
        }
        cls.rate_limiter = RateLimiter(15 * 60, 'TestDeribitOptions')

    def test_initialization(self):
        fetcher = DeribitOptionsFetcher(self.config, self.rate_limiter)
        self.assertEqual(fetcher.name, "DeribitOptions")
        self.assertEqual(fetcher.base_url, 'https://www.deribit.com/api/v2')
        self.assertIsNotNone(fetcher.session)
        self.logger.info("[PASS] 初始化测试通过")

    def test_symbol_mapping(self):
        fetcher = DeribitOptionsFetcher(self.config, self.rate_limiter)
        mapping = fetcher.get_symbol_mapping()
        self.assertEqual(mapping['BTC'], 'BTC')
        self.assertEqual(mapping['ETH'], 'ETH')
        self.logger.info("[PASS] 符号映射测试通过")

    def test_unified_symbol_conversion(self):
        fetcher = DeribitOptionsFetcher(self.config, self.rate_limiter)
        self.assertEqual(fetcher.get_unified_symbol('BTC-28MAR25-95000-C'), 'BTC-28MAR25-95000-C')
        self.logger.info("[PASS] 统一符号转换测试通过")


class TestDeribitOptionsAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger("TestDeribitOptionsAPI")
        cls.base_url = 'https://www.deribit.com/api/v2'
        cls.session = requests.Session()

    def test_get_instruments_options_btc(self):
        self.logger.info("\n[Test] 获取BTC期权合约列表...")
        params = {
            'currency': 'BTC',
            'kind': 'option',
            'expired': 'false'
        }
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertIn('result', result)
        instruments = result['result']
        self.assertGreater(len(instruments), 0, "应该有可用的BTC期权合约")

        active = [i for i in instruments if i.get('is_active', False)]
        self.assertGreater(len(active), 0, "应该有活跃的BTC期权")

        for inst in instruments[:2]:
            self.assertEqual(inst['kind'], 'option')
            self.assertIn('strike', inst, "期权应该有strike字段")
            self.assertIn('option_type', inst, "期权应该有option_type字段")
            self.assertIn('instrument_name', inst)
            self.logger.info(f"  [INFO] {inst['instrument_name']}: strike={inst.get('strike')}, type={inst.get('option_type')}")

        self.logger.info(f"[PASS] 获取到 {len(instruments)} 个BTC期权合约，{len(active)} 个活跃")

    def test_get_instruments_options_eth(self):
        self.logger.info("\n[Test] 获取ETH期权合约列表...")
        params = {
            'currency': 'ETH',
            'kind': 'option',
            'expired': 'false'
        }
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertIn('result', result)
        instruments = result['result']
        self.assertGreater(len(instruments), 0, "应该有可用的ETH期权合约")

        self.logger.info(f"[PASS] 获取到 {len(instruments)} 个ETH期权合约")

    def test_get_tradingview_chart_data_options(self):
        self.logger.info("\n[Test] 获取期权K线数据...")

        params = {'currency': 'BTC', 'kind': 'option', 'expired': 'false'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        self.assertEqual(response.status_code, 200)
        instruments = response.json()['result']
        active_options = [i for i in instruments if i.get('is_active', False)]

        if not active_options:
            self.logger.warning("[SKIP] 没有活跃期权")
            return

        found_data = False
        for opt in active_options[:10]:
            inst_name = opt['instrument_name']
            now_ms = int(time.time() * 1000)
            start_ms = now_ms - 7 * 86400 * 1000

            params = {
                'instrument_name': inst_name,
                'start_timestamp': start_ms,
                'end_timestamp': now_ms,
                'resolution': '1D'
            }

            response = self.session.get(
                f'{self.base_url}/public/get_tradingview_chart_data',
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()['result']
                ticks = data.get('ticks', [])

                if len(ticks) > 0:
                    found_data = True
                    self.logger.info(f"  [INFO] {inst_name}: {len(ticks)} 条数据")
                    self.logger.info(f"    首条: {datetime.fromtimestamp(ticks[0]/1000)}")
                    self.logger.info(f"    末条: {datetime.fromtimestamp(ticks[-1]/1000)}")

                    self.assertIn('open', data)
                    self.assertIn('high', data)
                    self.assertIn('low', data)
                    self.assertIn('close', data)
                    self.assertIn('volume', data)
                    break

        self.assertTrue(found_data, "应该至少找到一个有历史数据的期权")
        self.logger.info("[PASS] 期权K线数据测试通过")


class TestDeribitOptionsDataIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger("TestDeribitOptionsDataIntegration")
        cls.base_url = 'https://www.deribit.com/api/v2'
        cls.session = requests.Session()

    def test_btc_options_daily_data(self):
        self.logger.info("\n[Test] BTC期权日频历史数据获取")

        params = {'currency': 'BTC', 'kind': 'option', 'expired': 'false'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        self.assertEqual(response.status_code, 200)
        instruments = response.json()['result']

        active_options = [inst for inst in instruments if inst.get('is_active', False)]
        self.assertGreater(len(active_options), 0, "应该有活跃的BTC期权")
        self.logger.info(f"  [INFO] 活跃BTC期权数量: {len(active_options)}")

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 30 * 86400 * 1000

        test_count = 0
        for inst in active_options[:5]:
            inst_name = inst['instrument_name']
            params = {
                'instrument_name': inst_name,
                'start_timestamp': start_ms,
                'end_timestamp': now_ms,
                'resolution': '1D'
            }

            response = self.session.get(
                f'{self.base_url}/public/get_tradingview_chart_data',
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()['result']
                ticks = data.get('ticks', [])
                if len(ticks) > 0:
                    test_count += 1
                    self.logger.info(f"  [INFO] {inst_name}: {len(ticks)} 条数据")

            time.sleep(0.2)

        self.assertGreater(test_count, 0, "应该至少有一个期权有历史数据")
        self.logger.info(f"[PASS] BTC期权日频数据测试完成 (获取到 {test_count} 个期权的数据)")

    def test_eth_options_daily_data(self):
        self.logger.info("\n[Test] ETH期权日频历史数据获取")

        params = {'currency': 'ETH', 'kind': 'option', 'expired': 'false'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        self.assertEqual(response.status_code, 200)
        instruments = response.json()['result']

        active_options = [inst for inst in instruments if inst.get('is_active', False)]
        self.assertGreater(len(active_options), 0, "应该有活跃的ETH期权")
        self.logger.info(f"  [INFO] 活跃ETH期权数量: {len(active_options)}")

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 30 * 86400 * 1000

        test_count = 0
        for inst in active_options[:5]:
            inst_name = inst['instrument_name']
            params = {
                'instrument_name': inst_name,
                'start_timestamp': start_ms,
                'end_timestamp': now_ms,
                'resolution': '1D'
            }

            response = self.session.get(
                f'{self.base_url}/public/get_tradingview_chart_data',
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()['result']
                ticks = data.get('ticks', [])
                if len(ticks) > 0:
                    test_count += 1
                    self.logger.info(f"  [INFO] {inst_name}: {len(ticks)} 条数据")

            time.sleep(0.2)

        self.assertGreater(test_count, 0, "应该至少有一个期权有历史数据")
        self.logger.info(f"[PASS] ETH期权日频数据测试完成 (获取到 {test_count} 个期权的数据)")

    def test_data_integrity(self):
        self.logger.info("\n[Test] 数据完整性检查")

        params = {'currency': 'BTC', 'kind': 'option', 'expired': 'false'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        instruments = response.json()['result']
        active_options = [inst for inst in instruments if inst.get('is_active', False)]

        if len(active_options) == 0:
            self.logger.warning("[SKIP] 没有活跃期权")
            return

        found_data = False
        for opt in active_options[:10]:
            inst_name = opt['instrument_name']
            now_ms = int(time.time() * 1000)
            start_ms = now_ms - 7 * 86400 * 1000

            params = {
                'instrument_name': inst_name,
                'start_timestamp': start_ms,
                'end_timestamp': now_ms,
                'resolution': '1D'
            }

            response = self.session.get(
                f'{self.base_url}/public/get_tradingview_chart_data',
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()['result']
                ticks = data.get('ticks', [])
                if len(ticks) > 0:
                    found_data = True
                    opens = data.get('open', [])
                    highs = data.get('high', [])
                    lows = data.get('low', [])
                    closes = data.get('close', [])
                    volumes = data.get('volume', [])

                    self.assertEqual(len(ticks), len(opens))
                    self.assertEqual(len(ticks), len(highs))
                    self.assertEqual(len(ticks), len(lows))
                    self.assertEqual(len(ticks), len(closes))
                    self.assertEqual(len(ticks), len(volumes))

                    for i in range(len(ticks)):
                        self.assertGreaterEqual(highs[i], opens[i])
                        self.assertGreaterEqual(highs[i], closes[i])
                        self.assertLessEqual(lows[i], opens[i])
                        self.assertLessEqual(lows[i], closes[i])
                        self.assertGreaterEqual(highs[i], lows[i])

                    self.logger.info(f"  [INFO] 数据完整性检查通过 ({len(ticks)} 条记录)")
                    break

        self.assertTrue(found_data, "应该至少找到一个有历史数据的期权")
        self.logger.info("[PASS] 数据完整性测试通过")

    def test_time_series_correctness(self):
        self.logger.info("\n[Test] 时间序列正确性检查")

        params = {'currency': 'BTC', 'kind': 'option', 'expired': 'false'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )
        instruments = response.json()['result']
        active_options = [inst for inst in instruments if inst.get('is_active', False)]

        if len(active_options) == 0:
            self.logger.warning("[SKIP] 没有活跃期权")
            return

        found_data = False
        for opt in active_options[:10]:
            inst_name = opt['instrument_name']
            now_ms = int(time.time() * 1000)
            start_ms = now_ms - 30 * 86400 * 1000

            params = {
                'instrument_name': inst_name,
                'start_timestamp': start_ms,
                'end_timestamp': now_ms,
                'resolution': '1D'
            }

            response = self.session.get(
                f'{self.base_url}/public/get_tradingview_chart_data',
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()['result']
                ticks = data.get('ticks', [])

                if len(ticks) > 1:
                    found_data = True
                    for i in range(1, len(ticks)):
                        self.assertGreater(ticks[i], ticks[i-1])

                    self.logger.info(f"  [INFO] 时间范围: {datetime.fromtimestamp(ticks[0]/1000)} ~ {datetime.fromtimestamp(ticks[-1]/1000)}")
                    self.logger.info(f"  [INFO] 时间序列正确性检查通过")
                    break

        self.assertTrue(found_data, "应该至少找到一个有历史数据的期权")
        self.logger.info("[PASS] 时间序列正确性测试通过")


class TestDeribitOptionsErrorHandling(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger("TestDeribitOptionsErrorHandling")
        cls.base_url = 'https://www.deribit.com/api/v2'
        cls.session = requests.Session()

    def test_invalid_instrument(self):
        self.logger.info("\n[Test] 无效合约处理")
        params = {
            'instrument_name': 'INVALID-INSTRUMENT-NAME',
            'start_timestamp': int(time.time() * 1000) - 86400000,
            'end_timestamp': int(time.time() * 1000),
            'resolution': '1D'
        }

        response = self.session.get(
            f'{self.base_url}/public/get_tradingview_chart_data',
            params=params,
            timeout=30
        )

        self.assertIn(response.status_code, [200, 400, 404])
        self.logger.info(f"  [INFO] 无效合约响应: {response.status_code}")
        self.logger.info("[PASS] 无效合约处理测试通过")

    def test_expired_instrument(self):
        self.logger.info("\n[Test] 过期期权处理")
        params = {'currency': 'BTC', 'kind': 'option', 'expired': 'true'}
        response = self.session.get(
            f'{self.base_url}/public/get_instruments',
            params=params,
            timeout=30
        )

        if response.status_code == 200:
            instruments = response.json()['result']
            self.logger.info(f"  [INFO] 过期BTC期权数量: {len(instruments)}")

        self.logger.info("[PASS] 过期期权处理测试通过")

    def test_rate_limit_handling(self):
        self.logger.info("\n[Test] 限流处理")
        params = {'currency': 'BTC', 'kind': 'option'}

        for i in range(10):
            response = self.session.get(
                f'{self.base_url}/public/get_instruments',
                params=params,
                timeout=30
            )
            if response.status_code == 429:
                self.logger.info(f"  [INFO] 检测到限流 (请求 #{i+1})")
                time.sleep(5)
                break
            time.sleep(0.05)

        self.logger.info("[PASS] 限流处理测试完成")


class TestDeribitOptionsAuthentication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = get_logger("TestDeribitOptionsAuthentication")
        cls.base_url = 'https://www.deribit.com/api/v2'
        cls.session = requests.Session()

    def test_public_endpoints_no_auth(self):
        self.logger.info("\n[Test] 公共端点无需鉴权")
        endpoints = [
            ('/public/get_time', {}),
            ('/public/get_instruments', {'currency': 'BTC', 'kind': 'option'}),
            ('/public/get_currencies', {}),
        ]

        for endpoint, params in endpoints:
            response = self.session.get(
                f'{self.base_url}{endpoint}',
                params=params,
                timeout=30
            )
            self.assertEqual(response.status_code, 200,
                           f"端点 {endpoint} 应该无需鉴权即可访问")
            self.logger.info(f"  [PASS] {endpoint}")

        self.logger.info("[PASS] 公共端点鉴权测试完成")

    def test_private_endpoints_require_auth(self):
        self.logger.info("\n[Test] 私有端点需要鉴权")
        response = self.session.get(
            f'{self.base_url}/private/get_account_summary',
            params={'currency': 'BTC'},
            timeout=30
        )

        self.assertIn(response.status_code, [400, 401, 403],
                    "私有端点应该需要鉴权")
        self.logger.info(f"  [INFO] 私有端点响应: {response.status_code} (需要鉴权)")
        self.logger.info("[PASS] 私有端点鉴权测试完成")


if __name__ == '__main__':
    print("=" * 70)
    print("Deribit 期权历史数据接口测试")
    print("=" * 70)

    unittest.main(verbosity=2)
