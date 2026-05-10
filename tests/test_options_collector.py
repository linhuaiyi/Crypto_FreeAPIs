import unittest
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from options_collector import (
    OptionsChainParser,
    VolatilityCalculator,
    OptionsDataStorage,
    OptionOHLCV,
)


class TestOptionsChainParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = OptionsChainParser()

    def test_parse_instrument_name_call(self):
        instrument_name = "BTC-6MAY26-69000-C"
        result = self.parser.parse_instrument_name(instrument_name)

        self.assertIsNotNone(result)
        self.assertEqual(result['base_currency'], 'BTC')
        self.assertEqual(result['strike'], 69000.0)
        self.assertEqual(result['option_type'], 'C')
        self.assertEqual(result['expiry_str'], '6MAY26')

    def test_parse_instrument_name_put(self):
        instrument_name = "ETH-28MAR25-2000-P"
        result = self.parser.parse_instrument_name(instrument_name)

        self.assertIsNotNone(result)
        self.assertEqual(result['base_currency'], 'ETH')
        self.assertEqual(result['strike'], 2000.0)
        self.assertEqual(result['option_type'], 'P')
        self.assertEqual(result['expiry_str'], '28MAR25')

    def test_parse_invalid_instrument_name(self):
        instrument_name = "INVALID"
        result = self.parser.parse_instrument_name(instrument_name)
        self.assertIsNone(result)

    def test_filter_by_expiry(self):
        now = datetime.now()
        instruments = [
            {'instrument_name': 'BTC-6MAY26-69000-C', 'expiration_timestamp': int((now + timedelta(days=5)).timestamp() * 1000)},
            {'instrument_name': 'BTC-28MAR25-70000-C', 'expiration_timestamp': int((now + timedelta(days=15)).timestamp() * 1000)},
            {'instrument_name': 'BTC-25DEC25-71000-C', 'expiration_timestamp': int((now + timedelta(days=45)).timestamp() * 1000)},
        ]

        filtered = self.parser.filter_by_expiry(instruments, days=30)

        self.assertEqual(len(filtered), 2)
        self.assertTrue(all(
            (datetime.fromtimestamp(i['expiration_timestamp']/1000) - now).days < 30
            for i in filtered
        ))

    def test_group_by_expiry(self):
        instruments = [
            {'instrument_name': 'BTC-6MAY26-69000-C'},
            {'instrument_name': 'BTC-6MAY26-70000-P'},
            {'instrument_name': 'BTC-28MAR25-71000-C'},
        ]

        groups = self.parser.group_by_expiry(instruments)

        self.assertIn('6MAY26', groups)
        self.assertIn('28MAR25', groups)
        self.assertEqual(len(groups['6MAY26']), 2)
        self.assertEqual(len(groups['28MAR25']), 1)

    def test_calculate_moneyness(self):
        instrument = {'strike': 50000}
        spot_price = 55000

        moneyness = self.parser.calculate_moneyness(instrument, spot_price)

        self.assertAlmostEqual(moneyness, 1.1, places=4)

    def test_get_strike_range(self):
        instruments = [
            {'strike': 60000},
            {'strike': 65000},
            {'strike': 70000},
        ]

        min_strike, max_strike = self.parser.get_strike_range(instruments)

        self.assertEqual(min_strike, 60000)
        self.assertEqual(max_strike, 70000)


class TestVolatilityCalculator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.calc = VolatilityCalculator(risk_free_rate=0.05)
        try:
            import scipy
            cls.scipy_available = True
        except ImportError:
            cls.scipy_available = False

    def test_black_scholes_call_price(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.2

        price = self.calc.black_scholes_call(S, K, T, r, sigma)

        self.assertGreater(price, 0)
        self.assertLess(price, S)

    def test_black_scholes_put_price(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.2

        price = self.calc.black_scholes_put(S, K, T, r, sigma)

        self.assertGreater(price, 0)
        self.assertLess(price, K)

    def test_calculate_greeks_call(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.2

        greeks = self.calc.calculate_greeks(S, K, T, r, sigma, 'call')

        self.assertIn('delta', greeks)
        self.assertIn('gamma', greeks)
        self.assertIn('vega', greeks)
        self.assertIn('theta', greeks)
        self.assertIn('rho', greeks)

        self.assertGreater(greeks['delta'], 0)
        self.assertLess(greeks['delta'], 1)
        self.assertGreater(greeks['gamma'], 0)

    def test_calculate_greeks_put(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.2

        greeks = self.calc.calculate_greeks(S, K, T, r, sigma, 'put')

        self.assertIn('delta', greeks)
        self.assertLess(greeks['delta'], 0)
        self.assertGreater(greeks['gamma'], 0)

    def test_call_put_parity(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.2

        call_price = self.calc.black_scholes_call(S, K, T, r, sigma)
        put_price = self.calc.black_scholes_put(S, K, T, r, sigma)

        parity = call_price - put_price
        import math
        intrinsic = S - K * pow(math.e, -r * T)

        self.assertAlmostEqual(parity, intrinsic, places=2)

    def test_calculate_implied_volatility(self):
        if not self.scipy_available:
            self.skipTest("scipy not available")

        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.3

        market_price = self.calc.black_scholes_call(S, K, T, r, sigma)

        implied_vol = self.calc.calculate_implied_volatility(market_price, S, K, T, r, 'call')

        self.assertAlmostEqual(implied_vol, sigma, places=2)

    def test_zero_time_to_expiry(self):
        S = 100.0
        K = 100.0
        T = 0.0
        r = 0.05
        sigma = 0.2

        price = self.calc.black_scholes_call(S, K, T, r, sigma)

        self.assertEqual(price, 0.0)

    def test_zero_volatility(self):
        S = 100.0
        K = 100.0
        T = 0.25
        r = 0.05
        sigma = 0.0

        price = self.calc.black_scholes_call(S, K, T, r, sigma)

        self.assertEqual(price, 0.0)


class TestOptionsDataStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import tempfile
        import shutil
        cls.temp_dir = tempfile.mkdtemp()
        cls.storage = OptionsDataStorage(cls.temp_dir, "options_ohlcv")
        cls.records = [
            OptionOHLCV(
                timestamp=1704067200000,
                instrument_name="BTC-6MAY26-69000-C",
                open=100.0, high=105.0, low=98.0, close=103.0,
                volume=100.0, underlying_price=69000, mark_price=100.0,
                bid_price=99.0, ask_price=101.0, open_interest=1000.0,
                delta=0.5, gamma=0.01, vega=0.1, theta=-0.05, rho=0.02,
                implied_volatility=0.3
            ),
            OptionOHLCV(
                timestamp=1704153600000,
                instrument_name="BTC-6MAY26-69000-C",
                open=103.0, high=108.0, low=101.0, close=106.0,
                volume=120.0, underlying_price=69500, mark_price=106.0,
                bid_price=105.0, ask_price=107.0, open_interest=1100.0,
                delta=0.52, gamma=0.012, vega=0.11, theta=-0.06, rho=0.025,
                implied_volatility=0.32
            ),
        ]
        cls.storage.save("BTC", "1d", cls.records)

    @classmethod
    def tearDownClass(cls):
        import shutil
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_save_records(self):
        temp_storage = OptionsDataStorage(self.temp_dir, "options_ohlcv")
        records = [
            OptionOHLCV(
                timestamp=1700000000000,
                instrument_name="ETH-6MAY26-2000-C",
                open=50.0, high=55.0, low=48.0, close=52.0,
                volume=80.0, underlying_price=2000, mark_price=50.0,
                bid_price=49.0, ask_price=51.0, open_interest=500.0,
            ),
        ]
        added = temp_storage.save("ETH", "1d", records)
        self.assertEqual(added, 1)

    def test_save_duplicate_records(self):
        temp_storage = OptionsDataStorage(self.temp_dir, "test_dup")
        records = [
            OptionOHLCV(
                timestamp=1705000000000,
                instrument_name="ETH-6MAY26-2000-C",
                open=50.0, high=55.0, low=48.0, close=52.0,
                volume=80.0, underlying_price=2000, mark_price=50.0,
                bid_price=49.0, ask_price=51.0, open_interest=500.0,
            ),
        ]
        temp_storage.save("ETH", "1d", records)
        added = temp_storage.save("ETH", "1d", records)
        self.assertEqual(added, 0)

    def test_get_last_timestamp(self):
        last_ts = self.storage.get_last_timestamp("BTC", "1d")
        self.assertIsNotNone(last_ts)
        self.assertGreater(last_ts, 0)

    def test_get_last_timestamp_no_data(self):
        last_ts = self.storage.get_last_timestamp("ETH", "1d")
        self.assertIsNone(last_ts)

    def test_get_stats(self):
        stats = self.storage.get_stats("BTC", "1d")
        self.assertTrue(stats['exists'])
        self.assertEqual(stats['count'], 2)
        self.assertIsNotNone(stats['start_time'])
        self.assertIsNotNone(stats['end_time'])

    def test_get_stats_no_data(self):
        stats = self.storage.get_stats("ETH", "1d")
        self.assertFalse(stats['exists'])
        self.assertEqual(stats['count'], 0)

    def test_empty_records(self):
        temp_storage = OptionsDataStorage(self.temp_dir, "test_empty")
        added = temp_storage.save("SOL", "1d", [])
        self.assertEqual(added, 0)


class TestOptionOHLCV(unittest.TestCase):
    def test_to_dict(self):
        record = OptionOHLCV(
            timestamp=1704067200000,
            instrument_name="BTC-6MAY26-69000-C",
            open=100.0, high=105.0, low=98.0, close=103.0,
            volume=100.0, underlying_price=69000, mark_price=100.0,
            bid_price=99.0, ask_price=101.0, open_interest=1000.0,
            delta=0.5, gamma=0.01, vega=0.1, theta=-0.05, rho=0.02,
            implied_volatility=0.3
        )

        d = record.to_dict()

        self.assertEqual(d['timestamp'], 1704067200000)
        self.assertEqual(d['instrument_name'], "BTC-6MAY26-69000-C")
        self.assertEqual(d['open'], 100.0)
        self.assertEqual(d['close'], 103.0)
        self.assertEqual(d['delta'], 0.5)
        self.assertEqual(d['implied_volatility'], 0.3)

    def test_optional_greeks(self):
        record = OptionOHLCV(
            timestamp=1704067200000,
            instrument_name="BTC-6MAY26-69000-C",
            open=100.0, high=105.0, low=98.0, close=103.0,
            volume=100.0, underlying_price=69000, mark_price=100.0,
            bid_price=99.0, ask_price=101.0, open_interest=1000.0,
        )

        d = record.to_dict()

        self.assertIsNone(d['delta'])
        self.assertIsNone(d['gamma'])
        self.assertIsNone(d['vega'])
        self.assertIsNone(d['theta'])
        self.assertIsNone(d['rho'])
        self.assertIsNone(d['implied_volatility'])


if __name__ == '__main__':
    import math
    unittest.main(verbosity=2)
