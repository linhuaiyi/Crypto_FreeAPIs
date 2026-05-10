"""Tests for processors/basis_calculator.py"""

from math import exp

import pandas as pd
import pytest

from processors.basis_calculator import DAYS_PER_YEAR, BasisCalculator


@pytest.fixture
def calc():
    return BasisCalculator()


class TestCalcSpotPerp:
    def test_correct_basis_values(self, calc):
        spot_df = pd.DataFrame({"timestamp": [100, 200, 300], "price": [100.0, 101.0, 102.0]})
        perp_df = pd.DataFrame({"timestamp": [100, 200, 300], "price": [100.5, 101.5, 102.5]})
        results = calc.calc_spot_perp(spot_df, perp_df, "BTC_USDT", days_to_expiry=30)

        assert len(results) == 3
        assert results[0].basis == pytest.approx(0.5, abs=1e-6)
        assert results[0].basis_pct == pytest.approx(0.5, abs=0.01)
        assert results[0].annualized_basis == pytest.approx(0.5 * DAYS_PER_YEAR / 30, abs=0.01)

    def test_basis_pct_and_annualized(self, calc):
        spot_df = pd.DataFrame({"timestamp": [1000], "price": [50000.0]})
        perp_df = pd.DataFrame({"timestamp": [1000], "price": [50500.0]})
        result = calc.calc_spot_perp(spot_df, perp_df, "ETH_USDT", days_to_expiry=365)
        assert result[0].basis_pct == pytest.approx(1.0, abs=0.01)
        assert result[0].annualized_basis == pytest.approx(1.0, abs=0.01)


class TestCalcSynthetic:
    def test_put_call_parity(self, calc):
        call_mid = 10.0
        put_mid = 5.0
        strike = 100.0
        perp_price = 105.0
        rate = 0.05
        dte = 30
        result = calc.calc_synthetic(call_mid, put_mid, strike, perp_price, rate, dte)

        synthetic_long = call_mid - put_mid + strike * exp(-rate * dte / DAYS_PER_YEAR)
        expected_basis = perp_price - synthetic_long
        assert result.basis == pytest.approx(expected_basis, abs=1e-6)


class TestCalcCrossExchange:
    def test_cross_exchange_basis(self, calc):
        perp_a = pd.DataFrame({"timestamp": [100, 200], "price": [100.0, 101.0]})
        perp_b = pd.DataFrame({"timestamp": [100, 200], "price": [99.5, 100.5]})
        results = calc.calc_cross_exchange(perp_a, perp_b, "BTC_BINANCE_BYBIT")

        assert len(results) == 2
        assert results[0].basis == pytest.approx(0.5, abs=1e-6)
        assert results[0].basis_type == "cross_exchange"
