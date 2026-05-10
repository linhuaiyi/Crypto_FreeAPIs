"""Tests for processors/greeks_processor.py"""

import time
from datetime import datetime

import numpy as np
import pytest

from processors.greeks_processor import (
    GreeksProcessor,
    GreeksSnapshot,
    InstrumentMeta,
    DeribitOptionsChainFetcher,
    _norm_pdf,
)


class TestInstrumentMeta:
    def test_frozen(self):
        meta = InstrumentMeta("BTC", "9MAY26", 1700000000000, 100000.0, "C")
        with pytest.raises(AttributeError):
            meta.strike = 101000.0  # type: ignore[misc]


class TestParseInstrumentName:
    def test_call_option(self):
        meta = GreeksProcessor.parse_instrument_name("BTC-9MAY26-100000-C")
        assert meta is not None
        assert meta.currency == "BTC"
        assert meta.strike == 100000.0
        assert meta.option_type == "C"

    def test_put_option(self):
        meta = GreeksProcessor.parse_instrument_name("ETH-28MAR26-3000-P")
        assert meta is not None
        assert meta.currency == "ETH"
        assert meta.strike == 3000.0
        assert meta.option_type == "P"

    def test_perpetual_returns_none(self):
        assert GreeksProcessor.parse_instrument_name("BTC-PERPETUAL") is None

    def test_invalid_format(self):
        assert GreeksProcessor.parse_instrument_name("invalid") is None

    def test_partial_format(self):
        assert GreeksProcessor.parse_instrument_name("BTC-100000-C") is None


class TestNormPdf:
    def test_at_zero(self):
        result = _norm_pdf(np.array([0.0]))
        expected = 1.0 / np.sqrt(2.0 * np.pi)
        assert float(result[0]) == pytest.approx(expected, rel=1e-6)

    def test_symmetry(self):
        x = np.array([1.0, -1.0])
        result = _norm_pdf(x)
        assert result[0] == pytest.approx(result[1])

    def test_vectorized(self):
        x = np.linspace(-3, 3, 100)
        result = _norm_pdf(x)
        assert len(result) == 100
        assert np.all(result > 0)


def _make_chain_item(
    instrument_name="BTC-9MAY26-100000-C",
    bid_price=5000.0,
    ask_price=5200.0,
    mid_price=5100.0,
    underlying_price=103000.0,
    mark_iv=0.65,
    bid_iv=0.63,
    ask_iv=0.67,
):
    return {
        "instrument_name": instrument_name,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "mid_price": mid_price,
        "underlying_price": underlying_price,
        "mark_iv": mark_iv,
        "bid_iv": bid_iv,
        "ask_iv": ask_iv,
        "open_interest": 100.0,
    }


class TestGreeksProcessorComputeBatch:
    def _future_expiry_str(self) -> str:
        """Generate an expiry string 30 days in the future."""
        from datetime import timedelta
        future = datetime.utcnow() + timedelta(days=30)
        return future.strftime("%d%b%y").upper()

    def test_basic_call_greeks(self):
        expiry = self._future_expiry_str()
        chain = [_make_chain_item(
            instrument_name=f"BTC-{expiry}-100000-C",
            mark_iv=0.65,
            underlying_price=103000.0,
        )]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain, risk_free_rate=0.05)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["option_type"] == "C"
        assert 0 < row["delta"] < 1  # call delta in (0, 1)
        assert row["gamma"] > 0
        assert row["vega"] > 0
        assert row["iv_source"] == "rest_api"

    def test_basic_put_greeks(self):
        expiry = self._future_expiry_str()
        chain = [_make_chain_item(
            instrument_name=f"BTC-{expiry}-100000-P",
            mark_iv=0.65,
            underlying_price=103000.0,
        )]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain, risk_free_rate=0.05)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["option_type"] == "P"
        assert -1 < row["delta"] < 0  # put delta in (-1, 0)

    def test_float32_output(self):
        expiry = self._future_expiry_str()
        chain = [_make_chain_item(instrument_name=f"BTC-{expiry}-100000-C")]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain)

        float_cols = ["delta", "gamma", "vega", "theta", "rho", "iv", "strike"]
        for col in float_cols:
            assert result[col].dtype == np.float32, f"{col} is {result[col].dtype}"

    def test_empty_input(self):
        proc = GreeksProcessor()
        result = proc.compute_batch([])
        assert len(result) == 0

    def test_zombie_filter_no_liquidity(self):
        expiry = self._future_expiry_str()
        chain = [_make_chain_item(
            instrument_name=f"BTC-{expiry}-100000-C",
            bid_price=0.0,
            ask_price=0.0,
            mid_price=0.0,
        )]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain)
        assert len(result) == 0  # filtered out

    def test_multiple_instruments(self):
        expiry = self._future_expiry_str()
        chain = [
            _make_chain_item(instrument_name=f"BTC-{expiry}-100000-C", mark_iv=0.65),
            _make_chain_item(instrument_name=f"BTC-{expiry}-105000-C", mark_iv=0.60),
            _make_chain_item(instrument_name=f"BTC-{expiry}-95000-P", mark_iv=0.70),
        ]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain)

        assert len(result) == 3

    def test_iv_fallback_when_no_mark_iv(self):
        expiry = self._future_expiry_str()
        chain = [_make_chain_item(
            instrument_name=f"BTC-{expiry}-100000-C",
            mark_iv=0,
            bid_iv=0,
            ask_iv=0,
        )]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain)

        if len(result) > 0:
            assert result.iloc[0]["iv_source"] == "fallback"
            assert result.iloc[0]["iv"] == pytest.approx(0.5)

    def test_expired_option_filtered(self):
        chain = [_make_chain_item(
            instrument_name="BTC-1JAN20-100000-C",
            mark_iv=0.65,
        )]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain, now_ms=int(time.time() * 1000))
        assert len(result) == 0  # expired, filtered

    def test_perpetual_in_chain_skipped(self):
        expiry = self._future_expiry_str()
        chain = [
            _make_chain_item(instrument_name=f"BTC-{expiry}-100000-C"),
            {"instrument_name": "BTC-PERPETUAL", "bid_price": 100, "ask_price": 101},
        ]
        proc = GreeksProcessor()
        result = proc.compute_batch(chain)
        assert len(result) == 1  # only the option, not the perp


class TestGreeksSnapshot:
    def test_creation(self):
        gs = GreeksSnapshot(
            timestamp=1700000000000, instrument_name="BTC-9MAY26-100000-C",
            exchange="deribit", underlying_price=103000.0,
            strike=100000.0, time_to_expiry_years=0.082,
            option_type="C", iv=0.65, iv_source="rest_api",
            delta=0.7, gamma=0.00001, vega=30.0, theta=-5.0, rho=5.0,
            mid_price=5100.0, bid_price=5000.0, ask_price=5200.0,
        )
        assert gs.delta == 0.7

    def test_to_dict(self):
        gs = GreeksSnapshot(
            1700000000000, "BTC-9MAY26-100000-C", "deribit",
            103000.0, 100000.0, 0.082, "C",
            0.65, "rest_api", 0.7, 0.00001, 30.0, -5.0, 5.0,
            5100.0, 5000.0, 5200.0,
        )
        d = gs.to_dict()
        assert "delta" in d
        assert "gamma" in d
        assert d["instrument_name"] == "BTC-9MAY26-100000-C"
