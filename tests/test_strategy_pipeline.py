"""Tests for pipeline.strategy_pipeline.StrategyDataPipeline — construction, dispatch, and buffer integration."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.strategy_pipeline import StrategyDataPipeline
from pipeline.strategy_configs import get_all_strategies, register_strategy, StrategyConfig, DataRequirement


class TestPipelineConstruction:
    """Pipeline init and fetcher registration."""

    def test_construct_with_temp_data_dir(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        assert pipeline.buffer is not None
        assert pipeline._fetchers == {}

    def test_register_fetcher_stores_by_data_type(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        mock_fetcher = MagicMock()
        pipeline.register_fetcher("funding_rate", mock_fetcher)
        assert pipeline._fetchers["funding_rate"] is mock_fetcher


class TestRunStrategy:
    """Strategy execution via run_strategy / run_all / run_strategies."""

    def test_unknown_strategy_returns_empty(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        result = pipeline.run_strategy("nonexistent_strategy")
        assert result == {}

    def test_run_all_iterates_all_strategies(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        # No fetchers registered, so each strategy returns zero rows
        results = pipeline.run_all()
        assert len(results) == len(get_all_strategies())
        for name, sub in results.items():
            assert isinstance(sub, dict)

    def test_run_strategies_with_subset(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        subset = ["short_strangle", "collar"]
        results = pipeline.run_strategies(subset)
        assert set(results.keys()) == {"short_strangle", "collar"}

    def test_run_strategies_skips_unknown(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        results = pipeline.run_strategies(["short_strangle", "nope"])
        assert "short_strangle" in results
        assert "nope" not in results


class TestFetcherIntegration:
    """Mock fetcher dispatches feed data into the buffer."""

    def test_mock_fetcher_feeds_buffer(self, tmp_path):
        pipeline = StrategyDataPipeline(data_dir=str(tmp_path))
        fetcher = MagicMock()
        fetcher.fetch_binance.return_value = [
            {"symbol": "BTCUSDT", "rate": 0.0001, "ts": 1700000000000},
            {"symbol": "BTCUSDT", "rate": 0.0002, "ts": 1700000080000},
        ]
        pipeline.register_fetcher("funding_rate", fetcher)

        # Use short_strangle — it requires funding_rate from binance
        result = pipeline.run_strategy("short_strangle")

        # funding_rate should report at least some collected rows
        assert result.get("funding_rate", 0) > 0
        fetcher.fetch_binance.assert_called()
