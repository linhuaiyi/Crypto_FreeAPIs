"""Tests for pipeline.strategy_configs — registry, config lookup, dataclass behavior."""

import pytest

from pipeline.strategy_configs import (
    DataRequirement,
    StrategyConfig,
    STRATEGY_REGISTRY,
    get_all_strategies,
    get_strategy,
    register_strategy,
)

EXPECTED_STRATEGIES = [
    "short_strangle",
    "synthetic_covered_call",
    "collar",
    "funding_arb",
    "gamma_scalping",
    "vol_term_structure",
]


class TestStrategyRegistry:
    """Strategy registration and lookup."""

    def test_all_six_strategies_registered(self):
        assert len(STRATEGY_REGISTRY) == 6
        for name in EXPECTED_STRATEGIES:
            assert name in STRATEGY_REGISTRY

    def test_get_strategy_returns_correct_config(self):
        config = get_strategy("short_strangle")
        assert config is not None
        assert config.name == "short_strangle"
        assert config.display_name == "Short Strangle + Perp Hedge"
        assert config.priority == "P0"

    def test_get_strategy_unknown_returns_none(self):
        assert get_strategy("nonexistent_strategy") is None

    def test_get_all_strategies_returns_six_entries(self):
        all_strats = get_all_strategies()
        assert isinstance(all_strats, dict)
        assert len(all_strats) == 6

    def test_each_strategy_has_valid_priority(self):
        valid = {"P0", "P1", "P2"}
        for name, config in get_all_strategies().items():
            assert config.priority in valid, f"{name} has invalid priority {config.priority}"

    def test_each_strategy_has_at_least_one_requirement(self):
        for name, config in get_all_strategies().items():
            assert len(config.requirements) >= 1, f"{name} has no requirements"


class TestDataRequirement:
    """Frozen dataclass behavior and field defaults."""

    def test_frozen_dataclass_rejects_mutation(self):
        req = DataRequirement("funding_rate", ["binance"], ["BTCUSDT"], "8h", 0)
        with pytest.raises(AttributeError):
            req.data_type = "mark_price"

    def test_default_priority_is_zero(self):
        req = DataRequirement("test_type", ["ex"], ["sym"], "1d")
        assert req.priority == 0


class TestCustomRegistration:
    """Dynamic strategy registration."""

    def test_register_strategy_adds_to_registry(self):
        original_count = len(STRATEGY_REGISTRY)
        custom = StrategyConfig(
            name="test_custom",
            display_name="Test Custom",
            priority="P2",
            requirements=[DataRequirement("custom_data", ["ex"], ["SYM"], "1h")],
            description="A custom test strategy",
        )
        register_strategy(custom)
        assert get_strategy("test_custom") is not None
        assert get_strategy("test_custom").display_name == "Test Custom"
        assert len(STRATEGY_REGISTRY) == original_count + 1
        # Cleanup
        del STRATEGY_REGISTRY["test_custom"]
