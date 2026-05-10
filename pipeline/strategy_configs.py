"""
Strategy data requirement configurations.

Each strategy declares which data sources it needs and at what frequency.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True)
class DataRequirement:
    """A single data source requirement."""
    data_type: str          # e.g. 'funding_rate', 'mark_price', 'options_greeks'
    exchanges: List[str]    # e.g. ['binance', 'deribit']
    symbols: List[str]      # e.g. ['BTCUSDT', 'BTC-PERPETUAL']
    frequency: str          # e.g. '8h', '1m', '1s', '1d'
    priority: int = 0       # 0=highest


@dataclass(frozen=True)
class StrategyConfig:
    """Strategy configuration with its data requirements."""
    name: str
    display_name: str
    priority: str           # P0, P1, P2
    requirements: List[DataRequirement]
    description: str = ""


STRATEGY_REGISTRY: Dict[str, StrategyConfig] = {}


def register_strategy(config: StrategyConfig) -> None:
    STRATEGY_REGISTRY[config.name] = config


def get_strategy(name: str) -> Optional[StrategyConfig]:
    return STRATEGY_REGISTRY.get(name)


def get_all_strategies() -> Dict[str, StrategyConfig]:
    return dict(STRATEGY_REGISTRY)


# ── Strategy Definitions ──

register_strategy(StrategyConfig(
    name="short_strangle",
    display_name="Short Strangle + Perp Hedge",
    priority="P0",
    requirements=[
        DataRequirement("options_greeks", ["deribit"], ["BTC", "ETH"], "5s", 0),
        DataRequirement("mark_price", ["deribit"], ["BTC-PERPETUAL", "ETH-PERPETUAL"], "1s", 0),
        DataRequirement("funding_rate", ["binance", "deribit"], ["BTCUSDT", "BTC-PERPETUAL"], "8h", 2),
        DataRequirement("margin_params", ["deribit"], ["BTC", "ETH"], "1d", 2),
    ],
    description="Short strangle with delta hedge via perpetual",
))

register_strategy(StrategyConfig(
    name="synthetic_covered_call",
    display_name="Synthetic Covered Call",
    priority="P0",
    requirements=[
        DataRequirement("options_greeks", ["deribit"], ["BTC", "ETH"], "5s", 0),
        DataRequirement("mark_price", ["deribit"], ["BTC-PERPETUAL", "ETH-PERPETUAL"], "1m", 1),
        DataRequirement("funding_rate", ["binance", "deribit", "hyperliquid"], ["BTCUSDT", "BTC-PERPETUAL", "BTC"], "8h", 2),
    ],
    description="Covered call via short put + long perp",
))

register_strategy(StrategyConfig(
    name="collar",
    display_name="Collar Strategy",
    priority="P0",
    requirements=[
        DataRequirement("options_greeks", ["deribit"], ["BTC", "ETH"], "5s", 0),
        DataRequirement("mark_price", ["deribit"], ["BTC-PERPETUAL"], "1m", 1),
    ],
    description="Protective put + covered call collar",
))

register_strategy(StrategyConfig(
    name="funding_arb",
    display_name="Funding Rate Arbitrage + Option Protection",
    priority="P1",
    requirements=[
        DataRequirement("funding_rate", ["binance", "deribit", "hyperliquid"], ["BTCUSDT", "BTC-PERPETUAL", "BTC"], "8h", 0),
        DataRequirement("mark_price", ["binance", "deribit"], ["BTCUSDT", "BTC-PERPETUAL"], "1m", 1),
        DataRequirement("options_greeks", ["deribit"], ["BTC"], "5s", 2),
        DataRequirement("basis", ["binance", "deribit"], ["BTCUSDT", "BTC-PERPETUAL"], "1m", 1),
    ],
    description="Funding rate arb with put protection",
))

register_strategy(StrategyConfig(
    name="gamma_scalping",
    display_name="Gamma Scalping",
    priority="P1",
    requirements=[
        DataRequirement("options_greeks", ["deribit"], ["BTC", "ETH"], "5s", 0),
        DataRequirement("mark_price", ["deribit"], ["BTC-PERPETUAL"], "1s", 0),
        DataRequirement("margin_params", ["deribit"], ["BTC"], "1d", 2),
    ],
    description="Delta-neutral gamma scalping",
))

register_strategy(StrategyConfig(
    name="vol_term_structure",
    display_name="Volatility Term Structure Arb",
    priority="P2",
    requirements=[
        DataRequirement("options_greeks", ["deribit"], ["BTC", "ETH"], "5s", 0),
        DataRequirement("vol_surface", ["deribit"], ["BTC", "ETH"], "10s", 1),
        DataRequirement("risk_free_rate", ["fred"], ["USD"], "1d", 2),
    ],
    description="Near-far IV spread trading",
))
