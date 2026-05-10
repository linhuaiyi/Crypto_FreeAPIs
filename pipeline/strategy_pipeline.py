"""
Strategy data pipeline — orchestrates data collection per strategy.

Resolves strategy data requirements → dispatches fetchers → stores via ChunkedBuffer.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from storage.chunked_buffer import ChunkedBuffer
from pipeline.strategy_configs import StrategyConfig, DataRequirement, get_strategy, get_all_strategies
from utils import get_logger

logger = get_logger("StrategyPipeline")


class StrategyDataPipeline:
    """Orchestrates data collection based on strategy requirements."""

    def __init__(
        self,
        data_dir: str = "./data",
        buffer_max_rows: int = 100_000,
        buffer_max_memory_mb: int = 200,
    ) -> None:
        self.buffer = ChunkedBuffer(
            data_dir=data_dir,
            max_rows=buffer_max_rows,
            max_memory_mb=buffer_max_memory_mb,
        )
        self._fetchers: Dict[str, object] = {}

    def register_fetcher(self, data_type: str, fetcher: object) -> None:
        self._fetchers[data_type] = fetcher

    def run_strategy(
        self,
        strategy_name: str,
        mode: str = "daily",
        days: int = 1,
    ) -> Dict[str, int]:
        """Run data collection for a specific strategy.

        Returns:
            Dict mapping data_type → rows collected
        """
        config = get_strategy(strategy_name)
        if not config:
            logger.error(f"Unknown strategy: {strategy_name}")
            return {}

        logger.info(f"=== Running strategy: {config.display_name} ({config.priority}) ===")

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - days * 86400 * 1000

        results: Dict[str, int] = {}

        for req in sorted(config.requirements, key=lambda r: r.priority):
            collected = self._collect_requirement(req, start_ms, now_ms, mode)
            results[req.data_type] = collected

        # Flush remaining buffer
        self.buffer.flush_all()

        total = sum(results.values())
        logger.info(
            f"=== Strategy {config.display_name} complete: "
            f"{total} total rows across {len(results)} data types ==="
        )
        return results

    def run_all(self, mode: str = "daily", days: int = 1) -> Dict[str, Dict[str, int]]:
        """Run all registered strategies."""
        all_results: Dict[str, Dict[str, int]] = {}

        for name, config in get_all_strategies().items():
            all_results[name] = self.run_strategy(name, mode=mode, days=days)

        return all_results

    def run_strategies(
        self,
        strategy_names: List[str],
        mode: str = "daily",
        days: int = 1,
    ) -> Dict[str, Dict[str, int]]:
        """Run selected strategies."""
        results: Dict[str, Dict[str, int]] = {}

        for name in strategy_names:
            config = get_strategy(name)
            if config:
                results[name] = self.run_strategy(name, mode=mode, days=days)
            else:
                logger.warning(f"Unknown strategy: {name}")

        return results

    def _collect_requirement(
        self,
        req: DataRequirement,
        start_ms: int,
        end_ms: int,
        mode: str,
    ) -> int:
        """Collect data for a single requirement."""
        fetcher = self._fetchers.get(req.data_type)
        if not fetcher:
            logger.warning(
                f"No fetcher registered for '{req.data_type}', skipping"
            )
            return 0

        total_rows = 0

        for exchange in req.exchanges:
            for symbol in req.symbols:
                try:
                    rows = self._fetch_and_store(
                        fetcher, req.data_type, exchange, symbol,
                        start_ms, end_ms,
                    )
                    total_rows += rows
                except Exception as e:
                    logger.error(
                        f"[{req.data_type}] {exchange}/{symbol}: {e}"
                    )

        return total_rows

    def _fetch_and_store(
        self,
        fetcher: object,
        data_type: str,
        exchange: str,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> int:
        """Call fetcher and store results via ChunkedBuffer."""
        # Dispatch to appropriate fetcher method based on data_type
        records = []

        if data_type == "funding_rate" and hasattr(fetcher, 'fetch_binance'):
            if exchange == "binance":
                records = fetcher.fetch_binance(symbol, start_ms, end_ms)
            elif exchange == "deribit":
                records = fetcher.fetch_deribit(symbol, start_ms, end_ms)
            elif exchange == "hyperliquid":
                records = fetcher.fetch_hyperliquid(symbol)

        elif data_type == "mark_price" and hasattr(fetcher, 'fetch_binance'):
            if exchange == "binance":
                records = fetcher.fetch_binance(symbol, start_ms, end_ms)
            elif exchange == "deribit":
                records = fetcher.fetch_deribit(symbol, start_ms, end_ms)
            elif exchange == "hyperliquid":
                records = fetcher.fetch_hyperliquid(symbol)

        elif data_type == "margin_params" and hasattr(fetcher, 'fetch_deribit_instruments'):
            if exchange == "deribit":
                records = fetcher.fetch_deribit_instruments(symbol)
            elif exchange == "binance":
                records = fetcher.fetch_binance_exchange_info()

        if not records:
            return 0

        # Convert to DataFrame
        if hasattr(records[0], 'to_dict'):
            rows = [r.to_dict() for r in records]
        elif isinstance(records[0], dict):
            rows = records
        else:
            rows = [vars(r) for r in records]

        df = pd.DataFrame(rows)
        self.buffer.append(exchange, data_type, symbol, df)

        return len(records)
