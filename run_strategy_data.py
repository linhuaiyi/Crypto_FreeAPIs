"""
Unified CLI entry point for the options + perpetual strategy data system.

Usage:
    python run_strategy_data.py --mode daily --strategies all
    python run_strategy_data.py --mode daily --strategies short_strangle
    python run_strategy_data.py --mode backfill --days 90 --strategies funding_arb
    python run_strategy_data.py --mode validate --check-gaps --check-outliers
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.strategy_configs import get_strategy, get_all_strategies
from pipeline.strategy_pipeline import StrategyDataPipeline
from fetchers.funding_rate import FundingRateFetcher
from fetchers.mark_price import MarkPriceFetcher
from fetchers.margin_params import MarginParamsFetcher
from utils import get_logger

logger = get_logger("CLI")


def build_pipeline(data_dir: str = "./data") -> StrategyDataPipeline:
    """Construct pipeline with registered fetchers."""
    pipeline = StrategyDataPipeline(data_dir=data_dir)

    pipeline.register_fetcher("funding_rate", FundingRateFetcher())
    pipeline.register_fetcher("mark_price", MarkPriceFetcher())
    pipeline.register_fetcher("margin_params", MarginParamsFetcher())

    return pipeline


def run_daily(args) -> None:
    strategies = _resolve_strategies(args.strategies)
    pipeline = build_pipeline(args.data_dir)
    results = pipeline.run_strategies(strategies, mode="daily", days=1)

    _print_results(results)


def run_backfill(args) -> None:
    strategies = _resolve_strategies(args.strategies)
    pipeline = build_pipeline(args.data_dir)
    results = pipeline.run_strategies(strategies, mode="backfill", days=args.days)

    _print_results(results)


def run_validate(args) -> None:
    logger.info("=== Data Validation Mode ===")

    if args.check_gaps:
        from processors import GapDetector
        detector = GapDetector()
        logger.info("Gap detection: module loaded OK")

    if args.check_outliers:
        from processors import OutlierFilter
        filter_ = OutlierFilter()
        logger.info("Outlier filter: module loaded OK")

    logger.info("Validation complete (detailed checks require existing data)")


def _resolve_strategies(strategy_arg: str) -> list[str]:
    if strategy_arg == "all":
        return list(get_all_strategies().keys())

    names = [s.strip() for s in strategy_arg.split(",")]
    valid = []
    for name in names:
        if get_strategy(name):
            valid.append(name)
        else:
            available = ", ".join(get_all_strategies().keys())
            logger.warning(
                f"Unknown strategy '{name}', skipping. Available: {available}"
            )
    return valid


def _print_results(results: dict) -> None:
    for strategy_name, data_types in results.items():
        total = sum(data_types.values())
        logger.info(f"  {strategy_name}: {total} rows")
        for dt, count in data_types.items():
            logger.info(f"    {dt}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Options + Perpetual Strategy Data Collection System"
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill", "validate"],
        default="daily",
        help="Run mode: daily=incremental, backfill=historical, validate=quality checks",
    )
    parser.add_argument(
        "--strategies",
        default="all",
        help="Strategy name(s), comma-separated, or 'all'",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days to backfill (backfill mode only)",
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        help="Root data directory",
    )
    parser.add_argument(
        "--check-gaps",
        action="store_true",
        help="Check for data gaps (validate mode)",
    )
    parser.add_argument(
        "--check-outliers",
        action="store_true",
        help="Check for outliers (validate mode)",
    )

    args = parser.parse_args()

    if args.mode == "daily":
        run_daily(args)
    elif args.mode == "backfill":
        run_backfill(args)
    elif args.mode == "validate":
        run_validate(args)


if __name__ == "__main__":
    main()
