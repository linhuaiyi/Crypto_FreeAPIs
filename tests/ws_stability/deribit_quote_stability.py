"""Deribit WebSocket stability harness.

Standalone investigation script to find a reliable subscription strategy
for collecting L1 quote sizes (bid_size/ask_size) across all BTC+ETH options.

Context: production engine subscribes to ~1548 quote.{instrument} channels
on a single WS connection and hits code=1009 (Message Too Big) every ~5s.
This harness lets us sweep batch sizes, connection counts, and channel
types to find a config that survives for hours.

Not a pytest unit test — meant to run for minutes/hours against real Deribit.

Usage:
    python -m tests.ws_stability.deribit_quote_stability diagnose
    python -m tests.ws_stability.deribit_quote_stability sweep
    python -m tests.ws_stability.deribit_quote_stability multi --connections 4
    python -m tests.ws_stability.deribit_quote_stability alt --channel-type book
    python -m tests.ws_stability.deribit_quote_stability longrun --duration 1800
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

import requests
import websockets

DERIBIT_WS_URL = "wss://www.deribit.com/ws/api/v2"
DERIBIT_REST_URL = "https://www.deribit.com/api/v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ws_stability")


# ─── Discovery ────────────────────────────────────────────────────────────────


def discover_option_instruments(currencies: Iterable[str]) -> list[str]:
    out: list[str] = []
    session = requests.Session()
    for c in currencies:
        r = session.get(
            f"{DERIBIT_REST_URL}/public/get_instruments",
            params={"currency": c, "kind": "option", "expired": "false"},
            timeout=15,
        )
        r.raise_for_status()
        for inst in r.json().get("result", []):
            name = inst.get("instrument_name")
            if name:
                out.append(name)
    return out


def build_channels(instruments: list[str], channel_type: str) -> list[str]:
    if channel_type == "quote":
        return [f"quote.{n}" for n in instruments]
    if channel_type == "book":
        # book.{instrument}.{interval}.{depth} — depth=1 means top of book only
        return [f"book.{n}.100ms.1" for n in instruments]
    if channel_type == "ticker":
        return [f"ticker.{n}.100ms" for n in instruments]
    raise ValueError(f"unknown channel_type: {channel_type}")


# ─── Stats ────────────────────────────────────────────────────────────────────


@dataclass
class ConnStats:
    name: str
    channels_subscribed: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    first_msg_at: float | None = None
    messages_received: int = 0
    bytes_received: int = 0
    max_message_bytes: int = 0
    size_buckets: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    instruments_seen: set[str] = field(default_factory=set)
    close_code: int | None = None
    close_reason: str | None = None
    heartbeat_failures: int = 0
    subscribe_response: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)

    def observe(self, raw_bytes: bytes) -> None:
        size = len(raw_bytes)
        self.messages_received += 1
        self.bytes_received += size
        if size > self.max_message_bytes:
            self.max_message_bytes = size
        if self.first_msg_at is None:
            self.first_msg_at = time.time()
        if size < 1024:
            b = "<1KB"
        elif size < 10240:
            b = "1-10KB"
        elif size < 102400:
            b = "10-100KB"
        elif size < 1_048_576:
            b = "100KB-1MB"
        elif size < 10_485_760:
            b = "1-10MB"
        else:
            b = ">10MB"
        self.size_buckets[b] += 1

    def mark_instrument(self, name: str) -> None:
        self.instruments_seen.add(name)

    def report(self) -> dict[str, Any]:
        elapsed = (self.end_time or time.time()) - self.start_time
        first_lat = (
            round(self.first_msg_at - self.start_time, 2) if self.first_msg_at else None
        )
        return {
            "name": self.name,
            "elapsed_sec": round(elapsed, 1),
            "channels_subscribed": self.channels_subscribed,
            "messages_received": self.messages_received,
            "bytes_received": self.bytes_received,
            "bytes_received_mb": round(self.bytes_received / 1_048_576, 2),
            "max_message_bytes": self.max_message_bytes,
            "max_message_mb": round(self.max_message_bytes / 1_048_576, 3),
            "msg_per_sec": round(self.messages_received / elapsed, 1) if elapsed else 0,
            "first_msg_latency_sec": first_lat,
            "instruments_seen": len(self.instruments_seen),
            "instruments_seen_pct": (
                round(100 * len(self.instruments_seen) / self.channels_subscribed, 1)
                if self.channels_subscribed
                else 0
            ),
            "size_buckets": dict(self.size_buckets),
            "close_code": self.close_code,
            "close_reason": self.close_reason,
            "heartbeat_failures": self.heartbeat_failures,
            "subscribe_response_success": (
                self.subscribe_response.get("success")
                if self.subscribe_response
                else None
            ),
            "errors": self.errors,
        }


# ─── Connection runner ────────────────────────────────────────────────────────


async def heartbeat_loop(ws: websockets.WebSocketClientProtocol, stats: ConnStats) -> None:
    req_id = 1000
    while True:
        try:
            await asyncio.sleep(15)
            req_id += 1
            await ws.send(
                json.dumps(
                    {"jsonrpc": "2.0", "id": req_id, "method": "public/test"}
                )
            )
        except websockets.ConnectionClosed:
            return
        except Exception as e:
            stats.heartbeat_failures += 1
            logger.warning(f"[{stats.name}] heartbeat error: {e}")
            return


async def progress_loop(stats: ConnStats, interval_sec: int = 60) -> None:
    """Log periodic progress so we can tell the run is alive."""
    while True:
        await asyncio.sleep(interval_sec)
        elapsed = int(time.time() - stats.start_time)
        logger.info(
            f"[{stats.name}] progress t={elapsed}s "
            f"msgs={stats.messages_received} "
            f"seen={len(stats.instruments_seen)}/{stats.channels_subscribed} "
            f"max={stats.max_message_bytes:,}B"
        )


async def run_connection(
    name: str,
    channels: list[str],
    duration_sec: int,
    max_size: int,
) -> ConnStats:
    stats = ConnStats(name=name, channels_subscribed=len(channels))
    deadline = stats.start_time + duration_sec
    logger.info(f"[{name}] connecting, {len(channels)} channels, max_size={max_size}")

    try:
        async with websockets.connect(
            DERIBIT_WS_URL,
            ping_interval=None,
            ping_timeout=None,
            max_size=max_size,
            open_timeout=20,
        ) as ws:
            stats.channels_subscribed = len(channels)
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "public/subscribe",
                        "params": {"channels": channels},
                    }
                )
            )
            logger.info(f"[{name}] subscribe sent, listening for {duration_sec}s")
            hb_task = asyncio.create_task(heartbeat_loop(ws, stats))
            prog_task = asyncio.create_task(progress_loop(stats, 60))
            try:
                async for raw in ws:
                    stats.observe(raw)
                    if time.time() >= deadline:
                        logger.info(f"[{name}] duration reached, closing cleanly")
                        break
                    try:
                        msg = json.loads(raw)
                        if "id" in msg and msg["id"] == 1 and stats.subscribe_response is None:
                            stats.subscribe_response = {
                                "success": not msg.get("error"),
                                "result": msg.get("result"),
                            }
                            if msg.get("error"):
                                logger.warning(
                                    f"[{name}] subscribe error: {msg['error']}"
                                )
                        params = msg.get("params") or {}
                        ch = params.get("channel") or ""
                        if ch.startswith(("quote.", "book.", "ticker.")):
                            stats.mark_instrument(ch.split(".", 1)[1])
                    except Exception:
                        pass
            except websockets.ConnectionClosedError as e:
                stats.close_code = e.code
                stats.close_reason = str(e)
                logger.warning(f"[{name}] closed: code={e.code} reason={e}")
            except websockets.ConnectionClosedOK:
                stats.close_code = 1000
                logger.info(f"[{name}] closed OK")
            finally:
                hb_task.cancel()
                prog_task.cancel()
                for t in (hb_task, prog_task):
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
    except TimeoutError:
        stats.errors.append("connect timeout (20s)")
        logger.error(f"[{name}] connect timeout")
    except Exception as e:
        stats.errors.append(f"{type(e).__name__}: {e}")
        logger.error(f"[{name}] error: {type(e).__name__}: {e}")

    stats.end_time = time.time()
    return stats


async def run_multi_connection(
    channels: list[str],
    n_connections: int,
    duration_sec: int,
    max_size: int,
) -> list[ConnStats]:
    chunks: list[list[str]] = [[] for _ in range(n_connections)]
    for i, ch in enumerate(channels):
        chunks[i % n_connections].append(ch)
    tasks = [
        run_connection(f"conn{i}", chunk, duration_sec, max_size)
        for i, chunk in enumerate(chunks)
        if chunk
    ]
    return await asyncio.gather(*tasks)


# ─── Test modes ───────────────────────────────────────────────────────────────


async def mode_diagnose(args: argparse.Namespace) -> None:
    """Single connection, full channel set — reproduce production failure."""
    instruments = discover_option_instruments(args.currencies)
    channels = build_channels(instruments, args.channel_type)
    logger.info(f"discovered {len(instruments)} instruments")
    stats = await run_connection(
        "diagnose", channels, args.duration, args.max_size_mb * 1024 * 1024
    )
    _print_report([stats])


async def mode_sweep(args: argparse.Namespace) -> None:
    """Sequentially try batch sizes to find survival threshold."""
    instruments = discover_option_instruments(args.currencies)
    channels_all = build_channels(instruments, args.channel_type)
    logger.info(f"discovered {len(instruments)} instruments")

    results: list[ConnStats] = []
    for batch in args.batch_sizes:
        subset = channels_all[:batch]
        logger.info(f"=== batch_size={batch} ===")
        stats = await run_connection(
            f"batch_{batch}", subset, args.duration, args.max_size_mb * 1024 * 1024
        )
        results.append(stats)
        _print_report([stats])
        # Short pause between runs
        await asyncio.sleep(2)

    logger.info("=== sweep summary ===")
    for r in results:
        rep = r.report()
        print(
            f"  {rep['name']}: survived={rep['close_code'] in (None, 1000)} "
            f"msgs={rep['messages_received']} max={rep['max_message_bytes']:,}B "
            f"seen_pct={rep['instruments_seen_pct']}% close={rep['close_code']}"
        )


async def mode_multi(args: argparse.Namespace) -> None:
    """Split channels across N parallel connections."""
    instruments = discover_option_instruments(args.currencies)
    channels = build_channels(instruments, args.channel_type)
    logger.info(f"discovered {len(instruments)} instruments, splitting into {args.connections}")
    stats_list = await run_multi_connection(
        channels, args.connections, args.duration, args.max_size_mb * 1024 * 1024
    )
    _print_report(stats_list)
    total_msgs = sum(s.messages_received for s in stats_list)
    total_seen = len(set().union(*(s.instruments_seen for s in stats_list)))
    logger.info(
        f"=== multi summary: {args.connections} conns, "
        f"{total_msgs} total msgs, {total_seen}/{len(instruments)} instruments seen "
        f"({100*total_seen/len(instruments):.1f}%) ==="
    )


async def mode_alt(args: argparse.Namespace) -> None:
    """Compare quote vs book.{instrument}.100ms.1 vs ticker channel types."""
    instruments = discover_option_instruments(args.currencies)
    logger.info(f"discovered {len(instruments)} instruments")
    for ct in ("quote", "book", "ticker"):
        channels = build_channels(instruments, ct)
        logger.info(f"=== channel_type={ct} ({len(channels)} channels) ===")
        stats = await run_connection(
            ct, channels, args.duration, args.max_size_mb * 1024 * 1024
        )
        _print_report([stats])
        await asyncio.sleep(2)


async def mode_longrun(args: argparse.Namespace) -> None:
    """Long-running test of chosen config — use after picking best strategy."""
    instruments = discover_option_instruments(args.currencies)
    channels = build_channels(instruments, args.channel_type)
    logger.info(f"discovered {len(instruments)} instruments")
    if args.connections > 1:
        stats_list = await run_multi_connection(
            channels, args.connections, args.duration, args.max_size_mb * 1024 * 1024
        )
    else:
        s = await run_connection(
            "longrun_single", channels, args.duration, args.max_size_mb * 1024 * 1024
        )
        stats_list = [s]
    _print_report(stats_list)
    if args.report_file:
        import pathlib
        report = {
            "mode": "longrun",
            "config": {
                "connections": args.connections,
                "duration_sec": args.duration,
                "channel_type": args.channel_type,
                "total_instruments": len(instruments),
            },
            "results": [s.report() for s in stats_list],
        }
        pathlib.Path(args.report_file).write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        logger.info(f"report written to {args.report_file}")


def _print_report(stats_list: list[ConnStats]) -> None:
    print("\n" + "=" * 78)
    for s in stats_list:
        print(json.dumps(s.report(), indent=2))
    print("=" * 78 + "\n")


# ─── CLI ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="mode", required=True)

    common_max = dict(type=int, default=16, help="max_size in MiB (default 16)")
    common_dur = dict(type=int, default=60, help="duration in seconds per run")
    common_cur = dict(
        nargs="+",
        default=["BTC", "ETH"],
        help="currencies (default BTC ETH)",
    )
    common_ct = dict(
        default="quote",
        choices=["quote", "book", "ticker"],
    )

    p_diag = sub.add_parser("diagnose", help="reproduce production failure (single conn, all channels)")
    p_diag.add_argument("--duration", **common_dur)
    p_diag.add_argument("--max-size-mb", **common_max)
    p_diag.add_argument("--currencies", **common_cur)
    p_diag.add_argument("--channel-type", **common_ct)
    p_diag.set_defaults(func=mode_diagnose)

    p_sweep = sub.add_parser("sweep", help="try increasing batch sizes")
    p_sweep.add_argument("--batch-sizes", type=int, nargs="+", default=[10, 50, 100, 200, 500, 1000])
    p_sweep.add_argument("--duration", **common_dur)
    p_sweep.add_argument("--max-size-mb", **common_max)
    p_sweep.add_argument("--currencies", **common_cur)
    p_sweep.add_argument("--channel-type", **common_ct)
    p_sweep.set_defaults(func=mode_sweep)

    p_multi = sub.add_parser("multi", help="split across N parallel connections")
    p_multi.add_argument("--connections", type=int, default=4)
    p_multi.add_argument("--duration", **common_dur)
    p_multi.add_argument("--max-size-mb", **common_max)
    p_multi.add_argument("--currencies", **common_cur)
    p_multi.add_argument("--channel-type", **common_ct)
    p_multi.set_defaults(func=mode_multi)

    p_alt = sub.add_parser("alt", help="compare quote/book/ticker channel types")
    p_alt.add_argument("--duration", **common_dur)
    p_alt.add_argument("--max-size-mb", **common_max)
    p_alt.add_argument("--currencies", **common_cur)
    p_alt.set_defaults(func=mode_alt)

    p_long = sub.add_parser("longrun", help="long-run validation of chosen config")
    p_long.add_argument("--duration", type=int, default=1800)
    p_long.add_argument("--connections", type=int, default=1)
    p_long.add_argument("--max-size-mb", **common_max)
    p_long.add_argument("--currencies", **common_cur)
    p_long.add_argument("--channel-type", **common_ct)
    p_long.add_argument(
        "--report-file",
        default="tests/ws_stability/longrun_report.json",
        help="path to write final JSON report",
    )
    p_long.set_defaults(func=mode_longrun)

    return ap.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
