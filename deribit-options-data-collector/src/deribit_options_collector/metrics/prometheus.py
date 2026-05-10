"""Prometheus metrics and alerting for Deribit data collection."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Optional

import structlog
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import CollectorRegistry
from aiohttp import web

from deribit_options_collector.config import Settings

logger = structlog.get_logger(__name__)


class MetricsCollector:
    """Prometheus metrics collector with alerting support."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._config = settings.metrics
        self._alerts_config = settings.alerts

        self._registry = CollectorRegistry()

        self._messages_lag = Gauge(
            "deribit_options_messages_lag_seconds",
            "Time lag since last received message",
            registry=self._registry,
        )

        self._write_errors_total = Counter(
            "deribit_options_write_errors_total",
            "Total number of write errors",
            registry=self._registry,
        )

        self._last_snapshot_timestamp = Gauge(
            "deribit_options_last_snapshot_timestamp",
            "Unix timestamp of last successful snapshot",
            registry=self._registry,
        )

        self._ticker_count = Counter(
            "deribit_options_tickers_collected_total",
            "Total number of tickers collected",
            registry=self._registry,
        )

        self._data_flush_size = Histogram(
            "deribit_options_data_flush_size",
            "Number of records per flush",
            buckets=[1, 10, 50, 100, 500, 1000, 5000],
            registry=self._registry,
        )

        self._ws_connected = Gauge(
            "deribit_options_ws_connected",
            "WebSocket connection status (1=connected, 0=disconnected)",
            registry=self._registry,
        )

        self._collection_duration = Histogram(
            "deribit_options_collection_duration_seconds",
            "Duration of collection cycles",
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
            registry=self._registry,
        )

        self._buffer_size = Gauge(
            "deribit_options_buffer_size",
            "Current size of in-memory buffer",
            registry=self._registry,
        )

        self._write_error_count = 0
        self._ws_disconnect_time: Optional[float] = None
        self._last_message_time = time.time()

        self._health_app: Optional[web.Application] = None
        self._metrics_app: Optional[web.Application] = None
        self._health_runner: Optional[web.AppRunner] = None
        self._metrics_runner: Optional[web.AppRunner] = None

    async def start(self) -> None:
        """Start the metrics HTTP servers."""
        if not self._config.enabled:
            return

        self._health_app = web.Application()
        self._health_app.router.add_get(self._config.health_path, self._health_handler)
        self._health_app.router.add_get("/ready", self._ready_handler)

        self._health_runner = web.AppRunner(self._health_app)
        await self._health_runner.setup()
        health_site = web.TCPSite(self._health_runner, "0.0.0.0", self._config.health_port)
        await health_site.start()

        self._metrics_app = web.Application()
        self._metrics_app.router.add_get(self._config.path, self._metrics_handler)

        self._metrics_runner = web.AppRunner(self._metrics_app)
        await self._metrics_runner.setup()
        metrics_site = web.TCPSite(self._metrics_runner, "0.0.0.0", self._config.port)
        await metrics_site.start()

        logger.info(
            "metrics_servers_started",
            health_port=self._config.health_port,
            metrics_port=self._config.port,
        )

    async def stop(self) -> None:
        """Stop the metrics HTTP servers."""
        if self._health_runner:
            await self._health_runner.cleanup()
        if self._metrics_runner:
            await self._metrics_runner.cleanup()

    async def _health_handler(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.Response(
            text="OK",
            content_type="text/plain",
            status=200,
        )

    async def _ready_handler(self, request: web.Request) -> web.Response:
        """Readiness check endpoint."""
        return web.Response(
            text="READY",
            content_type="text/plain",
            status=200,
        )

    async def _metrics_handler(self, request: web.Request) -> web.Response:
        """Prometheus metrics endpoint."""
        return web.Response(
            body=generate_latest(self._registry),
            content_type=CONTENT_TYPE_LATEST,
        )

    def record_message_received(self) -> None:
        """Record that a message was received."""
        self._last_message_time = time.time()
        self._ws_disconnect_time = None

    def update_messages_lag(self) -> None:
        """Update the messages lag gauge."""
        lag = time.time() - self._last_message_time
        self._messages_lag.set(lag)

        if lag > self._alerts_config.ws_disconnect_threshold_seconds:
            if self._ws_disconnect_time is None:
                self._ws_disconnect_time = time.time()
            else:
                disconnect_duration = time.time() - self._ws_disconnect_time
                if disconnect_duration > self._alerts_config.ws_disconnect_threshold_seconds:
                    self._trigger_pagerduty_alert(
                        "WebSocket disconnection",
                        f"WebSocket disconnected for {disconnect_duration:.0f} seconds",
                        severity="critical",
                    )
        else:
            self._ws_disconnect_time = None

    def record_ticker_count(self, count: int) -> None:
        """Record ticker count."""
        self._ticker_count.inc(count)

    def record_data_flush(self, size: int) -> None:
        """Record data flush event."""
        self._data_flush_size.observe(size)
        self._buffer_size.set(0)

    def record_snapshot_timestamp(self) -> None:
        """Record snapshot timestamp."""
        self._last_snapshot_timestamp.set_to_current_time()

    def increment_write_errors(self) -> None:
        """Increment write error counter."""
        self._write_errors_total.inc()
        self._write_error_count += 1

        if self._write_error_count >= self._alerts_config.write_failure_threshold:
            self._trigger_pagerduty_alert(
                "Write failures",
                f"Consecutive write failures: {self._write_error_count}",
                severity="critical",
            )

    def reset_write_error_count(self) -> None:
        """Reset write error counter after successful write."""
        self._write_error_count = 0

    def set_ws_connected(self, connected: bool) -> None:
        """Set WebSocket connection status."""
        self._ws_connected.set(1 if connected else 0)

    def record_collection_duration(self, duration: float) -> None:
        """Record collection duration."""
        self._collection_duration.observe(duration)

    def update_buffer_size(self, size: int) -> None:
        """Update buffer size gauge."""
        self._buffer_size.set(size)

    def _trigger_pagerduty_alert(
        self,
        title: str,
        message: str,
        severity: str = "critical",
    ) -> None:
        """Trigger PagerDuty alert."""
        if not self._alerts_config.pagerduty.enabled:
            return

        if not self._alerts_config.pagerduty.routing_key:
            logger.warning(
                "pagerduty_alert_skipped",
                reason="no_routing_key",
                title=title,
            )
            return

        asyncio.create_task(self._send_pagerduty_alert(title, message, severity))

    async def _send_pagerduty_alert(
        self,
        title: str,
        message: str,
        severity: str,
    ) -> None:
        """Send alert to PagerDuty."""
        try:
            import aiohttp

            payload = {
                "routing_key": self._alerts_config.pagerduty.routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": title,
                    "severity": severity,
                    "source": "deribit-options-collector",
                    "custom_details": {
                        "message": message,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                },
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status == 202:
                        logger.info("pagerduty_alert_sent", title=title)
                    else:
                        logger.warning(
                            "pagerduty_alert_failed",
                            title=title,
                            status=response.status,
                        )
        except Exception as e:
            logger.error("pagerduty_alert_error", error=str(e))


class MetricsMonitor:
    """Background metrics monitoring task."""

    def __init__(self, metrics: MetricsCollector) -> None:
        self._metrics = metrics
        self._running = False
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Start the monitoring task."""
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("metrics_monitor_started")

    async def stop(self) -> None:
        """Stop the monitoring task."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("metrics_monitor_stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            try:
                self._metrics.update_messages_lag()
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("monitor_loop_error", error=str(e))
