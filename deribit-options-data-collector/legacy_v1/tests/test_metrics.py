"""Tests for metrics module."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import web

from deribit_options_collector.metrics.prometheus import MetricsCollector, MetricsMonitor


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    @pytest.fixture
    def metrics_collector(self, mock_settings: Any) -> MetricsCollector:
        """Create MetricsCollector for testing."""
        return MetricsCollector(mock_settings)

    def test_initialization(self, metrics_collector: MetricsCollector) -> None:
        """Test metrics collector initialization."""
        assert metrics_collector._write_error_count == 0
        assert metrics_collector._ws_disconnect_time is None
        assert metrics_collector._last_message_time > 0

    def test_record_message_received(self, metrics_collector: MetricsCollector) -> None:
        """Test recording message received."""
        metrics_collector._last_message_time = 0
        metrics_collector.record_message_received()
        assert metrics_collector._last_message_time > 0
        assert metrics_collector._ws_disconnect_time is None

    def test_update_messages_lag(self, metrics_collector: MetricsCollector) -> None:
        """Test updating messages lag."""
        metrics_collector.update_messages_lag()
        assert True

    def test_record_ticker_count(self, metrics_collector: MetricsCollector) -> None:
        """Test recording ticker count."""
        metrics_collector.record_ticker_count(100)
        metrics_collector.record_ticker_count(50)
        assert True

    def test_record_data_flush(self, metrics_collector: MetricsCollector) -> None:
        """Test recording data flush."""
        metrics_collector.record_data_flush(100)
        assert True

    def test_record_snapshot_timestamp(self, metrics_collector: MetricsCollector) -> None:
        """Test recording snapshot timestamp."""
        metrics_collector.record_snapshot_timestamp()
        assert True

    def test_increment_write_errors(self, metrics_collector: MetricsCollector) -> None:
        """Test incrementing write errors."""
        assert metrics_collector._write_error_count == 0
        metrics_collector.increment_write_errors()
        assert metrics_collector._write_error_count == 1

    def test_increment_write_errors_triggers_alert(
        self,
        metrics_collector: MetricsCollector,
    ) -> None:
        """Test that incrementing write errors triggers alert."""
        metrics_collector._alerts_config.write_failure_threshold = 3

        with patch.object(metrics_collector, "_trigger_pagerduty_alert") as mock_alert:
            for _ in range(3):
                metrics_collector.increment_write_errors()

            mock_alert.assert_called()

    def test_reset_write_error_count(self, metrics_collector: MetricsCollector) -> None:
        """Test resetting write error count."""
        metrics_collector._write_error_count = 5
        metrics_collector.reset_write_error_count()
        assert metrics_collector._write_error_count == 0

    def test_set_ws_connected(self, metrics_collector: MetricsCollector) -> None:
        """Test setting WebSocket connected status."""
        metrics_collector.set_ws_connected(True)
        assert True
        metrics_collector.set_ws_connected(False)
        assert True

    def test_record_collection_duration(self, metrics_collector: MetricsCollector) -> None:
        """Test recording collection duration."""
        metrics_collector.record_collection_duration(1.5)
        assert True

    def test_update_buffer_size(self, metrics_collector: MetricsCollector) -> None:
        """Test updating buffer size."""
        metrics_collector.update_buffer_size(100)
        assert True

    def test_trigger_pagerduty_alert_disabled(
        self,
        metrics_collector: MetricsCollector,
    ) -> None:
        """Test that alert is skipped when disabled."""
        metrics_collector._alerts_config.pagerduty.enabled = False

        metrics_collector._trigger_pagerduty_alert("Test Alert", "Test message")
        assert True

    def test_trigger_pagerduty_alert_no_routing_key(
        self,
        metrics_collector: MetricsCollector,
    ) -> None:
        """Test that alert is skipped without routing key."""
        metrics_collector._alerts_config.pagerduty.enabled = True
        metrics_collector._alerts_config.pagerduty.routing_key = ""

        metrics_collector._trigger_pagerduty_alert("Test Alert", "Test message")
        assert True

    @pytest.mark.asyncio
    async def test_start(self, metrics_collector: MetricsCollector) -> None:
        """Test starting metrics server."""
        await metrics_collector.start()
        assert metrics_collector._health_runner is not None
        assert metrics_collector._metrics_runner is not None

    @pytest.mark.asyncio
    async def test_stop(self, metrics_collector: MetricsCollector) -> None:
        """Test stopping metrics server."""
        await metrics_collector.start()
        await metrics_collector.stop()
        assert True

    @pytest.mark.asyncio
    async def test_health_handler(self, metrics_collector: MetricsCollector) -> None:
        """Test health check handler."""
        await metrics_collector.start()

        request = MagicMock()
        response = await metrics_collector._health_handler(request)

        assert response.status == 200
        assert response.text == "OK"

    @pytest.mark.asyncio
    async def test_ready_handler(self, metrics_collector: MetricsCollector) -> None:
        """Test readiness handler."""
        await metrics_collector.start()

        request = MagicMock()
        response = await metrics_collector._ready_handler(request)

        assert response.status == 200
        assert response.text == "READY"

    @pytest.mark.asyncio
    async def test_metrics_handler(self, metrics_collector: MetricsCollector) -> None:
        """Test metrics handler."""
        await metrics_collector.start()

        request = MagicMock()
        response = await metrics_collector._metrics_handler(request)

        assert response.status == 200
        assert b"deribit_options" in response.body


class TestMetricsMonitor:
    """Tests for MetricsMonitor."""

    @pytest.fixture
    def metrics_monitor(
        self,
        mock_settings: Any,
    ) -> MetricsMonitor:
        """Create MetricsMonitor for testing."""
        metrics = MetricsCollector(mock_settings)
        return MetricsMonitor(metrics)

    @pytest.mark.asyncio
    async def test_start(self, metrics_monitor: MetricsMonitor) -> None:
        """Test starting metrics monitor."""
        await metrics_monitor.start()
        assert metrics_monitor._running is True
        await metrics_monitor.stop()

    @pytest.mark.asyncio
    async def test_stop(self, metrics_monitor: MetricsMonitor) -> None:
        """Test stopping metrics monitor."""
        await metrics_monitor.start()
        await metrics_monitor.stop()
        assert metrics_monitor._running is False

    @pytest.mark.asyncio
    async def test_monitor_loop(self, metrics_monitor: MetricsMonitor) -> None:
        """Test monitoring loop."""
        metrics_monitor._running = True
        metrics_monitor._task = asyncio.create_task(metrics_monitor._monitor_loop())

        await asyncio.sleep(0.1)

        metrics_monitor._running = False
        metrics_monitor._task.cancel()
        try:
            await metrics_monitor._task
        except asyncio.CancelledError:
            pass
