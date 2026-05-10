"""Tests for configuration management."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from deribit_options_collector.config import (
    Settings,
    DeribitConfig,
    CollectionConfig,
    ParquetConfig,
    SQLiteConfig,
    MetricsConfig,
    AlertsConfig,
    LoggingConfig,
    WhitelistConfig,
    load_settings,
    get_settings,
)


class TestSettings:
    """Tests for Settings model."""

    def test_default_settings(self) -> None:
        """Test creating default settings."""
        settings = Settings()
        assert settings.deribit.base_url == "https://www.deribit.com"
        assert settings.deribit.ws_url == "wss://www.deribit.com/ws/api/v2"
        assert settings.collection.currencies == ["BTC", "ETH"]
        assert settings.storage.parquet.compression == "snappy"
        assert settings.metrics.enabled is True

    def test_settings_from_dict(self) -> None:
        """Test creating settings from dictionary."""
        data = {
            "deribit": {
                "base_url": "https://custom.deribit.com",
                "api_key": "test_key",
            },
            "collection": {
                "currencies": ["SOL"],
                "incremental_interval_seconds": 5,
            },
        }
        settings = Settings.model_validate(data)
        assert settings.deribit.base_url == "https://custom.deribit.com"
        assert settings.deribit.api_key == "test_key"
        assert settings.collection.currencies == ["SOL"]
        assert settings.collection.incremental_interval_seconds == 5

    def test_nested_config_access(self) -> None:
        """Test accessing nested configuration."""
        settings = Settings()
        assert settings.deribit.rate_limit.requests_per_second == 20.0
        assert settings.storage.parquet.block_size_mb == 128

    def test_settings_equality(self) -> None:
        """Test settings equality."""
        settings1 = Settings()
        settings2 = Settings()
        assert settings1 == settings2


class TestDeribitConfig:
    """Tests for DeribitConfig."""

    def test_default_deribit_config(self) -> None:
        """Test default Deribit configuration."""
        config = DeribitConfig()
        assert config.base_url == "https://www.deribit.com"
        assert config.ws_url == "wss://www.deribit.com/ws/api/v2"
        assert config.timeout_seconds == 30
        assert config.max_retries == 3

    def test_custom_deribit_config(self) -> None:
        """Test custom Deribit configuration."""
        config = DeribitConfig(
            base_url="https://test.com",
            api_key="key123",
            max_retries=5,
        )
        assert config.base_url == "https://test.com"
        assert config.api_key == "key123"
        assert config.max_retries == 5


class TestCollectionConfig:
    """Tests for CollectionConfig."""

    def test_default_collection_config(self) -> None:
        """Test default collection configuration."""
        config = CollectionConfig()
        assert config.currencies == ["BTC", "ETH"]
        assert config.kind == "option"
        assert config.incremental_interval_seconds == 1
        assert config.snapshot_depth == 20

    def test_custom_channels(self) -> None:
        """Test custom channels configuration."""
        config = CollectionConfig(channels=["ticker", "greeks"])
        assert len(config.channels) == 2
        assert "ticker" in config.channels
        assert "greeks" in config.channels


class TestParquetConfig:
    """Tests for ParquetConfig."""

    def test_default_parquet_config(self) -> None:
        """Test default Parquet configuration."""
        config = ParquetConfig()
        assert config.base_path == "data/raw/option"
        assert config.compression == "snappy"
        assert config.block_size_mb == 128

    def test_block_size_bytes_property(self) -> None:
        """Test block size in bytes calculation."""
        config = ParquetConfig(block_size_mb=64)
        assert config.block_size_bytes == 64 * 1024 * 1024


class TestSQLiteConfig:
    """Test SQLiteConfig."""

    def test_default_sqlite_config(self) -> None:
        """Test default SQLite configuration."""
        config = SQLiteConfig()
        assert config.path == "db/deribit_options.db"
        assert config.pool_size == 5
        assert config.timeout == 30.0


class TestMetricsConfig:
    """Tests for MetricsConfig."""

    def test_default_metrics_config(self) -> None:
        """Test default metrics configuration."""
        config = MetricsConfig()
        assert config.enabled is True
        assert config.port == 9090
        assert config.health_port == 8080
        assert config.path == "/metrics"


class TestAlertsConfig:
    """Tests for AlertsConfig."""

    def test_default_alerts_config(self) -> None:
        """Test default alerts configuration."""
        config = AlertsConfig()
        assert config.ws_disconnect_threshold_seconds == 30
        assert config.write_failure_threshold == 5
        assert config.pagerduty.enabled is False


class TestLoggingConfig:
    """Tests for LoggingConfig."""

    def test_default_logging_config(self) -> None:
        """Test default logging configuration."""
        config = LoggingConfig()
        assert config.level == "INFO"
        assert config.format == "json"
        assert config.output == "stdout"


class TestWhitelistConfig:
    """Tests for WhitelistConfig."""

    def test_default_whitelist_config(self) -> None:
        """Test default whitelist configuration."""
        config = WhitelistConfig()
        assert config.instruments == []
        assert config.currencies == ["BTC", "ETH"]


class TestSettingsMethods:
    """Tests for Settings methods."""

    def test_get_whitelisted_currencies_with_whitelist(self) -> None:
        """Test getting whitelisted currencies."""
        settings = Settings(
            whitelist=WhitelistConfig(currencies=["ETH", "SOL"])
        )
        assert settings.get_whitelisted_currencies() == ["ETH", "SOL"]

    def test_get_whitelisted_currencies_fallback(self) -> None:
        """Test fallback to collection currencies."""
        settings = Settings()
        assert settings.get_whitelisted_currencies() == ["BTC", "ETH"]

    def test_get_whitelisted_instruments(self) -> None:
        """Test getting whitelisted instruments."""
        settings = Settings(
            whitelist=WhitelistConfig(
                instruments=["BTC-28MAR26-80000-C"]
            )
        )
        assert settings.get_whitelisted_instruments() == ["BTC-28MAR26-80000-C"]

    def test_resolve_paths_relative(self, temp_dir: Path) -> None:
        """Test resolving relative paths to absolute."""
        settings = Settings()
        settings.resolve_paths(temp_dir)

        parquet_path = Path(settings.storage.parquet.base_path)
        assert parquet_path.is_absolute()

        sqlite_path = Path(settings.storage.sqlite.path)
        assert sqlite_path.is_absolute()

    def test_resolve_paths_absolute(self, temp_dir: Path) -> None:
        """Test that absolute paths remain unchanged."""
        settings = Settings(
            storage=MagicMock(
                parquet=MagicMock(base_path=str(temp_dir / "parquet")),
                sqlite=MagicMock(path=str(temp_dir / "db" / "test.db")),
            ),
        )
        settings.resolve_paths(temp_dir)
        assert Path(settings.storage.parquet.base_path).is_absolute()


class TestEnvOverrides:
    """Tests for environment variable overrides."""

    def test_env_override_api_key(self) -> None:
        """Test API key override from environment."""
        with patch.dict(os.environ, {"DERIBIT_API_KEY": "env_key"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.deribit.api_key == "env_key"

    def test_env_override_log_level(self) -> None:
        """Test log level override from environment."""
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.logging.level == "DEBUG"

    def test_env_override_metrics_port(self) -> None:
        """Test metrics port override from environment."""
        with patch.dict(os.environ, {"METRICS_PORT": "9091"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.metrics.port == 9091

    def test_env_override_collection_interval(self) -> None:
        """Test collection interval override from environment."""
        with patch.dict(os.environ, {"COLLECTION_INTERVAL": "5"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.collection.incremental_interval_seconds == 5

    def test_env_override_currencies(self) -> None:
        """Test currencies override from environment."""
        with patch.dict(os.environ, {"COLLECTION_CURRENCIES": "BTC,ETH,SOL"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.collection.currencies == ["BTC", "ETH", "SOL"]

    def test_env_override_boolean_true(self) -> None:
        """Test boolean override with true value."""
        with patch.dict(os.environ, {"METRICS_ENABLED": "true"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.metrics.enabled is True

    def test_env_override_boolean_false(self) -> None:
        """Test boolean override with false value."""
        with patch.dict(os.environ, {"METRICS_ENABLED": "false"}, clear=False):
            settings = Settings()
            settings.apply_env_overrides()
            assert settings.metrics.enabled is False


class TestLoadSettings:
    """Tests for load_settings function."""

    def test_load_from_nonexistent_file(self) -> None:
        """Test loading from non-existent file returns defaults."""
        settings = load_settings("/nonexistent/path.yaml")
        assert settings.deribit.base_url == "https://www.deribit.com"

    def test_load_from_yaml_file(self, temp_dir: Path) -> None:
        """Test loading from YAML file."""
        config_path = temp_dir / "test_config.yaml"
        config_data = {
            "deribit": {
                "base_url": "https://custom.com",
                "api_key": "test_key",
            },
            "collection": {
                "currencies": ["SOL"],
            },
        }
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        settings = load_settings(config_path)
        assert settings.deribit.base_url == "https://custom.com"
        assert settings.deribit.api_key == "test_key"
        assert settings.collection.currencies == ["SOL"]

    def test_load_settings_with_env_var_path(self, temp_dir: Path) -> None:
        """Test loading settings using environment variable path."""
        config_path = temp_dir / "env_config.yaml"
        config_data = {"logging": {"level": "WARNING"}}
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        with patch.dict(os.environ, {"COLLECTOR_CONFIG_PATH": str(config_path)}, clear=False):
            settings = load_settings()
            assert settings.logging.level == "WARNING"

    def test_get_settings_caches(self) -> None:
        """Test that get_settings caches settings."""
        from deribit_options_collector.config import _settings

        settings1 = get_settings()
        settings2 = get_settings()
        assert settings1 is settings2


class TestSettingsValidation:
    """Tests for Settings validation."""

    def test_invalid_log_level_defaults_to_info(self) -> None:
        """Test that invalid log level defaults to INFO."""
        settings = Settings()
        with patch.object(settings, "logging") as mock_logging:
            mock_logging.level = "INVALID"
            mock_logging_copy = LoggingConfig(level="INVALID")
            Settings.model_validate({"logging": {"level": "INVALID"}})
