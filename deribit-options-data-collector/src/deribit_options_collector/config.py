"""Configuration management with YAML and environment variable support."""

import os
from functools import cached_property
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class DeribitConfig(BaseModel):
    """Deribit API configuration."""

    base_url: str = "https://www.deribit.com"
    ws_url: str = "wss://www.deribit.com/ws/api/v2"
    api_key: str = ""
    api_secret: str = ""
    timeout_seconds: int = 30
    max_retries: int = 3
    retry_base_delay: float = 1.0

    class RateLimit(BaseModel):
        """Rate limiting configuration."""

        requests_per_second: float = 20.0
        batch_delay_ms: int = 50

    rate_limit: RateLimit = Field(default_factory=RateLimit)


class CollectionConfig(BaseModel):
    """Data collection configuration."""

    currencies: list[str] = Field(default_factory=lambda: ["BTC", "ETH"])
    kind: str = "option"
    incremental_interval_seconds: int = 1
    snapshot_cron: str = "0 8 * * *"
    snapshot_depth: int = 20
    channels: list[str] = Field(
        default_factory=lambda: ["ticker", "book", "trades", "markprice", "greeks"]
    )


class ParquetConfig(BaseModel):
    """Parquet storage configuration."""

    base_path: str = "data/raw/option"
    compression: str = "snappy"
    block_size_mb: int = 128
    row_group_size: int = 100000

    @cached_property
    def block_size_bytes(self) -> int:
        """Get block size in bytes."""
        return self.block_size_mb * 1024 * 1024


class SQLiteConfig(BaseModel):
    """SQLite storage configuration."""

    path: str = "db/deribit_options.db"
    pool_size: int = 5
    timeout: float = 30.0
    check_same_thread: bool = False


class StorageConfig(BaseModel):
    """Storage configuration."""

    parquet: ParquetConfig = Field(default_factory=ParquetConfig)
    sqlite: SQLiteConfig = Field(default_factory=SQLiteConfig)


class MetricsLabels(BaseModel):
    """Prometheus metrics labels."""

    service: str = "deribit-options-collector"
    environment: str = "production"


class MetricsConfig(BaseModel):
    """Metrics and monitoring configuration."""

    enabled: bool = True
    port: int = 9090
    path: str = "/metrics"
    health_port: int = 8080
    health_path: str = "/health"
    labels: MetricsLabels = Field(default_factory=MetricsLabels)


class PagerDutyConfig(BaseModel):
    """PagerDuty alert configuration."""

    enabled: bool = False
    routing_key: str = ""
    severity: str = "critical"


class AlertsConfig(BaseModel):
    """Alerting configuration."""

    pagerduty: PagerDutyConfig = Field(default_factory=PagerDutyConfig)
    ws_disconnect_threshold_seconds: int = 30
    write_failure_threshold: int = 5


class LogRotation(BaseModel):
    """Log rotation configuration."""

    max_bytes: int = 100 * 1024 * 1024
    backup_count: int = 5


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"
    format: str = "json"
    output: str = "stdout"
    file_path: str = ""
    rotation: LogRotation = Field(default_factory=LogRotation)


class WhitelistConfig(BaseModel):
    """Instrument whitelist configuration."""

    instruments: list[str] = Field(default_factory=list)
    currencies: list[str] = Field(default_factory=lambda: ["BTC", "ETH"])


class Settings(BaseModel):
    """Main application settings."""

    deribit: DeribitConfig = Field(default_factory=DeribitConfig)
    collection: CollectionConfig = Field(default_factory=CollectionConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    whitelist: WhitelistConfig = Field(default_factory=WhitelistConfig)

    @field_validator("logging")
    @classmethod
    def validate_log_level(cls, v: Any) -> Any:
        """Validate log level."""
        if isinstance(v, LoggingConfig):
            level = v.level.upper()
            if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                v.level = "INFO"
        return v

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        """Load settings from YAML file."""
        path = Path(path)
        if not path.exists():
            return cls()
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)

    def apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        env_mappings: dict[str, str] = {
            "DERIBIT_API_KEY": "deribit.api_key",
            "DERIBIT_API_SECRET": "deribit.api_secret",
            "DERIBIT_BASE_URL": "deribit.base_url",
            "DERIBIT_WS_URL": "deribit.ws_url",
            "COLLECTION_CURRENCIES": "collection.currencies",
            "COLLECTION_INTERVAL": "collection.incremental_interval_seconds",
            "SNAPSHOT_CRON": "collection.snapshot_cron",
            "PARQUET_BASE_PATH": "storage.parquet.base_path",
            "PARQUET_COMPRESSION": "storage.parquet.compression",
            "SQLITE_PATH": "storage.sqlite.path",
            "METRICS_PORT": "metrics.port",
            "METRICS_ENABLED": "metrics.enabled",
            "LOG_LEVEL": "logging.level",
            "LOG_FILE_PATH": "logging.file_path",
            "PAGERDUTY_ENABLED": "alerts.pagerduty.enabled",
            "PAGERDUTY_ROUTING_KEY": "alerts.pagerduty.routing_key",
            "WS_DISCONNECT_THRESHOLD": "alerts.ws_disconnect_threshold_seconds",
            "WRITE_FAILURE_THRESHOLD": "alerts.write_failure_threshold",
        }

        for env_var, path in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                self._set_nested(path, value)

    def _set_nested(self, path: str, value: str) -> None:
        """Set a nested configuration value using dot notation."""
        parts = path.split(".")
        if len(parts) < 2:
            return

        obj: Any = self
        for part in parts[:-1]:
            obj = getattr(obj, part)

        final_part = parts[-1]
        current_value = getattr(obj, final_part, None)

        if isinstance(current_value, bool):
            setattr(obj, final_part, value.lower() in ("true", "1", "yes"))
        elif isinstance(current_value, int):
            try:
                setattr(obj, final_part, int(value))
            except ValueError:
                pass
        elif isinstance(current_value, float):
            try:
                setattr(obj, final_part, float(value))
            except ValueError:
                pass
        elif isinstance(current_value, list):
            if value.startswith("[") and value.endswith("]"):
                try:
                    setattr(obj, final_part, yaml.safe_load(value))
                except yaml.YAMLError:
                    pass
            else:
                setattr(obj, final_part, [v.strip() for v in value.split(",")])
        else:
            setattr(obj, final_part, value)

    def resolve_paths(self, base_dir: Path | None = None) -> None:
        """Resolve relative paths to absolute paths."""
        if base_dir is None:
            base_dir = Path.cwd()

        parquet_path = Path(self.storage.parquet.base_path)
        if not parquet_path.is_absolute():
            self.storage.parquet.base_path = str(base_dir / parquet_path)

        sqlite_path = Path(self.storage.sqlite.path)
        if not sqlite_path.is_absolute():
            self.storage.sqlite.path = str(base_dir / sqlite_path)

        if self.logging.file_path:
            log_path = Path(self.logging.file_path)
            if not log_path.is_absolute():
                self.logging.file_path = str(base_dir / log_path)

    def get_whitelisted_currencies(self) -> list[str]:
        """Get whitelisted currencies, falling back to collection config."""
        if self.whitelist.currencies:
            return self.whitelist.currencies
        return self.collection.currencies

    def get_whitelisted_instruments(self) -> list[str]:
        """Get whitelisted instruments."""
        return self.whitelist.instruments


_settings: Optional[Settings] = None


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Load settings from config file with environment overrides."""
    global _settings

    if config_path is None:
        config_path = os.environ.get(
            "COLLECTOR_CONFIG_PATH", "config/collector.yaml"
        )

    settings = Settings.from_yaml(config_path)
    settings.apply_env_overrides()
    settings.resolve_paths()

    _settings = settings
    return settings


def get_settings() -> Settings:
    """Get cached settings instance."""
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings
