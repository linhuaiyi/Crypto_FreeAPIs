"""Thread-safe singleton config loader."""
import os
import threading
from typing import Any, Dict, Optional

import yaml

from utils import get_logger

logger = get_logger("ConfigLoader")


class ConfigLoader:
    _instance: Optional["ConfigLoader"] = None
    _lock = threading.Lock()

    def __init__(self, config_path: str = "config_strategy.yaml") -> None:
        self._config_path = config_path
        self._data: Dict[str, Any] = {}
        self._load()

    @classmethod
    def get(cls, config_path: str = "config_strategy.yaml") -> "ConfigLoader":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(config_path)
        return cls._instance

    def _load(self) -> None:
        if not os.path.exists(self._config_path):
            logger.warning(f"Config file not found: {self._config_path}, using defaults")
            self._data = self._defaults()
            return
        with open(self._config_path, "r", encoding="utf-8") as f:
            self._data = yaml.safe_load(f) or {}
        logger.info(f"Config loaded from {self._config_path}")

    def reload(self) -> None:
        self._load()

    def get_value(self, *keys: str, default: Any = None) -> Any:
        """Get nested config value. E.g. get_value("storage", "chunked_buffer", "max_rows")"""
        current = self._data
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key, default)
            if current is None:
                return default
        return current

    @property
    def data(self) -> Dict[str, Any]:
        return dict(self._data)

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "global": {"data_dir": "./data", "log_level": "INFO"},
            "storage": {"chunked_buffer": {"max_rows": 100000, "max_memory_mb": 200, "flush_interval_sec": 300}},
            "processors": {"outlier_filter": {"z_threshold": 5.0, "window_size": 100}},
            "websocket": {"heartbeat_timeout_sec": 15, "max_instruments": 2000},
        }
