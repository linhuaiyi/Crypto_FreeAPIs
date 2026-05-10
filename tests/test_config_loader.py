"""Tests for utils/config_loader.py — thread-safe singleton config."""

import os
import tempfile
import pytest
import yaml

from utils.config_loader import ConfigLoader


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton between tests."""
    ConfigLoader._instance = None
    yield
    ConfigLoader._instance = None


class TestSingleton:
    def test_get_returns_same_instance(self):
        a = ConfigLoader.get()
        b = ConfigLoader.get()
        assert a is b

    def test_get_with_missing_file_uses_defaults(self):
        loader = ConfigLoader.get("nonexistent.yaml")
        assert loader.get_value("global", "data_dir") == "./data"

    def test_get_with_real_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump({"global": {"data_dir": "/tmp/test"}}, f)
            f.flush()
            path = f.name

        try:
            loader = ConfigLoader.get(path)
            assert loader.get_value("global", "data_dir") == "/tmp/test"
        finally:
            os.unlink(path)


class TestGetValue:
    def test_nested_key(self):
        loader = ConfigLoader.__new__(ConfigLoader)
        loader._data = {"a": {"b": {"c": 42}}}
        assert loader.get_value("a", "b", "c") == 42

    def test_missing_key_returns_default(self):
        loader = ConfigLoader.__new__(ConfigLoader)
        loader._data = {"a": 1}
        assert loader.get_value("x", "y", default="fallback") == "fallback"

    def test_non_dict_intermediate(self):
        loader = ConfigLoader.__new__(ConfigLoader)
        loader._data = {"a": "string"}
        assert loader.get_value("a", "b", default=None) is None


class TestReload:
    def test_reload_picks_up_changes(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump({"key": "old"}, f)
            f.flush()
            path = f.name

        try:
            loader = ConfigLoader(path)
            assert loader.get_value("key") == "old"

            with open(path, "w") as f:
                yaml.dump({"key": "new"}, f)

            loader.reload()
            assert loader.get_value("key") == "new"
        finally:
            os.unlink(path)


class TestDataProperty:
    def test_returns_copy(self):
        loader = ConfigLoader.__new__(ConfigLoader)
        loader._data = {"a": 1}
        d = loader.data
        d["a"] = 999
        assert loader._data["a"] == 1
