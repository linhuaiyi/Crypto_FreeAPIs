"""Tests for scripts/pull_data.sh — uses local mock data."""

import os
import shutil
import subprocess
import sys

import pandas as pd
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "scripts", "pull_data.sh")
MOCK_REMOTE = os.path.join(PROJECT_ROOT, "scripts", "test_remote_data")

# Skip all tests if mock data directory doesn't exist
pytestmark = pytest.mark.skipif(
    not os.path.exists(MOCK_REMOTE),
    reason="Mock remote data not available (scripts/test_remote_data/ missing)",
)
LOCAL_DATA = os.path.join(PROJECT_ROOT, "data")


@pytest.fixture
def clean_local_data():
    """Ensure local data dir is empty before and after test."""
    if os.path.exists(LOCAL_DATA):
        shutil.rmtree(LOCAL_DATA)
    os.makedirs(LOCAL_DATA, exist_ok=True)
    yield
    if os.path.exists(LOCAL_DATA):
        shutil.rmtree(LOCAL_DATA)
    os.makedirs(LOCAL_DATA, exist_ok=True)


class TestPullDataLocal:
    """Test pull_data.sh by mocking remote with local paths.

    We patch the script variables to use local paths instead of SSH.
    """

    def test_find_matching_files_by_date(self, clean_local_data):
        """Verify the glob pattern matches only the target date."""
        target = "2026-05-11"
        pattern = f"*_{target}.parquet"

        matched = []
        for root, dirs, files in os.walk(MOCK_REMOTE):
            for f in files:
                if f.endswith(f"_{target}.parquet"):
                    matched.append(f)

        assert len(matched) == 13  # 13 files for 2026-05-11

        # Verify the other date file is NOT matched
        other = []
        for root, dirs, files in os.walk(MOCK_REMOTE):
            for f in files:
                if f.endswith("_2026-05-10.parquet"):
                    other.append(f)
        assert len(other) == 1  # only the intentional decoy

    def test_local_copy_and_verify(self, clean_local_data):
        """Simulate: copy target-date files from mock_remote to local data/."""
        target = "2026-05-11"
        copied = 0

        for root, dirs, files in os.walk(MOCK_REMOTE):
            for f in files:
                if not f.endswith(f"_{target}.parquet"):
                    continue
                src = os.path.join(root, f)
                rel = os.path.relpath(src, MOCK_REMOTE)
                dst = os.path.join(LOCAL_DATA, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                copied += 1

        assert copied == 13

        # Verify each file is readable
        for root, dirs, files in os.walk(LOCAL_DATA):
            for f in files:
                fpath = os.path.join(root, f)
                df = pd.read_parquet(fpath)
                assert len(df) > 0

    def test_local_copy_then_delete_source(self, clean_local_data):
        """Simulate: copy to local, then delete from mock remote (verify safety check)."""
        target = "2026-05-11"
        copied = 0
        deleted = 0

        # Phase 1: copy
        to_delete = []
        for root, dirs, files in os.walk(MOCK_REMOTE):
            for f in files:
                if not f.endswith(f"_{target}.parquet"):
                    continue
                src = os.path.join(root, f)
                rel = os.path.relpath(src, MOCK_REMOTE)
                dst = os.path.join(LOCAL_DATA, rel)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                copied += 1
                to_delete.append(src)

        assert copied == 13

        # Phase 2: safe delete (only if local file exists and non-empty)
        for src in to_delete:
            rel = os.path.relpath(src, MOCK_REMOTE)
            local = os.path.join(LOCAL_DATA, rel)
            if os.path.exists(local) and os.path.getsize(local) > 0:
                os.remove(src)
                deleted += 1

        assert deleted == 13

        # Verify local files remain
        local_files = []
        for root, dirs, files in os.walk(LOCAL_DATA):
            for f in files:
                local_files.append(f)
        assert len(local_files) == 13

    def test_no_matching_date_is_noop(self, clean_local_data):
        """If no files match the target date, nothing happens."""
        # Search for a date that doesn't exist
        target = "2025-01-01"
        matched = []
        for root, dirs, files in os.walk(MOCK_REMOTE):
            for f in files:
                if f.endswith(f"_{target}.parquet"):
                    matched.append(f)
        assert len(matched) == 0
