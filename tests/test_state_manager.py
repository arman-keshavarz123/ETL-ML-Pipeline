"""Tests for StateManager — cursor persistence for incremental loading."""

from __future__ import annotations

import json

import pytest

from data_extractor.state import StateManager


class TestStateManager:
    """Core get/save round-trip tests."""

    def test_missing_file_returns_none(self, tmp_path):
        sm = StateManager(tmp_path / "nonexistent.json")
        assert sm.get_cursor("my_pipeline") is None

    def test_save_and_get_round_trip(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("pipeline_a", 42)
        assert sm.get_cursor("pipeline_a") == 42

    def test_multiple_pipelines(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("p1", 10)
        sm.save_cursor("p2", 20)
        assert sm.get_cursor("p1") == 10
        assert sm.get_cursor("p2") == 20

    def test_overwrite_cursor(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("p", 100)
        sm.save_cursor("p", 200)
        assert sm.get_cursor("p") == 200

    def test_unknown_pipeline_returns_none(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("known", 1)
        assert sm.get_cursor("unknown") is None

    def test_atomic_write_produces_valid_json(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("p", "hello")
        data = json.loads(path.read_text())
        assert data == {"p": "hello"}

    def test_corrupted_file_returns_none(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("NOT VALID JSON {{{")
        sm = StateManager(path)
        assert sm.get_cursor("any") is None

    def test_creates_parent_directories(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "state.json"
        sm = StateManager(path)
        sm.save_cursor("p", 5)
        assert sm.get_cursor("p") == 5
        assert path.exists()

    def test_string_cursor(self, tmp_path):
        path = tmp_path / "state.json"
        sm = StateManager(path)
        sm.save_cursor("p", "2024-01-15T10:00:00")
        assert sm.get_cursor("p") == "2024-01-15T10:00:00"

    def test_non_dict_json_resets(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("[1, 2, 3]")
        sm = StateManager(path)
        assert sm.get_cursor("any") is None
