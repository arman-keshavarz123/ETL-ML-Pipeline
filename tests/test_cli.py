"""Tests for CLI (__main__.py) and full-pipeline integration."""

from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import pytest

from data_extractor.__main__ import main
from data_extractor.engine import PipelineEngine
from data_extractor.registry import list_registered
from data_extractor.state import StateManager


# ── Helpers ─────────────────────────────────────────────────────────


def _make_json_pipeline_yaml(
    tmp_path: Path,
    *,
    pipeline_name: str = "test_pipe",
    data: list[dict] | None = None,
    incremental: dict | None = None,
) -> Path:
    """Create a self-contained JSON->JSON pipeline in *tmp_path*.

    Returns the path to the pipeline YAML file.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    records = data or [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    (data_dir / "input.json").write_text(json.dumps(records))

    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir(exist_ok=True)
    (cfg_dir / "source.yaml").write_text(
        f'file_path: "{data_dir / "input.json"}"\norient: "records"\n'
    )

    out_path = tmp_path / "output" / "result.json"
    (cfg_dir / "loader.yaml").write_text(
        f'output_path: "{out_path}"\norient: "records"\nindent: 2\n'
    )

    incremental_block = ""
    if incremental:
        lines = [
            f'  incremental:',
            f'    cursor_field: "{incremental["cursor_field"]}"',
            f'    cursor_param: "{incremental["cursor_param"]}"',
        ]
        if "initial_value" in incremental:
            lines.append(f'    initial_value: {incremental["initial_value"]}')
        incremental_block = "\n".join(lines) + "\n"

    state_path = tmp_path / "state.json"
    pipeline_yaml = (
        f'version: "1.0"\n'
        f"pipeline:\n"
        f'  name: "{pipeline_name}"\n'
        f"  extract:\n"
        f'    source: "json_file"\n'
        f'    config_file: "{cfg_dir / "source.yaml"}"\n'
        f"  transform:\n"
        f'    - name: "pass_through"\n'
        f"  load:\n"
        f'    destination: "json_local"\n'
        f'    config_file: "{cfg_dir / "loader.yaml"}"\n'
        f"{incremental_block}"
        f"settings:\n"
        f'  log_level: "WARNING"\n'
        f"  retry:\n"
        f"    max_attempts: 1\n"
        f"    backoff_seconds: 0\n"
        f'  state_file: "{state_path}"\n'
    )
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(pipeline_yaml)
    return config_path


# =====================================================================
# CLI argument parsing
# =====================================================================


class TestCLIArgParsing:
    """Verify argparse wiring and validation."""

    def test_list_modules_flag(self, capsys):
        """--list-modules prints modules and exits without needing --config."""
        main(["--list-modules"])
        captured = capsys.readouterr()
        assert "EXTRACTORS" in captured.out
        assert "TRANSFORMERS" in captured.out
        assert "LOADERS" in captured.out

    def test_list_modules_short_flag(self, capsys):
        """-l also works."""
        main(["-l"])
        captured = capsys.readouterr()
        assert "EXTRACTORS" in captured.out

    def test_list_modules_shows_known_modules(self, capsys):
        """All registered modules should appear in the output."""
        main(["--list-modules"])
        captured = capsys.readouterr()
        assert "rest_api" in captured.out
        assert "json_file" in captured.out
        assert "pass_through" in captured.out
        assert "pydantic_validation" in captured.out
        assert "data_cleaning" in captured.out
        assert "json_local" in captured.out
        assert "sql_database" in captured.out
        assert "playwright_scraper" in captured.out

    def test_missing_config_without_list_modules_errors(self):
        """Running without --config (and without --list-modules) should error."""
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code != 0

    def test_config_short_flag(self, tmp_path):
        """-c should work the same as --config."""
        config_path = _make_json_pipeline_yaml(tmp_path)
        main(["-c", str(config_path)])
        out = tmp_path / "output" / "result.json"
        assert out.exists()

    def test_config_long_flag(self, tmp_path):
        """--config runs the pipeline."""
        config_path = _make_json_pipeline_yaml(tmp_path)
        main(["--config", str(config_path)])
        out = tmp_path / "output" / "result.json"
        assert out.exists()

    def test_list_modules_ignores_config(self, capsys):
        """When --list-modules is passed, --config is ignored (no pipeline runs)."""
        main(["--list-modules", "--config", "nonexistent.yaml"])
        captured = capsys.readouterr()
        assert "EXTRACTORS" in captured.out


# =====================================================================
# list_registered() function
# =====================================================================


class TestListRegistered:
    """Direct tests for the registry listing helper."""

    def test_returns_three_categories(self):
        result = list_registered()
        assert set(result.keys()) == {"extractors", "transformers", "loaders"}

    def test_extractors_contain_expected_keys(self):
        result = list_registered()
        assert "rest_api" in result["extractors"]
        assert "json_file" in result["extractors"]
        assert "playwright_scraper" in result["extractors"]

    def test_transformers_contain_expected_keys(self):
        result = list_registered()
        assert "pass_through" in result["transformers"]
        assert "pydantic_validation" in result["transformers"]
        assert "data_cleaning" in result["transformers"]

    def test_loaders_contain_expected_keys(self):
        result = list_registered()
        assert "json_local" in result["loaders"]
        assert "sql_database" in result["loaders"]

    def test_values_are_class_names(self):
        result = list_registered()
        assert result["extractors"]["rest_api"] == "RESTAPIExtractor"
        assert result["loaders"]["json_local"] == "JSONLocalLoader"


# =====================================================================
# --full-refresh flag
# =====================================================================


class TestFullRefresh:
    """Verify --full-refresh bypasses cursor but still saves new cursor."""

    def test_full_refresh_ignores_stored_cursor(self, tmp_path):
        """With a stored cursor, --full-refresh should use initial_value instead."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="incr_pipe",
            data=[{"id": 10, "val": "x"}, {"id": 20, "val": "y"}],
            incremental={"cursor_field": "id", "cursor_param": "since_id"},
        )

        # First run — saves cursor (max id = 20)
        engine = PipelineEngine(config_path)
        engine.run()

        state_path = tmp_path / "state.json"
        state = json.loads(state_path.read_text())
        assert state["incr_pipe"] == 20

        # Manually set a cursor to simulate a prior run
        StateManager(state_path).save_cursor("incr_pipe", 999)
        state = json.loads(state_path.read_text())
        assert state["incr_pipe"] == 999

        # Run with full_refresh — should ignore 999 and still produce output
        engine2 = PipelineEngine(config_path)
        engine2.run(full_refresh=True)

        # Cursor should be updated to 20 (max of extracted data), not 999
        state = json.loads(state_path.read_text())
        assert state["incr_pipe"] == 20

    def test_full_refresh_still_saves_cursor(self, tmp_path):
        """--full-refresh should save the new cursor after load."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="refresh_pipe",
            data=[{"id": 5, "val": "a"}, {"id": 15, "val": "b"}],
            incremental={"cursor_field": "id", "cursor_param": "since_id"},
        )

        engine = PipelineEngine(config_path)
        engine.run(full_refresh=True)

        state_path = tmp_path / "state.json"
        state = json.loads(state_path.read_text())
        assert state["refresh_pipe"] == 15

    def test_full_refresh_via_cli(self, tmp_path):
        """CLI -f flag wires through to engine.run(full_refresh=True)."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="cli_refresh",
            data=[{"id": 3, "val": "z"}],
            incremental={"cursor_field": "id", "cursor_param": "since_id"},
        )

        # Pre-seed cursor
        state_path = tmp_path / "state.json"
        StateManager(state_path).save_cursor("cli_refresh", 888)

        main(["-c", str(config_path), "-f"])

        # Cursor should be overwritten with 3 (max of extracted data)
        state = json.loads(state_path.read_text())
        assert state["cli_refresh"] == 3

    def test_full_refresh_with_initial_value(self, tmp_path):
        """When full_refresh is True, cursor_value should be initial_value."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="init_val_pipe",
            data=[{"id": 50, "val": "q"}],
            incremental={
                "cursor_field": "id",
                "cursor_param": "since_id",
                "initial_value": 0,
            },
        )

        # Pre-seed cursor to something non-zero
        state_path = tmp_path / "state.json"
        StateManager(state_path).save_cursor("init_val_pipe", 42)

        engine = PipelineEngine(config_path)
        engine.run(full_refresh=True)

        # After full_refresh, cursor should be 50 (max of data), not 42
        state = json.loads(state_path.read_text())
        assert state["init_val_pipe"] == 50

    def test_full_refresh_no_incremental_is_noop(self, tmp_path):
        """--full-refresh on a non-incremental pipeline runs normally."""
        config_path = _make_json_pipeline_yaml(tmp_path)
        main(["-c", str(config_path), "--full-refresh"])
        out = tmp_path / "output" / "result.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert len(data) == 2


# =====================================================================
# Pipeline E2E with incremental + state
# =====================================================================


class TestIncrementalPipelineE2E:
    """Full-pipeline tests for incremental cursor tracking."""

    def test_first_run_saves_cursor(self, tmp_path):
        """First run with incremental config should save cursor."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="first_run",
            data=[{"id": 1, "val": "a"}, {"id": 5, "val": "b"}],
            incremental={"cursor_field": "id", "cursor_param": "since_id"},
        )

        engine = PipelineEngine(config_path)
        engine.run()

        state_path = tmp_path / "state.json"
        assert state_path.exists()
        state = json.loads(state_path.read_text())
        assert state["first_run"] == 5

    def test_second_run_uses_stored_cursor(self, tmp_path):
        """Second run should pick up stored cursor and still save new one."""
        config_path = _make_json_pipeline_yaml(
            tmp_path,
            pipeline_name="second_run",
            data=[{"id": 10, "val": "x"}, {"id": 20, "val": "y"}],
            incremental={"cursor_field": "id", "cursor_param": "since_id"},
        )

        # First run
        engine = PipelineEngine(config_path)
        engine.run()

        # Second run with same data — cursor should still be 20
        engine2 = PipelineEngine(config_path)
        engine2.run()

        state_path = tmp_path / "state.json"
        state = json.loads(state_path.read_text())
        assert state["second_run"] == 20

    def test_no_incremental_no_state_file(self, tmp_path):
        """Without incremental config, no state file should be created."""
        config_path = _make_json_pipeline_yaml(tmp_path)
        engine = PipelineEngine(config_path)
        engine.run()

        state_path = tmp_path / "state.json"
        assert not state_path.exists()

    def test_cursor_not_saved_on_load_failure(self, tmp_path):
        """If the load step fails, cursor should NOT be updated."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        records = [{"id": 1, "val": "a"}, {"id": 5, "val": "b"}]
        (data_dir / "input.json").write_text(json.dumps(records))

        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()
        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "input.json"}"\norient: "records"\n'
        )

        # Create a *file* at the parent path so mkdir inside json_local's
        # connect() raises NotADirectoryError when it tries to create
        # "blocker/deep/result.json".
        blocker = tmp_path / "blocker"
        blocker.write_text("I am a file, not a directory")
        bad_out = blocker / "deep" / "result.json"
        (cfg_dir / "loader.yaml").write_text(
            f'output_path: "{bad_out}"\norient: "records"\n'
        )

        state_path = tmp_path / "state.json"
        pipeline_yaml = (
            f'version: "1.0"\n'
            f"pipeline:\n"
            f'  name: "fail_pipe"\n'
            f"  extract:\n"
            f'    source: "json_file"\n'
            f'    config_file: "{cfg_dir / "source.yaml"}"\n'
            f"  transform: []\n"
            f"  load:\n"
            f'    destination: "json_local"\n'
            f'    config_file: "{cfg_dir / "loader.yaml"}"\n'
            f"  incremental:\n"
            f'    cursor_field: "id"\n'
            f'    cursor_param: "since_id"\n'
            f"settings:\n"
            f'  log_level: "WARNING"\n'
            f"  retry:\n"
            f"    max_attempts: 1\n"
            f"    backoff_seconds: 0\n"
            f'  state_file: "{state_path}"\n'
        )
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(pipeline_yaml)

        with pytest.raises(Exception):
            PipelineEngine(config_path).run()

        # State should NOT have been created/updated
        assert not state_path.exists()


# =====================================================================
# Pipeline E2E with SQL upsert
# =====================================================================


class TestSQLUpsertPipelineE2E:
    """Full pipeline with SQL upsert loader."""

    def test_upsert_via_pipeline(self, tmp_path):
        """Run a pipeline that uses sql_database upsert loader."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        (data_dir / "input.json").write_text(json.dumps(records))

        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()
        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "input.json"}"\norient: "records"\n'
        )

        out_dir = tmp_path / "output"
        out_dir.mkdir(exist_ok=True)
        db_path = out_dir / "test.db"
        (cfg_dir / "loader.yaml").write_text(dedent(f"""\
            connection_string: "sqlite:///{db_path}"
            table_name: "people"
            if_exists: "upsert"
            primary_keys: ["id"]
            index: false
        """))

        pipeline_yaml = (
            f'version: "1.0"\n'
            f"pipeline:\n"
            f'  name: "upsert_e2e"\n'
            f"  extract:\n"
            f'    source: "json_file"\n'
            f'    config_file: "{cfg_dir / "source.yaml"}"\n'
            f"  transform: []\n"
            f"  load:\n"
            f'    destination: "sql_database"\n'
            f'    config_file: "{cfg_dir / "loader.yaml"}"\n'
            f"settings:\n"
            f'  log_level: "WARNING"\n'
            f"  retry:\n"
            f"    max_attempts: 1\n"
            f"    backoff_seconds: 0\n"
        )
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(pipeline_yaml)

        PipelineEngine(config_path).run()

        # Verify data was written
        from sqlalchemy import create_engine, text
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM people ORDER BY id")).fetchall()
        assert len(rows) == 2
        assert rows[0] == (1, "alice")
        assert rows[1] == (2, "bob")

        # Now run again with updated data
        records2 = [{"id": 2, "name": "BOB_UPDATED"}, {"id": 3, "name": "charlie"}]
        (data_dir / "input.json").write_text(json.dumps(records2))

        PipelineEngine(config_path).run()

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM people ORDER BY id")).fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "alice")
        assert rows[1] == (2, "BOB_UPDATED")
        assert rows[2] == (3, "charlie")
