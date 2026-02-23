"""End-to-end pipeline tests using local JSON fixtures (no network)."""

from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import pytest

from data_extractor.engine import PipelineEngine


class TestPipelineEndToEnd:
    """Run full pipelines against local fixtures and verify output."""

    @pytest.fixture()
    def fixture_dir(self, tmp_path: Path) -> Path:
        """Set up a self-contained pipeline with configs and data in tmp_path."""
        # ---- source data ----
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        users = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob",   "email": "not-valid"},
            {"id": 3, "name": "Eve",   "email": "eve@test.com"},
        ]
        (data_dir / "users.json").write_text(json.dumps(users))

        # ---- configs ----
        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()

        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "users.json"}"\norient: "records"\n'
        )
        (cfg_dir / "validation.yaml").write_text(dedent("""\
            model: "data_extractor.schemas.user.User"
            chunk_size: 100
            strict: false
        """))
        (cfg_dir / "cleaning.yaml").write_text(dedent("""\
            strip_whitespace: true
            deduplicate: true
        """))

        out_path = tmp_path / "output" / "result.json"
        (cfg_dir / "loader.yaml").write_text(
            f'output_path: "{out_path}"\norient: "records"\nindent: 2\n'
        )

        # ---- pipeline config ----
        pipeline_yaml = dedent(f"""\
            version: "1.0"
            pipeline:
              name: "test_pipeline"
              description: "E2E test"
              extract:
                source: "json_file"
                config_file: "{cfg_dir / 'source.yaml'}"
              transform:
                - name: "pydantic_validation"
                  config_file: "{cfg_dir / 'validation.yaml'}"
                - name: "data_cleaning"
                  config_file: "{cfg_dir / 'cleaning.yaml'}"
              load:
                destination: "json_local"
                config_file: "{cfg_dir / 'loader.yaml'}"
            settings:
              log_level: "WARNING"
              retry:
                max_attempts: 1
                backoff_seconds: 0
              on_failure: "abort"
        """)
        (tmp_path / "pipeline.yaml").write_text(pipeline_yaml)

        return tmp_path

    def test_bad_records_dropped_good_saved(self, fixture_dir: Path):
        engine = PipelineEngine(fixture_dir / "pipeline.yaml")
        engine.run()

        out = fixture_dir / "output" / "result.json"
        assert out.exists()
        data = json.loads(out.read_text())
        # Bob (bad email) should be dropped
        assert len(data) == 2
        names = [r["name"] for r in data]
        assert "Alice" in names
        assert "Eve" in names
        assert "Bob" not in names

    def test_all_valid_records_pass(self, tmp_path: Path):
        """When all records are valid, none should be dropped."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        todos = [
            {"userId": 1, "id": 1, "title": "a", "completed": False},
            {"userId": 2, "id": 2, "title": "b", "completed": True},
        ]
        (data_dir / "todos.json").write_text(json.dumps(todos))

        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()

        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "todos.json"}"\n'
        )
        (cfg_dir / "validation.yaml").write_text(dedent("""\
            model: "data_extractor.schemas.todo.TodoItem"
        """))

        out_path = tmp_path / "output" / "todos_out.json"
        (cfg_dir / "loader.yaml").write_text(
            f'output_path: "{out_path}"\norient: "records"\n'
        )

        pipeline_yaml = dedent(f"""\
            version: "1.0"
            pipeline:
              name: "all_valid"
              extract:
                source: "json_file"
                config_file: "{cfg_dir / 'source.yaml'}"
              transform:
                - name: "pydantic_validation"
                  config_file: "{cfg_dir / 'validation.yaml'}"
              load:
                destination: "json_local"
                config_file: "{cfg_dir / 'loader.yaml'}"
            settings:
              log_level: "WARNING"
              retry:
                max_attempts: 1
                backoff_seconds: 0
        """)
        (tmp_path / "pipeline.yaml").write_text(pipeline_yaml)

        PipelineEngine(tmp_path / "pipeline.yaml").run()

        data = json.loads(out_path.read_text())
        assert len(data) == 2

    def test_pass_through_preserves_data(self, tmp_path: Path):
        """Pipeline with only pass_through should keep all data intact."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        records = [{"x": 1}, {"x": 2}, {"x": 3}]
        (data_dir / "input.json").write_text(json.dumps(records))

        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()

        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "input.json"}"\n'
        )
        out_path = tmp_path / "output" / "passthrough.json"
        (cfg_dir / "loader.yaml").write_text(
            f'output_path: "{out_path}"\norient: "records"\n'
        )

        pipeline_yaml = dedent(f"""\
            version: "1.0"
            pipeline:
              name: "passthrough_test"
              extract:
                source: "json_file"
                config_file: "{cfg_dir / 'source.yaml'}"
              transform:
                - name: "pass_through"
              load:
                destination: "json_local"
                config_file: "{cfg_dir / 'loader.yaml'}"
            settings:
              log_level: "WARNING"
              retry:
                max_attempts: 1
                backoff_seconds: 0
        """)
        (tmp_path / "pipeline.yaml").write_text(pipeline_yaml)

        PipelineEngine(tmp_path / "pipeline.yaml").run()

        data = json.loads(out_path.read_text())
        assert len(data) == 3
        assert [r["x"] for r in data] == [1, 2, 3]
