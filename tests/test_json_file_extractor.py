"""Tests for the JSONFileExtractor."""

from __future__ import annotations

from pathlib import Path

from data_extractor.extractors.json_file import JSONFileExtractor


class TestJSONFileExtractor:
    def test_reads_json(self, tmp_json_file: Path):
        ext = JSONFileExtractor({"file_path": str(tmp_json_file)})
        df = ext.extract()
        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "email"]
        assert df.iloc[0]["name"] == "Alice"

    def test_context_manager(self, tmp_json_file: Path):
        ext = JSONFileExtractor({"file_path": str(tmp_json_file)})
        with ext:
            df = ext.extract()
        assert len(df) == 2
