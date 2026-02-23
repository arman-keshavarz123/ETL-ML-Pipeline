"""Tests for the decorator-based plugin registry."""

from __future__ import annotations

import pytest

from data_extractor.registry import (
    get_extractor,
    get_loader,
    get_transformer,
)


class TestRegistry:
    """Verify that all Phase 1-3 plugins are registered."""

    def test_rest_api_extractor_registered(self):
        cls = get_extractor("rest_api")
        assert cls.__name__ == "RESTAPIExtractor"

    def test_json_file_extractor_registered(self):
        cls = get_extractor("json_file")
        assert cls.__name__ == "JSONFileExtractor"

    def test_pass_through_transformer_registered(self):
        cls = get_transformer("pass_through")
        assert cls.__name__ == "PassThroughTransformer"

    def test_pydantic_validation_transformer_registered(self):
        cls = get_transformer("pydantic_validation")
        assert cls.__name__ == "PydanticValidationTransformer"

    def test_data_cleaning_transformer_registered(self):
        cls = get_transformer("data_cleaning")
        assert cls.__name__ == "DataCleaningTransformer"

    def test_json_local_loader_registered(self):
        cls = get_loader("json_local")
        assert cls.__name__ == "JSONLocalLoader"

    def test_playwright_scraper_extractor_registered(self):
        cls = get_extractor("playwright_scraper")
        assert cls.__name__ == "PlaywrightScraperExtractor"

    def test_sql_database_loader_registered(self):
        cls = get_loader("sql_database")
        assert cls.__name__ == "SQLAlchemyLoader"

    def test_unknown_extractor_raises(self):
        with pytest.raises(KeyError, match="Unknown extractor"):
            get_extractor("does_not_exist")

    def test_unknown_transformer_raises(self):
        with pytest.raises(KeyError, match="Unknown transformer"):
            get_transformer("does_not_exist")

    def test_unknown_loader_raises(self):
        with pytest.raises(KeyError, match="Unknown loader"):
            get_loader("does_not_exist")
