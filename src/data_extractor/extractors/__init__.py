"""Extractor subpackage — imports trigger @register_extractor decorators."""

from data_extractor.extractors.rest_api import RESTAPIExtractor  # noqa: F401
from data_extractor.extractors.json_file import JSONFileExtractor  # noqa: F401
from data_extractor.extractors.playwright_scraper import PlaywrightScraperExtractor  # noqa: F401
from data_extractor.extractors.alpha_vantage import AlphaVantageExtractor  # noqa: F401
