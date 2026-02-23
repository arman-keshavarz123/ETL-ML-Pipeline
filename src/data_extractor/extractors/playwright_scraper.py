"""Playwright web scraper extractor — scrapes structured data from web pages.

Uses CSS selectors to extract text content from matched elements and returns
the results as a DataFrame.  The entire browser lifecycle is packed into a
single ``asyncio.run()`` call so the synchronous engine never needs to manage
async resources.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

from data_extractor.extractors.base import BaseExtractor
from data_extractor.registry import register_extractor

logger = logging.getLogger(__name__)


@register_extractor("playwright_scraper")
class PlaywrightScraperExtractor(BaseExtractor):
    """Scrape a web page using headless Chromium and CSS selectors."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._url: str = config["url"]
        self._wait_for: str | None = config.get("wait_for")
        self._timeout: int = config.get("timeout", 30000)
        self._headless: bool = config.get("headless", True)
        self._selectors: list[dict[str, str]] = config["selectors"]

    def extract(self) -> pd.DataFrame:
        return asyncio.run(self._async_extract())

    async def _async_extract(self) -> pd.DataFrame:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self._headless)
            try:
                page = await browser.new_page()
                await page.goto(self._url, timeout=self._timeout)

                if self._wait_for:
                    await page.wait_for_selector(
                        self._wait_for, timeout=self._timeout
                    )

                data: dict[str, list[str]] = {}
                row_count: int | None = None

                for selector in self._selectors:
                    col_name = selector["name"]
                    css = selector["css"]
                    elements = await page.query_selector_all(css)
                    texts = [
                        (await el.text_content() or "").strip()
                        for el in elements
                    ]

                    if row_count is None:
                        row_count = len(texts)
                    elif len(texts) != row_count:
                        raise ValueError(
                            f"Selector {css!r} matched {len(texts)} elements, "
                            f"but previous selectors matched {row_count}. "
                            f"All selectors must match the same number of elements."
                        )

                    data[col_name] = texts

                logger.info(
                    "Scraped %d rows from %s", row_count or 0, self._url
                )
                return pd.DataFrame(data)
            finally:
                await browser.close()
