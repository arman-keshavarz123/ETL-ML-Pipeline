"""REST API extractor — pulls data from any JSON REST endpoint.

Supports pagination (page_param, link_header, or none), path-param
interpolation, and token auth via environment variables.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
import pandas as pd

from data_extractor.extractors.base import BaseExtractor
from data_extractor.registry import register_extractor

logger = logging.getLogger(__name__)


@register_extractor("rest_api")
class RESTAPIExtractor(BaseExtractor):
    """Fetch JSON data from a REST API and return it as a DataFrame."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None

    def connect(self) -> None:
        headers: dict[str, str] = dict(self._config.get("headers", {}))

        # Token auth from env var
        token_env = self._config.get("auth_token_env")
        if token_env:
            token = os.environ.get(token_env, "")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                logger.warning(
                    "auth_token_env=%r is set in config but the env var is empty/unset",
                    token_env,
                )

        base_url = self._config.get("base_url", "")
        self._client = httpx.Client(
            base_url=base_url,
            headers=headers,
            timeout=self._config.get("timeout", 30),
        )
        logger.info("Connected to %s", base_url or "(no base_url)")

    def extract(self) -> pd.DataFrame:
        if self._client is None:
            self.connect()

        endpoint: str = self._config["endpoint"]

        # Path-param interpolation (e.g. /orgs/{org}/repos)
        params = self._config.get("path_params", {})
        endpoint = endpoint.format(**params)

        pagination = self._config.get("pagination", "none")
        query = dict(self._config.get("query_params", {}))

        logger.info(
            "Extracting from endpoint=%r  pagination=%r", endpoint, pagination
        )

        if pagination == "page_param":
            return self._paginate_page_param(endpoint, query)
        elif pagination == "link_header":
            return self._paginate_link_header(endpoint, query)
        else:
            return self._single_request(endpoint, query)

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.info("Disconnected HTTP client")

    # ------------------------------------------------------------------
    # Pagination strategies
    # ------------------------------------------------------------------

    def _single_request(
        self, endpoint: str, query: dict[str, Any]
    ) -> pd.DataFrame:
        resp = self._client.get(endpoint, params=query)  # type: ignore[union-attr]
        resp.raise_for_status()
        data = resp.json()
        return pd.DataFrame(data if isinstance(data, list) else [data])

    def _paginate_page_param(
        self, endpoint: str, query: dict[str, Any]
    ) -> pd.DataFrame:
        page_key = self._config.get("page_param_name", "page")
        per_page_key = self._config.get("per_page_param_name", "per_page")
        per_page = self._config.get("per_page", 100)
        max_pages = self._config.get("max_pages", 10)

        frames: list[pd.DataFrame] = []
        for page in range(1, max_pages + 1):
            query[page_key] = page
            query[per_page_key] = per_page
            resp = self._client.get(endpoint, params=query)  # type: ignore[union-attr]
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            frames.append(pd.DataFrame(data if isinstance(data, list) else [data]))
            if len(data) < per_page:
                break
            logger.info("Fetched page %d (%d records)", page, len(data))
        total = sum(len(f) for f in frames) if frames else 0
        logger.info("page_param pagination complete — %d total records across %d pages", total, len(frames))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _paginate_link_header(
        self, endpoint: str, query: dict[str, Any]
    ) -> pd.DataFrame:
        max_pages = self._config.get("max_pages", 10)
        frames: list[pd.DataFrame] = []
        url: str | None = endpoint

        for page in range(1, max_pages + 1):
            resp = self._client.get(url, params=query if page == 1 else None)  # type: ignore[union-attr]
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            frames.append(pd.DataFrame(data if isinstance(data, list) else [data]))
            logger.info("Fetched page %d (%d records)", page, len(data))

            # Parse Link header for next URL
            url = self._parse_next_link(resp.headers.get("link", ""))
            if url is None:
                break

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    @staticmethod
    def _parse_next_link(link_header: str) -> str | None:
        """Extract the 'next' URL from a GitHub-style Link header."""
        for part in link_header.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                return url
        return None
