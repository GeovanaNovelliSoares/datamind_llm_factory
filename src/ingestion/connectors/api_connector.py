"""REST API connector."""
from typing import Any

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from src.ingestion.connectors.base import BaseConnector, ConnectorResult
from src.logger import get_logger

logger = get_logger(__name__)


class APIConnector(BaseConnector):
    def __init__(
        self,
        url: str,
        headers: dict | None = None,
        params: dict | None = None,
        data_key: str | None = None,
        source_name: str = "api",
    ):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.data_key = data_key
        self.source_name = source_name

    def connect(self) -> None:
        pass

    def validate_config(self) -> bool:
        return bool(self.url)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_raw(self) -> Any:
        with httpx.Client(timeout=30) as client:
            resp = client.get(self.url, headers=self.headers, params=self.params)
            resp.raise_for_status()
            return resp.json()

    def fetch(self, **kwargs) -> ConnectorResult:
        try:
            raw = self._fetch_raw()

            # Extract nested data if key specified
            if self.data_key:
                keys = self.data_key.split(".")
                for k in keys:
                    raw = raw[k]

            if isinstance(raw, list):
                df = pd.json_normalize(raw)
            elif isinstance(raw, dict):
                df = pd.json_normalize([raw])
            else:
                raise ValueError(f"Cannot convert API response of type {type(raw)} to DataFrame")

            df.columns = [
                c.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
                for c in df.columns
            ]

            logger.info("api_fetched", url=self.url[:80], rows=len(df))
            return ConnectorResult(
                df=df,
                source_name=self.source_name,
                source_type="api",
                metadata={"rows": len(df), "columns": list(df.columns), "url": self.url},
            )
        except Exception as e:
            logger.error("api_fetch_error", url=self.url, error=str(e))
            raise
