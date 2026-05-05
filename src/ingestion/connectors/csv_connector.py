"""CSV and Excel connector."""
import io
from pathlib import Path

import pandas as pd

from src.ingestion.connectors.base import BaseConnector, ConnectorResult
from src.logger import get_logger

logger = get_logger(__name__)


class CSVConnector(BaseConnector):
    def __init__(self, file_bytes: bytes, filename: str):
        self.file_bytes = file_bytes
        self.filename = filename
        self.ext = Path(filename).suffix.lower()

    def connect(self) -> None:
        pass  # no persistent connection needed

    def validate_config(self) -> bool:
        return len(self.file_bytes) > 0 and self.ext in (".csv", ".xlsx", ".xls", ".tsv")

    def fetch(self, **kwargs) -> ConnectorResult:
        try:
            if self.ext == ".csv":
                df = pd.read_csv(
                    io.BytesIO(self.file_bytes),
                    sep=kwargs.get("sep", None),
                    engine="python",
                    on_bad_lines="warn",
                )
            elif self.ext == ".tsv":
                df = pd.read_csv(io.BytesIO(self.file_bytes), sep="\t")
            elif self.ext in (".xlsx", ".xls"):
                df = pd.read_excel(io.BytesIO(self.file_bytes), sheet_name=kwargs.get("sheet", 0))
            else:
                raise ValueError(f"Unsupported extension: {self.ext}")

            # Normalize column names
            df.columns = [
                c.strip().lower().replace(" ", "_").replace("-", "_")
                for c in df.columns
            ]

            logger.info("csv_fetched", filename=self.filename, rows=len(df), cols=len(df.columns))
            return ConnectorResult(
                df=df,
                source_name=self.filename,
                source_type="csv",
                metadata={"rows": len(df), "columns": list(df.columns), "filename": self.filename},
            )
        except Exception as e:
            logger.error("csv_fetch_error", filename=self.filename, error=str(e))
            raise
