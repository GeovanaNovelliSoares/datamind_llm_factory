"""PostgreSQL connector."""
import pandas as pd
from sqlalchemy import create_engine, text

from src.ingestion.connectors.base import BaseConnector, ConnectorResult
from src.logger import get_logger

logger = get_logger(__name__)


class SQLConnector(BaseConnector):
    def __init__(self, connection_url: str, query: str, source_name: str = "sql"):
        self.connection_url = connection_url
        self.query = query
        self.source_name = source_name
        self._engine = None

    def connect(self) -> None:
        self._engine = create_engine(self.connection_url, pool_pre_ping=True)

    def validate_config(self) -> bool:
        return bool(self.connection_url and self.query)

    def fetch(self, **kwargs) -> ConnectorResult:
        if not self._engine:
            self.connect()
        try:
            df = pd.read_sql(text(self.query), self._engine)
            df.columns = [
                c.strip().lower().replace(" ", "_").replace("-", "_")
                for c in df.columns
            ]
            logger.info("sql_fetched", source=self.source_name, rows=len(df))
            return ConnectorResult(
                df=df,
                source_name=self.source_name,
                source_type="sql",
                metadata={"rows": len(df), "columns": list(df.columns), "query": self.query[:200]},
            )
        except Exception as e:
            logger.error("sql_fetch_error", source=self.source_name, error=str(e))
            raise
        finally:
            if self._engine:
                self._engine.dispose()
