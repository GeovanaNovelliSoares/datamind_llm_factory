"""Connector factory — routes to the right connector based on source type."""
from src.ingestion.connectors import APIConnector, BaseConnector, CSVConnector, SQLConnector
from src.logger import get_logger

logger = get_logger(__name__)


def build_connector(source_type: str, **kwargs) -> BaseConnector:
    """
    Factory that returns the appropriate connector.

    Args:
        source_type: 'csv' | 'sql' | 'api'
        **kwargs: connector-specific params

    Returns:
        Configured connector instance ready to call .fetch()
    """
    source_type = source_type.lower()

    if source_type == "csv":
        return CSVConnector(
            file_bytes=kwargs["file_bytes"],
            filename=kwargs["filename"],
        )
    elif source_type == "sql":
        return SQLConnector(
            connection_url=kwargs["connection_url"],
            query=kwargs["query"],
            source_name=kwargs.get("source_name", "sql"),
        )
    elif source_type == "api":
        return APIConnector(
            url=kwargs["url"],
            headers=kwargs.get("headers"),
            params=kwargs.get("params"),
            data_key=kwargs.get("data_key"),
            source_name=kwargs.get("source_name", "api"),
        )
    else:
        raise ValueError(f"Unknown source_type: {source_type}. Use 'csv', 'sql', or 'api'.")
