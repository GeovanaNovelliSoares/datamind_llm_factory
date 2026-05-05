"""Base connector interface."""
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class ConnectorResult:
    df: pd.DataFrame
    source_name: str
    source_type: str
    metadata: dict


class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def fetch(self, **kwargs) -> ConnectorResult: ...

    @abstractmethod
    def validate_config(self) -> bool: ...
