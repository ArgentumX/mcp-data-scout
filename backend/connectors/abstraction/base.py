"""Base connector interface for all data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    sample_values: list[Any] = field(default_factory=list)


@dataclass
class TableMeta:
    name: str
    source_id: str
    columns: list[ColumnMeta] = field(default_factory=list)
    row_count: int = 0
    path: str = ""  # logical path within source, e.g. "tablename" or "file.csv"


@dataclass
class SourceInfo:
    source_id: str
    source_type: str  # "sqlite" | "csv"
    description: str
    location: str  # file path or directory


class BaseConnector(ABC):
    """Abstract base class for data source connectors."""

    @abstractmethod
    def get_source_info(self) -> SourceInfo:
        """Return metadata about this source."""

    @abstractmethod
    def list_tables(self) -> list[TableMeta]:
        """Return list of tables/datasets with column metadata."""

    @abstractmethod
    def get_schema(self, path: str) -> TableMeta | None:
        """Return detailed schema for a specific table/file."""

    @abstractmethod
    def get_sample(self, path: str, limit: int = 5) -> list[dict[str, Any]]:
        """Return sample rows from a table/file."""
