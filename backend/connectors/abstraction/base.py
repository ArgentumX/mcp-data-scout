"""Base connector interface for all data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class IndexingRules:
    include_tables: set[str] | None = None
    exclude_tables: set[str] | None = None
    include_columns: dict[str, set[str]] | None = None
    exclude_columns: dict[str, set[str]] | None = None
    row_value_tables: set[str] | None = None
    row_value_columns: dict[str, set[str]] | None = None

    def should_index_table(self, table_name: str) -> bool:
        if self.include_tables is not None and table_name not in self.include_tables:
            return False
        if self.exclude_tables is not None and table_name in self.exclude_tables:
            return False
        return True

    def should_index_column(self, table_name: str, column_name: str) -> bool:
        if not self.should_index_table(table_name):
            return False

        if self.include_columns is not None:
            allowed = self.include_columns.get(table_name)
            if allowed is not None and column_name not in allowed:
                return False

        if self.exclude_columns is not None:
            blocked = self.exclude_columns.get(table_name, set())
            if column_name in blocked:
                return False

        return True

    def should_index_row_values(self, table_name: str, column_name: str) -> bool:
        if not self.should_index_column(table_name, column_name):
            return False

        if self.row_value_tables is not None and table_name not in self.row_value_tables:
            return False

        if self.row_value_columns is None:
            return True

        allowed = self.row_value_columns.get(table_name)
        if allowed is None:
            return False
        return column_name in allowed


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

    def __init__(self, indexing_rules: IndexingRules | None = None) -> None:
        self.indexing_rules = indexing_rules or IndexingRules()

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

    def should_index_table(self, table_name: str) -> bool:
        return self.indexing_rules.should_index_table(table_name)

    def should_index_column(self, table_name: str, column_name: str) -> bool:
        return self.indexing_rules.should_index_column(table_name, column_name)

    def should_index_row_values(self, table_name: str, column_name: str) -> bool:
        return self.indexing_rules.should_index_row_values(table_name, column_name)
