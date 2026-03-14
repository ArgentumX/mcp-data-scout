"""
Source registry — holds all configured connectors.
Loaded from environment variables / config at startup.
"""

import os
from pathlib import Path

from connectors.base import BaseConnector, SourceInfo
from connectors.csv_connector import CSVConnector
from connectors.sqlite_connector import SQLiteConnector

# Defaults for Docker Compose environment
DEFAULT_SQLITE_PATH = os.getenv("SQLITE_DB_PATH", "/data/sqlite/sample.db")
DEFAULT_CSV_DIR = os.getenv("CSV_DIR", "/data/csv")


class SourceRegistry:
    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {}

    def register(self, connector: BaseConnector) -> None:
        info = connector.get_source_info()
        self._connectors[info.source_id] = connector

    def get(self, source_id: str) -> BaseConnector | None:
        return self._connectors.get(source_id)

    def all(self) -> list[BaseConnector]:
        return list(self._connectors.values())

    def list_sources(self) -> list[SourceInfo]:
        return [c.get_source_info() for c in self._connectors.values()]


def build_default_registry() -> SourceRegistry:
    """Build registry from environment configuration."""
    registry = SourceRegistry()

    sqlite_path = Path(DEFAULT_SQLITE_PATH)
    if sqlite_path.exists():
        registry.register(
            SQLiteConnector(
                source_id="sqlite_main",
                db_path=str(sqlite_path),
                description="Main SQLite database with business data",
            )
        )

    csv_dir = Path(DEFAULT_CSV_DIR)
    if csv_dir.exists() and any(csv_dir.glob("*.csv")):
        registry.register(
            CSVConnector(
                source_id="csv_datasets",
                directory=str(csv_dir),
                description="CSV dataset files",
            )
        )

    return registry
