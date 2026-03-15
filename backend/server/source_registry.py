"""
Source registry — holds all configured connectors.
Loaded from environment variables / config at startup.
"""

import os
from pathlib import Path

from connectors.abstraction.base import BaseConnector, IndexingRules, SourceInfo
from connectors.csv_connector import CSVConnector
from connectors.sqlite_connector import SQLiteConnector

DEFAULT_SQLITE_PATH = os.getenv("SQLITE_DB_PATH") or "/data/sqlite/sample.db"
DEFAULT_CSV_DIR = os.getenv("CSV_DIR") or "/data/csv"


SQLITE_INDEXING_RULES = IndexingRules(
    exclude_tables={"sqlite_sequence"},
    exclude_columns={
        "customers": {"customer_id"},
        "employees": {"employee_id", "manager_id"},
        "order_items": {"item_id", "order_id", "product_id"},
        "orders": {"order_id", "customer_id"},
        "products": {"product_id"},
    },
    row_value_tables={"customers", "employees", "orders", "products"},
    row_value_columns={
        "customers": {"first_name", "last_name", "email", "city", "signup_date"},
        "employees": {"full_name", "department", "position", "hire_date"},
        "orders": {"order_date", "status", "shipping_city"},
        "products": {"name", "category", "created_at"},
    },
)

CSV_INDEXING_RULES = IndexingRules(
    row_value_columns={
        "inventory_snapshot": {"product_name", "category", "warehouse", "last_restocked_date"},
        "marketing_campaigns": {"campaign_name", "channel", "start_date", "end_date"},
        "sales_regions": {"region", "month", "year"},
    },
)


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
                indexing_rules=SQLITE_INDEXING_RULES,
            )
        )

    csv_dir = Path(DEFAULT_CSV_DIR)
    if csv_dir.exists() and any(csv_dir.glob("*.csv")):
        registry.register(
            CSVConnector(
                source_id="csv_datasets",
                directory=str(csv_dir),
                description="CSV dataset files",
                indexing_rules=CSV_INDEXING_RULES,
            )
        )

    return registry
