"""SQLite data source connector."""

import sqlite3
from pathlib import Path
from typing import Any

from connectors.abstraction.base import BaseConnector, ColumnMeta, SourceInfo, TableMeta


class SQLiteConnector(BaseConnector):
    def __init__(self, source_id: str, db_path: str, description: str = ""):
        self.source_id = source_id
        self.db_path = Path(db_path)
        self.description = description or f"SQLite database: {self.db_path.name}"

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_source_info(self) -> SourceInfo:
        return SourceInfo(
            source_id=self.source_id,
            source_type="sqlite",
            description=self.description,
            location=str(self.db_path),
        )

    def list_tables(self) -> list[TableMeta]:
        conn = self._connect()
        try:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            table_names = [row[0] for row in cursor.fetchall()]
            tables = []
            for name in table_names:
                meta = self._build_table_meta(conn, name)
                tables.append(meta)
            return tables
        finally:
            conn.close()

    def get_schema(self, path: str) -> TableMeta | None:
        conn = self._connect()
        try:
            # Check table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (path,)
            )
            if cursor.fetchone() is None:
                return None
            return self._build_table_meta(conn, path)
        finally:
            conn.close()

    def get_sample(self, path: str, limit: int = 5) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            cursor = conn.execute(f"SELECT * FROM [{path}] LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def _build_table_meta(self, conn: sqlite3.Connection, table_name: str) -> TableMeta:
        # Column info
        cursor = conn.execute(f"PRAGMA table_info([{table_name}])")
        col_rows = cursor.fetchall()

        # Row count
        count_row = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()
        row_count = count_row[0] if count_row else 0

        # Sample rows for sample values
        sample_rows = conn.execute(f"SELECT * FROM [{table_name}] LIMIT 3").fetchall()

        columns = []
        for col in col_rows:
            col_name = col["name"]
            col_type = col["type"] or "TEXT"
            sample_values = []
            for row in sample_rows:
                val = row[col_name]
                if val is not None:
                    sample_values.append(val)
            columns.append(
                ColumnMeta(
                    name=col_name,
                    data_type=col_type,
                    sample_values=sample_values[:3],
                )
            )

        return TableMeta(
            name=table_name,
            source_id=self.source_id,
            columns=columns,
            row_count=row_count,
            path=table_name,
        )
