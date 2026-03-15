"""Metadata and row-value indexer using SQLite FTS5."""

import json
import sqlite3
from pathlib import Path
from typing import Any

from connectors.abstraction.base import BaseConnector, TableMeta


DEFAULT_INDEX_DB_PATH = Path("/data/index.db")


class MetadataIndexer:
    def __init__(self, index_db: str | Path = DEFAULT_INDEX_DB_PATH):
        self.index_db = Path(index_db)
        self.index_db.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.index_db)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            # Main metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_meta (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    path TEXT NOT NULL,
                    row_count INTEGER DEFAULT 0,
                    columns_json TEXT NOT NULL,
                    indexed_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(source_id, path)
                )
            """)

            # FTS5 virtual table for full-text search
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS table_fts USING fts5(
                    source_id,
                    source_type,
                    table_name,
                    path,
                    columns_text,
                    content='table_meta',
                    content_rowid='id'
                )
            """)

            # Triggers to keep FTS in sync
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS table_meta_ai AFTER INSERT ON table_meta BEGIN
                    INSERT INTO table_fts(rowid, source_id, source_type, table_name, path, columns_text)
                    VALUES (new.id, new.source_id, new.source_type, new.table_name, new.path,
                            new.table_name || ' ' || new.source_id);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS table_meta_ad AFTER DELETE ON table_meta BEGIN
                    INSERT INTO table_fts(table_fts, rowid, source_id, source_type, table_name, path, columns_text)
                    VALUES ('delete', old.id, old.source_id, old.source_type, old.table_name, old.path,
                            old.table_name || ' ' || old.source_id);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS table_meta_au AFTER UPDATE ON table_meta BEGIN
                    INSERT INTO table_fts(table_fts, rowid, source_id, source_type, table_name, path, columns_text)
                    VALUES ('delete', old.id, old.source_id, old.source_type, old.table_name, old.path,
                            old.table_name || ' ' || old.source_id);
                    INSERT INTO table_fts(rowid, source_id, source_type, table_name, path, columns_text)
                    VALUES (new.id, new.source_id, new.source_type, new.table_name, new.path,
                            new.table_name || ' ' || new.source_id);
                END
            """)

            # Column-level FTS
            conn.execute("""
                CREATE TABLE IF NOT EXISTS column_meta (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    table_path TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    sample_values TEXT NOT NULL,
                    UNIQUE(source_id, table_path, column_name)
                )
            """)
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS column_fts USING fts5(
                    source_id,
                    table_name,
                    column_name,
                    data_type,
                    content='column_meta',
                    content_rowid='id'
                )
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS column_meta_ai AFTER INSERT ON column_meta BEGIN
                    INSERT INTO column_fts(rowid, source_id, table_name, column_name, data_type)
                    VALUES (new.id, new.source_id, new.table_name, new.column_name, new.data_type);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS column_meta_ad AFTER DELETE ON column_meta BEGIN
                    INSERT INTO column_fts(column_fts, rowid, source_id, table_name, column_name, data_type)
                    VALUES ('delete', old.id, old.source_id, old.table_name, old.column_name, old.data_type);
                END
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS row_meta (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id TEXT NOT NULL,
                    table_path TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    row_json TEXT NOT NULL,
                    row_text TEXT NOT NULL,
                    UNIQUE(source_id, table_path, row_number)
                )
            """)
            conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS row_fts USING fts5(
                    source_id,
                    table_name,
                    row_text,
                    content='row_meta',
                    content_rowid='id'
                )
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS row_meta_ai AFTER INSERT ON row_meta BEGIN
                    INSERT INTO row_fts(rowid, source_id, table_name, row_text)
                    VALUES (new.id, new.source_id, new.table_name, new.row_text);
                END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS row_meta_ad AFTER DELETE ON row_meta BEGIN
                    INSERT INTO row_fts(row_fts, rowid, source_id, table_name, row_text)
                    VALUES ('delete', old.id, old.source_id, old.table_name, old.row_text);
                END
            """)

            self._ensure_row_text_column(conn)

            conn.commit()
        finally:
            conn.close()

    def _ensure_row_text_column(self, conn: sqlite3.Connection) -> None:
        columns = conn.execute("PRAGMA table_info(row_meta)").fetchall()
        if columns and not any(column[1] == "row_text" for column in columns):
            conn.execute("ALTER TABLE row_meta ADD COLUMN row_text TEXT NOT NULL DEFAULT ''")

    def index_source(self, connector: BaseConnector) -> int:
        """Index all tables from a connector. Returns number of tables indexed."""
        source_info = connector.get_source_info()
        tables = connector.list_tables()
        conn = self._connect()
        try:
            # Remove old entries for this source
            old_rows = conn.execute(
                "SELECT id FROM table_meta WHERE source_id = ?", (source_info.source_id,)
            ).fetchall()
            if old_rows:
                ids = [r["id"] for r in old_rows]
                for row_id in ids:
                    conn.execute("DELETE FROM table_meta WHERE id = ?", (row_id,))
                conn.execute(
                    "DELETE FROM column_meta WHERE source_id = ?", (source_info.source_id,)
                )
                conn.execute(
                    "DELETE FROM row_meta WHERE source_id = ?", (source_info.source_id,)
                )

            for table in tables:
                sample_rows = connector.get_sample(table.path, limit=25)
                indexed_column_names = {column.name for column in table.columns}
                columns_json = json.dumps(
                    [
                        {
                            "name": c.name,
                            "data_type": c.data_type,
                            "sample_values": [str(v) for v in c.sample_values],
                        }
                        for c in table.columns
                    ]
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO table_meta
                        (source_id, source_type, table_name, path, row_count, columns_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_info.source_id,
                        source_info.source_type,
                        table.name,
                        table.path,
                        table.row_count,
                        columns_json,
                    ),
                )
                # Index columns
                for col in table.columns:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO column_meta
                            (source_id, table_path, table_name, column_name, data_type, sample_values)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_info.source_id,
                            table.path,
                            table.name,
                            col.name,
                            col.data_type,
                            json.dumps([str(v) for v in col.sample_values]),
                        ),
                    )

                for row_number, sample_row in enumerate(sample_rows, start=1):
                    normalized_row = {
                        key: "" if value is None else str(value)
                        for key, value in sample_row.items()
                    }
                    filtered_row = {}
                    row_text_values = []
                    for key, value in normalized_row.items():
                        if key not in indexed_column_names:
                            continue
                        filtered_row[key] = value
                        if connector.should_index_row_values(table.name, key) and value:
                            row_text_values.append(value)

                    if not filtered_row:
                        continue

                    row_text = " ".join(row_text_values)
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO row_meta
                            (source_id, table_path, table_name, row_number, row_json, row_text)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source_info.source_id,
                            table.path,
                            table.name,
                            row_number,
                            json.dumps(filtered_row),
                            row_text,
                        ),
                    )
            conn.commit()
            return len(tables)
        finally:
            conn.close()

    def get_indexed_sources(self) -> list[dict[str, Any]]:
        """Return list of all indexed source_ids with their table counts."""
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT source_id, source_type,
                       COUNT(*) as table_count,
                       MAX(indexed_at) as last_indexed
                FROM table_meta
                GROUP BY source_id, source_type
                ORDER BY source_id
                """
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_all_tables(self, source_id: str | None = None) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            if source_id:
                rows = conn.execute(
                    "SELECT * FROM table_meta WHERE source_id = ? ORDER BY table_name",
                    (source_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM table_meta ORDER BY source_id, table_name"
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["columns"] = json.loads(d.pop("columns_json"))
                result.append(d)
            return result
        finally:
            conn.close()

    def is_source_indexed(self, source_id: str) -> bool:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM table_meta WHERE source_id = ? LIMIT 1", (source_id,)
            ).fetchone()
            return row is not None
        finally:
            conn.close()
