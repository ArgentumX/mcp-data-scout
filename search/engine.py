"""
Search engine — full-text search over indexed metadata.

Uses SQLite FTS5 with LIKE fallback for partial matches.
Returns ranked results with source, table, and column info.
"""

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SearchResult:
    match_type: str          # "table" | "column"
    source_id: str
    source_type: str
    table_name: str
    table_path: str
    column_name: str | None = None
    column_type: str | None = None
    row_count: int = 0
    columns: list[dict] = field(default_factory=list)
    score: float = 1.0


class SearchEngine:
    def __init__(self, index_db: str | Path):
        self.index_db = Path(index_db)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.index_db)
        conn.row_factory = sqlite3.Row
        return conn

    def search(self, query: str, limit: int = 20) -> list[SearchResult]:
        """
        Search across table names and column names.
        Uses FTS5 for exact/prefix matches, then LIKE for partial.
        """
        if not query.strip():
            return []

        results: list[SearchResult] = []
        seen: set[str] = set()  # dedup key = source_id:path:column

        conn = self._connect()
        try:
            # --- Table-level FTS search ---
            fts_query = self._build_fts_query(query)
            try:
                rows = conn.execute(
                    """
                    SELECT tm.id, tm.source_id, tm.source_type, tm.table_name,
                           tm.path, tm.row_count, tm.columns_json,
                           rank as score
                    FROM table_fts
                    JOIN table_meta tm ON table_fts.rowid = tm.id
                    WHERE table_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (fts_query, limit),
                ).fetchall()
                for row in rows:
                    key = f"{row['source_id']}:{row['path']}:"
                    if key not in seen:
                        seen.add(key)
                        results.append(
                            SearchResult(
                                match_type="table",
                                source_id=row["source_id"],
                                source_type=row["source_type"],
                                table_name=row["table_name"],
                                table_path=row["path"],
                                row_count=row["row_count"],
                                columns=json.loads(row["columns_json"]),
                                score=abs(row["score"]),
                            )
                        )
            except sqlite3.OperationalError:
                pass  # FTS syntax error — fall through to LIKE

            # --- Column-level FTS search ---
            try:
                col_rows = conn.execute(
                    """
                    SELECT cm.id, cm.source_id, cm.table_name, cm.table_path,
                           cm.column_name, cm.data_type, cm.sample_values,
                           rank as score,
                           tm.source_type, tm.row_count, tm.columns_json
                    FROM column_fts
                    JOIN column_meta cm ON column_fts.rowid = cm.id
                    JOIN table_meta tm ON tm.source_id = cm.source_id AND tm.path = cm.table_path
                    WHERE column_fts MATCH ?
                    ORDER BY rank
                    LIMIT ?
                    """,
                    (fts_query, limit),
                ).fetchall()
                for row in col_rows:
                    key = f"{row['source_id']}:{row['table_path']}:{row['column_name']}"
                    if key not in seen:
                        seen.add(key)
                        results.append(
                            SearchResult(
                                match_type="column",
                                source_id=row["source_id"],
                                source_type=row["source_type"],
                                table_name=row["table_name"],
                                table_path=row["table_path"],
                                column_name=row["column_name"],
                                column_type=row["data_type"],
                                row_count=row["row_count"],
                                columns=json.loads(row["columns_json"]),
                                score=abs(row["score"]),
                            )
                        )
            except sqlite3.OperationalError:
                pass

            # --- LIKE fallback for partial matches not caught by FTS ---
            like_pattern = f"%{query.lower()}%"

            # Table names LIKE
            like_table_rows = conn.execute(
                """
                SELECT source_id, source_type, table_name, path, row_count, columns_json
                FROM table_meta
                WHERE LOWER(table_name) LIKE ? OR LOWER(source_id) LIKE ?
                LIMIT ?
                """,
                (like_pattern, like_pattern, limit),
            ).fetchall()
            for row in like_table_rows:
                key = f"{row['source_id']}:{row['path']}:"
                if key not in seen:
                    seen.add(key)
                    results.append(
                        SearchResult(
                            match_type="table",
                            source_id=row["source_id"],
                            source_type=row["source_type"],
                            table_name=row["table_name"],
                            table_path=row["path"],
                            row_count=row["row_count"],
                            columns=json.loads(row["columns_json"]),
                            score=0.5,
                        )
                    )

            # Column names LIKE
            like_col_rows = conn.execute(
                """
                SELECT cm.source_id, cm.table_name, cm.table_path,
                       cm.column_name, cm.data_type, cm.sample_values,
                       tm.source_type, tm.row_count, tm.columns_json
                FROM column_meta cm
                JOIN table_meta tm ON tm.source_id = cm.source_id AND tm.path = cm.table_path
                WHERE LOWER(cm.column_name) LIKE ?
                LIMIT ?
                """,
                (like_pattern, limit),
            ).fetchall()
            for row in like_col_rows:
                key = f"{row['source_id']}:{row['table_path']}:{row['column_name']}"
                if key not in seen:
                    seen.add(key)
                    results.append(
                        SearchResult(
                            match_type="column",
                            source_id=row["source_id"],
                            source_type=row["source_type"],
                            table_name=row["table_name"],
                            table_path=row["table_path"],
                            column_name=row["column_name"],
                            column_type=row["data_type"],
                            row_count=row["row_count"],
                            columns=json.loads(row["columns_json"]),
                            score=0.5,
                        )
                    )

        finally:
            conn.close()

        # Sort: table matches first, then by score
        results.sort(key=lambda r: (r.match_type != "table", r.score))
        return results[:limit]

    @staticmethod
    def _build_fts_query(query: str) -> str:
        """Build an FTS5-safe query string from user input."""
        # Escape FTS5 special characters
        special = set('".:-*^()OR AND NOT')
        tokens = query.strip().split()
        safe_tokens = []
        for token in tokens:
            token_clean = "".join(c for c in token if c not in special)
            if token_clean:
                safe_tokens.append(f'"{token_clean}"*')  # prefix match
        if not safe_tokens:
            return f'"{query}"'
        return " OR ".join(safe_tokens)
