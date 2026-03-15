"""Search engine over indexed metadata and row values."""

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SearchResult:
    match_type: str          # "table" | "column" | "row"
    source_id: str
    source_type: str
    table_name: str
    table_path: str
    column_name: str | None = None
    column_type: str | None = None
    matched_row: dict[str, Any] | None = None
    matched_row_number: int | None = None
    row_count: int = 0
    columns: list[dict] = field(default_factory=list)
    score: float = 1.0


def _source_clause(alias: str, ids: list[str]) -> tuple[str, list[str]]:
    """
    Build (sql_fragment, params) for a source_id IN (...) filter.
    Returns ("", []) when ids is empty (no filtering).
    alias — the SQL column reference, e.g. "tm.source_id" or "source_id".
    """
    if not ids:
        return "", []
    placeholders = ", ".join("?" * len(ids))
    return f" AND {alias} IN ({placeholders})", ids


class SearchEngine:
    def __init__(self, index_db: str | Path):
        self.index_db = Path(index_db)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.index_db)
        conn.row_factory = sqlite3.Row
        return conn

    def search(
        self,
        query: str,
        limit: int = 20,
        source_ids: list[str] | None = None,
        match_types: list[str] | None = None,
    ) -> list[SearchResult]:
        """
        Search across table names, column names, and indexed row values.

        source_ids  — restrict to specific sources; None = all sources.
        match_types — subset of {"table","column","row"} to include in results;
                      None = all types; [] = return nothing.

        Invariant: when a query matches a TABLE, columns and rows belonging to
        that same table are suppressed (they are redundant).  matched_table_keys
        is always populated for table hits regardless of match_types so that
        the suppression works even when "table" is not requested.
        """
        query = query.strip()
        if not query:
            return []

        allowed = self._parse_match_types(match_types)
        if not allowed:
            return []

        # Normalise source_ids once.
        ids: list[str] = [s.strip() for s in (source_ids or []) if s.strip()]

        results: list[SearchResult] = []
        seen: set[str] = set()
        matched_table_keys: set[tuple[str, str]] = set()  # (source_id, path)

        conn = self._connect()
        try:
            fts_q = self._build_fts_query(query)
            like_q = f"%{query.lower()}%"

            # 1. Collect every table that matches the query (both FTS and LIKE).
            #    Always done regardless of allowed set — needed for suppression.
            self._collect_matched_tables(conn, fts_q, like_q, ids, limit, matched_table_keys)

            # 2. Emit table results.
            if "table" in allowed:
                self._emit_tables_fts(conn, fts_q, ids, limit, seen, results)
                self._emit_tables_like(conn, like_q, ids, limit, seen, results)

            # 3. Emit column results (skip if column belongs to a matched table).
            if "column" in allowed:
                self._emit_columns_fts(conn, fts_q, ids, limit, matched_table_keys, seen, results)
                self._emit_columns_like(conn, like_q, ids, limit, matched_table_keys, seen, results)

            # 4. Emit row results (skip if row belongs to a matched table).
            if "row" in allowed:
                self._emit_rows_fts(conn, fts_q, ids, limit, matched_table_keys, seen, results)
                self._emit_rows_like(conn, like_q, ids, limit, matched_table_keys, seen, results)

        finally:
            conn.close()

        order = {"table": 0, "column": 1, "row": 2}
        results.sort(key=lambda r: (order.get(r.match_type, 99), r.score))
        return results[:limit]

    # ------------------------------------------------------------------
    # 1. Collect matched table keys
    # ------------------------------------------------------------------

    def _collect_matched_tables(
        self,
        conn: sqlite3.Connection,
        fts_q: str,
        like_q: str,
        ids: list[str],
        limit: int,
        out: set[tuple[str, str]],
    ) -> None:
        sf_sql, sf_params = _source_clause("tm.source_id", ids)
        try:
            for row in conn.execute(
                f"""
                SELECT tm.source_id, tm.path
                FROM table_fts
                JOIN table_meta tm ON table_fts.rowid = tm.id
                WHERE table_fts MATCH ?{sf_sql}
                LIMIT ?
                """,
                (fts_q, *sf_params, limit),
            ).fetchall():
                out.add((row["source_id"], row["path"]))
        except sqlite3.OperationalError:
            pass

        sf2_sql, sf2_params = _source_clause("source_id", ids)
        for row in conn.execute(
            f"""
            SELECT source_id, path
            FROM table_meta
            WHERE (LOWER(table_name) LIKE ? OR LOWER(source_id) LIKE ?){sf2_sql}
            LIMIT ?
            """,
            (like_q, like_q, *sf2_params, limit),
        ).fetchall():
            out.add((row["source_id"], row["path"]))

    # ------------------------------------------------------------------
    # 2. Table results
    # ------------------------------------------------------------------

    def _emit_tables_fts(
        self,
        conn: sqlite3.Connection,
        fts_q: str,
        ids: list[str],
        limit: int,
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("tm.source_id", ids)
        try:
            rows = conn.execute(
                f"""
                SELECT tm.source_id, tm.source_type, tm.table_name,
                       tm.path, tm.row_count, tm.columns_json,
                       rank AS score
                FROM table_fts
                JOIN table_meta tm ON table_fts.rowid = tm.id
                WHERE table_fts MATCH ?{sf_sql}
                ORDER BY rank
                LIMIT ?
                """,
                (fts_q, *sf_params, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            return
        for row in rows:
            key = f"t:{row['source_id']}:{row['path']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
                match_type="table",
                source_id=row["source_id"],
                source_type=row["source_type"],
                table_name=row["table_name"],
                table_path=row["path"],
                row_count=row["row_count"],
                columns=json.loads(row["columns_json"]),
                score=abs(row["score"]),
            ))

    def _emit_tables_like(
        self,
        conn: sqlite3.Connection,
        like_q: str,
        ids: list[str],
        limit: int,
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("source_id", ids)
        for row in conn.execute(
            f"""
            SELECT source_id, source_type, table_name, path, row_count, columns_json
            FROM table_meta
            WHERE (LOWER(table_name) LIKE ? OR LOWER(source_id) LIKE ?){sf_sql}
            LIMIT ?
            """,
            (like_q, like_q, *sf_params, limit),
        ).fetchall():
            key = f"t:{row['source_id']}:{row['path']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
                match_type="table",
                source_id=row["source_id"],
                source_type=row["source_type"],
                table_name=row["table_name"],
                table_path=row["path"],
                row_count=row["row_count"],
                columns=json.loads(row["columns_json"]),
                score=0.5,
            ))

    # ------------------------------------------------------------------
    # 3. Column results
    # ------------------------------------------------------------------

    def _emit_columns_fts(
        self,
        conn: sqlite3.Connection,
        fts_q: str,
        ids: list[str],
        limit: int,
        skip: set[tuple[str, str]],
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("cm.source_id", ids)
        try:
            rows = conn.execute(
                f"""
                SELECT cm.source_id, cm.table_name, cm.table_path,
                       cm.column_name, cm.data_type,
                       tm.source_type, tm.row_count, tm.columns_json,
                       rank AS score
                FROM column_fts
                JOIN column_meta cm ON column_fts.rowid = cm.id
                JOIN table_meta tm
                  ON tm.source_id = cm.source_id AND tm.path = cm.table_path
                WHERE column_fts MATCH ?{sf_sql}
                ORDER BY rank
                LIMIT ?
                """,
                (fts_q, *sf_params, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            return
        for row in rows:
            if (row["source_id"], row["table_path"]) in skip:
                continue
            key = f"c:{row['source_id']}:{row['table_path']}:{row['column_name']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
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
            ))

    def _emit_columns_like(
        self,
        conn: sqlite3.Connection,
        like_q: str,
        ids: list[str],
        limit: int,
        skip: set[tuple[str, str]],
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("cm.source_id", ids)
        for row in conn.execute(
            f"""
            SELECT cm.source_id, cm.table_name, cm.table_path,
                   cm.column_name, cm.data_type,
                   tm.source_type, tm.row_count, tm.columns_json
            FROM column_meta cm
            JOIN table_meta tm
              ON tm.source_id = cm.source_id AND tm.path = cm.table_path
            WHERE LOWER(cm.column_name) LIKE ?{sf_sql}
            LIMIT ?
            """,
            (like_q, *sf_params, limit),
        ).fetchall():
            if (row["source_id"], row["table_path"]) in skip:
                continue
            key = f"c:{row['source_id']}:{row['table_path']}:{row['column_name']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
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
            ))

    # ------------------------------------------------------------------
    # 4. Row results
    # ------------------------------------------------------------------

    def _emit_rows_fts(
        self,
        conn: sqlite3.Connection,
        fts_q: str,
        ids: list[str],
        limit: int,
        skip: set[tuple[str, str]],
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("rm.source_id", ids)
        try:
            rows = conn.execute(
                f"""
                SELECT rm.source_id, rm.table_name, rm.table_path,
                       rm.row_number, rm.row_json,
                       tm.source_type, tm.row_count, tm.columns_json,
                       rank AS score
                FROM row_fts
                JOIN row_meta rm ON row_fts.rowid = rm.id
                JOIN table_meta tm
                  ON tm.source_id = rm.source_id AND tm.path = rm.table_path
                WHERE row_fts MATCH ?{sf_sql}
                ORDER BY rank
                LIMIT ?
                """,
                (fts_q, *sf_params, limit),
            ).fetchall()
        except sqlite3.OperationalError:
            return
        for row in rows:
            if (row["source_id"], row["table_path"]) in skip:
                continue
            key = f"r:{row['source_id']}:{row['table_path']}:{row['row_number']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
                match_type="row",
                source_id=row["source_id"],
                source_type=row["source_type"],
                table_name=row["table_name"],
                table_path=row["table_path"],
                matched_row=json.loads(row["row_json"]),
                matched_row_number=row["row_number"],
                row_count=row["row_count"],
                columns=json.loads(row["columns_json"]),
                score=abs(row["score"]),
            ))

    def _emit_rows_like(
        self,
        conn: sqlite3.Connection,
        like_q: str,
        ids: list[str],
        limit: int,
        skip: set[tuple[str, str]],
        seen: set[str],
        out: list[SearchResult],
    ) -> None:
        sf_sql, sf_params = _source_clause("rm.source_id", ids)
        for row in conn.execute(
            f"""
            SELECT rm.source_id, rm.table_name, rm.table_path,
                   rm.row_number, rm.row_json,
                   tm.source_type, tm.row_count, tm.columns_json
            FROM row_meta rm
            JOIN table_meta tm
              ON tm.source_id = rm.source_id AND tm.path = rm.table_path
            WHERE LOWER(rm.row_text) LIKE ?{sf_sql}
            LIMIT ?
            """,
            (like_q, *sf_params, limit),
        ).fetchall():
            if (row["source_id"], row["table_path"]) in skip:
                continue
            key = f"r:{row['source_id']}:{row['table_path']}:{row['row_number']}"
            if key in seen:
                continue
            seen.add(key)
            out.append(SearchResult(
                match_type="row",
                source_id=row["source_id"],
                source_type=row["source_type"],
                table_name=row["table_name"],
                table_path=row["table_path"],
                matched_row=json.loads(row["row_json"]),
                matched_row_number=row["row_number"],
                row_count=row["row_count"],
                columns=json.loads(row["columns_json"]),
                score=0.75,
            ))

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_match_types(match_types: list[str] | None) -> set[str]:
        """
        None  → {"table", "column", "row"}  (default: all)
        []    → set()                        (explicitly nothing)
        [...] → validated intersection with allowed set
        """
        valid = {"table", "column", "row"}
        if match_types is None:
            return valid
        return {t.strip().lower() for t in match_types if t.strip().lower() in valid}

    @staticmethod
    def _build_fts_query(query: str) -> str:
        """
        Build an FTS5-safe prefix-match query string.

        Each whitespace-separated token is stripped of FTS5 special characters
        (individual characters, not keywords) and wrapped in a quoted prefix
        expression so that e.g. "ord" matches "order".  Tokens that become empty
        after stripping are skipped.  The special characters removed are those
        that have syntactic meaning inside an FTS5 MATCH expression.
        """
        # Characters that are special in FTS5 expressions and must be removed
        # from user input to prevent syntax errors.  Note: this is a set of
        # *characters*, not of keywords like OR/AND/NOT.  Treating OR/AND/NOT
        # as keywords here would be incorrect because a user might legitimately
        # search for a column called "not_null".
        _FTS5_SPECIAL_CHARS = set('"\'.-:*^()+')
        safe = []
        for token in query.strip().split():
            clean = "".join(c for c in token if c not in _FTS5_SPECIAL_CHARS)
            if clean:
                safe.append(f'"{clean}"*')
        # Fall back to a plain quoted term if nothing survived stripping.
        if not safe:
            fallback = "".join(c for c in query if c not in _FTS5_SPECIAL_CHARS).strip()
            return f'"{fallback}"' if fallback else '""'
        return " OR ".join(safe)
