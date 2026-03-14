"""
Data Scout — combined MCP + REST API server.

Layout:
  /mcp/sse        — MCP SSE endpoint for AI agents (fastmcp)
  /mcp/messages/  — MCP message endpoint
  /api/...        — REST endpoints for Streamlit UI (FastAPI)
  /health         — health check

Run:
  python -m server.server
"""

import os
import sys
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastmcp import FastMCP
from fastmcp.server.http import create_sse_app

# Ensure project root is on sys.path when running directly
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from index.indexer import MetadataIndexer
from server.registry import build_default_registry  # noqa: E402
from search.engine import SearchEngine

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INDEX_DB = os.getenv("INDEX_DB_PATH", "/data/index.db")
HOST = os.getenv("MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("MCP_PORT", "8000"))

# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------

_registry = build_default_registry()
_indexer = MetadataIndexer(index_db=INDEX_DB)
_engine = SearchEngine(index_db=INDEX_DB)

# ---------------------------------------------------------------------------
# FastMCP — MCP tool definitions
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="data-scout",
    instructions=(
        "Data Discovery Tool. "
        "Use list_sources to see available data sources, "
        "index_source to index them, search to find tables/columns, "
        "and get_schema to inspect a specific table."
    ),
)


@mcp.tool()
def list_sources() -> list[dict[str, Any]]:
    """
    Return all registered data sources with their metadata.

    Each entry contains:
    - source_id: unique identifier
    - source_type: "sqlite" or "csv"
    - description: human-readable description
    - location: file path
    - is_indexed: whether the source has been indexed
    """
    result = []
    for info in _registry.list_sources():
        result.append(
            {
                "source_id": info.source_id,
                "source_type": info.source_type,
                "description": info.description,
                "location": info.location,
                "is_indexed": _indexer.is_source_indexed(info.source_id),
            }
        )
    return result


@mcp.tool()
def index_source(source_id: str) -> dict[str, Any]:
    """
    Index (or re-index) a data source by its source_id.

    Reads the source schema, collects table/column metadata,
    and stores it in the search index.

    Args:
        source_id: The ID of the source to index (from list_sources).

    Returns a summary with tables_indexed count.
    """
    connector = _registry.get(source_id)
    if connector is None:
        return {"success": False, "error": f"Source '{source_id}' not found"}

    tables_count = _indexer.index_source(connector)
    return {
        "success": True,
        "source_id": source_id,
        "tables_indexed": tables_count,
    }


@mcp.tool()
def search(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Search for tables and columns matching the query string.

    Searches across table names, column names, and source IDs.
    Returns ranked results with metadata.

    Args:
        query: Search terms (e.g. "customer", "order_date", "sales")
        limit: Maximum number of results (default 20, max 100)

    Each result contains:
    - match_type: "table" or "column"
    - source_id, source_type
    - table_name, table_path
    - column_name, column_type (for column matches)
    - row_count
    - columns: list of {name, data_type, sample_values}
    """
    limit = min(limit, 100)
    results = _engine.search(query, limit=limit)
    return [
        {
            "match_type": r.match_type,
            "source_id": r.source_id,
            "source_type": r.source_type,
            "table_name": r.table_name,
            "table_path": r.table_path,
            "column_name": r.column_name,
            "column_type": r.column_type,
            "row_count": r.row_count,
            "columns": r.columns,
            "score": r.score,
        }
        for r in results
    ]


@mcp.tool()
def get_schema(source_id: str, path: str) -> dict[str, Any]:
    """
    Get the full schema and sample data for a specific table.

    Args:
        source_id: The data source ID (from list_sources).
        path: Table name (SQLite) or filename (CSV), e.g. "orders" or "sales.csv"

    Returns:
    - table_name, source_id, source_type, row_count
    - columns: list of {name, data_type, sample_values}
    - sample_rows: first 5 rows of data
    """
    connector = _registry.get(source_id)
    if connector is None:
        return {"success": False, "error": f"Source '{source_id}' not found"}

    schema = connector.get_schema(path)
    if schema is None:
        return {"success": False, "error": f"Table/file '{path}' not found in '{source_id}'"}

    sample_rows = connector.get_sample(path, limit=5)
    source_info = connector.get_source_info()
    return {
        "success": True,
        "table_name": schema.name,
        "source_id": source_id,
        "source_type": source_info.source_type,
        "row_count": schema.row_count,
        "path": schema.path,
        "columns": [
            {
                "name": c.name,
                "data_type": c.data_type,
                "sample_values": [str(v) for v in c.sample_values],
            }
            for c in schema.columns
        ],
        "sample_rows": sample_rows,
    }


# ---------------------------------------------------------------------------
# FastAPI — REST endpoints for Streamlit UI
# ---------------------------------------------------------------------------

api = FastAPI(title="Data Scout API", version="1.0.0")

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@api.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "data-scout"}


@api.get("/api/sources")
def api_list_sources() -> list[dict]:
    return list_sources()


@api.post("/api/index/{source_id}")
def api_index_source(source_id: str) -> dict:
    return index_source(source_id)


@api.post("/api/index-all")
def api_index_all() -> dict:
    results = {}
    for info in _registry.list_sources():
        results[info.source_id] = index_source(info.source_id)
    return results


@api.get("/api/search")
def api_search(q: str, limit: int = 20) -> list[dict]:
    return search(q, limit=limit)


@api.get("/api/schema/{source_id}/{path:path}")
def api_get_schema(source_id: str, path: str) -> dict:
    return get_schema(source_id, path)


@api.get("/api/tables")
def api_list_tables(source_id: str | None = None) -> list[dict]:
    return _indexer.get_all_tables(source_id=source_id)


# ---------------------------------------------------------------------------
# Mount MCP SSE under /mcp prefix and combine with FastAPI
# ---------------------------------------------------------------------------

def build_app() -> FastAPI:
    """Build the combined ASGI application."""
    sse_app = create_sse_app(
        server=mcp,
        sse_path="/sse",
        message_path="/messages/",
    )
    api.mount("/mcp", sse_app)
    return api


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = build_app()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
