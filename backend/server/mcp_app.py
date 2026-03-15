from typing import Any

from fastmcp import FastMCP

from server.services import engine, indexer, registry


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
    result = []
    for info in registry.list_sources():
        result.append(
            {
                "source_id": info.source_id,
                "source_type": info.source_type,
                "description": info.description,
                "location": info.location,
                "is_indexed": indexer.is_source_indexed(info.source_id),
            }
        )
    return result


@mcp.tool()
def index_source(source_id: str) -> dict[str, Any]:
    connector = registry.get(source_id)
    if connector is None:
        return {"success": False, "error": f"Source '{source_id}' not found"}

    tables_count = indexer.index_source(connector)
    return {
        "success": True,
        "source_id": source_id,
        "tables_indexed": tables_count,
    }


@mcp.tool()
def search(
    query: str,
    limit: int = 20,
    source_ids: list[str] | None = None,
    match_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    limit = min(limit, 100)
    results = engine.search(
        query,
        limit=limit,
        source_ids=source_ids,
        match_types=match_types,
    )
    return [
        {
            "match_type": r.match_type,
            "source_id": r.source_id,
            "source_type": r.source_type,
            "table_name": r.table_name,
            "table_path": r.table_path,
            "column_name": r.column_name,
            "column_type": r.column_type,
            "matched_row": r.matched_row,
            "matched_row_number": r.matched_row_number,
            "row_count": r.row_count,
            "columns": r.columns,
            "score": r.score,
        }
        for r in results
    ]


@mcp.tool()
def get_schema(source_id: str, path: str) -> dict[str, Any]:
    connector = registry.get(source_id)
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
