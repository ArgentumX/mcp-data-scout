import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Annotated, Any

from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from server.config import MASTER_API_KEY
from server.mcp_app import get_schema, index_source, list_sources, search
from server.services import indexer, registry
from connectors.csv_connector import CSVFileConnector
from connectors.sqlite_connector import SQLiteConnector

logger = logging.getLogger(__name__)

UPLOADS_DIR = os.getenv("UPLOADS_DIR") or "/data/uploads"


def create_api_app() -> FastAPI:
    api = FastAPI(title="Data Scout API", version="1.0.0")

    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @api.middleware("http")
    async def api_key_middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Any]],
    ):
        path = request.url.path.rstrip("/") or "/"
        public_paths = {"/health", "/docs", "/openapi.json"}

        if path in public_paths or not path.startswith("/api"):
            return await call_next(request)

        if not MASTER_API_KEY:
            return JSONResponse(
                status_code=500,
                content={"detail": "Server API key is not configured"},
            )

        if request.headers.get("X-API-Key") != MASTER_API_KEY:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid API key"},
            )

        return await call_next(request)

    @api.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "data-scout"}

    @api.get("/api/sources")
    def api_list_sources() -> list[dict[str, Any]]:
        sources = list_sources()
        # Annotate which sources are dynamic (user-uploaded)
        for src in sources:
            src["is_dynamic"] = registry.is_dynamic(src["source_id"])
        return sources

    @api.post("/api/index/{source_id}")
    def api_index_source(source_id: str) -> dict[str, Any]:
        result = index_source(source_id)
        if not result.get("success"):
            return JSONResponse(status_code=400, content=result)
        return result

    @api.post("/api/index-all")
    def api_index_all() -> dict[str, Any]:
        results = {}
        errors = []
        for info in registry.list_sources():
            r = index_source(info.source_id)
            results[info.source_id] = r
            if not r.get("success"):
                errors.append(info.source_id)
        return {
            "success": len(errors) == 0,
            "results": results,
            "failed": errors,
        }

    @api.get("/api/search")
    def api_search(
        q: str,
        limit: int = 20,
        source_ids: Annotated[list[str] | None, Query()] = None,
        match_types: Annotated[list[str] | None, Query()] = None,
    ) -> list[dict[str, Any]]:
        return search(
            q,
            limit=limit,
            source_ids=source_ids,
            match_types=match_types,
        )

    @api.get("/api/schema/{source_id}/{path:path}")
    def api_get_schema(source_id: str, path: str) -> dict[str, Any]:
        return get_schema(source_id, path)

    @api.get("/api/tables")
    def api_list_tables(source_id: str | None = None) -> list[dict[str, Any]]:
        return indexer.get_all_tables(source_id=source_id)

    @api.get("/api/index-stats")
    def api_index_stats() -> list[dict[str, Any]]:
        """Return indexing statistics per source."""
        return indexer.get_indexed_sources()

    # ------------------------------------------------------------------
    # Upload endpoints
    # ------------------------------------------------------------------

    @api.post("/api/upload/csv")
    async def api_upload_csv(
        file: Annotated[UploadFile, File()],
        source_id: Annotated[str, Form()],
        description: Annotated[str, Form()] = "",
        indexing_rules_json: Annotated[str, Form()] = "{}",
    ) -> dict[str, Any]:
        """
        Upload a CSV file and register it as a new per-file data source.

        Form fields:
          - file              — the .csv file
          - source_id         — unique identifier for this source
          - description       — optional human-readable description
          - indexing_rules_json — optional JSON object describing IndexingRules
        """
        import json as _json

        if not file.filename or not file.filename.lower().endswith(".csv"):
            return JSONResponse(
                status_code=400,
                content={"detail": "Only .csv files are accepted"},
            )

        # Validate source_id uniqueness
        if registry.get(source_id) is not None:
            return JSONResponse(
                status_code=409,
                content={"detail": f"Source '{source_id}' already exists. Choose a different ID."},
            )

        # Parse indexing rules
        try:
            rules_dict = _json.loads(indexing_rules_json) if indexing_rules_json.strip() else {}
        except _json.JSONDecodeError as exc:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Invalid indexing_rules_json: {exc}"},
            )

        from server.source_registry import _indexing_rules_from_dict
        rules = _indexing_rules_from_dict(rules_dict)

        # Save file
        upload_dir = Path(UPLOADS_DIR)
        upload_dir.mkdir(parents=True, exist_ok=True)
        dest = upload_dir / file.filename
        # Avoid overwriting existing files by appending source_id prefix
        dest = upload_dir / f"{source_id}__{file.filename}"
        content = await file.read()
        dest.write_bytes(content)

        connector = CSVFileConnector(
            source_id=source_id,
            file_path=str(dest),
            description=description or f"Uploaded CSV: {file.filename}",
            indexing_rules=rules,
        )
        registry.register_dynamic(connector)

        logger.info("Registered new CSV source '%s' from '%s'", source_id, dest)
        return {
            "success": True,
            "source_id": source_id,
            "file": file.filename,
            "location": str(dest),
        }

    @api.post("/api/upload/sqlite")
    async def api_upload_sqlite(
        file: Annotated[UploadFile, File()],
        source_id: Annotated[str, Form()],
        description: Annotated[str, Form()] = "",
        indexing_rules_json: Annotated[str, Form()] = "{}",
    ) -> dict[str, Any]:
        """
        Upload a SQLite .db file and register it as a new data source.
        """
        import json as _json

        fname = file.filename or ""
        if not (fname.lower().endswith(".db") or fname.lower().endswith(".sqlite")):
            return JSONResponse(
                status_code=400,
                content={"detail": "Only .db / .sqlite files are accepted"},
            )

        if registry.get(source_id) is not None:
            return JSONResponse(
                status_code=409,
                content={"detail": f"Source '{source_id}' already exists. Choose a different ID."},
            )

        try:
            rules_dict = _json.loads(indexing_rules_json) if indexing_rules_json.strip() else {}
        except _json.JSONDecodeError as exc:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Invalid indexing_rules_json: {exc}"},
            )

        from server.source_registry import _indexing_rules_from_dict
        rules = _indexing_rules_from_dict(rules_dict)

        upload_dir = Path(UPLOADS_DIR)
        upload_dir.mkdir(parents=True, exist_ok=True)
        dest = upload_dir / f"{source_id}__{fname}"
        content = await file.read()
        dest.write_bytes(content)

        connector = SQLiteConnector(
            source_id=source_id,
            db_path=str(dest),
            description=description or f"Uploaded SQLite DB: {fname}",
            indexing_rules=rules,
        )
        registry.register_dynamic(connector)

        logger.info("Registered new SQLite source '%s' from '%s'", source_id, dest)
        return {
            "success": True,
            "source_id": source_id,
            "file": fname,
            "location": str(dest),
        }

    @api.delete("/api/sources/{source_id}")
    def api_delete_source(source_id: str) -> dict[str, Any]:
        """Remove a dynamic (user-uploaded) source from the registry."""
        if not registry.is_dynamic(source_id):
            return JSONResponse(
                status_code=403,
                content={"detail": "Only user-uploaded sources can be deleted."},
            )
        registry.remove_dynamic(source_id)
        return {"success": True, "removed": source_id}

    return api
