from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from server.config import MASTER_API_KEY
from server.mcp_app import get_schema, index_source, list_sources, search
from server.services import indexer, registry


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
        return list_sources()

    @api.post("/api/index/{source_id}")
    def api_index_source(source_id: str) -> dict[str, Any]:
        return index_source(source_id)

    @api.post("/api/index-all")
    def api_index_all() -> dict[str, Any]:
        results = {}
        for info in registry.list_sources():
            results[info.source_id] = index_source(info.source_id)
        return results

    @api.get("/api/search")
    def api_search(
        q: str,
        limit: int = 20,
        # Repeated query params: ?source_ids=a&source_ids=b
        source_ids: Annotated[list[str] | None, Query()] = None,
        # Repeated query params: ?match_types=table&match_types=row
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

    return api
