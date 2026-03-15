"""Application assembly and runtime entrypoint."""

import uvicorn
from fastapi import FastAPI
from fastmcp.server.http import create_sse_app

from server.api_app import create_api_app
from server.config import HOST, PORT
from server.mcp_app import mcp


def build_app() -> FastAPI:
    """Build the combined ASGI application."""
    api = create_api_app()
    sse_app = create_sse_app(
        server=mcp,
        sse_path="/sse",
        message_path="/messages/",
    )
    api.mount("/mcp", sse_app)
    return api


if __name__ == "__main__":
    app = build_app()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
