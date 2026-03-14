# MCP Data Scout

A **Data Discovery Tool** built as an MCP (Model Context Protocol) server.
Allows AI agents and humans to search for tables, columns, and data across multiple data sources.

## Quick Start

```bash
docker compose up --build
```

- **Streamlit UI**: http://localhost:8501
- **REST API / MCP server**: http://localhost:8000
- **API docs**: http://localhost:8000/docs

On first startup the server auto-generates sample data (SQLite + CSV).
Then click **"Index All Sources"** in the sidebar to index them.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                     │
│                                                     │
│  ┌──────────────────┐      ┌──────────────────────┐ │
│  │   data-scout-ui  │      │   data-scout-mcp     │ │
│  │   (Streamlit)    │─────▶│   (FastAPI + MCP)    │ │
│  │   port 8501      │ HTTP │   port 8000          │ │
│  └──────────────────┘      └──────────┬───────────┘ │
│                                       │             │
│                             ┌─────────▼──────────┐  │
│                             │   /data  (volume)  │  │
│                             │   sqlite/sample.db │  │
│                             │   csv/*.csv        │  │
│                             │   index.db (FTS5)  │  │
│                             └────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Component breakdown

| Layer | Module | Responsibility |
|-------|--------|----------------|
| **Connectors** | `connectors/` | Read schemas & data from sources |
| **Indexer** | `index/indexer.py` | Store metadata in SQLite FTS5 index |
| **Search** | `search/engine.py` | Full-text + LIKE search over metadata |
| **MCP Tools** | `server/server.py` | FastMCP tool definitions (SSE endpoint) |
| **REST API** | `server/server.py` | FastAPI endpoints for Streamlit UI |
| **UI** | `ui/app.py` | Streamlit web interface |
| **Registry** | `server/registry.py` | Manages connector instances |

---

## Data Sources

| ID | Type | Description |
|----|------|-------------|
| `sqlite_main` | SQLite | Business database: customers, orders, products, employees, order_items |
| `csv_datasets` | CSV | Flat files: sales_regions, marketing_campaigns, inventory_snapshot |

---

## MCP Tools

The server exposes four MCP tools at `http://localhost:8000/mcp/sse`:

### `list_sources()`
Returns all registered data sources with indexing status.

```json
[
  {
    "source_id": "sqlite_main",
    "source_type": "sqlite",
    "description": "Main SQLite database with business data",
    "location": "/data/sqlite/sample.db",
    "is_indexed": true
  }
]
```

### `index_source(source_id)`
Reads schema from a source and stores metadata in the FTS5 index.

```json
{ "success": true, "source_id": "sqlite_main", "tables_indexed": 5 }
```

### `search(query, limit?)`
Full-text search across table names and column names. Returns ranked results.

```json
[
  {
    "match_type": "table",
    "source_id": "sqlite_main",
    "table_name": "customers",
    "row_count": 200,
    "columns": [{"name": "customer_id", "data_type": "INTEGER", ...}]
  }
]
```

### `get_schema(source_id, path)`
Returns full column schema + 5 sample rows for a specific table.

```json
{
  "success": true,
  "table_name": "orders",
  "row_count": 500,
  "columns": [...],
  "sample_rows": [...]
}
```

---

## REST API (for UI / external consumers)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/sources` | List all sources |
| POST | `/api/index/{source_id}` | Index a source |
| POST | `/api/index-all` | Index all sources |
| GET | `/api/search?q={query}` | Search metadata |
| GET | `/api/schema/{source_id}/{path}` | Get table schema |
| GET | `/api/tables?source_id=...` | List all indexed tables |
| GET | `/docs` | Interactive Swagger UI |

---

## Connecting an AI Agent

### Claude Desktop / Cursor

Add to your MCP config:

```json
{
  "mcpServers": {
    "data-scout": {
      "url": "http://localhost:8000/mcp/sse"
    }
  }
}
```

The agent can then call:
```
list_sources()
index_source("sqlite_main")
search("customer email")
get_schema("sqlite_main", "customers")
```

---

## Project Structure

```
mcp-data-scout/
├── connectors/               # Data source connectors
│   ├── base.py               # Abstract BaseConnector + data models
│   ├── sqlite_connector.py   # SQLite connector
│   └── csv_connector.py      # CSV directory connector
├── index/
│   └── indexer.py            # SQLite FTS5 metadata indexer
├── search/
│   └── engine.py             # Full-text search engine
├── server/
│   ├── registry.py           # Source registry (connector manager)
│   └── server.py             # FastMCP + FastAPI combined server
├── ui/
│   └── app.py                # Streamlit web UI
├── scripts/
│   ├── seed_data.py          # Generates sample SQLite + CSV data
│   └── entrypoint.sh         # Docker entrypoint
├── Dockerfile.server
├── Dockerfile.ui
├── docker-compose.yml
└── requirements.txt
```

---

## Local Development (without Docker)

```bash
pip install -r requirements.txt

# Generate sample data
SQLITE_DB_PATH=./data/sqlite/sample.db CSV_DIR=./data/csv python scripts/seed_data.py

# Start MCP + REST server
SQLITE_DB_PATH=./data/sqlite/sample.db \
CSV_DIR=./data/csv \
INDEX_DB_PATH=./data/index.db \
python -m server.server

# In another terminal — start UI
API_BASE_URL=http://localhost:8000 streamlit run ui/app.py
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SQLITE_DB_PATH` | `/data/sqlite/sample.db` | Path to SQLite database |
| `CSV_DIR` | `/data/csv` | Directory containing CSV files |
| `INDEX_DB_PATH` | `/data/index.db` | Path to FTS5 index database |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8000` | Server port |
| `API_BASE_URL` | `http://mcp:8000` | Backend URL for Streamlit UI |

---

## Search Index Design

Metadata is stored in a **SQLite FTS5** virtual table — a built-in full-text search engine with no external dependencies.

Two indexed entities:
- **`table_fts`** — `source_id`, `source_type`, `table_name`, `path`
- **`column_fts`** — `source_id`, `table_name`, `column_name`, `data_type`

Search strategy (priority order):
1. FTS5 prefix match — fast, ranked by BM25 relevance
2. `LIKE %query%` fallback — catches partial mid-word matches

Results are deduplicated and sorted: table matches first, then column matches.
