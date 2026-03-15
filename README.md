# MCP Data Scout

A **Data Discovery Tool** built as an MCP (Model Context Protocol) server.
Allows AI agents and humans to search for tables, columns, and data across multiple data sources.

## Quick Start

```bash
cp .env.example .env
```

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
│                             │   uploads/         │  │
│                             │   index.db (FTS5)  │  │
│                             │   sources.json     │  │
│                             └────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Component breakdown

| Layer | Module | Responsibility |
|-------|--------|----------------|
| **Connectors** | `connectors/` | Read schemas & data from sources |
| **CSVFileConnector** | `connectors/csv_connector.py` | Single-file CSV connector (per upload) |
| **Indexer** | `index/indexer.py` | Store metadata in SQLite FTS5 index |
| **Search** | `search/engine.py` | Full-text + LIKE search over metadata |
| **MCP Tools** | `server/mcp_app.py` | FastMCP tool definitions (SSE endpoint) |
| **REST API** | `server/api_app.py` | FastAPI endpoints for Streamlit UI |
| **UI** | `frontend/app.py` | Streamlit web interface |
| **Registry** | `server/source_registry.py` | Manages connector instances + manifest persistence |

---

## Built-in Test Data Sources

| ID | Type | Description |
|----|------|-------------|
| `sqlite_main` | SQLite | Business database: customers, orders, products, employees, order_items |
| `csv_datasets` | CSV (directory) | Flat files: sales_regions, marketing_campaigns, inventory_snapshot |

---

## Adding Your Own Data

### Via the Web UI

Open the **Add Source** tab in the Streamlit UI:

1. **CSV File** — upload any `.csv` file, give it a unique Source ID, and optionally provide Indexing Rules as JSON.
2. **SQLite Database** — upload a `.db` / `.sqlite` file, give it a unique Source ID, and optionally provide Indexing Rules.

Uploaded files are stored under `/data/uploads/` and survive container restarts thanks to the
sources manifest at `/data/sources.json`.

After uploading, click **Index now** next to the new source in the sidebar.

### Via the REST API

```bash
# Upload a CSV file
curl -X POST http://localhost:8000/api/upload/csv \
  -H "X-API-Key: $MASTER_API_KEY" \
  -F "file=@/path/to/data.csv" \
  -F "source_id=my_data" \
  -F "description=My custom dataset" \
  -F 'indexing_rules_json={"row_value_columns":{"data":["name","city"]}}'

# Upload a SQLite database
curl -X POST http://localhost:8000/api/upload/sqlite \
  -H "X-API-Key: $MASTER_API_KEY" \
  -F "file=@/path/to/db.sqlite" \
  -F "source_id=my_db" \
  -F "description=My SQLite database"
```

### Indexing Rules JSON

Each uploaded source can have its own indexing rules that control what gets indexed
and whether row values are searchable. All fields are optional.

```json
{
  "include_tables": ["sales", "products"],
  "exclude_tables": ["logs", "audit"],
  "exclude_columns": {
    "users": ["password_hash", "token"]
  },
  "row_value_tables": ["products", "orders"],
  "row_value_columns": {
    "products": ["name", "category"],
    "orders": ["status", "shipping_city"]
  }
}
```

| Field | Type | Effect |
|---|---|---|
| `include_tables` | list of strings | Index only these tables |
| `exclude_tables` | list of strings | Skip these tables |
| `include_columns` | `{table: [cols]}` | Index only listed columns per table |
| `exclude_columns` | `{table: [cols]}` | Skip listed columns per table |
| `row_value_tables` | list of strings | Enable row-value indexing for these tables |
| `row_value_columns` | `{table: [cols]}` | Index these columns as searchable row values |

Leave blank or `{}` to index everything with no restrictions.

---

## MCP Tools

The server exposes MCP tools at `http://localhost:8000/mcp/sse`:

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

### `search(query, limit?, source_ids?, match_types?)`
Full-text search across table names, column names, and indexed row values.
Returns ranked results grouped by match type (table → column → row).

```json
[
  {
    "match_type": "table",
    "source_id": "sqlite_main",
    "table_name": "customers",
    "row_count": 200,
    "columns": [{"name": "email", "data_type": "TEXT", "sample_values": ["..."]}]
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

## REST API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/sources` | List all sources (includes `is_dynamic` flag) |
| POST | `/api/index/{source_id}` | Index a specific source |
| POST | `/api/index-all` | Index all sources (returns per-source results + failed list) |
| GET | `/api/search?q=...` | Search metadata |
| GET | `/api/schema/{source_id}/{path}` | Get table schema |
| GET | `/api/tables?source_id=...` | List all indexed tables |
| GET | `/api/index-stats` | Indexing statistics per source |
| POST | `/api/upload/csv` | Upload a CSV file as a new source |
| POST | `/api/upload/sqlite` | Upload a SQLite DB as a new source |
| DELETE | `/api/sources/{source_id}` | Remove a user-uploaded source |
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
├── backend/
│   ├── connectors/
│   │   ├── abstraction/base.py      # BaseConnector, IndexingRules, data models
│   │   ├── csv_connector.py         # CSV directory connector + CSVFileConnector
│   │   └── sqlite_connector.py      # SQLite connector
│   ├── index/
│   │   └── indexer.py               # SQLite FTS5 metadata indexer
│   ├── search/
│   │   └── engine.py                # Full-text search engine
│   ├── server/
│   │   ├── api_app.py               # FastAPI REST endpoints (incl. upload)
│   │   ├── config.py                # Environment config
│   │   ├── mcp_app.py               # FastMCP tool definitions
│   │   ├── server.py                # Combined server entry point
│   │   ├── services.py              # Shared singletons (registry, indexer, engine)
│   │   └── source_registry.py       # ManagedRegistry + manifest persistence
│   └── scripts/
│       ├── seed_data.py             # Generates sample SQLite + CSV data
│       └── entrypoint.sh            # Docker entrypoint
├── frontend/
│   └── app.py                       # Streamlit web UI
├── docker-compose.yml
├── .env.example
└── requirements.txt
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SQLITE_DB_PATH` | `/data/sqlite/sample.db` | Path to the built-in SQLite database |
| `CSV_DIR` | `/data/csv` | Directory containing built-in CSV files |
| `UPLOADS_DIR` | `/data/uploads` | Directory where user-uploaded files are stored |
| `SOURCES_MANIFEST` | `/data/sources.json` | JSON file persisting dynamically added sources |
| `INDEX_DB_PATH` | `/data/index.db` | Path to FTS5 index database |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8000` | Server port |
| `API_BASE_URL` | `http://backend:8000` | Backend URL used by the Streamlit UI |
| `MASTER_API_KEY` | — | Required. API key for all `/api/*` endpoints |

---

## Search Index Design

Metadata is stored in a **SQLite FTS5** virtual table — a built-in full-text search engine with no external dependencies.

Three indexed entities:
- **`table_fts`** — `source_id`, `source_type`, `table_name`, `path`
- **`column_fts`** — `source_id`, `table_name`, `column_name`, `data_type`
- **`row_fts`** — `source_id`, `table_name`, `row_text` (concatenated indexed column values)

Search strategy (priority order):
1. FTS5 prefix match — fast, ranked by BM25 relevance
2. `LIKE %query%` fallback — catches partial mid-word matches

When a table matches the query, its columns and rows are suppressed in results
(they are redundant). Results are sorted: tables first, then columns, then rows.
