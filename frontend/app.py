"""
Streamlit UI for Data Scout.

Communicates with the backend via the REST API
exposed by server/server.py on /api/* endpoints.
"""

import json
import os
from typing import Any

import pandas as pd
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE_URL")
API_KEY_HEADER = "X-API-Key"

if "api_key" not in st.session_state:
    st.session_state["api_key"] = ""

# Tracks the last committed search so that results don't change
# when the user edits filters without clicking Search.
if "committed_query" not in st.session_state:
    st.session_state["committed_query"] = ""
if "committed_params" not in st.session_state:
    st.session_state["committed_params"] = None

st.set_page_config(
    page_title="Data Scout",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def get_api_key() -> str:
    return st.session_state.get("api_key", "").strip()


def get_headers() -> dict[str, str]:
    api_key = get_api_key()
    return {API_KEY_HEADER: api_key} if api_key else {}


def handle_request_error(path: str, error: Exception) -> None:
    if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
        detail = ""
        try:
            payload = error.response.json()
            detail = payload.get("detail", "") if isinstance(payload, dict) else ""
        except ValueError:
            detail = error.response.text.strip()

        if error.response.status_code == 401:
            st.error(detail or "Invalid API key. Please check it and try again.")
            return
        if error.response.status_code == 500:
            st.error(detail or "Backend authentication is not configured correctly.")
            return
        st.error(f"Request failed [{path}]: {detail or error}")
        return

    st.error(f"Request failed [{path}]: {error}")


def get(path: str, params: Any = None) -> Any | None:
    """
    GET request to the backend.
    params can be a dict or a list of (key, value) tuples.
    Passing a list of tuples allows repeated keys, e.g.:
      [("match_types", "table"), ("match_types", "row")]
    which FastAPI parses as list[str].
    """
    try:
        resp = requests.get(
            f"{API_BASE}{path}",
            params=params,
            headers=get_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"Cannot connect to backend at {API_BASE}. Is the server running?")
        return None
    except Exception as e:
        handle_request_error(path, e)
        return None


def post(path: str, data: dict | None = None, files: dict | None = None) -> Any | None:
    try:
        resp = requests.post(
            f"{API_BASE}{path}",
            headers=get_headers(),
            data=data,
            files=files,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"Cannot connect to backend at {API_BASE}. Is the server running?")
        return None
    except Exception as e:
        handle_request_error(path, e)
        return None


def delete(path: str) -> Any | None:
    try:
        resp = requests.delete(
            f"{API_BASE}{path}",
            headers=get_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(f"Cannot connect to backend at {API_BASE}. Is the server running?")
        return None
    except Exception as e:
        handle_request_error(path, e)
        return None


# ---------------------------------------------------------------------------
# Sidebar — Sources panel
# ---------------------------------------------------------------------------

def render_sidebar() -> list[dict]:
    st.sidebar.title("Data Scout")
    st.sidebar.caption("Data Discovery Tool")
    st.sidebar.divider()
    st.sidebar.subheader("API Access")
    st.sidebar.text_input(
        "API Key",
        type="password",
        key="api_key",
        placeholder="Enter API key",
        help="Used for all backend requests.",
    )

    if not get_api_key():
        st.sidebar.info("Enter your API key to load sources and search data.")
        return []

    st.sidebar.divider()
    st.sidebar.subheader("Data Sources")

    sources: list[dict] | None = get("/api/sources")
    if sources is None:
        st.sidebar.warning("Backend not reachable")
        return []

    if not sources:
        st.sidebar.info("No sources configured")
    else:
        for src in sources:
            indexed = src.get("is_indexed", False)
            is_dynamic = src.get("is_dynamic", False)
            icon = "✅" if indexed else "⚪"
            label = f"{icon} {src['source_id']}"
            if is_dynamic:
                label += " 📤"
            with st.sidebar.expander(label, expanded=False):
                st.write(f"**Type:** `{src['source_type']}`")
                st.write(f"**Description:** {src['description']}")
                st.write(f"**Location:** `{src['location']}`")
                btn_label = "Re-index" if indexed else "Index now"
                col_btn, col_del = st.columns([2, 1])
                with col_btn:
                    if st.button(btn_label, key=f"idx_{src['source_id']}"):
                        with st.spinner(f"Indexing {src['source_id']}..."):
                            result = post(f"/api/index/{src['source_id']}")
                        if result and result.get("success"):
                            st.success(f"Indexed {result['tables_indexed']} tables")
                            st.rerun()
                        else:
                            err = result.get("error", "Unknown error") if result else "No response"
                            st.error(err)
                if is_dynamic:
                    with col_del:
                        if st.button("Delete", key=f"del_{src['source_id']}", type="secondary"):
                            result = delete(f"/api/sources/{src['source_id']}")
                            if result and result.get("success"):
                                st.success(f"Removed {src['source_id']}")
                                st.rerun()

    st.sidebar.divider()
    if st.sidebar.button("Index All Sources", type="primary"):
        with st.spinner("Indexing all sources..."):
            result = post("/api/index-all")
        if result is None:
            st.sidebar.error("No response from backend.")
        elif result.get("success"):
            st.sidebar.success("All sources indexed successfully.")
            st.rerun()
        else:
            failed = result.get("failed", [])
            st.sidebar.error(
                f"Indexing completed with errors. Failed sources: {', '.join(failed) or 'unknown'}"
            )
            st.rerun()

    return sources or []


# ---------------------------------------------------------------------------
# Search page
# ---------------------------------------------------------------------------

def render_search_page(sources: list[dict]) -> None:
    st.title("Data Scout")
    st.markdown(
        "Search across **tables**, **columns**, and indexed **row values** "
        "in all indexed data sources."
    )

    if not get_api_key():
        st.info("Enter your API key in the sidebar to start searching.")
        return

    # Search bar
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        query = st.text_input(
            "Search",
            placeholder="e.g. customer, order_date, sales, product_id ...",
            label_visibility="collapsed",
            key="search_query",
        )
    with col_btn:
        search_clicked = st.button("Search", type="primary", use_container_width=True)

    # Filters
    indexed_source_ids = [s["source_id"] for s in sources if s.get("is_indexed")]

    with st.expander("Filters", expanded=True):
        st.caption("Restrict results by source and index type.")

        if indexed_source_ids:
            st.markdown("**Sources**")
            source_cols = st.columns(min(3, len(indexed_source_ids)))
            for i, sid in enumerate(indexed_source_ids):
                with source_cols[i % len(source_cols)]:
                    st.checkbox(sid, value=True, key=f"filter_src_{sid}")
        else:
            st.info("No indexed sources available yet.")

        st.markdown("**Result types**")
        type_cols = st.columns(3)
        with type_cols[0]:
            st.checkbox("Tables", value=True, key="filter_type_table")
        with type_cols[1]:
            st.checkbox("Columns", value=True, key="filter_type_column")
        with type_cols[2]:
            st.checkbox("Rows", value=True, key="filter_type_row")

    # Only fire search when the button is explicitly clicked
    if search_clicked:
        if not query:
            st.session_state["committed_query"] = ""
            st.session_state["committed_params"] = None
        else:
            params: list[tuple[str, str]] = [("q", query), ("limit", "50")]

            selected_sources = [
                sid for sid in indexed_source_ids
                if st.session_state.get(f"filter_src_{sid}", True)
            ]
            for sid in selected_sources:
                params.append(("source_ids", sid))

            selected_types = [
                t for t in ("table", "column", "row")
                if st.session_state.get(f"filter_type_{t}", True)
            ]
            for t in selected_types:
                params.append(("match_types", t))

            st.session_state["committed_query"] = query
            st.session_state["committed_params"] = params

    committed_query = st.session_state.get("committed_query", "")
    committed_params = st.session_state.get("committed_params")

    if not committed_query or committed_params is None:
        st.info("Enter a search term above and click **Search** to discover tables and columns.")
        return

    results: list[dict] | None = get("/api/search", params=committed_params)
    if results is None:
        return

    if not results:
        st.warning(f"No results found for **{committed_query}**")
        return

    # Group by source
    by_source: dict[str, list[dict]] = {}
    for r in results:
        by_source.setdefault(r["source_id"], []).append(r)

    st.markdown(f"**{len(results)} result(s)** for `{committed_query}`")
    st.divider()

    for source_id, source_results in by_source.items():
        source_type = source_results[0].get("source_type", "")
        if source_type == "sqlite":
            badge = "🗄️ SQLite"
        elif source_type == "csv_file":
            badge = "📄 CSV (file)"
        else:
            badge = "📄 CSV"

        # Collapsible per-source section (expanded by default)
        with st.expander(f"{badge} — `{source_id}` ({len(source_results)} result(s))", expanded=True):
            for res in source_results:
                render_result_card(res)


def render_result_card(res: dict) -> None:
    match_type = res.get("match_type", "table")
    table_name = res.get("table_name", "")
    table_path = res.get("table_path", "")
    source_id = res.get("source_id", "")
    col_name = res.get("column_name")
    matched_row = res.get("matched_row")
    matched_row_number = res.get("matched_row_number")

    if match_type == "table":
        title = f"📋 Table: **{table_name}**"
    elif match_type == "row":
        title = f"🧾 Row match in **{table_name}**"
    else:
        title = f"🔤 Column: **{col_name}** in `{table_name}`"

    with st.container(border=True):
        h_col, m_col = st.columns([3, 1])
        with h_col:
            st.markdown(title)
        with m_col:
            st.caption(f"{res.get('row_count', 0):,} rows")

        columns = res.get("columns", [])
        if columns:
            col_names = [c["name"] for c in columns]
            if col_name:
                col_labels = [f"**{c}**" if c == col_name else c for c in col_names]
            else:
                col_labels = col_names
            st.caption(
                "Columns: " + " · ".join(col_labels[:15])
                + ("…" if len(col_names) > 15 else "")
            )

        if matched_row:
            if matched_row_number is not None:
                st.caption(f"Sample row #{matched_row_number}")
            st.dataframe(pd.DataFrame([matched_row]), use_container_width=True, hide_index=True)

        with st.expander("View schema & sample data"):
            schema: dict | None = get(f"/api/schema/{source_id}/{table_path}")
            if schema and schema.get("success"):
                col_data = schema.get("columns", [])
                if col_data:
                    st.markdown("**Schema**")
                    df_schema = pd.DataFrame([
                        {
                            "Column": c["name"],
                            "Type": c["data_type"],
                            "Sample values": ", ".join(str(v) for v in c.get("sample_values", [])),
                        }
                        for c in col_data
                    ])
                    st.dataframe(df_schema, use_container_width=True, hide_index=True)

                sample = schema.get("sample_rows", [])
                if sample:
                    st.markdown("**Sample rows**")
                    st.dataframe(pd.DataFrame(sample), use_container_width=True, hide_index=True)
            elif schema:
                st.error(schema.get("error", "Failed to load schema"))


# ---------------------------------------------------------------------------
# Browse page
# ---------------------------------------------------------------------------

def render_browse_page() -> None:
    st.title("Browse Sources")
    st.markdown("View all indexed tables and their schemas.")

    if not get_api_key():
        st.info("Enter your API key in the sidebar to browse indexed tables.")
        return

    tables: list[dict] | None = get("/api/tables")
    if tables is None:
        return

    if not tables:
        st.warning("No tables indexed yet. Use the sidebar to index sources first.")
        return

    by_source: dict[str, list[dict]] = {}
    for t in tables:
        by_source.setdefault(t["source_id"], []).append(t)

    for source_id, source_tables in by_source.items():
        source_type = source_tables[0].get("source_type", "")
        if source_type == "sqlite":
            badge = "🗄️"
        elif source_type == "csv_file":
            badge = "📄"
        else:
            badge = "📄"
        st.subheader(f"{badge} {source_id}")
        st.caption(f"{len(source_tables)} table(s)")

        for table in source_tables:
            label = f"📋 {table['table_name']}  ({table.get('row_count', 0):,} rows)"
            with st.expander(label):
                cols = table.get("columns", [])
                if cols:
                    df = pd.DataFrame([
                        {
                            "Column": c["name"],
                            "Type": c["data_type"],
                            "Sample values": ", ".join(str(v) for v in c.get("sample_values", [])),
                        }
                        for c in cols
                    ])
                    st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()


# ---------------------------------------------------------------------------
# Upload page
# ---------------------------------------------------------------------------

_INDEXING_RULES_HELP = """
Optional JSON to control which tables/columns are indexed and whether row values
are searchable. All fields are optional. Example:

```json
{
  "exclude_tables": ["log", "audit"],
  "exclude_columns": {
    "users": ["password_hash", "token"]
  },
  "row_value_columns": {
    "products": ["name", "category"],
    "orders": ["status", "shipping_city"]
  }
}
```

**Fields**

| Field | Type | Effect |
|---|---|---|
| `include_tables` | list of strings | Only index these tables |
| `exclude_tables` | list of strings | Skip these tables |
| `include_columns` | `{table: [cols]}` | Only index listed columns per table |
| `exclude_columns` | `{table: [cols]}` | Skip listed columns per table |
| `row_value_tables` | list of strings | Enable row-value indexing for these tables |
| `row_value_columns` | `{table: [cols]}` | Index only these columns as searchable row values |

Leave blank (or `{}`) to index everything with no restrictions.
"""


def render_upload_page() -> None:
    st.title("Add Data Source")
    st.markdown(
        "Upload a **CSV** or **SQLite** database file to register it as a new data source. "
        "Each uploaded CSV file gets its own connector with individual indexing rules."
    )

    if not get_api_key():
        st.info("Enter your API key in the sidebar before uploading.")
        return

    tab_csv, tab_sqlite = st.tabs(["CSV File", "SQLite Database"])

    with tab_csv:
        _render_csv_upload()

    with tab_sqlite:
        _render_sqlite_upload()


def _render_csv_upload() -> None:
    st.subheader("Upload CSV File")
    with st.form("csv_upload_form", clear_on_submit=True):
        uploaded = st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help="The file will be stored on the server and registered as a new data source.",
        )
        source_id = st.text_input(
            "Source ID",
            placeholder="e.g. sales_q1_2025",
            help="A unique identifier for this data source. Only letters, digits, underscores and hyphens.",
        )
        description = st.text_input(
            "Description (optional)",
            placeholder="e.g. Q1 2025 sales data",
        )
        rules_json = st.text_area(
            "Indexing Rules (optional JSON)",
            value="",
            height=150,
            help=_INDEXING_RULES_HELP,
            placeholder='{\n  "row_value_columns": {\n    "my_table": ["name", "category"]\n  }\n}',
        )

        submitted = st.form_submit_button("Upload & Register", type="primary")

    if submitted:
        if not uploaded:
            st.error("Please select a CSV file.")
            return
        if not source_id.strip():
            st.error("Source ID is required.")
            return

        # Validate JSON before sending
        rules_text = rules_json.strip() or "{}"
        try:
            json.loads(rules_text)
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON in Indexing Rules: {exc}")
            return

        with st.spinner(f"Uploading {uploaded.name}..."):
            result = post(
                "/api/upload/csv",
                data={
                    "source_id": source_id.strip(),
                    "description": description.strip(),
                    "indexing_rules_json": rules_text,
                },
                files={"file": (uploaded.name, uploaded.getvalue(), "text/csv")},
            )

        if result and result.get("success"):
            st.success(
                f"Source **{result['source_id']}** registered from `{result['file']}`. "
                "Use **Index now** in the sidebar to index it."
            )
            st.rerun()
        elif result:
            st.error(result.get("detail", "Upload failed."))


def _render_sqlite_upload() -> None:
    st.subheader("Upload SQLite Database")
    with st.form("sqlite_upload_form", clear_on_submit=True):
        uploaded = st.file_uploader(
            "Choose a SQLite file (.db or .sqlite)",
            type=["db", "sqlite"],
            help="The file will be stored on the server and registered as a new data source.",
        )
        source_id = st.text_input(
            "Source ID",
            placeholder="e.g. crm_backup",
            help="A unique identifier for this data source.",
        )
        description = st.text_input(
            "Description (optional)",
            placeholder="e.g. CRM database backup March 2025",
        )
        rules_json = st.text_area(
            "Indexing Rules (optional JSON)",
            value="",
            height=150,
            help=_INDEXING_RULES_HELP,
            placeholder='{\n  "exclude_tables": ["logs", "sessions"]\n}',
        )

        submitted = st.form_submit_button("Upload & Register", type="primary")

    if submitted:
        if not uploaded:
            st.error("Please select a SQLite file.")
            return
        if not source_id.strip():
            st.error("Source ID is required.")
            return

        rules_text = rules_json.strip() or "{}"
        try:
            json.loads(rules_text)
        except json.JSONDecodeError as exc:
            st.error(f"Invalid JSON in Indexing Rules: {exc}")
            return

        with st.spinner(f"Uploading {uploaded.name}..."):
            result = post(
                "/api/upload/sqlite",
                data={
                    "source_id": source_id.strip(),
                    "description": description.strip(),
                    "indexing_rules_json": rules_text,
                },
                files={"file": (uploaded.name, uploaded.getvalue(), "application/octet-stream")},
            )

        if result and result.get("success"):
            st.success(
                f"Source **{result['source_id']}** registered from `{result['file']}`. "
                "Use **Index now** in the sidebar to index it."
            )
            st.rerun()
        elif result:
            st.error(result.get("detail", "Upload failed."))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    sources = render_sidebar()

    tab_search, tab_browse, tab_upload = st.tabs(["Search", "Browse Sources", "Add Source"])
    with tab_search:
        render_search_page(sources)
    with tab_browse:
        render_browse_page()
    with tab_upload:
        render_upload_page()


if __name__ == "__main__":
    main()
