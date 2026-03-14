"""
Streamlit UI for Data Scout.

Communicates with the backend via the REST API
exposed by server/server.py on /api/* endpoints.
"""

import os
from typing import Any

import pandas as pd
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE_URL", "http://mcp:8000")

st.set_page_config(
    page_title="Data Scout",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def get(path: str, params: dict | None = None) -> Any | None:
    try:
        resp = requests.get(f"{API_BASE}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(
            f"Cannot connect to backend at {API_BASE}. Is the MCP server running?")
        return None
    except Exception as e:
        st.error(f"Request failed [{path}]: {e}")
        return None


def post(path: str) -> Any | None:
    try:
        resp = requests.post(f"{API_BASE}{path}", timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.error(
            f"Cannot connect to backend at {API_BASE}. Is the MCP server running?")
        return None
    except Exception as e:
        st.error(f"Request failed [{path}]: {e}")
        return None


# ---------------------------------------------------------------------------
# Sidebar — Sources panel
# ---------------------------------------------------------------------------

def render_sidebar() -> list[dict]:
    st.sidebar.title("Data Scout")
    st.sidebar.caption("Data Discovery Tool")
    st.sidebar.divider()
    st.sidebar.subheader("Data Sources")

    sources: list[dict] | None = get("/api/sources")
    if sources is None:
        st.sidebar.warning("Backend not reachable")
        return []

    if not sources:
        st.sidebar.info("No sources configured")
        return []

    for src in sources:
        indexed = src.get("is_indexed", False)
        icon = "✅" if indexed else "⚪"
        with st.sidebar.expander(f"{icon} {src['source_id']}", expanded=False):
            st.write(f"**Type:** `{src['source_type']}`")
            st.write(f"**Description:** {src['description']}")
            st.write(f"**Location:** `{src['location']}`")
            btn_label = "Re-index" if indexed else "Index now"
            if st.button(btn_label, key=f"idx_{src['source_id']}"):
                with st.spinner(f"Indexing {src['source_id']}..."):
                    result = post(f"/api/index/{src['source_id']}")
                if result and result.get("success"):
                    st.success(f"Indexed {result['tables_indexed']} tables")
                    st.rerun()
                else:
                    err = result.get(
                        "error", "Unknown error") if result else "No response"
                    st.error(err)

    st.sidebar.divider()
    if st.sidebar.button("Index All Sources", type="primary"):
        with st.spinner("Indexing all sources..."):
            post("/api/index-all")
        st.rerun()

    return sources


# ---------------------------------------------------------------------------
# Search page
# ---------------------------------------------------------------------------

def render_search_page():
    st.title("Data Scout")
    st.markdown(
        "Search across **tables** and **columns** in all indexed data sources.")

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        query = st.text_input(
            "Search",
            placeholder="e.g. customer, order_date, sales, product_id ...",
            label_visibility="collapsed",
            key="search_query",
        )
    with col_btn:
        search_clicked = st.button(
            "Search", type="primary", use_container_width=True)

    if not query:
        st.info("Enter a search term above to discover tables and columns.")
        return

    if query:
        results: list[dict] | None = get(
            "/api/search", params={"q": query, "limit": 50})
        if results is None:
            return

        if not results:
            st.warning(f"No results found for **{query}**")
            return

        # Group by source
        by_source: dict[str, list[dict]] = {}
        for r in results:
            sid = r["source_id"]
            by_source.setdefault(sid, []).append(r)

        st.markdown(f"**{len(results)} result(s)** for `{query}`")
        st.divider()

        for source_id, source_results in by_source.items():
            source_type = source_results[0].get("source_type", "")
            type_badge = "🗄️ SQLite" if source_type == "sqlite" else "📄 CSV"
            st.subheader(f"{type_badge} — `{source_id}`")
            for res in source_results:
                render_result_card(res)
            st.divider()


def render_result_card(res: dict):
    match_type = res.get("match_type", "table")
    table_name = res.get("table_name", "")
    table_path = res.get("table_path", "")
    source_id = res.get("source_id", "")
    col_name = res.get("column_name")

    if match_type == "table":
        title = f"📋 Table: **{table_name}**"
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
                col_labels = [f"**{c}**" if c ==
                              col_name else c for c in col_names]
            else:
                col_labels = col_names
            st.caption(
                "Columns: " + " · ".join(col_labels[:15])
                + ("…" if len(col_names) > 15 else "")
            )

        with st.expander("View schema & sample data"):
            schema: dict | None = get(f"/api/schema/{source_id}/{table_path}")
            if schema and schema.get("success"):
                col_data = schema.get("columns", [])
                if col_data:
                    st.markdown("**Schema**")
                    df_schema = pd.DataFrame(
                        [
                            {
                                "Column": c["name"],
                                "Type": c["data_type"],
                                "Sample values": ", ".join(
                                    str(v) for v in c.get("sample_values", [])
                                ),
                            }
                            for c in col_data
                        ]
                    )
                    st.dataframe(
                        df_schema, use_container_width=True, hide_index=True)

                sample = schema.get("sample_rows", [])
                if sample:
                    st.markdown("**Sample rows**")
                    st.dataframe(
                        pd.DataFrame(sample), use_container_width=True, hide_index=True
                    )
            elif schema:
                st.error(schema.get("error", "Failed to load schema"))


# ---------------------------------------------------------------------------
# Browse page
# ---------------------------------------------------------------------------

def render_browse_page():
    st.title("Browse Sources")
    st.markdown("View all indexed tables and their schemas.")

    tables: list[dict] | None = get("/api/tables")
    if tables is None:
        return

    if not tables:
        st.warning(
            "No tables indexed yet. Use the sidebar to index sources first.")
        return

    # Group by source_id
    by_source: dict[str, list[dict]] = {}
    for t in tables:
        sid = t["source_id"]
        by_source.setdefault(sid, []).append(t)

    for source_id, source_tables in by_source.items():
        source_type = source_tables[0].get("source_type", "")
        type_badge = "🗄️" if source_type == "sqlite" else "📄"
        st.subheader(f"{type_badge} {source_id}")
        st.caption(f"{len(source_tables)} table(s)")

        for table in source_tables:
            label = f"📋 {table['table_name']}  ({table.get('row_count', 0):,} rows)"
            with st.expander(label):
                cols = table.get("columns", [])
                if cols:
                    df = pd.DataFrame(
                        [
                            {
                                "Column": c["name"],
                                "Type": c["data_type"],
                                "Sample values": ", ".join(
                                    str(v) for v in c.get("sample_values", [])
                                ),
                            }
                            for c in cols
                        ]
                    )
                    st.dataframe(df, use_container_width=True, hide_index=True)

        st.divider()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    render_sidebar()

    tab_search, tab_browse = st.tabs(["Search", "Browse Sources"])
    with tab_search:
        render_search_page()
    with tab_browse:
        render_browse_page()


if __name__ == "__main__":
    main()
