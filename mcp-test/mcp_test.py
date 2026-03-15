import anyio
import json
from fastmcp import Client
from fastmcp.client import SSETransport


def extract_tool_result(raw_result):
    if raw_result.content and len(raw_result.content) > 0:
        content = raw_result.content[0]
        return json.loads(content.text)
    return raw_result  


async def main():
    transport = SSETransport(
        url="http://127.0.0.1:8000/mcp/sse",
        headers={"X-API-KEY": "DEV_MASTER_API_KEY"}
    )

    print("🚀 Connecting to MCP Data Scout...\n")

    async with Client(transport=transport) as client:
        # 1. List available tools
        tools = await client.list_tools()
        print("📋 AVAILABLE TOOLS")
        print("=" * 50)
        print(json.dumps([t.name for t in tools], indent=2))
        print("\n")

        # 2. List registered sources
        sources_raw = await client.call_tool("list_sources", {})
        sources = extract_tool_result(sources_raw)
        print("📚 REGISTERED SOURCES")
        print("=" * 50)
        print(json.dumps(sources, indent=2, ensure_ascii=False))
        print("\n")

        # 3. Index a source
        index_raw = await client.call_tool("index_source", {"source_id": "seeded_sqlite_main"})
        index_result = extract_tool_result(index_raw)
        print("🔄 INDEX SOURCE RESULT")
        print("=" * 50)
        print(json.dumps(index_result, indent=2, ensure_ascii=False))
        print("\n")

        # 4. Perform search
        search_raw = await client.call_tool(
            "search",
            {
                "query": "customer email",
                "limit": 3,
                "source_ids": ["seeded_sqlite_main"]
            }
        )
        search_result = extract_tool_result(search_raw)
        print("🔍 SEARCH RESULTS")
        print("=" * 50)
        print(json.dumps(search_result, indent=2, ensure_ascii=False))
        print("\n")

        # 5. Get table schema + sample rows
        schema_raw = await client.call_tool(
            "get_schema",
            {
                "source_id": "seeded_sqlite_main",
                "path": "customers"
            }
        )
        schema_result = extract_tool_result(schema_raw)
        print("📋 TABLE SCHEMA + SAMPLE")
        print("=" * 50)
        print(json.dumps(schema_result, indent=2, ensure_ascii=False))
        print("\n")

    print("✅ All MCP calls completed successfully!")


if __name__ == "__main__":
    anyio.run(main)