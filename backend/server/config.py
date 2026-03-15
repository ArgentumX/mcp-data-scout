import os


INDEX_DB = os.getenv("INDEX_DB_PATH") or "/data/index.db"
HOST = os.getenv("MCP_HOST") or "0.0.0.0"
PORT = int(os.getenv("MCP_PORT") or "8000")
MASTER_API_KEY = os.getenv("MASTER_API_KEY")
