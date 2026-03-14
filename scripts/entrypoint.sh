#!/bin/sh
set -e

DB_PATH="${SQLITE_DB_PATH:-/data/sqlite/sample.db}"
CSV_DIR="${CSV_DIR:-/data/csv}"

# Seed data if SQLite database doesn't exist yet
if [ ! -f "$DB_PATH" ]; then
    echo "[entrypoint] Seeding sample data..."
    python /app/scripts/seed_data.py
    echo "[entrypoint] Seed complete."
else
    echo "[entrypoint] Data already present, skipping seed."
fi

echo "[entrypoint] Starting Data Scout server..."
exec python -m server.server
