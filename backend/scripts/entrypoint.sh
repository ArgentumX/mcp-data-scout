#!/bin/sh
set -e

UPLOADS_DIR="${UPLOADS_DIR:-/data/uploads}"
DB_PATH="${UPLOADS_DIR}/seeded_sqlite_main.db"
SEED="${SEED:-false}"

# Seed data only when SEED=true and the DB hasn't been created yet
if [ "$SEED" = "true" ]; then
  if [ ! -f "$DB_PATH" ]; then
    echo "[entrypoint] SEED=true — seeding sample data..."
    python3 /app/scripts/seed_data.py
    echo "[entrypoint] Seed complete."
  else
    echo "[entrypoint] SEED=true but data already present, skipping seed."
  fi
else
  echo "[entrypoint] SEED is not enabled, skipping seed."
fi

echo "[entrypoint] Starting Data Scout server..."
exec python3 -m server.server
