#!/bin/bash
set -euo pipefail

case "${1:-}" in
  db-setup)
    echo "Creating database (if it does not exist)..."
    sqlx database create --database-url "$DATABASE_URL"
    echo "Running migrations..."
    sqlx migrate run --database-url "$DATABASE_URL" --source /app/migrations
    echo "Database setup complete."
    ;;
  *)
    exec harvester "$@"
    ;;
esac
