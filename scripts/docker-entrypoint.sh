#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
db-setup)
  : "${DATABASE_URL:?DATABASE_URL must be set for db-setup}"
  echo "Running database role/privilege setup..."
  /app/scripts/init_db.sh
  echo "Database setup complete."
  ;;
*)
  exec harvester "$@"
  ;;
esac
