#!/usr/bin/env bash
set -euo pipefail

TARGET_DB="harvester"
ADMIN_ROLE="admin"
READER_ROLE="harvester_reader"

if [[ -n "${DATABASE_URL:-}" ]]; then
  DB_URL_BASE="${DATABASE_URL%/*}"
  ADMIN_PSQL=(psql -v ON_ERROR_STOP=1 "${DB_URL_BASE}/postgres")
  TARGET_PSQL=(psql -v ON_ERROR_STOP=1 "${DB_URL_BASE}/${TARGET_DB}")
else
  # Intentionally do not default PGHOST:
  # - In postgres docker-entrypoint init scripts, psql should use the Unix socket.
  # - For TCP, callers can explicitly set PGHOST (e.g. localhost).
  export PGPORT="${PGPORT:-5432}"
  export PGUSER="${PGUSER:-${POSTGRES_USER:-${ADMIN_ROLE}}}"
  export PGPASSWORD="${PGPASSWORD:-${POSTGRES_PASSWORD:-admin}}"

  ADMIN_PSQL=(psql -v ON_ERROR_STOP=1 -d postgres)
  TARGET_PSQL=(psql -v ON_ERROR_STOP=1 -d "${TARGET_DB}")
fi

if ! "${ADMIN_PSQL[@]}" -tAc "SELECT 1 FROM pg_database WHERE datname = '${TARGET_DB}'" | grep -q 1; then
  "${ADMIN_PSQL[@]}" -c "CREATE DATABASE ${TARGET_DB} OWNER ${ADMIN_ROLE};"
fi

"${ADMIN_PSQL[@]}" -c "ALTER DATABASE ${TARGET_DB} OWNER TO ${ADMIN_ROLE};"
"${ADMIN_PSQL[@]}" -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${READER_ROLE}') THEN CREATE ROLE ${READER_ROLE} NOLOGIN; END IF; END \$\$;"
"${ADMIN_PSQL[@]}" -c "GRANT ${READER_ROLE} TO ${ADMIN_ROLE};"
"${TARGET_PSQL[@]}" -c "GRANT USAGE ON SCHEMA public TO ${READER_ROLE};"
"${TARGET_PSQL[@]}" -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${READER_ROLE};"
"${TARGET_PSQL[@]}" -c "ALTER DEFAULT PRIVILEGES FOR ROLE ${ADMIN_ROLE} IN SCHEMA public GRANT SELECT ON TABLES TO ${READER_ROLE};"
