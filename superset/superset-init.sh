#!/bin/sh
# Bootstrap script for the LPPM ITERA Superset container.
# Runs migrations, creates the admin user and finishes Superset init.
set -e

ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@superset.local}"
ADMIN_FIRSTNAME="${ADMIN_FIRSTNAME:-Superset}"
ADMIN_LASTNAME="${ADMIN_LASTNAME:-Admin}"

DB_HOST="${SUPERSET_DB_HOST:-superset-db}"
DB_PORT="${SUPERSET_DB_PORT:-5432}"

echo "[superset-init] Waiting for Postgres at ${DB_HOST}:${DB_PORT}..."
i=0
until python -c "import socket,sys; s=socket.socket(); s.settimeout(2); s.connect(('${DB_HOST}', ${DB_PORT})); s.close()" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
        echo "[superset-init] Timed out waiting for Postgres." >&2
        exit 1
    fi
    sleep 2
done
echo "[superset-init] Postgres is reachable."

echo "[superset-init] Upgrading metadata DB schema..."
superset db upgrade

echo "[superset-init] Ensuring admin user exists..."
superset fab create-admin \
    --username "${ADMIN_USERNAME}" \
    --firstname "${ADMIN_FIRSTNAME}" \
    --lastname "${ADMIN_LASTNAME}" \
    --email "${ADMIN_EMAIL}" \
    --password "${ADMIN_PASSWORD}" || true

echo "[superset-init] Initializing roles and permissions..."
superset init

echo "[superset-init] Done."
