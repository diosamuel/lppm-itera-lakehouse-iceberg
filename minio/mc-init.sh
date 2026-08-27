#!/bin/sh
set -e

MC_ALIAS="${MC_ALIAS:-minio}"
MC_ENDPOINT="${MC_ENDPOINT:-http://minio:9000}"
MC_ROOT_USER="${MC_ROOT_USER:-${AWS_ACCESS_KEY_ID:-admin}}"
MC_ROOT_PASS="${MC_ROOT_PASS:-${AWS_SECRET_ACCESS_KEY:-password}}"

echo "[mc-init] Waiting for MinIO at ${MC_ENDPOINT}..."
i=0
until /usr/bin/mc alias set "${MC_ALIAS}" "${MC_ENDPOINT}" "${MC_ROOT_USER}" "${MC_ROOT_PASS}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
        echo "[mc-init] Timed out waiting for MinIO." >&2
        exit 1
    fi
    sleep 2
done
echo "[mc-init] Connected to MinIO."
echo "[mc-init] Creating bucket 'warehouse'..."
/usr/bin/mc mb --ignore-existing "${MC_ALIAS}/warehouse"
/usr/bin/mc anonymous set public "${MC_ALIAS}/warehouse" >/dev/null 2>&1 || true
echo "[mc-init] Creating bucket 'sipaper'..."
/usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper"
/usr/bin/mc anonymous set public "${MC_ALIAS}/sipaper" >/dev/null 2>&1 || true
