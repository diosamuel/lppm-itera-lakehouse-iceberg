#!/bin/sh
# Bootstrap script for the MinIO client (mc) container.
# Configures an alias to the MinIO server, creates the lakehouse buckets
# expected by the Iceberg REST catalog, and sets a permissive policy so
# Spark/Iceberg/Trino can read and write through the gateway.
set -e

MC_ALIAS="${MC_ALIAS:-minio}"
MC_ENDPOINT="${MC_ENDPOINT:-http://minio:9000}"
MC_ROOT_USER="${MC_ROOT_USER:-${AWS_ACCESS_KEY_ID:-admin}}"
MC_ROOT_PASS="${MC_ROOT_PASS:-${AWS_SECRET_ACCESS_KEY:-password}}"

# Buckets to create on first run. Override with WAREHOUSE_BUCKETS="a b c".
BUCKETS="${WAREHOUSE_BUCKETS:-warehouse}"

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

for bucket in ${BUCKETS}; do
    target="${MC_ALIAS}/${bucket}"
    if /usr/bin/mc ls "${target}" >/dev/null 2>&1; then
        echo "[mc-init] Bucket '${bucket}' already exists."
    else
        echo "[mc-init] Creating bucket '${bucket}'..."
        /usr/bin/mc mb --ignore-existing "${target}"
    fi
    # Public read/write so the local lakehouse stack can use anonymous S3
    # paths. Tighten this for any non-local deployment.
    /usr/bin/mc anonymous set public "${target}" >/dev/null 2>&1 || true
done

echo "[mc-init] Done."
# Keep the container alive only when explicitly requested; default is to
# exit cleanly so docker compose doesn't keep restarting it.
if [ "${MC_KEEP_ALIVE:-false}" = "true" ]; then
    tail -f /dev/null
fi
