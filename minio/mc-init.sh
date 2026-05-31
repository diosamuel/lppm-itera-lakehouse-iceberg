#!/bin/sh
# fungsi ini membuat bucket 
# bucket warehouse untuk lakehouse parquet
# bucket file_proposal dengan isi file_proposal/pdf/penelitian, file_proposal/pdf/pengabdian
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

# Create warehouse bucket
if ! /usr/bin/mc ls "${MC_ALIAS}/warehouse" >/dev/null 2>&1; then
    echo "[mc-init] Creating bucket 'warehouse'..."
    /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/warehouse"
fi
/usr/bin/mc anonymous set public "${MC_ALIAS}/warehouse" >/dev/null 2>&1 || true

# Create file_proposal bucket
if ! /usr/bin/mc ls "${MC_ALIAS}/file_proposal" >/dev/null 2>&1; then
    echo "[mc-init] Creating bucket 'file_proposal'..."
    /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/file_proposal"
fi
/usr/bin/mc anonymous set public "${MC_ALIAS}/file_proposal" >/dev/null 2>&1 || true

echo "[mc-mb] make bucket & folder for pdf"
/usr/bin/mc mb local/sipaper/pdf/penelitian/
/usr/bin/mc mb local/sipaper/pdf/pengabdian/
/usr/bin/mc mb local/sipaper/pdf/buku_keilmuan/

/usr/bin/mc mb local/sipaper/csv/penelitian/
/usr/bin/mc mb local/sipaper/csv/pengabdian/
/usr/bin/mc mb local/sipaper/csv/sitasi/
/usr/bin/mc mb local/sipaper/csv/buku_keilmuan/

echo "[mc-init] MinIO initialization complete."