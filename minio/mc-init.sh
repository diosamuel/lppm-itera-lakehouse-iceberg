#!/bin/sh
# fungsi ini membuat bucket
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
echo "[mc-init] Creating bucket 'warehouse'..."
/usr/bin/mc mb --ignore-existing "${MC_ALIAS}/warehouse"
/usr/bin/mc anonymous set public "${MC_ALIAS}/warehouse" >/dev/null 2>&1 || true

# Create sipaper bucket (S3 bucket names: lowercase, hyphens only, no underscores)
echo "[mc-init] Creating bucket 'sipaper'..."
/usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper"
/usr/bin/mc anonymous set public "${MC_ALIAS}/sipaper" >/dev/null 2>&1 || true

# echo "[mc-mb] Creating folder structure: /{jenis}/{year}/{filetype}"

# # penelitian (2021-2025)
# for year in 2021 2022 2023 2024 2025; do
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/penelitian/${year}/pdf"
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/penelitian/${year}/csv"
# done

# # pengabdian (2021-2025)
# for year in 2021 2022 2023 2024 2025; do
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/pengabdian/${year}/pdf"
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/pengabdian/${year}/csv"
# done

# # buku_keilmuan (2023-2024)
# for year in 2023 2024; do
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/buku_keilmuan/${year}/pdf"
#     /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/buku_keilmuan/${year}/csv"
# done

# # sitasi (2026)
# /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/sitasi/2026/pdf"
# /usr/bin/mc mb --ignore-existing "${MC_ALIAS}/sipaper/sitasi/2026/csv"

# echo "[mc-init] MinIO initialization complete."
