#!/bin/bash
# Reset Iceberg Tables in MinIO + REST Catalog
# WARNING: Deletes all Iceberg table data in warehouse/silver and warehouse/gold
#
# Architecture:
#   - Iceberg table data (parquet + metadata) -> minio_data/warehouse/{silver,gold}
#   - Iceberg catalog metadata (table registry) -> docker volume: iceberg-rest-catalog (SQLite)
#   - Both must be wiped together to avoid orphaned catalog entries on next run.

set -euo pipefail

SKIP_CONFIRM=false
for arg in "$@"; do
    case $arg in
        -y|--yes) SKIP_CONFIRM=true ;;
    esac
done

echo "========================================"
echo "  Reset Iceberg Lakehouse Tables"
echo "========================================"
echo ""
echo "This will delete:"
echo "  [X] minio_data/warehouse/silver (Iceberg silver tables)"
echo "  [X] minio_data/warehouse/gold   (Iceberg gold tables)"
echo "  [X] Docker volume iceberg-rest-catalog (catalog SQLite DB)"
echo "  [OK] minio_data/sipaper  (raw source files -- KEPT)"
echo ""

if [ "$SKIP_CONFIRM" = false ]; then
    printf "Are you sure? (yes/no): "
    read -r CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
fi

# --- Step 1: Stop services that write to MinIO/catalog ----------------------
echo ""
echo "[1/4] Stopping dependent services..."
docker compose stop trino airflow-scheduler airflow-webserver spark-iceberg rest 2>/dev/null || \
docker-compose stop trino airflow-scheduler airflow-webserver spark-iceberg rest 2>/dev/null || true
echo "  Services stopped (or were already down)"

# --- Step 2: Delete Iceberg table data from MinIO host volume ----------------
echo ""
echo "[2/4] Deleting warehouse data from MinIO volume..."

for layer in silver gold bronze; do
    target="./minio_data/warehouse/${layer}"
    if [ -d "$target" ]; then
        rm -rf "${target:?}"
        echo "  Deleted ${target}"
    else
        echo "  ${target} not found (skipping)"
    fi
done

# --- Step 3: Wipe the Iceberg REST catalog SQLite DB ------------------------
# The REST catalog (iceberg-rest-fixture) stores table metadata in a SQLite DB
# inside the docker volume 'iceberg-rest-catalog'. Wiping only MinIO without
# wiping this will leave orphaned table entries and cause errors on the next run.
echo ""
echo "[3/4] Wiping Iceberg REST catalog volume..."

# Start the rest container briefly and delete the catalog DB file
docker compose run --rm --entrypoint "" rest \
    sh -c "rm -f /var/lib/iceberg/rest_catalog.db && echo '  Catalog DB deleted'" \
    2>/dev/null || \
docker-compose run --rm --entrypoint "" rest \
    sh -c "rm -f /var/lib/iceberg/rest_catalog.db && echo '  Catalog DB deleted'" \
    2>/dev/null || {
        echo "  Could not access rest container - removing volume directly"
        docker volume rm lppm-itera-lakehouse-iceberg_iceberg-rest-catalog 2>/dev/null || \
        docker volume rm iceberg-rest-catalog 2>/dev/null || \
        echo "  Volume not found or already removed"
    }
echo "  REST catalog wiped"

# --- Step 4: Restart services ------------------------------------------------
echo ""
echo "[4/4] Restarting services..."
docker compose up -d rest minio 2>/dev/null || \
docker-compose up -d rest minio 2>/dev/null || true
echo "  Services restarted"

echo ""
echo "========================================"
echo "  Reset Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Re-run the full pipeline:"
echo "       uv run python pipeline/index.py"
echo ""
echo "  2. Re-run only the catalog setup (if needed):"
echo "       uv run python pipeline/setup_catalog.py"
echo ""
echo "To wipe EVERYTHING including raw source files in sipaper/:"
echo "  docker compose down -v   # removes all volumes including sipaper data"
echo "  docker compose up -d"
