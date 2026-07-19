#!/bin/bash
# Reset Iceberg Tables in MinIO + REST Catalog
# WARNING: Deletes all Iceberg table data in warehouse/silver and warehouse/gold
#
# Architecture:
#   - Iceberg table data (parquet + metadata) → minio_data/warehouse/{silver,gold}
#   - Iceberg catalog metadata (table registry) → docker volume: iceberg-rest-catalog (SQLite)
#   - Both must be wiped together to avoid orphaned catalog entries on next run.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

SKIP_CONFIRM=false
for arg in "$@"; do
    case $arg in
        -y|--yes) SKIP_CONFIRM=true ;;
    esac
done

echo -e "${BOLD}${YELLOW}================================${NC}"
echo -e "${BOLD}${YELLOW}  Reset Iceberg Lakehouse Tables${NC}"
echo -e "${BOLD}${YELLOW}================================${NC}"
echo ""
echo -e "This will delete:"
echo -e "  ${RED}✗${NC} minio_data/warehouse/silver (Iceberg silver tables)"
echo -e "  ${RED}✗${NC} minio_data/warehouse/gold   (Iceberg gold tables)"
echo -e "  ${RED}✗${NC} Docker volume iceberg-rest-catalog (catalog SQLite DB)"
echo -e "  ${GREEN}✓${NC} minio_data/sipaper  (raw source files — KEPT)"
echo ""

if [ "$SKIP_CONFIRM" = false ]; then
    printf "Are you sure? (yes/no): "
    read -r CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 0
    fi
fi

# ─── Step 1: Stop services that write to MinIO/catalog ───────────────────────
echo -e "\n${YELLOW}[1/4] Stopping dependent services...${NC}"
docker compose stop trino airflow-scheduler airflow-webserver spark-iceberg rest 2>/dev/null || \
docker-compose stop trino airflow-scheduler airflow-webserver spark-iceberg rest 2>/dev/null || true
echo -e "${GREEN}  Services stopped (or were already down)${NC}"

# ─── Step 2: Delete Iceberg table data from MinIO host volume ─────────────────
echo -e "\n${YELLOW}[2/4] Deleting warehouse data from MinIO volume...${NC}"

for layer in silver gold; do
    target="./minio_data/warehouse/${layer}"
    if [ -d "$target" ]; then
        rm -rf "${target:?}"
        echo -e "${GREEN}  Deleted ${target}${NC}"
    else
        echo -e "${YELLOW}  ${target} not found (skipping)${NC}"
    fi
done

# ─── Step 3: Wipe the Iceberg REST catalog SQLite DB ─────────────────────────
# The REST catalog (iceberg-rest-fixture) stores table metadata in a SQLite DB
# inside the docker volume 'iceberg-rest-catalog'. Wiping only MinIO without
# wiping this will leave orphaned table entries and cause errors on the next run.
echo -e "\n${YELLOW}[3/4] Wiping Iceberg REST catalog volume...${NC}"

# Start the rest container briefly and delete the catalog DB file
docker compose run --rm --entrypoint "" rest \
    sh -c "rm -f /var/lib/iceberg/rest_catalog.db && echo '  Catalog DB deleted'" \
    2>/dev/null || \
docker-compose run --rm --entrypoint "" rest \
    sh -c "rm -f /var/lib/iceberg/rest_catalog.db && echo '  Catalog DB deleted'" \
    2>/dev/null || {
        echo -e "${YELLOW}  Could not access rest container — removing volume directly${NC}"
        docker volume rm lppm-itera-lakehouse-iceberg_iceberg-rest-catalog 2>/dev/null || \
        docker volume rm iceberg-rest-catalog 2>/dev/null || \
        echo -e "${YELLOW}  Volume not found or already removed${NC}"
    }
echo -e "${GREEN}  REST catalog wiped${NC}"

# ─── Step 4: Restart services ────────────────────────────────────────────────
echo -e "\n${YELLOW}[4/4] Restarting services...${NC}"
docker compose up -d rest minio 2>/dev/null || \
docker-compose up -d rest minio 2>/dev/null || true
echo -e "${GREEN}  Services restarted${NC}"

echo ""
echo -e "${BOLD}${GREEN}================================${NC}"
echo -e "${BOLD}${GREEN}  Reset Complete!${NC}"
echo -e "${BOLD}${GREEN}================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Re-run the full pipeline:"
echo "       uv run python pipeline/index.py"
echo ""
echo "  2. Re-run only the catalog setup (if needed):"
echo "       uv run python pipeline/setup_catalog.py"
echo ""
echo -e "${YELLOW}To wipe EVERYTHING including raw source files in sipaper/:${NC}"
echo "  docker compose down -v   # removes all volumes including sipaper data"
echo "  docker compose up -d"
