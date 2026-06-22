#!/bin/sh
# Delete all MinIO buckets (WARNING: destructive operation)
set -e

MC_ALIAS="${MC_ALIAS:-minio}"
MC_ENDPOINT="${MC_ENDPOINT:-http://minio:9000}"
MC_ROOT_USER="${MC_ROOT_USER:-${AWS_ACCESS_KEY_ID:-admin}}"
MC_ROOT_PASS="${MC_ROOT_PASS:-${AWS_SECRET_ACCESS_KEY:-password}}"

SKIP_CONFIRM=false
for arg in "$@"; do
    case $arg in
        -y|--yes) SKIP_CONFIRM=true ;;
    esac
done

echo "[mc-delete] Waiting for MinIO at ${MC_ENDPOINT}..."
i=0
until /usr/bin/mc alias set "${MC_ALIAS}" "${MC_ENDPOINT}" "${MC_ROOT_USER}" "${MC_ROOT_PASS}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -ge 60 ]; then
        echo "[mc-delete] Timed out waiting for MinIO." >&2
        exit 1
    fi
    sleep 2
done
echo "[mc-delete] Connected to MinIO."

echo "[mc-delete] Listing all buckets..."
BUCKETS=""
while IFS= read -r line; do
    # mc ls output: "[2024-01-01 00:00:00 UTC]     0B bucket-name/"
    # Extract last word, remove trailing slash
    for word in $line; do
        lastword="$word"
    done
    lastword="${lastword%/}"
    if [ -n "$lastword" ]; then
        BUCKETS="${BUCKETS} ${lastword}"
    fi
done <<EOF
$(/usr/bin/mc ls "${MC_ALIAS}" 2>/dev/null)
EOF

BUCKETS=$(echo "$BUCKETS" | xargs 2>/dev/null || echo "$BUCKETS")

if [ -z "$BUCKETS" ]; then
    echo "[mc-delete] No buckets found."
    exit 0
fi

echo "[mc-delete] Found buckets:"
for b in $BUCKETS; do
    echo "  - $b"
done
echo ""

if [ "$SKIP_CONFIRM" = false ]; then
    printf "[mc-delete] Are you sure you want to DELETE ALL buckets? (yes/no): "
    read CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "[mc-delete] Aborted."
        exit 0
    fi
fi

for bucket in $BUCKETS; do
    echo "[mc-delete] Removing all objects in '${bucket}'..."
    /usr/bin/mc rm --recursive --force "${MC_ALIAS}/${bucket}" 2>/dev/null || true
    echo "[mc-delete] Deleting bucket '${bucket}'..."
    /usr/bin/mc rb "${MC_ALIAS}/${bucket}" 2>/dev/null || true
done

echo "[mc-delete] All buckets deleted."
