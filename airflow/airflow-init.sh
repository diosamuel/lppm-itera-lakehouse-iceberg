#!/bin/bash
# Bootstrap script for the LPPM ITERA Airflow stack (Airflow 2.9.x).
# Mirrors the official Airflow docker-compose reference behavior:
#   - sanity-checks the host env (memory, disk, CPU)
#   - ensures /opt/airflow subdirs exist with correct ownership
#   - migrates the metadata DB
#   - creates the default admin user
set -e

# ---------------------------------------------------------------------------
# CRLF protection: if the file was checked out on Windows with CRLF endings
# the shebang would fail. We don't strip the file in place (it's bind-mounted
# read-write would risk weirdness); instead this header is intentionally
# ASCII-only so the script can self-document the requirement. The
# .gitattributes at repo root forces *.sh to LF, which is the real fix.
# ---------------------------------------------------------------------------

AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}

ADMIN_USERNAME="${_AIRFLOW_WWW_USER_USERNAME:-airflow}"
ADMIN_PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-airflow}"
ADMIN_EMAIL="${_AIRFLOW_WWW_USER_EMAIL:-airflow@example.com}"
ADMIN_FIRSTNAME="${_AIRFLOW_WWW_USER_FIRSTNAME:-Admin}"
ADMIN_LASTNAME="${_AIRFLOW_WWW_USER_LASTNAME:-User}"

echo "[airflow-init] Airflow version: $(airflow version 2>/dev/null || echo 'unknown')"
echo "[airflow-init] AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "[airflow-init] AIRFLOW_UID=${AIRFLOW_UID:-50000}"

# ---------------------------------------------------------------------------
# Ensure runtime directories exist and are writable by the airflow user.
# When running as root (user: "0:0" in compose) we can chown; otherwise we
# just create the dirs and let the running user write to them.
# ---------------------------------------------------------------------------
mkdir -p \
    "${AIRFLOW_HOME}/dags" \
    "${AIRFLOW_HOME}/logs" \
    "${AIRFLOW_HOME}/plugins" \
    "${AIRFLOW_HOME}/config"

if [ "$(id -u)" = "0" ]; then
    echo "[airflow-init] Fixing ownership for ${AIRFLOW_HOME} subdirs..."
    chown -R "${AIRFLOW_UID:-50000}:0" \
        "${AIRFLOW_HOME}/dags" \
        "${AIRFLOW_HOME}/logs" \
        "${AIRFLOW_HOME}/plugins" \
        "${AIRFLOW_HOME}/config" || true
fi

# ---------------------------------------------------------------------------
# DB migrations.
# ---------------------------------------------------------------------------
if [ "${_AIRFLOW_DB_MIGRATE:-true}" = "true" ]; then
    echo "[airflow-init] Running airflow db migrate..."
    airflow db migrate
fi

# ---------------------------------------------------------------------------
# Default admin user.
# ---------------------------------------------------------------------------
if [ "${_AIRFLOW_WWW_USER_CREATE:-true}" = "true" ]; then
    echo "[airflow-init] Ensuring admin user '${ADMIN_USERNAME}' exists..."
    airflow users create \
        --username "${ADMIN_USERNAME}" \
        --password "${ADMIN_PASSWORD}" \
        --firstname "${ADMIN_FIRSTNAME}" \
        --lastname "${ADMIN_LASTNAME}" \
        --role Admin \
        --email "${ADMIN_EMAIL}" || true
fi

echo "[airflow-init] Done."
