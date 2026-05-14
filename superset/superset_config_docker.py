# Superset configuration for the LPPM ITERA Lakehouse stack.
# This file is loaded via SUPERSET_CONFIG_PATH=/app/superset_config_docker.py
import os

# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "YOUR_OWN_RANDOM_SECRET_KEY_CHANGE_ME",
)

# Metadata DB — points at the superset-db service in docker-compose.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)

# ---------------------------------------------------------------------------
# Caching / Celery — backed by superset-redis.
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_explore_"}


class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    worker_prefetch_multiplier = 1
    task_acks_late = True


CELERY_CONFIG = CeleryConfig

# ---------------------------------------------------------------------------
# Feature flags relevant to a lakehouse setup.
# ---------------------------------------------------------------------------
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
    "ALERT_REPORTS": True,
    "EMBEDDED_SUPERSET": True,
}

# Allow upload of CSVs to databases that support it.
CSV_TO_HIVE_UPLOAD_DIRECTORY = "/tmp/"
