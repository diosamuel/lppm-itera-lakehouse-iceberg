# Superset configuration for the LPPM ITERA Lakehouse stack.
# This file is loaded via SUPERSET_CONFIG_PATH=/app/superset_config_docker.py
import os

TALISMAN_ENABLED = False
TALISMAN_CONFIG = {
    "content_security_policy": {
        "default-src": "'self' * data: blob:",
        "script-src": "'self' 'unsafe-inline' 'unsafe-eval' *",
        "style-src": "'self' 'unsafe-inline' *",
        "img-src": "'self' data: blob: *",
        "connect-src": "'self' *",
        "frame-src": "'self' *",
        "font-src": "'self' *",
    },
    "force_https": False,
    "session_cookie_secure": False,
}
# Core
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "YOUR_OWN_RANDOM_SECRET_KEY_CHANGE_ME",
)

# Metadata DB — shared Postgres instance.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@shared-postgres:5432/superset",
)

# Caching — backed by superset-redis (no Celery workers in this stack).
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

# Feature flags
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
    "EMBEDDED_SUPERSET": True,
    "TALISMAN_ENABLED": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
