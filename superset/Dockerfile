# syntax=docker/dockerfile:1
# Superset image for the LPPM ITERA Lakehouse stack.
# Extends the official Apache Superset image with database drivers
# needed to query the lakehouse (Iceberg via Trino, Spark Thrift, Postgres).
ARG SUPERSET_VERSION=4.1.1
FROM apache/superset:${SUPERSET_VERSION}

USER root

# System packages required by some Python DB drivers (e.g. PyHive/Thrift, psycopg2).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-dev \
        libsasl2-modules \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install extra Python drivers for the lakehouse.
COPY requirements-local.txt /app/requirements-local.txt
RUN pip install --no-cache-dir -r /app/requirements-local.txt

# Bootstrap script + custom config baked into the image so the container
# is runnable on its own. The init script is intentionally NOT bind-mounted
# from the host in compose to avoid Windows CRLF breakage.
COPY superset-init.sh /app/superset-init.sh
COPY superset_config_docker.py /app/superset_config_docker.py
# Strip Windows CR characters (safe on already-LF files) and make executable.
RUN sed -i 's/\r$//' /app/superset-init.sh \
    && chmod +x /app/superset-init.sh

ENV SUPERSET_CONFIG_PATH=/app/superset_config_docker.py

USER superset
