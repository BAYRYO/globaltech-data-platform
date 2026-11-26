# Reproducible test environment: Python 3.11 + Java 17 + PySpark 3.5.3 (Spark local mode).
# Used by scripts/test.sh so contributors need nothing but Docker.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt

ENV PYTHONPATH=/workspace/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3
