FROM apache/airflow:2.10.5-python3.12

USER root

# Install uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow

# Dependency Management
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml

USER airflow