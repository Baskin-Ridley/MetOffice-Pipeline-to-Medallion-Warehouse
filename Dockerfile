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

# Switch to the airflow user BEFORE installing python packages
USER airflow
WORKDIR /opt/airflow

# Copy dependency management files
COPY pyproject.toml uv.lock ./

# Let uv install directly from the lockfile into the airflow user's environment
# Using --system is not needed when running as the airflow user
RUN uv pip install --no-cache "apache-airflow==${AIRFLOW_VERSION}" -r pyproject.toml