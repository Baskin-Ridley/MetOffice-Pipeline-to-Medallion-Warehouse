import os
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest

ROOT = Path(__file__).parent.parent
for _p in [str(ROOT), str(ROOT / "dags"), str(ROOT / "common")]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("GCS_BUCKET", "test-bucket")
os.environ.setdefault("GCP_REGION", "europe-west2")

# Patch Variable.get before any DAG or script module is imported, since several
# modules call Variable.get() at module level to set constants.
from airflow.models import Variable

_VARS = {
    "datalake_bucket": "test-bucket",
    "spark_jars_packages": "io.delta:delta-spark_2.13:3.1.0",
    "MET_OFFICE_API_KEY": "test-api-key",
}
Variable.get = MagicMock(side_effect=lambda key, default=None: _VARS.get(key, default))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("pytest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
