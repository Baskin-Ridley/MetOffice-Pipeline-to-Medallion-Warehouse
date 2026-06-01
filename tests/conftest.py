import os
import sys
from pathlib import Path
from unittest.mock import MagicMock
import pytest

ROOT = Path(__file__).parent.parent
for _p in [
    str(ROOT),
    str(ROOT / "dags"),
    str(ROOT / "common"),
    str(ROOT / "scripts" / "ingestion"),
    str(ROOT / "scripts" / "bronze"),
    str(ROOT / "scripts" / "silver"),
    str(ROOT / "scripts" / "gold"),
]:
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


def pytest_collection_modifyitems(items):
    """Auto-skip tests that use the `spark` fixture when Java is not available."""
    import shutil

    if shutil.which("java") is not None:
        return
    skip = pytest.mark.skip(reason="Java not found — run Spark tests inside the project Docker container")
    for item in items:
        if "spark" in item.fixturenames:
            item.add_marker(skip)


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
