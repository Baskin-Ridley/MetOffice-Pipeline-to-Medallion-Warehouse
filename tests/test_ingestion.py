"""
Tests for the Met Office API fetch functions.
All HTTP calls are mocked — no real API key or network needed.
"""
import pytest
from unittest.mock import MagicMock, patch
import requests


# ---------------------------------------------------------------------------
# Metadata ingestion
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fetch_metadata():
    from ingest_met_office_metadata_to_landed import fetch_met_office_metadata
    return fetch_met_office_metadata


def test_fetch_metadata_returns_first_item_on_success(fetch_metadata):
    mock_response = MagicMock()
    mock_response.json.return_value = [{"geohash": "gcpu", "area": "London"}]
    mock_response.raise_for_status.return_value = None

    with patch("ingest_met_office_metadata_to_landed.requests.get", return_value=mock_response):
        result = fetch_metadata(51.47, -0.45)

    assert result == {"geohash": "gcpu", "area": "London"}


def test_fetch_metadata_returns_none_on_empty_response(fetch_metadata):
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None

    with patch("ingest_met_office_metadata_to_landed.requests.get", return_value=mock_response):
        result = fetch_metadata(51.47, -0.45)

    assert result is None


def test_fetch_metadata_returns_none_on_http_error(fetch_metadata):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("401")

    with patch("ingest_met_office_metadata_to_landed.requests.get", return_value=mock_response):
        result = fetch_metadata(51.47, -0.45)

    assert result is None


def test_fetch_metadata_returns_none_on_connection_error(fetch_metadata):
    with patch(
        "ingest_met_office_metadata_to_landed.requests.get",
        side_effect=requests.exceptions.ConnectionError("timeout"),
    ):
        result = fetch_metadata(51.47, -0.45)

    assert result is None


# ---------------------------------------------------------------------------
# Observations ingestion
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fetch_observations():
    from ingest_met_office_land_observations_to_landed import fetch_met_office_data
    return fetch_met_office_data


def test_fetch_observations_returns_json_on_success(fetch_observations):
    payload = {"observations": [{"temperature": 12.0}]}
    mock_response = MagicMock()
    mock_response.json.return_value = payload
    mock_response.raise_for_status.return_value = None

    with patch("ingest_met_office_land_observations_to_landed.requests.get", return_value=mock_response):
        result = fetch_observations("gcpu")

    assert result == payload


def test_fetch_observations_returns_none_on_http_error(fetch_observations):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

    with patch("ingest_met_office_land_observations_to_landed.requests.get", return_value=mock_response):
        result = fetch_observations("gcpu")

    assert result is None
