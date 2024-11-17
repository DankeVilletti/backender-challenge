import pytest
from unittest.mock import MagicMock
from core.event_log_client import EventLogClient
from clickhouse_connect.driver.exceptions import DatabaseError


@pytest.fixture
def mock_client():
    return MagicMock(spec=EventLogClient)


def test_insert_success(mock_client):
    mock_data = [{'event_context': {'key': 'value'}}]
    mock_client.insert(mock_data)
    mock_client._client.insert.assert_called_once()


def test_insert_failure(mock_client):
    mock_client._client.insert.side_effect = DatabaseError("Insert failed")
    mock_data = [{'event_context': {'key': 'value'}}]
    with pytest.raises(DatabaseError):
        mock_client.insert(mock_data)
