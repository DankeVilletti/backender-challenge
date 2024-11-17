from unittest.mock import patch

import pytest

from core.task import process_event_outbox
from core.models import EventOutbox
from django.db import DatabaseError


@pytest.fixture
def mock_event_outbox():
    # Создание тестовых данных для outbox
    return EventOutbox.objects.create(event_context={'key': 'value'}, is_processed=False)

@patch('core.event_log_client.EventLogClient.insert')
@patch('core.tasks.EventOutbox.objects.filter')
def test_process_event_outbox(mock_filter, mock_insert, mock_event_outbox):
    mock_filter.return_value = [mock_event_outbox]
    mock_insert.return_value = None
    process_event_outbox()
    mock_insert.assert_called_once()
    mock_event_outbox.refresh_from_db()
    assert mock_event_outbox.is_processed is True

@patch('core.event_log_client.EventLogClient.insert')
@patch('core.tasks.EventOutbox.objects.filter')
def test_process_event_outbox_failure(mock_filter, mock_insert, mock_event_outbox):
    mock_insert.side_effect = DatabaseError("Insert failed")
    mock_filter.return_value = [mock_event_outbox]
    process_event_outbox()
    mock_event_outbox.refresh_from_db()
    assert mock_event_outbox.is_processed is False