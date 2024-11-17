import uuid
from collections.abc import Generator
from unittest.mock import ANY, patch

import pytest
from clickhouse_connect.driver import Client
from django.conf import settings

from core.models import EventOutbox
from users.models import User
from users.use_cases import CreateUser, CreateUserRequest, UserCreated

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def f_use_case() -> CreateUser:
    return CreateUser()


@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> Generator:
    f_ch_client.query(f'TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}')
    yield


def test_user_created(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@email.com', first_name='Test', last_name='Testovich',
    )

    response = f_use_case.execute(request)

    assert response.result.email == 'test@email.com'
    assert response.error == ''


def test_emails_are_unique(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@email.com', first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)
    response = f_use_case.execute(request)

    assert response.result is None
    assert response.error == 'User with this email already exists'


def test_event_log_entry_published(
    f_use_case: CreateUser,
    f_ch_client: Client,
) -> None:
    email = f'test_{uuid.uuid4()}@email.com'
    request = CreateUserRequest(
        email=email, first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)
    log = f_ch_client.query("SELECT * FROM default.event_log WHERE event_type = 'user_created'")

    assert log.result_rows == [
        (
            'user_created',
            ANY,
            'Local',
            UserCreated(email=email, first_name='Test', last_name='Testovich').model_dump_json(),
            1,
        ),
    ]


@pytest.mark.django_db
def test_create_user_success():
    request = CreateUserRequest(email='testuser@example.com', first_name='John', last_name='Doe')
    use_case = CreateUser()

    response = use_case._execute(request)

    assert response.result is not None
    assert response.result.email == 'testuser@example.com'
    assert response.error == ''
    assert EventOutbox.objects.filter(event_context__contains='testuser@example.com').exists()


@pytest.mark.django_db
def test_create_user_already_exists():
    User.objects.create(email='testuser@example.com', first_name='John', last_name='Doe')
    request = CreateUserRequest(email='testuser@example.com', first_name='Jane', last_name='Doe')
    use_case = CreateUser()

    response = use_case._execute(request)

    assert response.result is None
    assert response.error == 'User with this email already exists'


@pytest.mark.django_db
@patch('core.use_case.logger')
def test_create_user_error_handling(mock_logger):
    request = CreateUserRequest(email='testuser@example.com', first_name='John', last_name='Doe')
    use_case = CreateUser()

    with patch.object(User.objects, 'get_or_create', side_effect=Exception('Database error')):
        response = use_case._execute(request)

    assert response.result is None
    assert response.error == 'An unexpected error occurred'
    mock_logger.error.assert_called_with('An error occurred while creating the user', exc_info=True)


@pytest.mark.django_db
def test_event_logged_in_outbox():
    request = CreateUserRequest(email='newuser@example.com', first_name='Jane', last_name='Doe')
    use_case = CreateUser()

    response = use_case._execute(request)

    assert EventOutbox.objects.count() == 1
    event_outbox_entry = EventOutbox.objects.first()
    assert event_outbox_entry.event_type == UserCreated.__name__
    assert event_outbox_entry.event_context['email'] == 'newuser@example.com'


