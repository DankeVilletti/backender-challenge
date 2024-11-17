from typing import Any

import structlog
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from core.base_model import Model
from core.event_log_client import EventLogClient
from core.models import EventOutbox
from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from users.models import User

logger = structlog.get_logger(__name__)


class UserCreated(Model):
    email: str
    first_name: str
    last_name: str


class CreateUserRequest(UseCaseRequest):
    email: str
    first_name: str = ''
    last_name: str = ''


class CreateUserResponse(UseCaseResponse):
    result: User | None = None
    error: str = ''


class CreateUser(UseCase):
    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:
        return {
            'email': request.email,
            'first_name': request.first_name,
            'last_name': request.last_name,
        }

    def _execute(self, request: CreateUserRequest) -> CreateUserResponse:
        logger.info('creating a new user')
        try:
            user, created = User.objects.get_or_create(
                email=request.email,
                defaults={
                    'first_name': request.first_name, 'last_name': request.last_name,
                },
            )

            if created:
                logger.info('user has been created')
                self._log(user)
                return CreateUserResponse(result=user)

            logger.error('unable to create a new user')
            return CreateUserResponse(error='User with this email already exists')

        except Exception as e:
            logger.error('An error occurred while creating the user', exc_info=e)
            return CreateUserResponse(error='An unexpected error occurred')

    def _log(self, user: User) -> None:
        try:
            outbox_entry = EventOutbox(
                event_type=UserCreated,
                event_date_time=timezone.now(),
                environment=settings.ENVIRONMENT,
                event_context=user.model_dump_json(),
            )
            with transaction.atomic():
                outbox_entry.save()
            logger.info('User created event logged in the outbox')
        except Exception as e:
            logger.error('Failed to log the event', exc_info=e)

