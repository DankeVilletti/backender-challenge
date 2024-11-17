from django.utils import timezone

from core.models import EventOutbox
from django.db import transaction
from core.base_model import Model
from django.conf import settings


class EventPublisher:
    @staticmethod
    @transaction.atomic
    def publish_events(events: list[Model]) -> None:
        outbox_entries = [
            EventOutbox(
                event_type=event.__class__.__name__,
                event_date_time=timezone.now(),
                environment=settings.ENVIRONMENT,
                event_context=event.model_dump_json(),
            )
            for event in events
        ]
        EventOutbox.objects.bulk_create(outbox_entries)