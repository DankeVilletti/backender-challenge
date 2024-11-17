from celery import shared_task
from core.event_log_client import EventLogClient
from core.models import EventOutbox
from django.db import transaction


@shared_task
def process_event_outbox():
    events = EventOutbox.objects.filter(is_processed=False).select_for_update()
    if not events:
        return
    batch_size = 100
    event_batches = [events[i:i + batch_size] for i in range(0, len(events), batch_size)]

    with EventLogClient.init() as client:
        for batch in event_batches:
            client.insert([event.event_context for event in batch])
            with transaction.atomic():
                EventOutbox.objects.filter(id__in=[event.id for event in batch]).update(is_processed=True)
