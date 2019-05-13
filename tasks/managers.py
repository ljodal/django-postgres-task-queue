from django.db import models
from django.db.models.functions import Now


class TaskManager(models.Manager):
    def next_task(self, *, in_queues=None, exclude_task_ids=None):
        """
        Returns the next task that should be executed or None if no tasks are
        ready for executing.

        This issues an SELECT FOR UPDATE query, so it will only work within a
        transaction, and the returned task will be locked for the duration of
        that transaction.
        """

        qs = self.filter(started_at__isnull=True, run_at__lte=Now())

        if in_queues:
            qs = qs.filter(queue__in=in_queues)

        if exclude_task_ids:
            qs = qs.exclude(id__in=exclude_task_ids)

        return (
            qs.select_for_update(skip_locked=True, of=("self",))
            .select_related("from_schedule")
            .order_by("-run_at")
            .first()
        )
