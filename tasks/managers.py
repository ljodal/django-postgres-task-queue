from django.db import models


class TaskManager(models.Manager):
    def next_task(self, *, in_queues):
        """
        Returns the next task that should be executed or None if no tasks are
        ready for executing.

        This issues an SELECT FOR UPDATE query, so it will only work within a
        transaction, and the returned task will be locked for the duration of
        that transaction.
        """

        return (
            self.filter(started_at__isnull=True, queue__in=in_queues)
            .select_for_update(skip_locked=True, of=("self",))
            .select_related("from_schedule")
            .order_by("-run_at")
            .first()
        )
