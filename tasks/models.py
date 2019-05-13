from datetime import datetime

import pytz
from croniter import croniter
from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.db.models import Func, Q, Transform
from django.db.models.functions import Now
from django.utils import timezone

from .managers import TaskManager

# Allow task and schedule models to be swapped or replaced.
TASK_MODEL = getattr(settings, "TASKS_TASK_MODEL", "tasks.Task")
TASK_SCHEDULE_MODEL = getattr(
    settings, "TASKS_SCHEDULE_TASK_MODEL", "tasks.TaskSchedule"
)
DEFAULT_QUEUE = getattr(settings, "TASKS_DEFAULT_QUEUE", "default")


class JSONTypeof(Transform):
    """
    A custom lookup to get the type of a value in a JSON field. We use this to
    ensure that the task arguments specified are an object, and not any other
    JSON value.
    """

    lookup_name = "typeof"
    function = "jsonb_typeof"
    output_field = models.CharField()


# Register the custom typeof lookup on JSONField, as we use it for constraints below.
JSONField.register_lookup(JSONTypeof)


class Task(models.Model):
    """
    A task represents a task that should be executed at some point, either
    immediately or at a later time.
    """

    # Name used to look up the function that will execute this task
    task_name = models.CharField(max_length=255)

    # Task arguments are JSON serialized keyword arguments. Plain args are not supported
    task_arguments = JSONField()

    # A task is placed on a named queue
    queue = models.CharField(max_length=255, default=DEFAULT_QUEUE)

    # A task can be set to run at a later time. By default it will be run immediately
    run_at = models.DateTimeField(default=Now)

    # A reference back to the schedule that created this task.
    from_schedule = models.ForeignKey(
        TASK_SCHEDULE_MODEL, null=True, blank=True, on_delete=models.PROTECT
    )

    # Start time for when we started to execute this task
    started_at = models.DateTimeField(null=True, blank=True)

    # Time for when we finished executing this task
    finished_at = models.DateTimeField(null=True, blank=True)

    # The result returned from the task function is stored here for retrieval if needed
    result = JSONField(null=True, blank=True)

    objects = TaskManager()

    class Meta:
        constraints = [
            # Check constraint that validates that the task arguments are specified as
            # an object. This is required because these arguments are passed directly to
            # the function as keyword arguments.
            models.CheckConstraint(
                check=Q(task_arguments__typeof="object"),
                name="task_arguments_must_be_object",
            ),
            # Constraint to validate that we do not get any duplicate scheduled tasks.
            models.UniqueConstraint(
                name="no_duplicate_scheduled_tasks",
                fields=["from_schedule", "run_at"],
                condition=Q(from_schedule__isnull=False),
            ),
            # We only allow a single pending task from a schedule.
            models.UniqueConstraint(
                name="only_one_pending_task_from_schedule",
                fields=["from_schedule"],
                condition=Q(from_schedule__isnull=False, started_at__isnull=True),
            ),
        ]

    def run(self):
        """
        Perform this task. Sets started_at before execution and finished_at
        afterwards, and it also sets the result returned by the task. If the
        task was added from a schedule, it will also schedule the next instance
        of that schedule.
        """

        # Mark the task as started. This is used for statistics.
        self.started_at = timezone.now()
        self.save(update_fields=["started_at"])

        # TODO: Find and execute function with specified arguments
        func = ...

        self.result = func(**self.task_arguments)
        self.finished_at = timezone.now()
        self.save(update_fields=["result", "finished_at"])

        # If this task was added from a schedule, we should schedule the next
        # instance of that schedule.
        if self.from_schedule is not None:
            self.from_schedule.schedule_next(now=self.run_at)


class TaskSchedule(models.Model):

    # A cron expression that specifies when a task should be run
    schedule_expression = models.CharField(max_length=128)

    # The timezone that the task should be scheduled in.
    timezone = models.CharField(max_length=128, default=settings.TIME_ZONE)

    # A schedule can be disabled if new jobs should not be spawned. The schedule
    # is disabled by default because we need to create a task and set next_task
    # before we can set this flag to true.
    is_enabled = models.BooleanField(default=False)

    # Name of the task to executre
    task_name = models.CharField(max_length=255)

    # Arguments to give to the next task
    task_arguments = JSONField()

    # The queue to schedule tasks on
    queue = models.CharField(max_length=255)

    # The next task for this schedule. Can be null if the schedule is disabled.
    next_task = models.OneToOneField(
        TASK_MODEL, null=True, blank=True, on_delete=models.SET_NULL
    )

    class Meta:
        constraints = [
            # This constraint guarantees that enabled tasks have a next task scheduled,
            # while disabled task do not.
            models.CheckConstraint(
                check=Q(
                    Q(is_enabled=True, next_task__isnull=False)
                    | Q(is_enabled=False, next_task__isnull=True)
                ),
                name="enabled_task_require_next_task",
            ),
            # Check constraint that validates that the task arguments are specified as
            # an object. This is required because these arguments are passed directly to
            # the function as keyword arguments.
            models.CheckConstraint(
                check=Q(task_arguments__typeof="object"),
                name="task_arguments_must_be_object",
            ),
        ]

    def enable(self):
        """
        Enable this schedule. This will also schedule the next execution, as
        a next task must be scheduled for enabled tasks.
        """

        assert self.is_enabled is False
        self.schedule_next()

    def disable(self):
        """
        Disable this schedule. This will also delete the next task instance.
        """

        assert self.is_enabled is True
        next_task = self.next_task
        self.next_task = None
        self.is_enabled = False
        self.save(update_fields=["next_task", "is_enabled"])
        next_task.delete()

    def schedule_next(self, *, now=None):
        """
        Create the next task instance for this schedule, after the given time,
        which defaults to the current timestamp.
        """

        # Get the next run time from the cron expression
        if now is None:
            now = pytz.timezone(self.timezone).fromutc(datetime.utcnow())
        next_run = croniter(self.schedule_expression, now).get_next(datetime)

        # Dynamically look up the task model type to allow users to change the
        # task model through configuration.
        task_model = self._meta.get_field("next_task").model

        # Create a new task
        task = task_model.objects.create(
            task_name=self.task_name,
            task_arguments=self.task_arguments,
            queue=self.queue,
            run_at=next_run,
            from_schedule=self,
        )

        # Update the next task reference
        self.is_enabled = True
        self.next_task = task
        self.save(update_fields=["is_enabled", "next_task"])
