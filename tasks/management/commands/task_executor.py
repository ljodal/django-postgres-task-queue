import logging
import signal
import time

from django.core.management.base import BaseCommand
from django.db import transaction

from tasks.models import Task

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Run a task executor, optionally limited to one or more queues"

    def add_arguments(self, parser):
        # TODO: Add choices to limit the options to any defined queues
        parser.add_argument("--queue", action="append", dest="queues")

    def handle(self, *args, **options):
        self._executing_task = False
        self._shutdown = False

        queues = options["queues"]

        try:
            # Handle the signals for warm shutdown.
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)

            while True:
                self.process_tasks(queues)
                time.sleep(1)
        except InterruptedError:
            # got shutdown signal
            pass

    def handle_shutdown(self, sig, frame):
        if self._running_task:
            logger.info("Waiting for active tasks to finish...")
            self._shutdown = True
        else:
            raise InterruptedError

    def process_tasks(self, queues):
        # We keep track of any tasks that have failed to process, to make sure
        # that they don't block execution of the next task in the list. Failed
        # tasks will be retired on the next round if they have not been
        # processed by other workers.
        failed_task_ids = []

        while True:
            if self._shutdown:
                raise InterruptedError

            try:
                self._running_task = True
                with transaction.atomic():
                    task = Task.objects.next_task(
                        in_queues=queues, exclude_task_ids=failed_task_ids
                    )

                    # If there are no more tasks to process, exit the processing
                    # loop
                    if not task:
                        return

                    try:
                        task.run()
                    except Exception:
                        failed_task_ids.append(task.id)
                        # Re-raise the exception to make sure the transaction is
                        # rolled back
                        raise
            except Exception:
                logger.exception("Failed to execute task")

            self._running_task = False
