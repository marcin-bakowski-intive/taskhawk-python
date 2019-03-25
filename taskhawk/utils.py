from taskhawk.backends.base import get_consumer_backend
from taskhawk.models import Priority


def extend_visibility_timeout(priority: Priority, receipt: str, visibility_timeout_s: int) -> None:
    """
    Extends visibility timeout of a message on a given priority queue for long running tasks in SQS queues.
    """

    consumer_backend = get_consumer_backend()
    consumer_backend.extend_visibility_timeout(priority, visibility_timeout_s, receipt=receipt)
