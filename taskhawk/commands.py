from taskhawk import Priority
from taskhawk.backends.base import get_consumer_backend


def requeue_dead_letter(priority: Priority, num_messages: int = 10, visibility_timeout: int = None) -> None:
    """
    Re-queues everything in the Taskhawk DLQ back into the Taskhawk queue.

    :param priority: The priority queue to listen to
    :param num_messages: Maximum number of messages to fetch in one call. Defaults to 10.
    :param visibility_timeout: The number of seconds the message should remain invisible to other queue readers.
        Defaults to None, which is queue default
    """
    consumer_backend = get_consumer_backend()
    consumer_backend.requeue_dead_letter(priority, num_messages, visibility_timeout)


def extend_visibility_timeout(priority: Priority, visibility_timeout_s: int, **kwargs) -> None:
    """
    Extends visibility timeout of a message on a given priority queue for long running tasks in SQS queues.
    """

    consumer_backend = get_consumer_backend()
    consumer_backend.extend_visibility_timeout(priority, visibility_timeout_s, **kwargs)
