from taskhawk.backends.base import TaskhawkPublisherBaseBackend
from taskhawk.backends.publisher import get_publisher_backend
from taskhawk.models import Message


def publish(message: Message, backend: TaskhawkPublisherBaseBackend = None) -> None:
    """
    Publishes a message on Taskhawk queue
    """
    backend = backend or get_publisher_backend()
    backend.publish(message)
