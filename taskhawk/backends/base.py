import json
import logging
from decimal import Decimal

from taskhawk import Priority, Message
from taskhawk.backends.utils import get_queue_name, import_class
from taskhawk.conf import settings
from taskhawk.exceptions import ValidationError, IgnoreException, LoggingException, RetryException

logger = logging.getLogger(__name__)


class TaskhawkBaseBackend:
    @classmethod
    def build(cls, dotted_path: str):
        """
        Import a dotted module path and return the backend class instance.
        Raise ImportError if the import failed.
        """
        backend_cls = import_class(dotted_path)
        return backend_cls()

    @staticmethod
    def message_payload(data: dict) -> str:
        return json.dumps(data, default=_decimal_json_default)


class TaskhawkPublisherBaseBackend(TaskhawkBaseBackend):
    def publish(self, message: Message) -> None:
        raise NotImplementedError


class TaskhawkConsumerBaseBackend(TaskhawkBaseBackend):
    @staticmethod
    def pre_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return {}

    @staticmethod
    def process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return {}

    @staticmethod
    def post_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return {}

    def message_handler(self, message_json: str, **metadata) -> None:
        message = self._build_message(message_json)
        _log_received_message(message.as_dict())

        try:
            message.call_task(self, **metadata)
        except IgnoreException:
            logger.info(f'Ignoring task {message.id}')
            return
        except LoggingException as e:
            # log with message and extra
            logger.exception(str(e), extra=e.extra)
            # let it bubble up so message ends up in DLQ
            raise
        except RetryException:
            # Retry without logging exception
            logger.info('Retrying due to exception')
            # let it bubble up so message ends up in DLQ
            raise
        except Exception:
            logger.exception(f'Exception while processing message')
            # let it bubble up so message ends up in DLQ
            raise

    def fetch_and_process_messages(
        self, priority: Priority, num_messages: int = 1, visibility_timeout: int = None
    ) -> None:
        queue_name = get_queue_name(priority)
        for queue_message in self.pull_messages(queue_name, num_messages, visibility_timeout):
            settings.TASKHAWK_PRE_PROCESS_HOOK(**self.pre_process_hook_kwargs(queue_name, queue_message))
            process_hook_kwargs = self.process_hook_kwargs(queue_name, queue_message)
            try:
                self.process_message(queue_message, **process_hook_kwargs)
                try:
                    settings.TASKHAWK_POST_PROCESS_HOOK(**self.post_process_hook_kwargs(queue_name, queue_message))
                except Exception:
                    logger.exception(f'Exception in post process hook for message from {queue_name}')
                    raise
                try:
                    self.delete_message(queue_message, **process_hook_kwargs)
                except Exception:
                    logger.exception(f'Exception while deleting message from {queue_name}')
            except Exception:
                # already logged in message_handler
                pass

    def extend_visibility_timeout(self, priority: Priority, visibility_timeout_s: int, **metadata) -> None:
        """
        Extends visibility timeout of a message on a given priority queue for long running tasks.
        """
        raise NotImplementedError

    def requeue_dead_letter(self, priority: Priority, num_messages: int = 10, visibility_timeout: int = None) -> None:
        """
        Re-queues everything in the Taskhawk DLQ back into the Taskhawk queue.
        """
        raise NotImplementedError

    def pull_messages(self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None):
        raise NotImplementedError

    def process_message(self, queue_message, **kwargs) -> None:
        raise NotImplementedError

    def delete_message(self, queue_message, **kwargs) -> None:
        raise NotImplementedError

    @staticmethod
    def _build_message(message_json: str) -> Message:
        try:
            return Message(json.loads(message_json))
        except (ValidationError, ValueError):
            _log_invalid_message(message_json)
            raise


def _decimal_json_default(obj):
    if isinstance(obj, Decimal):
        int_val = int(obj)
        if int_val == obj:
            return int_val
        else:
            return float(obj)
    raise TypeError


def log_published_message(message_body: dict) -> None:
    logger.debug('Sent message', extra={'message_body': message_body})


def _log_received_message(message_body: dict) -> None:
    logger.debug('Received message', extra={'message_body': message_body})


def _log_invalid_message(message_json: str) -> None:
    logger.error('Received invalid message', extra={'message_json': message_json})


def get_publisher_backend():
    return TaskhawkPublisherBaseBackend.build(settings.TASKHAWK_PUBLISHER_BACKEND)


def get_consumer_backend():
    return TaskhawkConsumerBaseBackend.build(settings.TASKHAWK_CONSUMER_BACKEND)
