import logging
from collections import Counter
import typing

import redis
from google.api_core.exceptions import NotFound, DeadlineExceeded
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.proto.pubsub_pb2 import ReceivedMessage
from retrying import retry

from taskhawk import Message, Priority
from taskhawk.backends.base import TaskhawkPublisherBaseBackend, log_published_message, TaskhawkConsumerBaseBackend
from taskhawk.backends.utils import get_queue_name, import_class
from taskhawk.conf import settings


logger = logging.getLogger(__name__)


class GooglePubSubPublisherBackend(TaskhawkPublisherBaseBackend):
    def __init__(self) -> None:
        self.publisher = pubsub_v1.PublisherClient.from_service_account_file(settings.GOOGLE_APPLICATION_CREDENTIALS)

    def ensure_topic_exists(self, topic_path: str) -> None:
        try:
            self.publisher.get_topic(topic_path)
        except NotFound:
            self.publisher.create_topic(topic_path)

    @retry(stop_max_attempt_number=3, stop_max_delay=3000)
    def publish_to_topic(self, topic_path: str, data: bytes, attrs: typing.Optional[dict] = None) -> None:
        attrs = attrs or {}
        attrs = dict((str(key), str(value)) for key, value in attrs.items())
        self.publisher.publish(topic_path, data=data, **attrs)

    def publish(self, message: Message) -> None:
        queue_name = get_queue_name(message.priority)
        topic_path = self.publisher.topic_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)

        # TODO: POC only, remove topic auto-creation code
        self.ensure_topic_exists(topic_path)

        message_body = message.as_dict()
        payload = self.message_payload(message_body)
        self.publish_to_topic(topic_path, payload.encode(encoding="utf-8"), message.headers)
        log_published_message(message_body)


class GooglePubSubConsumerBackend(TaskhawkConsumerBaseBackend):
    def __init__(self) -> None:
        self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(settings.GOOGLE_APPLICATION_CREDENTIALS)
        self.message_retry_state: typing.Optional[MessageRetryStateBackend] = None
        self._publisher: typing.Optional[ReceivedMessage] = None
        if settings.GOOGLE_MESSAGE_RETRY_STATE_BACKEND:
            message_retry_state_cls = import_class(settings.GOOGLE_MESSAGE_RETRY_STATE_BACKEND)
            self.message_retry_state = message_retry_state_cls()

    @property
    def publisher(self) -> GooglePubSubPublisherBackend:
        if not self._publisher:
            self._publisher = GooglePubSubPublisherBackend()
        return self._publisher

    def pull_messages(
        self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None
    ) -> typing.List[ReceivedMessage]:
        subscription_path = self._get_subsciption_path(queue_name)

        # TODO: POC only, remove subscription auto-creation code
        self._ensure_subscription_exists(queue_name, subscription_path)

        try:
            return self.subscriber.pull(
                subscription_path, num_messages, retry=None, timeout=settings.GOOGLE_SUB_READ_TIMEOUT_S
            ).received_messages
        except DeadlineExceeded:
            logger.debug(f"Pulling deadline exceeded subscription={subscription_path}")
            return []

    def process_message(self, queue_message: ReceivedMessage, **kwargs) -> None:
        try:
            self.message_handler(queue_message.message.data.decode(), ack_id=queue_message.ack_id)
        except Exception:
            if self._can_reprocess_message(queue_message, kwargs['queue_name']):
                raise

    def delete_message(self, queue_message: ReceivedMessage, **kwargs) -> None:
        subscription_path = self._get_subsciption_path(kwargs['queue_name'])
        self.subscriber.acknowledge(subscription_path, [queue_message.ack_id])

    @staticmethod
    def process_hook_kwargs(queue_name: str, queue_message: ReceivedMessage) -> typing.Dict:
        return {"queue_name": queue_name}

    def extend_visibility_timeout(self, priority: Priority, visibility_timeout_s: int, **metadata) -> None:
        """
        Extends visibility timeout of a message on a given priority queue for long running tasks.
        """
        if visibility_timeout_s < 0 or visibility_timeout_s > 600:
            raise ValueError("Invalid visibility_timeout_s")
        ack_id = metadata['ack_id']
        queue_name = get_queue_name(priority)
        subscription = self._get_subsciption_path(queue_name)
        self.subscriber.modify_ack_deadline(subscription, [ack_id], visibility_timeout_s)

    def _ensure_subscription_exists(self, queue_name: str, subscription_path: str) -> None:
        topic_path = self.subscriber.topic_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)
        try:
            self.subscriber.get_subscription(subscription_path)
        except NotFound:
            self.subscriber.create_subscription(subscription_path, topic_path)

    def _get_subsciption_path(self, queue_name: str) -> str:
        return self.subscriber.subscription_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)

    def _can_reprocess_message(self, queue_message: ReceivedMessage, queue_name: str) -> bool:
        if not self.message_retry_state:
            return True

        message = self._build_message(queue_message.message.data)
        try:
            self.message_retry_state.inc(message, queue_name)
            return True
        except MaxRetriesExceededError:
            self._move_message_to_dlq(message, queue_name)
        return False

    def _move_message_to_dlq(self, message: Message, queue_name: str) -> None:
        dlq_queue_name = f"{queue_name}-DLQ"
        dlq_topic_path = self.publisher.publisher.topic_path(settings.GOOGLE_PUBSUB_PROJECT_ID, dlq_queue_name)
        dlq_subscription_path = self._get_subsciption_path(dlq_queue_name)

        self.publisher.ensure_topic_exists(dlq_topic_path)
        self._ensure_subscription_exists(dlq_queue_name, dlq_subscription_path)

        payload = self.message_payload(message.as_dict())
        self.publisher.publish_to_topic(dlq_topic_path, payload.encode(encoding="utf-8"), message.headers)
        logger.debug('Sent message to DLQ', extra={'message_body': payload})


class MaxRetriesExceededError(Exception):
    pass


class MessageRetryStateBackend:
    def __init__(self) -> None:
        self.max_tries = settings.GOOGLE_MESSAGE_MAX_RETRIES

    def inc(self, message: Message, queue_name: str) -> None:
        raise NotImplementedError

    @staticmethod
    def _get_hash(message: Message, queue_name: str) -> str:
        return f"{queue_name}-{message.id}"


class MessageRetryStateLocMem(MessageRetryStateBackend):
    DB: typing.Counter = Counter()

    def inc(self, message: Message, queue_name: str) -> None:
        key = self._get_hash(message, queue_name)
        self.DB[key] += 1
        if self.DB[key] >= self.max_tries:
            raise MaxRetriesExceededError


class MessageRetryStateRedis(MessageRetryStateBackend):
    def __init__(self) -> None:
        super().__init__()
        self.client = redis.from_url(settings.GOOGLE_MESSAGE_RETRY_STATE_REDIS_URL)

    def inc(self, message: Message, queue_name: str) -> None:
        key = self._get_hash(message, queue_name)
        value = self.client.incr(key)
        if value >= self.max_tries:
            self.client.expire(key, 0)
            raise MaxRetriesExceededError
