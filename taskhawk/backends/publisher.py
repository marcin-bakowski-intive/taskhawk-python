from typing import Optional

import boto3
from botocore.config import Config
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
from retrying import retry

from taskhawk import Message, Priority
from taskhawk.backends.base import log_published_message, TaskhawkPublisherBaseBackend
from taskhawk.conf import settings
from taskhawk.utils import get_queue_name


class AwsSQSPublisherBackend(TaskhawkPublisherBaseBackend):
    WAIT_TIME_SECONDS = 20

    def __init__(self):
        self.sqs = boto3.resource(
            'sqs',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SQS,
        )

    @staticmethod
    @retry(stop_max_attempt_number=3, stop_max_delay=3000)
    def _publish_over_sqs(queue, message_json: str, message_attributes: dict) -> dict:
        # transform (http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.send_message)
        message_attributes = {k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message_attributes.items()}
        return queue.send_message(MessageBody=message_json, MessageAttributes=message_attributes)

    def publish(self, message: Message) -> None:
        queue_name = get_queue_name(message.priority)
        queue = self.sqs.get_queue_by_name(QueueName=queue_name)

        message_body = message.as_dict()
        self._publish_over_sqs(queue, self.message_payload(message_body), message.headers)
        log_published_message(message_body)


class AwsSnsPublisherBackend(TaskhawkPublisherBaseBackend):
    def __init__(self):
        config = Config(connect_timeout=settings.AWS_CONNECT_TIMEOUT_S, read_timeout=settings.AWS_READ_TIMEOUT_S)
        self.sns_client = boto3.client(
            'sns',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SNS,
            config=config,
        )

    @staticmethod
    def _get_sns_topic(priority: Priority) -> str:
        topic = (
            f'arn:aws:sns:{settings.AWS_REGION}:{settings.AWS_ACCOUNT_ID}:taskhawk-{settings.TASKHAWK_QUEUE.lower()}'
        )
        if priority == Priority.high:
            topic += '-high-priority'
        elif priority == Priority.low:
            topic += '-low-priority'
        elif priority == Priority.bulk:
            topic += '-bulk'
        return topic

    @retry(stop_max_attempt_number=3, stop_max_delay=3000)
    def _publish_over_sns(self, topic: str, message_json: str, message_attributes: dict) -> None:
        # transform (http://boto.cloudhackers.com/en/latest/ref/sns.html#boto.sns.SNSConnection.publish)
        message_attributes = {k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message_attributes.items()}
        self.sns_client.publish(TopicArn=topic, Message=message_json, MessageAttributes=message_attributes)

    def publish(self, message: Message) -> None:
        message_body = message.as_dict()
        topic = self._get_sns_topic(message.priority)
        self._publish_over_sns(topic, self.message_payload(message_body), message.headers)
        log_published_message(message_body)


class GooglePubSubPublisherBackend(TaskhawkPublisherBaseBackend):
    def __init__(self) -> None:
        self.publisher = pubsub_v1.PublisherClient.from_service_account_file(settings.GOOGLE_APPLICATION_CREDENTIALS)

    def _ensure_topic_exists(self, topic_path) -> None:
        try:
            self.publisher.get_topic(topic_path)
        except NotFound:
            self.publisher.create_topic(topic_path)

    @retry(stop_max_attempt_number=3, stop_max_delay=3000)
    def _publish(self, topic_path: str, data: bytes, attrs: Optional[dict] = None) -> None:
        attrs = attrs or {}
        attrs = dict((str(key), str(value)) for key, value in attrs.items())
        self.publisher.publish(topic_path, data=data, **attrs)

    def publish(self, message: Message) -> None:
        queue_name = get_queue_name(message.priority)
        topic_path = self.publisher.topic_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)

        # TODO: POC only, remove topic auto-creation code
        self._ensure_topic_exists(topic_path)

        message_body = message.as_dict()
        payload = self.message_payload(message_body)
        self._publish(topic_path, payload.encode(encoding="utf-8"), message.headers)
        log_published_message(message_body)


def get_publisher_backend():
    return TaskhawkPublisherBaseBackend.build(settings.TASKHAWK_PUBLISHER_BACKEND)
