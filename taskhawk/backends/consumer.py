import logging

import boto3
from google.api_core.exceptions import NotFound, DeadlineExceeded
from google.cloud import pubsub_v1

from taskhawk.backends.base import TaskhawkConsumerBaseBackend
from taskhawk.conf import settings


logger = logging.getLogger(__name__)


class AwsSQSConsumerBackend(TaskhawkConsumerBaseBackend):
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

    def get_queue_by_name(self, queue_name):
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def get_queue_messages(self, queue, num_messages: int, visibility_timeout: int = None) -> list:
        params = {
            'MaxNumberOfMessages': num_messages,
            'WaitTimeSeconds': self.WAIT_TIME_SECONDS,
            'MessageAttributeNames': ['All'],
        }
        if visibility_timeout is not None:
            params['VisibilityTimeout'] = visibility_timeout
        return queue.receive_messages(**params)

    def pull_messages(self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None):
        params = {
            'MaxNumberOfMessages': num_messages,
            'WaitTimeSeconds': self.WAIT_TIME_SECONDS,
            'MessageAttributeNames': ['All'],
        }
        if visibility_timeout is not None:
            params['VisibilityTimeout'] = visibility_timeout
        return self.get_queue_by_name(queue_name).receive_messages(**params)

    def process_message(self, queue_message, **kwargs) -> None:
        message_json = queue_message.body
        receipt = queue_message.receipt_handle
        self.message_handler(message_json, receipt)

    def delete_message(self, queue_message, **kwargs) -> None:
        queue_message.delete()

    @staticmethod
    def pre_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return dict(queue_name=queue_name, sqs_queue_message=queue_message)

    @staticmethod
    def post_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return dict(queue_name=queue_name, sqs_queue_message=queue_message)


class AwsSnsConsumerBackend(TaskhawkConsumerBaseBackend):
    def get_queue_messages(self, queue, num_messages: int, visibility_timeout: int = None) -> list:
        pass

    def pull_messages(self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None):
        pass

    def process_message(self, queue_message, **kwargs) -> None:
        settings.TASKHAWK_PRE_PROCESS_HOOK(sns_record=queue_message)
        message_json = queue_message['Sns']['Message']
        self.message_handler(message_json, None)
        settings.TASKHAWK_POST_PROCESS_HOOK(sns_record=queue_message)

    def delete_message(self, queue_message, **kwargs) -> None:
        pass


class GooglePubSubConsumerBackend(TaskhawkConsumerBaseBackend):
    def __init__(self) -> None:
        self.subscriber = pubsub_v1.SubscriberClient.from_service_account_file(settings.GOOGLE_APPLICATION_CREDENTIALS)

    def _ensure_subscription_exists(self, queue_name: str, subscription_path: str) -> None:
        topic_path = self.subscriber.topic_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)
        try:
            self.subscriber.get_subscription(subscription_path)
        except NotFound:
            self.subscriber.create_subscription(subscription_path, topic_path)

    def _get_subsciption_path(self, queue_name):
        return self.subscriber.subscription_path(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)

    def pull_messages(self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None):
        subscription_path = self._get_subsciption_path(queue_name)

        # TODO: POC only, remove subscription auto-creation code
        self._ensure_subscription_exists(queue_name, subscription_path)

        try:
            return self.subscriber.pull(
                subscription_path,
                num_messages,
                retry=None,
                timeout=settings.GOOGLE_SUB_READ_TIMEOUT_S,
            ).received_messages
        except DeadlineExceeded:
            logger.debug(f"Pulling deadline exceeded subscription={subscription_path}")
            return []

    def process_message(self, queue_message, **kwargs) -> None:
        self.message_handler(queue_message.message.data.decode(), None)

    def delete_message(self, queue_message, **kwargs) -> None:
        subscription_path = self._get_subsciption_path(kwargs['queue_name'])
        self.subscriber.acknowledge(subscription_path, [queue_message.ack_id])

    @staticmethod
    def process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return {"queue_name": queue_name}


def get_consumer_backend():
    return TaskhawkConsumerBaseBackend.build(settings.TASKHAWK_CONSUMER_BACKEND)
