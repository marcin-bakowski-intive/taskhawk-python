import json
import logging

import boto3
import funcy
from botocore.config import Config
from retrying import retry

from taskhawk import Priority, Message
from taskhawk.backends.base import TaskhawkConsumerBaseBackend, log_published_message, TaskhawkPublisherBaseBackend
from taskhawk.backends.exceptions import PartialFailure
from taskhawk.backends.utils import get_queue_name
from taskhawk.conf import settings

logger = logging.getLogger(__name__)


class AwsSQSPublisherBackend(TaskhawkPublisherBaseBackend):
    WAIT_TIME_SECONDS = 20

    def __init__(self):
        self.sqs_resource = boto3.resource(
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
        queue = self.sqs_resource.get_queue_by_name(QueueName=queue_name)

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


class AwsSQSConsumerBackend(TaskhawkConsumerBaseBackend):
    WAIT_TIME_SECONDS = 20

    def __init__(self):
        self.sqs_resource = boto3.resource(
            'sqs',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SQS,
        )
        self.sqs_client = boto3.client(
            'sqs',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SQS,
        )

    def _get_queue_by_name(self, queue_name):
        return self.sqs_resource.get_queue_by_name(QueueName=queue_name)

    def pull_messages(self, queue_name: str, num_messages: int = 1, visibility_timeout: int = None):
        params = {
            'MaxNumberOfMessages': num_messages,
            'WaitTimeSeconds': self.WAIT_TIME_SECONDS,
            'MessageAttributeNames': ['All'],
        }
        if visibility_timeout is not None:
            params['VisibilityTimeout'] = visibility_timeout
        return self._get_queue_by_name(queue_name).receive_messages(**params)

    def process_message(self, queue_message, **kwargs) -> None:
        message_json = queue_message.body
        receipt = queue_message.receipt_handle
        self.message_handler(message_json, receipt=receipt)

    def delete_message(self, queue_message, **kwargs) -> None:
        queue_message.delete()

    def extend_visibility_timeout(self, priority: Priority, visibility_timeout_s: int, **metadata) -> None:
        """
        Extends visibility timeout of a message on a given priority queue for long running tasks.
        """
        receipt = metadata['receipt']
        queue_name = get_queue_name(priority)
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        self.sqs_client.change_message_visibility(
            QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=visibility_timeout_s
        )

    @staticmethod
    def _enqueue_messages(queue, queue_messages) -> None:
        params: dict = {}

        result = queue.send_messages(
            Entries=[
                funcy.merge(
                    {'Id': queue_message.message_id, 'MessageBody': queue_message.body},
                    {'MessageAttributes': queue_message.message_attributes} if queue_message.message_attributes else {},
                    params,
                )
                for queue_message in queue_messages
            ]
        )
        if result.get('Failed'):
            raise PartialFailure(result)

    def requeue_dead_letter(self, priority: Priority, num_messages: int = 10, visibility_timeout: int = None) -> None:
        """
        Re-queues everything in the Taskhawk DLQ back into the Taskhawk queue.

        :param priority: The priority queue to listen to
        :param num_messages: Maximum number of messages to fetch in one SQS call. Defaults to 10.
        :param visibility_timeout: The number of seconds the message should remain invisible to other queue readers.
        Defaults to None, which is queue default
        """
        queue_name = get_queue_name(priority)
        sqs_queue = self._get_queue_by_name(queue_name)
        dlq_queue_name = json.loads(sqs_queue.attributes['RedrivePolicy'])['deadLetterTargetArn'].split(':')[-1]
        dead_letter_queue = self._get_queue_by_name(dlq_queue_name)

        logging.info("Re-queueing messages from {} to {}".format(dead_letter_queue.url, sqs_queue.url))
        while True:
            queue_messages = self.pull_messages(
                dlq_queue_name, num_messages=num_messages, visibility_timeout=visibility_timeout
            )
            if not queue_messages:
                break

            logging.info("got {} messages from dlq".format(len(queue_messages)))

            self._enqueue_messages(sqs_queue, queue_messages)
            dead_letter_queue.delete_messages(
                Entries=[
                    {'Id': message.message_id, 'ReceiptHandle': message.receipt_handle} for message in queue_messages
                ]
            )

            logging.info("Re-queued {} messages".format(len(queue_messages)))

    @staticmethod
    def pre_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return dict(queue_name=queue_name, sqs_queue_message=queue_message)

    @staticmethod
    def post_process_hook_kwargs(queue_name: str, queue_message) -> dict:
        return dict(queue_name=queue_name, sqs_queue_message=queue_message)


class AwsSnsConsumerBackend(TaskhawkConsumerBaseBackend):
    def process_message(self, queue_message, **kwargs) -> None:
        settings.TASKHAWK_PRE_PROCESS_HOOK(sns_record=queue_message)
        message_json = queue_message['Sns']['Message']
        self.message_handler(message_json)
        settings.TASKHAWK_POST_PROCESS_HOOK(sns_record=queue_message)
