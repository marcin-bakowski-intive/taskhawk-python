import json
import uuid
from unittest import mock

import pytest

from taskhawk import process_messages_for_lambda_consumer, listen_for_messages
from taskhawk.backends import base
from taskhawk.backends.base import get_consumer_backend
from taskhawk.backends.google_cloud import GooglePubSubConsumerBackend
from taskhawk.backends.utils import get_queue_name
from taskhawk.conf import settings
from taskhawk.exceptions import RetryException, ValidationError, LoggingException, IgnoreException
from taskhawk.models import Priority


@mock.patch('taskhawk.backends.aws.boto3.resource', autospec=True)
def test__get_sqs_resource(mock_boto3_resource):
    sqs_backend = get_consumer_backend()
    mock_boto3_resource.assert_called_once_with(
        'sqs',
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        endpoint_url=settings.AWS_ENDPOINT_SQS,
    )
    assert sqs_backend.sqs == mock_boto3_resource.return_value


@mock.patch('taskhawk.backends.aws.boto3.resource', autospec=True)
def test_get_queue(mock_get_sqs_resource):
    queue_name = 'foo'
    mock_get_sqs_resource.return_value.get_queue_by_name.return_value = 'foo-bar'
    sqs_backend = get_consumer_backend()

    sqs_backend.get_queue_by_name(queue_name)
    mock_get_sqs_resource.return_value.get_queue_by_name.assert_called_once_with(QueueName=queue_name)


@mock.patch('taskhawk.backends.base.Message.call_task', autospec=True)
class TestMessageHandler:
    def test_success(self, mock_call_task, message_data, message, consumer_backend):
        receipt = str(uuid.uuid4())
        consumer_backend.message_handler(json.dumps(message_data), receipt=receipt)
        mock_call_task.assert_called_once_with(message, consumer_backend, receipt=receipt)

    def test_fails_on_invalid_json(self, mock_call_task, consumer_backend):
        with pytest.raises(ValueError):
            consumer_backend.message_handler("bad json")

    @mock.patch('taskhawk.backends.base.Message.validate', autospec=True)
    def test_fails_on_validation_error(self, mock_validate, mock_call_task, message_data, consumer_backend):
        error_message = 'Invalid message body'
        mock_validate.side_effect = ValidationError(error_message)
        with pytest.raises(ValidationError):
            consumer_backend.message_handler(json.dumps(message_data))
        mock_call_task.assert_not_called()

    def test_fails_on_task_failure(self, mock_call_task, message_data, message, consumer_backend):
        mock_call_task.side_effect = Exception
        with pytest.raises(mock_call_task.side_effect):
            consumer_backend.message_handler(json.dumps(message_data))

    def test_special_handling_logging_error(self, mock_call_task, message_data, message, consumer_backend):
        mock_call_task.side_effect = LoggingException('foo', extra={'mickey': 'mouse'})
        with pytest.raises(LoggingException), mock.patch.object(base.logger, 'exception') as logging_mock:
            consumer_backend.message_handler(json.dumps(message_data))

            logging_mock.assert_called_once_with('foo', extra={'mickey': 'mouse'})

    def test_special_handling_retry_error(self, mock_call_task, message_data, message, consumer_backend):

        mock_call_task.side_effect = RetryException
        with pytest.raises(mock_call_task.side_effect), mock.patch.object(base.logger, 'info') as logging_mock:
            consumer_backend.message_handler(json.dumps(message_data))

            logging_mock.assert_called_once()

    def test_special_handling_ignore_exception(self, mock_call_task, message_data, message, consumer_backend):
        mock_call_task.side_effect = IgnoreException
        # no exception raised
        with mock.patch.object(base.logger, 'info') as logging_mock:
            consumer_backend.message_handler(json.dumps(message_data))

            logging_mock.assert_called_once()


def test_message_handler_sqs(sqs_consumer_backend):
    queue_message = mock.MagicMock()
    sqs_consumer_backend.message_handler = mock.MagicMock()

    sqs_consumer_backend.process_message(queue_message)

    sqs_consumer_backend.message_handler.assert_called_once_with(
        queue_message.body, receipt=queue_message.receipt_handle
    )


def test_message_handler_lambda(sns_consumer_backend):
    lambda_event = mock.MagicMock()
    sns_consumer_backend.message_handler = mock.MagicMock()
    sns_consumer_backend.process_message(lambda_event)

    sns_consumer_backend.message_handler.assert_called_once_with(lambda_event['Sns']['Message'])


def test_get_queue_messages(sqs_consumer_backend):
    queue = mock.MagicMock()
    num_messages = 2
    visibility_timeout = 100

    messages = sqs_consumer_backend.get_queue_messages(queue, num_messages, visibility_timeout)

    queue.receive_messages.assert_called_once_with(
        MaxNumberOfMessages=num_messages,
        WaitTimeSeconds=sqs_consumer_backend.WAIT_TIME_SECONDS,
        MessageAttributeNames=['All'],
        VisibilityTimeout=visibility_timeout,
    )
    assert messages == queue.receive_messages.return_value


@pytest.mark.parametrize(
    'priority,suffix',
    [
        (Priority.default, ''),
        (Priority.low, '-LOW-PRIORITY'),
        (Priority.high, '-HIGH-PRIORITY'),
        (Priority.bulk, '-BULK'),
    ],
)
def test_get_queue_name(priority, suffix):
    assert get_queue_name(priority) == f'TASKHAWK-{settings.TASKHAWK_QUEUE.upper()}{suffix}'


pre_process_hook = mock.MagicMock()
post_process_hook = mock.MagicMock()


class TestFetchAndProcessMessages:
    def test_success(self, consumer_backend):
        priority = Priority.default
        queue_name = get_queue_name(priority)
        num_messages = 3
        visibility_timeout = 4

        consumer_backend.pull_messages = mock.MagicMock()
        consumer_backend.pull_messages.return_value = [mock.MagicMock(), mock.MagicMock()]
        consumer_backend.process_message = mock.MagicMock()
        consumer_backend.delete_message = mock.MagicMock()

        consumer_backend.fetch_and_process_messages(priority, num_messages, visibility_timeout)

        consumer_backend.pull_messages.assert_called_once_with(queue_name, num_messages, visibility_timeout)
        consumer_backend.process_message.assert_has_calls(
            [
                mock.call(x, **consumer_backend.process_hook_kwargs(queue_name, x))
                for x in consumer_backend.pull_messages.return_value
            ]
        )
        consumer_backend.delete_message.assert_has_calls(
            [
                mock.call(x, **consumer_backend.process_hook_kwargs(queue_name, x))
                for x in consumer_backend.pull_messages.return_value
            ]
        )

    def test_preserves_messages(self, consumer_backend):
        consumer_backend.pull_messages = mock.MagicMock()
        consumer_backend.pull_messages.return_value = [mock.MagicMock()]
        consumer_backend.process_message = mock.MagicMock()
        consumer_backend.process_message.side_effect = Exception

        consumer_backend.fetch_and_process_messages(Priority.default)

        consumer_backend.pull_messages.return_value[0].delete.assert_not_called()

    def test_ignore_delete_error(self, consumer_backend):
        queue_name = get_queue_name(Priority.default)
        queue_message = mock.MagicMock()
        process_hook_kwargs = consumer_backend.process_hook_kwargs(queue_name, queue_message)
        consumer_backend.pull_messages = mock.MagicMock(return_value=[queue_message])
        consumer_backend.process_message = mock.MagicMock()
        consumer_backend.delete_message = mock.MagicMock(side_effect=Exception)

        with mock.patch.object(base.logger, 'exception') as logging_mock:
            consumer_backend.fetch_and_process_messages(Priority.default)

            logging_mock.assert_called_once()

        consumer_backend.delete_message.assert_called_once_with(
            consumer_backend.pull_messages.return_value[0], **process_hook_kwargs
        )

    def test_pre_process_hook(self, consumer_backend, settings):
        pre_process_hook.reset_mock()
        queue_name = get_queue_name(Priority.default)
        settings.TASKHAWK_PRE_PROCESS_HOOK = 'tests.test_consumer.pre_process_hook'
        consumer_backend.pull_messages = mock.MagicMock(return_value=[mock.MagicMock(), mock.MagicMock()])

        consumer_backend.fetch_and_process_messages(Priority.default)

        pre_process_hook.assert_has_calls(
            [
                mock.call(**consumer_backend.pre_process_hook_kwargs(queue_name, x))
                for x in consumer_backend.pull_messages.return_value
            ]
        )

    def test_post_process_hook(self, consumer_backend, settings):
        post_process_hook.reset_mock()
        queue_name = get_queue_name(Priority.default)
        settings.TASKHAWK_POST_PROCESS_HOOK = 'tests.test_consumer.post_process_hook'
        consumer_backend.process_message = mock.MagicMock()
        consumer_backend.pull_messages = mock.MagicMock(return_value=[mock.MagicMock(), mock.MagicMock()])

        consumer_backend.fetch_and_process_messages(Priority.default)

        post_process_hook.assert_has_calls(
            [
                mock.call(**consumer_backend.post_process_hook_kwargs(queue_name, x))
                for x in consumer_backend.pull_messages.return_value
            ]
        )

    def test_post_process_hook_exception_raised(self, consumer_backend, settings):
        queue_name = get_queue_name(Priority.default)
        settings.TASKHAWK_POST_PROCESS_HOOK = 'tests.test_consumer.post_process_hook'
        consumer_backend.process_message = mock.MagicMock()
        mock_message = mock.MagicMock()
        consumer_backend.pull_messages = mock.MagicMock(return_value=[mock_message])
        post_process_hook.reset_mock()
        post_process_hook.side_effect = RuntimeError('fail')

        consumer_backend.fetch_and_process_messages(Priority.default)

        post_process_hook.assert_called_once_with(**consumer_backend.pre_process_hook_kwargs(queue_name, mock_message))
        mock_message.delete.assert_not_called()


@mock.patch('taskhawk.backends.aws.AwsSnsConsumerBackend.process_message', autospec=True)
class TestProcessMessagesForLambdaConsumer:
    def test_success(self, mock_message_handler):
        # copy from https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sns
        mock_record1 = {
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn",
            "EventSource": "aws:sns",
            "Sns": {
                "SignatureVersion": "1",
                "Timestamp": "1970-01-01T00:00:00.000Z",
                "Signature": "EXAMPLE",
                "SigningCertUrl": "EXAMPLE",
                "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                "Message": "Hello from SNS!",
                "MessageAttributes": {
                    "request_id": {"Type": "String", "Value": str(uuid.uuid4())},
                    "TestBinary": {"Type": "Binary", "Value": "TestBinary"},
                },
                "Type": "Notification",
                "UnsubscribeUrl": "EXAMPLE",
                "TopicArn": "arn",
                "Subject": "TestInvoke",
            },
        }
        mock_record2 = {
            "EventVersion": "1.0",
            "EventSubscriptionArn": "arn",
            "EventSource": "aws:sns",
            "Sns": {
                "SignatureVersion": "1",
                "Timestamp": "1970-01-01T00:00:00.000Z",
                "Signature": "EXAMPLE",
                "SigningCertUrl": "EXAMPLE",
                "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                "Message": "Hello from SNS!",
                "MessageAttributes": {
                    "request_id": {"Type": "String", "Value": str(uuid.uuid4())},
                    "TestBinary": {"Type": "Binary", "Value": "TestBinary"},
                },
                "Type": "Notification",
                "UnsubscribeUrl": "EXAMPLE",
                "TopicArn": "arn",
                "Subject": "TestInvoke",
            },
        }
        event = {"Records": [mock_record1, mock_record2]}
        process_messages_for_lambda_consumer(event)
        mock_message_handler.assert_has_calls([mock.call(mock.ANY, mock_record1), mock.call(mock.ANY, mock_record2)])

    def test_logs_and_preserves_message(self, mock_handler):
        event = {'Records': [mock.MagicMock()]}
        mock_handler.side_effect = RuntimeError
        with pytest.raises(RuntimeError):
            process_messages_for_lambda_consumer(event)


class TestListenForMessages:
    @mock.patch("taskhawk.consumer.get_consumer_backend")
    def test_listen_for_messages(self, get_consumer_backend_mock, consumer_backend):
        get_consumer_backend_mock.return_value = consumer_backend
        consumer_backend.fetch_and_process_messages = mock.MagicMock()
        num_messages = 3
        visibility_timeout_s = 4
        loop_count = 1

        listen_for_messages(Priority.high, num_messages, visibility_timeout_s, loop_count)

        consumer_backend.fetch_and_process_messages.assert_called_once_with(
            Priority.high, num_messages, visibility_timeout_s
        )


class TestGoogleConsumerDLQ:
    def google_consumer_backend(self, max_retries):
        settings.GOOGLE_MESSAGE_MAX_RETRIES = max_retries
        settings.GOOGLE_MESSAGE_RETRY_STATE_BACKEND = "taskhawk.backends.google_cloud.MessageRetryStateLocMem"
        with mock.patch("taskhawk.backends.google_cloud.pubsub_v1"):
            return GooglePubSubConsumerBackend()

    def setup_consumer_backend(self, consumer_backend, message):
        queue_message = mock.MagicMock()
        queue_message.message.data = json.dumps(message.as_dict()).encode("utf-8")
        consumer_backend._move_message_to_dlq = mock.MagicMock()

        consumer_backend.pull_messages = mock.MagicMock(return_value=[queue_message])
        consumer_backend.message_handler = mock.MagicMock(side_effect=Exception)

    def test_message_moved_to_dlq(self, message):
        consumer_backend = self.google_consumer_backend(1)
        queue_name = get_queue_name(message.priority)
        self.setup_consumer_backend(consumer_backend, message)

        consumer_backend.fetch_and_process_messages(message.priority)

        consumer_backend._move_message_to_dlq.assert_called_once_with(message, queue_name)

    def test_message_not_moved_to_dlq(self, message):
        consumer_backend = self.google_consumer_backend(5)
        self.setup_consumer_backend(consumer_backend, message)

        consumer_backend.fetch_and_process_messages(message.priority)

        consumer_backend._move_message_to_dlq.assert_not_called()
