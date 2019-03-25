import json
import uuid
from unittest import mock

import pytest

from taskhawk import Priority
from taskhawk.backends import aws
from taskhawk.backends.exceptions import PartialFailure
from taskhawk.conf import settings


@pytest.mark.parametrize(
    "priority, queue_name",
    [
        (Priority.default, "TASKHAWK-DEV-RTEP"),
        (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
        (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
        (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
    ],
)
def test_sqs_publisher_publish_success(priority, queue_name, mock_boto3, message):
    message.priority = priority
    sqs_publisher = aws.AwsSQSPublisherBackend()
    queue = mock.MagicMock()
    sqs_publisher.sqs_resource.get_queue_by_name = mock.MagicMock(return_value=queue)

    sqs_publisher.publish(message)

    mock_boto3.resource.assert_called_once_with(
        'sqs',
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        endpoint_url=settings.AWS_ENDPOINT_SQS,
    )
    sqs_publisher.sqs_resource.get_queue_by_name.assert_called_once_with(QueueName=queue_name)
    queue.send_message.assert_called_once_with(
        MessageBody=sqs_publisher.message_payload(message.as_dict()),
        MessageAttributes={k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()},
    )


@pytest.mark.parametrize(
    "priority, topic_arn",
    [
        (Priority.default, "arn:aws:sns:DUMMY_REGION:DUMMY_ACCOUNT:taskhawk-dev-rtep"),
        (Priority.high, "arn:aws:sns:DUMMY_REGION:DUMMY_ACCOUNT:taskhawk-dev-rtep-high-priority"),
        (Priority.low, "arn:aws:sns:DUMMY_REGION:DUMMY_ACCOUNT:taskhawk-dev-rtep-low-priority"),
        (Priority.bulk, "arn:aws:sns:DUMMY_REGION:DUMMY_ACCOUNT:taskhawk-dev-rtep-bulk"),
    ],
)
def test_sns_publisher_publish_success(priority, topic_arn, mock_boto3, message):
    message.priority = priority
    sns_publisher = aws.AwsSnsPublisherBackend()
    queue = mock.MagicMock()
    sns_publisher.sns_client.get_queue_by_name = mock.MagicMock(return_value=queue)

    sns_publisher.publish(message)

    mock_boto3.client.assert_called_once_with(
        'sns',
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        endpoint_url=settings.AWS_ENDPOINT_SQS,
        config=mock.ANY,
    )
    sns_publisher.sns_client.publish.assert_called_once_with(
        TopicArn=topic_arn,
        Message=sns_publisher.message_payload(message.as_dict()),
        MessageAttributes={k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()},
    )


pre_process_hook = mock.MagicMock()
post_process_hook = mock.MagicMock()


class TestSQSConsumer:
    def setup(self):
        self.consumer = aws.AwsSQSConsumerBackend()
        pre_process_hook.reset_mock()
        post_process_hook.reset_mock()

    def test_initialization(self, mock_boto3):
        mock_boto3.resource.assert_called_once_with(
            'sqs',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SQS,
        )
        mock_boto3.client.assert_called_once_with(
            'sqs',
            region_name=settings.AWS_REGION,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            aws_session_token=settings.AWS_SESSION_TOKEN,
            endpoint_url=settings.AWS_ENDPOINT_SQS,
        )

    def test_pull_messages(self, mock_boto3):
        queue_name = "default"
        num_messages = 1
        visibility_timeout = 10
        queue = mock.MagicMock()
        self.consumer.sqs_resource.get_queue_by_name = mock.MagicMock(return_value=queue)

        self.consumer.pull_messages(queue_name, num_messages, visibility_timeout)

        self.consumer.sqs_resource.get_queue_by_name.assert_called_once_with(QueueName='default')
        queue.receive_messages.assert_called_once_with(
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=visibility_timeout,
            WaitTimeSeconds=self.consumer.WAIT_TIME_SECONDS,
        )

    @pytest.mark.parametrize(
        "priority, queue_name",
        [
            (Priority.default, "TASKHAWK-DEV-RTEP"),
            (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
            (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
            (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
        ],
    )
    def test_extend_visibility_timeout(self, priority, queue_name, mock_boto3):
        visibility_timeout_s = 10
        receipt = "receipt"
        self.consumer.sqs_client.get_queue_url = mock.MagicMock(return_value={"QueueUrl": "DummyQueueUrl"})

        self.consumer.extend_visibility_timeout(priority, visibility_timeout_s, receipt=receipt)

        self.consumer.sqs_client.get_queue_url.assert_called_once_with(QueueName=queue_name)
        self.consumer.sqs_client.change_message_visibility.assert_called_once_with(
            QueueUrl='DummyQueueUrl', ReceiptHandle='receipt', VisibilityTimeout=10
        )

    @pytest.mark.parametrize(
        "priority, queue_name",
        [
            (Priority.default, "TASKHAWK-DEV-RTEP"),
            (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
            (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
            (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
        ],
    )
    def test_success_requeue_dead_letter(self, priority, queue_name, mock_boto3):
        num_messages = 3
        visibility_timeout = 4

        messages = [mock.MagicMock() for _ in range(num_messages)]
        self.consumer.pull_messages = mock.MagicMock(side_effect=iter([messages, None]))
        dlq_name = f'{queue_name}-DLQ'

        mock_queue, mock_dlq = mock.MagicMock(), mock.MagicMock()
        mock_queue.attributes = {'RedrivePolicy': json.dumps({'deadLetterTargetArn': dlq_name})}
        mock_queue.send_messages.return_value = {'Failed': []}
        self.consumer._get_queue_by_name = mock.MagicMock(side_effect=iter([mock_queue, mock_dlq]))
        mock_dlq.delete_messages.return_value = {'Failed': []}

        self.consumer.requeue_dead_letter(priority, num_messages=num_messages, visibility_timeout=visibility_timeout)

        self.consumer._get_queue_by_name.assert_has_calls([mock.call(queue_name), mock.call(dlq_name)])

        self.consumer.pull_messages.assert_has_calls(
            [
                mock.call(dlq_name, num_messages=num_messages, visibility_timeout=visibility_timeout),
                mock.call(dlq_name, num_messages=num_messages, visibility_timeout=visibility_timeout),
            ]
        )
        mock_queue.send_messages.assert_called_once_with(
            Entries=[
                {
                    'Id': queue_message.message_id,
                    'MessageBody': queue_message.body,
                    'MessageAttributes': queue_message.message_attributes,
                }
                for queue_message in messages
            ]
        )
        mock_dlq.delete_messages.assert_called_once_with(
            Entries=[
                {'Id': queue_message.message_id, 'ReceiptHandle': queue_message.receipt_handle}
                for queue_message in messages
            ]
        )

    def test_partial_failure_requeue_dead_letter(self, mock_boto3):
        num_messages = 1
        visibility_timeout = 4
        priority = Priority.default
        queue_name = "TASKHAWK-DEV-RTEP"

        messages = [mock.MagicMock() for _ in range(num_messages)]
        self.consumer.pull_messages = mock.MagicMock(side_effect=iter([messages, None]))
        dlq_name = f'{queue_name}-DLQ'

        mock_queue, mock_dlq = mock.MagicMock(), mock.MagicMock()
        mock_queue.attributes = {'RedrivePolicy': json.dumps({'deadLetterTargetArn': dlq_name})}
        mock_queue.send_messages.return_value = {'Successful': ['success_id'], 'Failed': ["fail_id"]}
        self.consumer._get_queue_by_name = mock.MagicMock(side_effect=iter([mock_queue, mock_dlq]))

        with pytest.raises(PartialFailure):
            self.consumer.requeue_dead_letter(
                priority, num_messages=num_messages, visibility_timeout=visibility_timeout
            )

    @pytest.mark.parametrize(
        "priority, queue_name",
        [
            (Priority.default, "TASKHAWK-DEV-RTEP"),
            (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
            (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
            (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
        ],
    )
    def test_fetch_and_process_messages_success(self, priority, queue_name, mock_boto3, settings, message_data):
        settings.TASKHAWK_PRE_PROCESS_HOOK = 'tests.test_backends.test_aws.pre_process_hook'
        settings.TASKHAWK_POST_PROCESS_HOOK = 'tests.test_backends.test_aws.post_process_hook'
        num_messages = 3
        visibility_timeout = 4
        queue = mock.MagicMock()
        self.consumer.sqs_resource.get_queue_by_name = mock.MagicMock(return_value=queue)

        queue_message = mock.MagicMock()
        queue_message.body = json.dumps(message_data)
        queue_message.receipt_handle = "dummy receipt"
        queue.receive_messages = mock.MagicMock(return_value=[queue_message])
        message_mock = mock.MagicMock()
        self.consumer._build_message = mock.MagicMock(return_value=message_mock)
        self.consumer.process_message = mock.MagicMock(wraps=self.consumer.process_message)
        self.consumer.message_handler = mock.MagicMock(wraps=self.consumer.message_handler)

        self.consumer.fetch_and_process_messages(priority, num_messages, visibility_timeout)

        self.consumer.sqs_resource.get_queue_by_name.assert_called_once_with(QueueName=queue_name)
        queue.receive_messages.assert_called_once_with(
            MaxNumberOfMessages=num_messages,
            MessageAttributeNames=['All'],
            VisibilityTimeout=visibility_timeout,
            WaitTimeSeconds=self.consumer.WAIT_TIME_SECONDS,
        )
        self.consumer.process_message.assert_called_once_with(queue_message)
        self.consumer.message_handler.assert_called_once_with(queue_message.body, receipt=queue_message.receipt_handle)
        message_mock.call_task.assert_called_once_with(self.consumer, receipt=queue_message.receipt_handle)
        queue_message.delete.assert_called_once_with()
        pre_process_hook.assert_called_once_with(queue_name=queue_name, sqs_queue_message=queue_message)
        post_process_hook.assert_called_once_with(queue_name=queue_name, sqs_queue_message=queue_message)


class TestSNSConsumer:
    def setup(self):
        self.consumer = aws.AwsSnsConsumerBackend()
        pre_process_hook.reset_mock()
        post_process_hook.reset_mock()

    def test_success_process_message(self, mock_boto3, settings):
        settings.TASKHAWK_PRE_PROCESS_HOOK = 'tests.test_backends.test_aws.pre_process_hook'
        settings.TASKHAWK_POST_PROCESS_HOOK = 'tests.test_backends.test_aws.post_process_hook'
        # copy from https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-sns
        mock_record = {
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
        message_mock = mock.MagicMock()
        self.consumer._build_message = mock.MagicMock(return_value=message_mock)

        self.consumer.process_message(mock_record)

        pre_process_hook.assert_called_once_with(sns_record=mock_record)
        post_process_hook.assert_called_once_with(sns_record=mock_record)
        message_mock.call_task.assert_called_once_with(self.consumer)
