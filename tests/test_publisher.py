import json
from decimal import Decimal
from unittest import mock

import pytest

from taskhawk import Priority, publish
from taskhawk.backends.base import get_publisher_backend, TaskhawkBaseBackend
from taskhawk.backends.gcp import GooglePubSubPublisherBackend
from taskhawk.backends.utils import get_queue_name
from taskhawk.conf import settings
from taskhawk.models import Message


@pytest.fixture()
def gcloud_publisher_backend():
    with mock.patch("taskhawk.backends.gcp.pubsub_v1"):
        yield GooglePubSubPublisherBackend()


@mock.patch('taskhawk.backends.aws.boto3.client', autospec=True)
def test_get_sns_client(mock_boto3_resource):
    settings.TASKHAWK_PUBLISHER_BACKEND = 'taskhawk.backends.aws.AwsSnsPublisherBackend'
    settings.AWS_REGION = 'us-west-1'
    sns_backend = get_publisher_backend()

    mock_boto3_resource.assert_called_once_with(
        'sns',
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
        aws_session_token=settings.AWS_SESSION_TOKEN,
        endpoint_url=settings.AWS_ENDPOINT_SNS,
        config=mock.ANY,
    )
    assert sns_backend.sns_client == mock_boto3_resource.return_value


@pytest.mark.parametrize(
    'priority,suffix',
    [
        (Priority.default, ''),
        (Priority.low, '-low-priority'),
        (Priority.high, '-high-priority'),
        (Priority.bulk, '-bulk'),
    ],
)
def test__get_sns_topic(priority, suffix, sns_publisher_backend):
    assert (
        sns_publisher_backend._get_sns_topic(priority)
        == f'arn:aws:sns:{settings.AWS_REGION}:{settings.AWS_ACCOUNT_ID}:taskhawk-'
        f'{settings.TASKHAWK_QUEUE.lower()}{suffix}'
    )


def test__publish_over_sns(message_data, sns_publisher_backend):
    priority = Priority.high
    message_data['metadata']['priority'] = priority.name
    message = Message(message_data)
    sns_publisher_backend.sns_client = mock.MagicMock()

    sns_publisher_backend.publish(message)

    sns_publisher_backend.sns_client.publish.assert_called_once_with(
        TopicArn=sns_publisher_backend._get_sns_topic(priority),
        Message=sns_publisher_backend.message_payload(message_data),
        MessageAttributes={k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()},
    )


def test__publish_over_sqs(message, sqs_publisher_backend):
    queue = mock.MagicMock()
    sqs_publisher_backend.sqs._get_queue_by_name = mock.MagicMock(return_value=queue)

    sqs_publisher_backend.publish(message)

    queue.send_message.assert_called_once_with(
        MessageBody=sqs_publisher_backend.message_payload(message.as_dict()),
        MessageAttributes={k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()},
    )


def test__publish_over_google_pubsub(message, gcloud_publisher_backend):
    publish_topic = 'dummy_topic'
    queue_name = get_queue_name(message.priority)
    gcloud_publisher_backend.publisher.topic_path = mock.MagicMock(return_value=publish_topic)

    gcloud_publisher_backend.publish(message)

    gcloud_publisher_backend.publisher.topic_path.assert_called_once_with(settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)
    gcloud_publisher_backend.publisher.publish.assert_called_once_with(
        publish_topic,
        data=gcloud_publisher_backend.message_payload(message.as_dict()).encode("utf-8"),
        **message.headers,
    )


@pytest.mark.parametrize('value', [1469056316326, 1469056316326.123])
def test__convert_to_json_decimal(value, message_data):
    backend = TaskhawkBaseBackend()
    message_data['args'][0] = Decimal(value)
    message = Message(message_data)
    assert json.loads(backend.message_payload(message.as_dict()))['args'][0] == float(message.args[0])


def test__convert_to_json_non_serializable(message_data):
    backend = TaskhawkBaseBackend()
    message_data['args'][0] = object()
    message = Message(message_data)
    with pytest.raises(TypeError):
        backend.message_payload(message.as_dict())


def test_publish_non_lambda(sqs_publisher_backend, message):
    assert settings.TASKHAWK_QUEUE
    message.priority = Priority.high
    queue = mock.MagicMock()
    queue_name = get_queue_name(message.priority)
    sqs_publisher_backend.sqs._get_queue_by_name = mock.MagicMock(return_value=queue)
    # mock_publish_over_sqs.return_value = {'MessageId': sqs_id}

    publish(message, sqs_publisher_backend)

    message_attributes = {k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()}
    sqs_publisher_backend.sqs._get_queue_by_name.assert_called_once_with(QueueName=queue_name)
    queue.send_message.assert_called_once_with(
        MessageBody=sqs_publisher_backend.message_payload(message.as_dict()), MessageAttributes=message_attributes
    )


def test_publish_lambda(sns_publisher_backend, message):
    message.priority = Priority.high
    sns_publisher_backend.sns_client = mock.MagicMock()

    publish(message, sns_publisher_backend)

    sns_publisher_backend.sns_client.publish.assert_called_once_with(
        TopicArn=sns_publisher_backend._get_sns_topic(message.priority),
        Message=sns_publisher_backend.message_payload(message.as_dict()),
        MessageAttributes={k: {'DataType': 'String', 'StringValue': str(v)} for k, v in message.headers.items()},
    )
