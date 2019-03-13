import logging
import time
import uuid
from unittest import mock

import pytest
from moto import mock_sqs, mock_sns

from taskhawk import Message
import taskhawk.conf

# initialize tasks
import tests.tasks  # noqa
from taskhawk.backends import aws
from taskhawk.backends.base import TaskhawkBaseBackend


def pytest_configure():
    logging.basicConfig()


@pytest.fixture
def settings():
    """
    Use this fixture to override settings. Changes are automatically reverted
    """
    overrides = {}
    original_module = taskhawk.conf.settings._user_settings

    class Wrapped:
        def __getattr__(self, name):
            return overrides.get(name, getattr(original_module, name))

    taskhawk.conf.settings._user_settings = Wrapped()
    taskhawk.conf.settings.clear_cache()

    try:
        yield taskhawk.conf.settings._user_settings
    finally:
        taskhawk.conf.settings._user_settings = original_module
        taskhawk.conf.settings.clear_cache()


@pytest.fixture(name='message_data')
def _message_data():
    return {
        "id": "b1328174-a21c-43d3-b303-964dfcc76efc",
        "metadata": {"timestamp": int(time.time() * 1000), "version": "1.0", "priority": "default"},
        "headers": {'request_id': str(uuid.uuid4())},
        "task": "tests.tasks.send_email",
        "args": ["example@email.com", "Hello!"],
        "kwargs": {"from_email": "hello@spammer.com"},
    }


@pytest.fixture()
def message(message_data):
    return Message(message_data)


@pytest.fixture()
def sqs_publisher_backend(settings):
    settings.AWS_REGION = 'us-west-1'
    with mock_sqs():
        yield aws.AwsSQSPublisherBackend()


@pytest.fixture()
def sns_publisher_backend(settings):
    settings.AWS_REGION = 'us-west-1'
    with mock_sns():
        yield aws.AwsSnsPublisherBackend()


@pytest.fixture()
def sqs_consumer_backend(settings):
    settings.AWS_REGION = 'us-west-1'
    with mock_sqs():
        yield aws.AwsSQSConsumerBackend()


@pytest.fixture()
def sns_consumer_backend(settings):
    settings.AWS_REGION = 'us-west-1'
    with mock_sns():
        yield aws.AwsSnsConsumerBackend()


@pytest.fixture(
    params=["taskhawk.backends.aws.AwsSQSConsumerBackend", "taskhawk.backends.gcp.GooglePubSubConsumerBackend"]
)
def consumer_backend(request, settings):
    settings.AWS_REGION = 'us-west-1'
    with mock_sqs(), mock.patch("taskhawk.backends.gcp.pubsub_v1"):
        yield TaskhawkBaseBackend.build(request.param)
