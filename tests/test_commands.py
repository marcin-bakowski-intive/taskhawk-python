from unittest import mock

from taskhawk import requeue_dead_letter, extend_visibility_timeout, Priority


def test_requeue_dead_letter_consumer_call(mock_boto3, settings):
    settings.TASKHAWK_CONSUMER_BACKEND = "taskhawk.backends.aws.AwsSQSConsumerBackend"
    priority = Priority.high
    num_messages = 3
    visibility_timeout = 4

    with mock.patch("taskhawk.backends.aws.AwsSQSConsumerBackend.requeue_dead_letter") as requeue_dead_letter_mock:
        requeue_dead_letter(priority, num_messages, visibility_timeout)

        requeue_dead_letter_mock.assert_called_once_with(priority, num_messages, visibility_timeout)


def test_extend_visibility_timeout_consumer_call(mock_boto3, settings):
    settings.TASKHAWK_CONSUMER_BACKEND = "taskhawk.backends.aws.AwsSQSConsumerBackend"
    priority = Priority.high
    visibility_timeout = 4

    with mock.patch("taskhawk.backends.aws.AwsSQSConsumerBackend.extend_visibility_timeout") as visibility_timeout_mock:
        extend_visibility_timeout(priority, visibility_timeout, receipe="receipt")

        visibility_timeout_mock.assert_called_once_with(priority, visibility_timeout, receipe="receipt")
