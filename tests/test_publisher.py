from unittest import mock

from taskhawk import publish


def test_publish_call(mock_boto3, settings, message):
    settings.TASKHAWK_PUBLISHER_BACKEND = "taskhawk.backends.aws.AwsSQSPublisherBackend"

    with mock.patch("taskhawk.backends.aws.AwsSQSPublisherBackend.publish") as publish_mock:
        publish(message)

        publish_mock.assert_called_once_with(message)
