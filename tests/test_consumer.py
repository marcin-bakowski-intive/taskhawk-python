import uuid
from unittest import mock

import pytest

from taskhawk import process_messages_for_lambda_consumer, listen_for_messages
from taskhawk.models import Priority


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
