import random
import uuid
from unittest import mock

from taskhawk import extend_visibility_timeout
from taskhawk.backends.utils import get_queue_name
from taskhawk.models import Priority


def test_extend_visibility_timeout(sqs_consumer_backend):
    priority = Priority.high
    receipt = str(uuid.uuid4())
    visibility_timeout_s = random.randint(0, 1000)
    queue_name = get_queue_name(priority)
    queue_url = "dummy_queue_url"
    sqs_consumer_backend.sqs_client.get_queue_url = mock.MagicMock(return_value={"QueueUrl": queue_url})
    sqs_consumer_backend.sqs_client.change_message_visibility = mock.MagicMock()

    extend_visibility_timeout(priority, receipt, visibility_timeout_s)

    sqs_consumer_backend.sqs_client.get_queue_url.assert_called_once_with(QueueName=queue_name)
    sqs_consumer_backend.sqs_client.change_message_visibility.assert_called_once_with(
        QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=visibility_timeout_s
    )
