import json
from unittest import mock

import pytest

from taskhawk import requeue_dead_letter, Priority
from taskhawk.backends.exceptions import PartialFailure
from taskhawk.backends.utils import get_queue_name


@mock.patch("taskhawk.commands.get_consumer_backend")
def test_requeue_dead_letter(backend_mock, sqs_consumer_backend):
    backend_mock.return_value = sqs_consumer_backend
    priority = Priority.high
    num_messages = 3
    visibility_timeout = 4

    messages = [mock.MagicMock() for _ in range(num_messages)]
    sqs_consumer_backend.get_queue_messages = mock.MagicMock()
    sqs_consumer_backend.get_queue_messages.side_effect = iter([messages, None])
    dlq_name = f'{get_queue_name(priority)}-DLQ'

    mock_queue, mock_dlq = mock.MagicMock(), mock.MagicMock()
    mock_queue.attributes = {'RedrivePolicy': json.dumps({'deadLetterTargetArn': dlq_name})}
    mock_queue.send_messages.return_value = {'Failed': []}
    sqs_consumer_backend._get_queue_by_name = mock.MagicMock(side_effect=iter([mock_queue, mock_dlq]))
    mock_dlq.delete_messages.return_value = {'Failed': []}

    requeue_dead_letter(priority, num_messages=num_messages, visibility_timeout=visibility_timeout)

    sqs_consumer_backend._get_queue_by_name.assert_has_calls([mock.call(get_queue_name(priority)), mock.call(dlq_name)])

    sqs_consumer_backend.get_queue_messages.assert_has_calls(
        [
            mock.call(mock_dlq, num_messages=num_messages, visibility_timeout=visibility_timeout),
            mock.call(mock_dlq, num_messages=num_messages, visibility_timeout=visibility_timeout),
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


@mock.patch("taskhawk.commands.get_consumer_backend")
def test_requeue_dead_letter_failure(backend_mock, sqs_consumer_backend):
    backend_mock.return_value = sqs_consumer_backend
    priority = Priority.high
    num_messages = 3
    visibility_timeout = 4

    messages = [mock.MagicMock() for _ in range(num_messages)]
    sqs_consumer_backend.get_queue_messages = mock.MagicMock(side_effect=iter([messages, None]))
    dlq_name = f'{get_queue_name(priority)}-DLQ'

    mock_queue, mock_dlq = mock.MagicMock(), mock.MagicMock()
    mock_queue.attributes = {'RedrivePolicy': json.dumps({'deadLetterTargetArn': dlq_name})}
    mock_queue.send_messages.return_value = {'Failed': [{'Id': 'string'}], 'Successful': []}
    sqs_consumer_backend._get_queue_by_name = mock.MagicMock(side_effect=iter([mock_queue, mock_dlq]))

    with pytest.raises(PartialFailure) as exc_info:
        requeue_dead_letter(priority, num_messages, visibility_timeout)

    assert exc_info.value.success_count == 0
    assert exc_info.value.failure_count == 1

    sqs_consumer_backend._get_queue_by_name.assert_has_calls([mock.call(get_queue_name(priority)), mock.call(dlq_name)])

    # not called a 2nd time after failure
    sqs_consumer_backend.get_queue_messages.assert_called_once_with(
        mock_dlq, num_messages=num_messages, visibility_timeout=visibility_timeout
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

    mock_dlq.delete_messages.assert_not_called()


def test_requeue_dead_letter_fail_lambda(settings):
    settings.IS_LAMBDA_APP = True

    requeue_dead_letter(Priority.default)
