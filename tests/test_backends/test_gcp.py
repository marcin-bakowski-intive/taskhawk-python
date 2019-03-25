import json
from unittest import mock

import pytest

from taskhawk import Priority
from taskhawk.backends import gcp
from taskhawk.backends.utils import get_queue_name
from taskhawk.conf import settings


@pytest.fixture
def gcp_settings(settings):
    settings.GOOGLE_APPLICATION_CREDENTIALS = "DUMMY_GOOGLE_APPLICATION_CREDENTIALS"
    settings.TASKHAWK_PUBLISHER_BACKEND = "taskhawk.backends.gcp.GooglePubSubPublisherBackend"
    settings.TASKHAWK_CONSUMER_BACKEND = "taskhawk.backends.gcp.GooglePubSubConsumerBackend"
    settings.GOOGLE_PUBSUB_PROJECT_ID = "DUMMY_PROJECT_ID"
    settings.GOOGLE_SUB_READ_TIMEOUT_S = 5
    settings.GOOGLE_MESSAGE_RETRY_STATE_BACKEND = 'taskhawk.backends.gcp.MessageRetryStateLocMem'
    settings.GOOGLE_MESSAGE_MAX_RETRIES = 5
    yield settings


@pytest.fixture
def retry_once_settings(gcp_settings):
    gcp_settings.GOOGLE_MESSAGE_MAX_RETRIES = 1
    yield gcp_settings


@pytest.mark.parametrize(
    "priority, queue_name",
    [
        (Priority.default, "TASKHAWK-DEV-RTEP"),
        (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
        (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
        (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
    ],
)
def test_gcp_pubsub_publish_success(priority, queue_name, mock_pubsub_v1, message, gcp_settings):
    message.priority = priority
    gcp_publisher = gcp.GooglePubSubPublisherBackend()
    gcp_publisher.publisher.topic_path = mock.MagicMock(return_value="dummy_topic_path")
    message_data = json.dumps(message.as_dict())

    gcp_publisher.publish(message)

    mock_pubsub_v1.PublisherClient.from_service_account_file.assert_called_once_with(
        gcp_settings.GOOGLE_APPLICATION_CREDENTIALS
    )
    gcp_publisher.publisher.topic_path.assert_called_once_with(gcp_settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name)
    gcp_publisher.publisher.publish.assert_called_once_with(
        "dummy_topic_path", data=message_data.encode(), **message.headers
    )


pre_process_hook = mock.MagicMock()
post_process_hook = mock.MagicMock()


class TestGCPConsumer:
    def setup(self):
        self.gcp_consumer = gcp.GooglePubSubConsumerBackend()
        self.subscription_path = "dummy_subscription_path"
        self.topic_path = "dummy_topic_path"

        pre_process_hook.reset_mock()
        post_process_hook.reset_mock()

    @staticmethod
    def _build_gcp_queue_message(message):
        queue_message = mock.MagicMock()
        queue_message.ack_id = "dummy_ack_id"
        queue_message.message.data = json.dumps(message.as_dict()).encode()
        return queue_message

    def test_initialization(self, mock_pubsub_v1, gcp_settings):
        mock_pubsub_v1.SubscriberClient.from_service_account_file.assert_called_once_with(
            settings.GOOGLE_APPLICATION_CREDENTIALS
        )

    def test_pull_messages(self, mock_pubsub_v1, gcp_settings):
        queue_name = "default"
        num_messages = 1
        visibility_timeout = 10
        self.gcp_consumer.subscriber.subscription_path = mock.MagicMock(return_value=self.subscription_path)

        self.gcp_consumer.pull_messages(queue_name, num_messages, visibility_timeout)

        self.gcp_consumer.subscriber.subscription_path.assert_called_once_with(
            gcp_settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name
        )
        self.gcp_consumer.subscriber.pull.assert_called_once_with(
            self.subscription_path, num_messages, retry=None, timeout=gcp_settings.GOOGLE_SUB_READ_TIMEOUT_S
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
    def test_success_extend_visibility_timeout(self, priority, queue_name, mock_pubsub_v1, gcp_settings):
        visibility_timeout_s = 10
        ack_id = "dummy_ack_id"
        self.gcp_consumer.subscriber.subscription_path = mock.MagicMock(return_value=self.subscription_path)

        self.gcp_consumer.extend_visibility_timeout(priority, visibility_timeout_s, ack_id=ack_id)

        self.gcp_consumer.subscriber.subscription_path.assert_called_once_with(
            gcp_settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name
        )
        self.gcp_consumer.subscriber.modify_ack_deadline.assert_called_once_with(
            self.subscription_path, [ack_id], visibility_timeout_s
        )

    @pytest.mark.parametrize("visibility_timeout", [-1, 601])
    def test_failure_extend_visibility_timeout(self, visibility_timeout, mock_pubsub_v1):
        with pytest.raises(ValueError):
            self.gcp_consumer.extend_visibility_timeout(Priority.default, visibility_timeout, ack_id="dummy_ack_id")

        self.gcp_consumer.subscriber.subscription_path.assert_not_called()
        self.gcp_consumer.subscriber.modify_ack_deadline.assert_not_called()

    @pytest.mark.parametrize(
        "priority, queue_name",
        [
            (Priority.default, "TASKHAWK-DEV-RTEP"),
            (Priority.high, "TASKHAWK-DEV-RTEP-HIGH-PRIORITY"),
            (Priority.low, "TASKHAWK-DEV-RTEP-LOW-PRIORITY"),
            (Priority.bulk, "TASKHAWK-DEV-RTEP-BULK"),
        ],
    )
    def test_success_requeue_dead_letter(self, priority, queue_name, mock_pubsub_v1, gcp_settings, message):
        num_messages = 1
        visibility_timeout = 4
        dlq_name = f'{queue_name}-DLQ'

        queue_message = self._build_gcp_queue_message(message)
        self.gcp_consumer.pull_messages = mock.MagicMock(side_effect=iter([[queue_message], None]))
        self.gcp_consumer.publisher.publisher.topic_path = mock.MagicMock(return_value=self.topic_path)
        self.gcp_consumer.subscriber.subscription_path = mock.MagicMock(return_value=f"{self.subscription_path}-DLQ")

        self.gcp_consumer.requeue_dead_letter(
            priority, num_messages=num_messages, visibility_timeout=visibility_timeout
        )

        self.gcp_consumer.subscriber.modify_ack_deadline.assert_called_once_with(
            f"{self.subscription_path}-DLQ", [queue_message.ack_id], visibility_timeout
        )
        self.gcp_consumer.pull_messages.assert_has_calls(
            [
                mock.call(dlq_name, num_messages=num_messages, visibility_timeout=visibility_timeout),
                mock.call(dlq_name, num_messages=num_messages, visibility_timeout=visibility_timeout),
            ]
        )
        self.gcp_consumer.publisher.publish_to_topic(self.topic_path, queue_message.message.data, message.headers)
        self.gcp_consumer.subscriber.acknowledge.assert_called_once_with(
            f"{self.subscription_path}-DLQ", [queue_message.ack_id]
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
    def test_fetch_and_process_messages_success(self, priority, queue_name, mock_pubsub_v1, gcp_settings, message):
        gcp_settings.TASKHAWK_PRE_PROCESS_HOOK = 'tests.test_backends.test_gcp.pre_process_hook'
        gcp_settings.TASKHAWK_POST_PROCESS_HOOK = 'tests.test_backends.test_gcp.post_process_hook'
        num_messages = 3
        visibility_timeout = 4

        queue_message = self._build_gcp_queue_message(message)
        received_messages = mock.MagicMock()
        received_messages.received_messages = [queue_message]
        self.gcp_consumer.subscriber.pull = mock.MagicMock(return_value=received_messages)
        self.gcp_consumer.subscriber.subscription_path = mock.MagicMock(return_value=self.subscription_path)
        self.gcp_consumer.process_message = mock.MagicMock(wraps=self.gcp_consumer.process_message)
        self.gcp_consumer.message_handler = mock.MagicMock(wraps=self.gcp_consumer.message_handler)

        self.gcp_consumer.fetch_and_process_messages(priority, num_messages, visibility_timeout)

        self.gcp_consumer.subscriber.subscription_path.assert_called_once_with(
            gcp_settings.GOOGLE_PUBSUB_PROJECT_ID, queue_name
        )
        self.gcp_consumer.subscriber.pull.assert_called_once_with(
            self.subscription_path, num_messages, retry=None, timeout=gcp_settings.GOOGLE_SUB_READ_TIMEOUT_S
        )
        self.gcp_consumer.process_message.assert_called_once_with(queue_message, queue_name=queue_name)
        self.gcp_consumer.message_handler.assert_called_once_with(
            queue_message.message.data.decode(), ack_id=queue_message.ack_id
        )
        self.gcp_consumer.subscriber.acknowledge.assert_called_once_with(self.subscription_path, [queue_message.ack_id])
        pre_process_hook.assert_called_once_with()
        post_process_hook.assert_called_once_with()

    def test_message_moved_to_dlq(self, mock_pubsub_v1, retry_once_settings, message):
        queue_message = self._build_gcp_queue_message(message)
        consumer_backend = gcp.GooglePubSubConsumerBackend()
        consumer_backend.subscriber.subscription_path = mock.MagicMock(
            side_effect=[self.subscription_path, f"{self.subscription_path}-DLQ"]
        )
        consumer_backend.pull_messages = mock.MagicMock(return_value=[queue_message])
        consumer_backend.message_handler = mock.MagicMock(side_effect=Exception)
        consumer_backend.publisher.publisher.topic_path = mock.MagicMock(return_value=f"{self.topic_path}-DLQ")

        consumer_backend.fetch_and_process_messages(message.priority)

        consumer_backend.subscriber.subscription_path.assert_has_calls(
            [
                mock.call(
                    retry_once_settings.GOOGLE_PUBSUB_PROJECT_ID, "{}-DLQ".format(get_queue_name(message.priority))
                ),
                mock.call(retry_once_settings.GOOGLE_PUBSUB_PROJECT_ID, get_queue_name(message.priority)),
            ]
        )
        consumer_backend.publisher.publisher.publish.assert_called_once_with(
            f"{self.topic_path}-DLQ", data=queue_message.message.data, **message.headers
        )

    def test_message_not_moved_to_dlq(self, mock_pubsub_v1, gcp_settings, message):
        queue_message = self._build_gcp_queue_message(message)
        consumer_backend = gcp.GooglePubSubConsumerBackend()
        consumer_backend.subscriber.subscription_path = mock.MagicMock(
            side_effect=[self.subscription_path, f"{self.subscription_path}-DLQ"]
        )
        consumer_backend.pull_messages = mock.MagicMock(return_value=[queue_message])
        consumer_backend.message_handler = mock.MagicMock(side_effect=Exception)

        consumer_backend.fetch_and_process_messages(message.priority)

        consumer_backend.publisher.publisher.publish.assert_not_called()
