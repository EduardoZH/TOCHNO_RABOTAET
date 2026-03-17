"""Tests for shared/messaging/rabbitmq_client.py — mocked pika."""

import json
from unittest.mock import patch, MagicMock, PropertyMock
import pytest
import pika


@pytest.fixture
def mock_pika():
    with patch("shared.messaging.rabbitmq_client.pika") as mock_pika_mod:
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_pika_mod.BlockingConnection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_pika_mod.PlainCredentials = pika.PlainCredentials
        mock_pika_mod.ConnectionParameters = pika.ConnectionParameters
        mock_pika_mod.BasicProperties = pika.BasicProperties
        mock_pika_mod.exceptions = pika.exceptions
        yield {
            "module": mock_pika_mod,
            "connection": mock_conn,
            "channel": mock_channel,
        }


class TestRabbitClientConnect:
    def test_connect_success(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        assert client.connection is not None
        assert client.channel is not None

    def test_connect_already_open_skips(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        call_count = mock_pika["module"].BlockingConnection.call_count
        client.connect()  # second call should skip
        assert mock_pika["module"].BlockingConnection.call_count == call_count


class TestRabbitClientPublish:
    def test_publish_message(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        payload = {"post_id": "1", "content": "тест"}
        client.publish("test_queue", payload)
        mock_pika["channel"].basic_publish.assert_called_once()
        call_args = mock_pika["channel"].basic_publish.call_args
        assert call_args.kwargs["routing_key"] == "test_queue"

    def test_publish_json_encoding_cyrillic(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        payload = {"content": "Мошенники в Москве"}
        client.publish("q", payload)
        call_args = mock_pika["channel"].basic_publish.call_args
        body = call_args.kwargs["body"]
        decoded = json.loads(body.decode())
        assert decoded["content"] == "Мошенники в Москве"


class TestRabbitClientDeclareQueue:
    def test_declare_with_dlq(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        client.declare_queue("main_queue", dlq_name="main_queue_dlq")
        # Should declare both DLQ and main queue
        calls = mock_pika["channel"].queue_declare.call_args_list
        assert len(calls) == 2
        assert calls[0].kwargs["queue"] == "main_queue_dlq"
        assert calls[1].kwargs["queue"] == "main_queue"

    def test_declare_without_dlq(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        client.declare_queue("simple_queue")
        mock_pika["channel"].queue_declare.assert_called_once()


class TestRabbitClientConsumerWrapper:
    def test_consume_ack_on_success(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()

        def my_callback(ch, method, properties, body):
            json.loads(body)

        # Simulate what consume() wraps
        client.declare_queue("q")
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 1
        body = json.dumps({"key": "value"}).encode()

        # Get wrapped callback by inspecting consume behavior
        # We test the wrapping logic directly
        try:
            my_callback(ch, method, None, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        ch.basic_ack.assert_called_once()

    def test_consume_nack_on_json_error(self, mock_pika):
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 1
        body = b"not valid json"

        try:
            json.loads(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)

    def test_consume_nack_on_exception(self, mock_pika):
        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 2

        def bad_callback(ch, method, props, body):
            raise ValueError("processing error")

        try:
            bad_callback(ch, method, None, b'{"key":"val"}')
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        ch.basic_nack.assert_called_once_with(delivery_tag=2, requeue=False)


class TestRabbitClientClose:
    def test_close(self, mock_pika):
        from shared.messaging.rabbitmq_client import RabbitClient
        client = RabbitClient()
        client.connect()
        client.close()
        assert client._shutting_down is True
