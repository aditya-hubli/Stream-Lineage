"""Tests for streamlineage.interceptor module."""

import json
import threading
from unittest.mock import MagicMock, call

import pytest

from streamlineage.envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    envelope_to_headers,
    headers_to_envelope,
)
from streamlineage.interceptor import (
    ProvenanceInterceptor,
    WrappedProducer,
    ThreadLocalParentEnvelope,
    _thread_local,
)


# --- Fixtures ---

@pytest.fixture
def interceptor():
    return ProvenanceInterceptor(
        producer_id="test-service-v1",
        schema_id="test.v1",
        schema_json='{"type": "object"}',
    )


@pytest.fixture
def mock_producer():
    producer = MagicMock()
    producer.flush.return_value = 0
    producer.poll.return_value = 0
    producer.__len__ = MagicMock(return_value=0)
    return producer


@pytest.fixture
def parent_envelope():
    return create_root_envelope(
        producer_id="upstream-service",
        schema_id="upstream.v1",
        schema_json="{}",
        topic="upstream-topic",
        payload_bytes=b"parent-data",
        partition=0,
        offset=10,
    )


# --- ThreadLocalParentEnvelope tests ---

class TestThreadLocalParentEnvelope:
    def test_sets_and_clears_envelope(self, parent_envelope):
        with ThreadLocalParentEnvelope(parent_envelope):
            assert getattr(_thread_local, "parent_envelope", None) is parent_envelope
        assert getattr(_thread_local, "parent_envelope", None) is None

    def test_sets_transform_ops(self, parent_envelope):
        with ThreadLocalParentEnvelope(parent_envelope, transform_ops=["filter"]):
            assert getattr(_thread_local, "transform_ops", None) == ["filter"]
        assert getattr(_thread_local, "transform_ops", None) is None

    def test_nesting_restores_previous(self, parent_envelope):
        other_envelope = create_root_envelope("other", "s", "{}", "t", b"d")

        with ThreadLocalParentEnvelope(parent_envelope):
            assert _thread_local.parent_envelope is parent_envelope
            with ThreadLocalParentEnvelope(other_envelope):
                assert _thread_local.parent_envelope is other_envelope
            assert _thread_local.parent_envelope is parent_envelope
        assert getattr(_thread_local, "parent_envelope", None) is None

    def test_none_envelope(self):
        with ThreadLocalParentEnvelope(None):
            assert getattr(_thread_local, "parent_envelope", None) is None

    def test_thread_isolation(self, parent_envelope):
        """Envelopes in one thread don't leak to another."""
        results = {}

        def thread_a():
            with ThreadLocalParentEnvelope(parent_envelope):
                results["a_inside"] = getattr(_thread_local, "parent_envelope", None)

        def thread_b():
            results["b_outside"] = getattr(_thread_local, "parent_envelope", None)

        t_a = threading.Thread(target=thread_a)
        t_b = threading.Thread(target=thread_b)
        t_a.start()
        t_a.join()
        t_b.start()
        t_b.join()

        assert results["a_inside"] is parent_envelope
        assert results["b_outside"] is None


# --- ProvenanceInterceptor tests ---

class TestProvenanceInterceptor:
    def test_on_send_creates_root_envelope(self, interceptor):
        topic, value, key, headers, kwargs = interceptor.on_send(
            topic="test-topic",
            value=b"test-value",
            key=b"test-key",
        )

        assert topic == "test-topic"
        assert value == b"test-value"
        assert key == b"test-key"

        # Headers should contain provenance fields
        headers_dict = dict(headers)
        assert b"prov_id" in headers_dict or "prov_id" in headers_dict
        envelope = headers_to_envelope(
            {k: v for k, v in headers} if isinstance(headers[0], tuple) else headers
        )
        assert envelope is not None
        assert envelope.producer_id == "test-service-v1"
        assert envelope.parent_prov_ids == []

    def test_on_send_creates_child_with_thread_local(self, interceptor, parent_envelope):
        with ThreadLocalParentEnvelope(parent_envelope, transform_ops=["map"]):
            topic, value, key, headers, kwargs = interceptor.on_send(
                topic="output-topic",
                value=b"derived-value",
            )

        headers_dict = {k: v for k, v in headers}
        envelope = headers_to_envelope(headers_dict)
        assert envelope is not None
        assert envelope.parent_prov_ids == [parent_envelope.prov_id]
        assert envelope.producer_id == "test-service-v1"

    def test_on_send_creates_child_from_existing_headers(self, interceptor, parent_envelope):
        parent_headers = list(envelope_to_headers(parent_envelope).items())

        topic, value, key, headers, kwargs = interceptor.on_send(
            topic="output-topic",
            value=b"derived-value",
            headers=parent_headers,
        )

        headers_dict = {k: v for k, v in headers}
        envelope = headers_to_envelope(headers_dict)
        assert envelope is not None
        assert envelope.parent_prov_ids == [parent_envelope.prov_id]

    def test_on_send_preserves_existing_non_prov_headers(self, interceptor):
        existing_headers = [("x-custom", b"my-value")]

        topic, value, key, headers, kwargs = interceptor.on_send(
            topic="t",
            value=b"v",
            headers=existing_headers,
        )

        headers_dict = {k: v for k, v in headers}
        assert headers_dict.get("x-custom") == b"my-value"

    def test_on_send_passes_kwargs_through(self, interceptor):
        _, _, _, _, kwargs = interceptor.on_send(
            topic="t",
            value=b"v",
            on_delivery=lambda err, msg: None,
        )
        assert "on_delivery" in kwargs

    def test_wrap_producer(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        assert isinstance(wrapped, WrappedProducer)


# --- WrappedProducer tests ---

class TestWrappedProducer:
    def test_produce_calls_underlying(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.produce(topic="test", value=b"data")

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "test"
        assert call_kwargs.kwargs["value"] == b"data"

    def test_produce_injects_headers(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.produce(topic="test", value=b"data")

        call_kwargs = mock_producer.produce.call_args
        headers = call_kwargs.kwargs["headers"]
        assert headers is not None
        assert len(headers) > 0

    def test_flush_delegates(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.flush()
        mock_producer.flush.assert_called_once()

    def test_flush_with_timeout(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.flush(timeout=5.0)
        mock_producer.flush.assert_called_once_with(5.0)

    def test_poll_delegates(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.poll()
        mock_producer.poll.assert_called_once()

    def test_poll_with_timeout(self, interceptor, mock_producer):
        wrapped = interceptor.wrap_producer(mock_producer)
        wrapped.poll(timeout=1.0)
        mock_producer.poll.assert_called_once_with(1.0)

    def test_len_delegates(self, interceptor, mock_producer):
        mock_producer.__len__.return_value = 5
        wrapped = interceptor.wrap_producer(mock_producer)
        assert len(wrapped) == 5

    def test_getattr_delegates(self, interceptor, mock_producer):
        mock_producer.list_topics = MagicMock(return_value=["t1"])
        wrapped = interceptor.wrap_producer(mock_producer)
        assert wrapped.list_topics() == ["t1"]
