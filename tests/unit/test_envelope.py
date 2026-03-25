"""Tests for streamlineage.envelope module."""

import hashlib
import json
import time
from unittest.mock import patch

import pytest

from streamlineage.envelope import (
    ProvenanceEnvelope,
    _compute_sha256,
    _compute_sha256_bytes,
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
    verify_chain_hash,
)


# --- Helper fixtures ---

@pytest.fixture
def sample_payload():
    return b'{"amount": 100.0, "user_id": "abc123"}'


@pytest.fixture
def sample_schema_json():
    return '{"type": "object", "properties": {"amount": {"type": "number"}}}'


@pytest.fixture
def root_envelope(sample_payload, sample_schema_json):
    return create_root_envelope(
        producer_id="payment-service-v1",
        schema_id="payments.v1",
        schema_json=sample_schema_json,
        topic="payments.raw",
        payload_bytes=sample_payload,
        partition=0,
        offset=42,
    )


@pytest.fixture
def child_envelope(root_envelope, sample_schema_json):
    return create_child_envelope(
        parent_envelopes=[root_envelope],
        producer_id="enricher-service-v1",
        schema_id="payments.enriched.v1",
        schema_json=sample_schema_json,
        topic="payments.enriched",
        payload_bytes=b'{"amount": 100.0, "enriched": true}',
        transform_ops=["filter", "enrich"],
        partition=1,
        offset=99,
    )


# --- Hash utility tests ---

class TestHashUtilities:
    def test_compute_sha256_deterministic(self):
        assert _compute_sha256("hello") == _compute_sha256("hello")

    def test_compute_sha256_different_inputs(self):
        assert _compute_sha256("hello") != _compute_sha256("world")

    def test_compute_sha256_matches_hashlib(self):
        expected = hashlib.sha256("test".encode("utf-8")).hexdigest()
        assert _compute_sha256("test") == expected

    def test_compute_sha256_bytes_deterministic(self):
        assert _compute_sha256_bytes(b"hello") == _compute_sha256_bytes(b"hello")

    def test_compute_sha256_bytes_matches_hashlib(self):
        expected = hashlib.sha256(b"test").hexdigest()
        assert _compute_sha256_bytes(b"test") == expected


# --- Root envelope tests ---

class TestCreateRootEnvelope:
    def test_creates_valid_envelope(self, root_envelope):
        assert isinstance(root_envelope, ProvenanceEnvelope)

    def test_prov_id_is_uuid(self, root_envelope):
        import uuid
        uuid.UUID(root_envelope.prov_id)  # raises ValueError if invalid

    def test_origin_id_equals_prov_id(self, root_envelope):
        assert root_envelope.origin_id == root_envelope.prov_id

    def test_no_parents(self, root_envelope):
        assert root_envelope.parent_prov_ids == []

    def test_no_transform_ops(self, root_envelope):
        assert root_envelope.transform_ops == []

    def test_producer_id_preserved(self, root_envelope):
        assert root_envelope.producer_id == "payment-service-v1"

    def test_schema_id_preserved(self, root_envelope):
        assert root_envelope.schema_id == "payments.v1"

    def test_topic_preserved(self, root_envelope):
        assert root_envelope.topic == "payments.raw"

    def test_partition_preserved(self, root_envelope):
        assert root_envelope.partition == 0

    def test_offset_preserved(self, root_envelope):
        assert root_envelope.offset == 42

    def test_producer_hash_is_sha256(self, root_envelope):
        expected = _compute_sha256("payment-service-v1")
        assert root_envelope.producer_hash == expected

    def test_schema_hash_is_sha256(self, root_envelope, sample_schema_json):
        expected = _compute_sha256(sample_schema_json)
        assert root_envelope.schema_hash == expected

    def test_chain_hash_computed_correctly(self, root_envelope, sample_payload):
        payload_hash = _compute_sha256_bytes(sample_payload)
        chain_input = f"{root_envelope.topic}{root_envelope.partition}{payload_hash}"
        expected = _compute_sha256(chain_input)
        assert root_envelope.chain_hash == expected

    def test_ingested_at_is_milliseconds(self, root_envelope):
        # Should be within a few seconds of now
        now_ms = int(time.time() * 1000)
        assert abs(root_envelope.ingested_at - now_ms) < 5000

    def test_default_partition_and_offset(self, sample_payload, sample_schema_json):
        env = create_root_envelope(
            producer_id="svc",
            schema_id="s",
            schema_json="{}",
            topic="t",
            payload_bytes=sample_payload,
        )
        assert env.partition == -1
        assert env.offset == -1

    def test_unique_prov_ids(self, sample_payload, sample_schema_json):
        env1 = create_root_envelope("svc", "s", "{}", "t", sample_payload)
        env2 = create_root_envelope("svc", "s", "{}", "t", sample_payload)
        assert env1.prov_id != env2.prov_id


# --- Child envelope tests ---

class TestCreateChildEnvelope:
    def test_creates_valid_envelope(self, child_envelope):
        assert isinstance(child_envelope, ProvenanceEnvelope)

    def test_inherits_origin_id_from_parent(self, root_envelope, child_envelope):
        assert child_envelope.origin_id == root_envelope.origin_id

    def test_parent_prov_ids_set(self, root_envelope, child_envelope):
        assert child_envelope.parent_prov_ids == [root_envelope.prov_id]

    def test_transform_ops_preserved(self, child_envelope):
        assert child_envelope.transform_ops == ["filter", "enrich"]

    def test_producer_id_is_child(self, child_envelope):
        assert child_envelope.producer_id == "enricher-service-v1"

    def test_chain_hash_uses_parent_hash(self, root_envelope, child_envelope):
        payload_hash = _compute_sha256_bytes(b'{"amount": 100.0, "enriched": true}')
        parent_hashes_joined = root_envelope.chain_hash  # single parent
        chain_input = f"{parent_hashes_joined}{payload_hash}"
        expected = _compute_sha256(chain_input)
        assert child_envelope.chain_hash == expected

    def test_empty_parents_raises(self, sample_schema_json):
        with pytest.raises(ValueError, match="at least one parent"):
            create_child_envelope(
                parent_envelopes=[],
                producer_id="svc",
                schema_id="s",
                schema_json=sample_schema_json,
                topic="t",
                payload_bytes=b"data",
                transform_ops=["filter"],
            )

    def test_fan_in_multiple_parents(self, sample_schema_json, sample_payload):
        parent_a = create_root_envelope("svc-a", "s", "{}", "t-a", b"data-a")
        parent_b = create_root_envelope("svc-b", "s", "{}", "t-b", b"data-b")

        child = create_child_envelope(
            parent_envelopes=[parent_a, parent_b],
            producer_id="joiner",
            schema_id="joined",
            schema_json=sample_schema_json,
            topic="t-joined",
            payload_bytes=b"joined-data",
            transform_ops=["join"],
        )

        assert len(child.parent_prov_ids) == 2
        assert parent_a.prov_id in child.parent_prov_ids
        assert parent_b.prov_id in child.parent_prov_ids

    def test_fan_in_chain_hash_order_independent(self, sample_schema_json):
        parent_a = create_root_envelope("svc-a", "s", "{}", "t-a", b"data-a")
        parent_b = create_root_envelope("svc-b", "s", "{}", "t-b", b"data-b")
        payload = b"joined-data"

        child_ab = create_child_envelope(
            parent_envelopes=[parent_a, parent_b],
            producer_id="joiner",
            schema_id="joined",
            schema_json=sample_schema_json,
            topic="t-joined",
            payload_bytes=payload,
            transform_ops=["join"],
        )
        child_ba = create_child_envelope(
            parent_envelopes=[parent_b, parent_a],
            producer_id="joiner",
            schema_id="joined",
            schema_json=sample_schema_json,
            topic="t-joined",
            payload_bytes=payload,
            transform_ops=["join"],
        )

        # Chain hash should be identical regardless of parent order
        assert child_ab.chain_hash == child_ba.chain_hash

    def test_origin_id_comes_from_first_parent(self, sample_schema_json):
        parent_a = create_root_envelope("svc-a", "s", "{}", "t-a", b"a")
        parent_b = create_root_envelope("svc-b", "s", "{}", "t-b", b"b")

        child = create_child_envelope(
            parent_envelopes=[parent_a, parent_b],
            producer_id="joiner",
            schema_id="s",
            schema_json=sample_schema_json,
            topic="t",
            payload_bytes=b"out",
            transform_ops=["join"],
        )
        assert child.origin_id == parent_a.origin_id


# --- Header serialization tests ---

class TestEnvelopeToHeaders:
    def test_all_fields_present(self, root_envelope):
        headers = envelope_to_headers(root_envelope)
        expected_keys = {
            "prov_id", "origin_id", "parent_prov_ids", "producer_id",
            "producer_hash", "schema_id", "schema_hash", "transform_ops",
            "chain_hash", "ingested_at", "topic", "partition", "offset",
        }
        assert set(headers.keys()) == expected_keys

    def test_all_values_are_bytes(self, root_envelope):
        headers = envelope_to_headers(root_envelope)
        for value in headers.values():
            assert isinstance(value, bytes)

    def test_list_fields_are_json_encoded(self, child_envelope):
        headers = envelope_to_headers(child_envelope)
        parent_ids = json.loads(headers["parent_prov_ids"].decode("utf-8"))
        assert isinstance(parent_ids, list)
        transform_ops = json.loads(headers["transform_ops"].decode("utf-8"))
        assert transform_ops == ["filter", "enrich"]


class TestHeadersToEnvelope:
    def test_roundtrip(self, root_envelope):
        headers = envelope_to_headers(root_envelope)
        restored = headers_to_envelope(headers)

        assert restored is not None
        assert restored.prov_id == root_envelope.prov_id
        assert restored.origin_id == root_envelope.origin_id
        assert restored.parent_prov_ids == root_envelope.parent_prov_ids
        assert restored.producer_id == root_envelope.producer_id
        assert restored.chain_hash == root_envelope.chain_hash
        assert restored.ingested_at == root_envelope.ingested_at

    def test_roundtrip_child(self, child_envelope):
        headers = envelope_to_headers(child_envelope)
        restored = headers_to_envelope(headers)

        assert restored is not None
        assert restored.parent_prov_ids == child_envelope.parent_prov_ids
        assert restored.transform_ops == child_envelope.transform_ops

    def test_missing_field_returns_none(self):
        headers = {"prov_id": b"test"}  # incomplete
        assert headers_to_envelope(headers) is None

    def test_empty_headers_returns_none(self):
        assert headers_to_envelope({}) is None

    def test_malformed_json_returns_none(self, root_envelope):
        headers = envelope_to_headers(root_envelope)
        headers["parent_prov_ids"] = b"not-valid-json"
        assert headers_to_envelope(headers) is None

    def test_non_integer_ingested_at_returns_none(self, root_envelope):
        headers = envelope_to_headers(root_envelope)
        headers["ingested_at"] = b"not-a-number"
        assert headers_to_envelope(headers) is None


# --- Chain hash verification tests ---

class TestVerifyChainHash:
    def test_root_envelope_valid(self, root_envelope, sample_payload):
        assert verify_chain_hash(root_envelope, sample_payload) is True

    def test_root_envelope_tampered_payload(self, root_envelope):
        assert verify_chain_hash(root_envelope, b"tampered-data") is False

    def test_child_envelope_valid(self, root_envelope, child_envelope):
        payload = b'{"amount": 100.0, "enriched": true}'
        assert verify_chain_hash(child_envelope, payload, [root_envelope]) is True

    def test_child_envelope_tampered_payload(self, root_envelope, child_envelope):
        assert verify_chain_hash(child_envelope, b"tampered", [root_envelope]) is False

    def test_child_envelope_wrong_parent(self, child_envelope, sample_payload):
        fake_parent = create_root_envelope("fake", "s", "{}", "t", b"fake")
        payload = b'{"amount": 100.0, "enriched": true}'
        assert verify_chain_hash(child_envelope, payload, [fake_parent]) is False

    def test_child_without_parents_raises(self, child_envelope):
        with pytest.raises(ValueError, match="parent_envelopes required"):
            verify_chain_hash(child_envelope, b"data", parent_envelopes=None)

    def test_fan_in_verification(self, sample_schema_json):
        parent_a = create_root_envelope("svc-a", "s", "{}", "t-a", b"a")
        parent_b = create_root_envelope("svc-b", "s", "{}", "t-b", b"b")
        payload = b"joined"

        child = create_child_envelope(
            parent_envelopes=[parent_a, parent_b],
            producer_id="joiner",
            schema_id="s",
            schema_json=sample_schema_json,
            topic="t",
            payload_bytes=payload,
            transform_ops=["join"],
        )

        # Verify with both parents — order shouldn't matter
        assert verify_chain_hash(child, payload, [parent_a, parent_b]) is True
        assert verify_chain_hash(child, payload, [parent_b, parent_a]) is True

    def test_tampered_chain_hash_detected(self, root_envelope, sample_payload):
        root_envelope.chain_hash = "0" * 64  # tampered
        assert verify_chain_hash(root_envelope, sample_payload) is False
