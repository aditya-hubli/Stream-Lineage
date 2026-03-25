"""Tests for streamlineage.simple_envelope module."""

import json

import pytest

from streamlineage.envelope import ProvenanceEnvelope, create_root_envelope
from streamlineage.simple_envelope import LineageEnvelope


# --- Fixtures ---

@pytest.fixture
def sample_data():
    return {"order_id": "ord-123", "amount": 99.95, "currency": "USD"}


@pytest.fixture
def root_lineage_envelope(sample_data):
    return LineageEnvelope(
        data=sample_data,
        dataset_id="orders.raw",
        producer_id="order-service-v1",
        operation="CREATE",
    )


@pytest.fixture
def parent_prov_envelope():
    return create_root_envelope(
        producer_id="upstream-svc",
        schema_id="upstream.v1",
        schema_json="{}",
        topic="upstream",
        payload_bytes=b"parent",
    )


# --- Construction tests ---

class TestLineageEnvelopeInit:
    def test_creates_envelope(self, root_lineage_envelope):
        assert root_lineage_envelope.envelope is not None
        assert isinstance(root_lineage_envelope.envelope, ProvenanceEnvelope)

    def test_stores_data(self, root_lineage_envelope, sample_data):
        assert root_lineage_envelope.data == sample_data

    def test_stores_metadata(self, root_lineage_envelope):
        assert root_lineage_envelope.dataset_id == "orders.raw"
        assert root_lineage_envelope.producer_id == "order-service-v1"
        assert root_lineage_envelope.operation == "CREATE"

    def test_auto_generates_schema_from_dict(self, root_lineage_envelope):
        assert root_lineage_envelope.schema == {
            "order_id": "str",
            "amount": "float",
            "currency": "str",
        }

    def test_explicit_schema_used(self, sample_data):
        schema = {"order_id": "string", "amount": "number"}
        env = LineageEnvelope(
            data=sample_data,
            dataset_id="orders",
            producer_id="svc",
            schema=schema,
        )
        assert env.schema == schema

    def test_default_operation_is_transform(self, sample_data):
        env = LineageEnvelope(data=sample_data, dataset_id="d", producer_id="p")
        assert env.operation == "TRANSFORM"

    def test_root_envelope_no_parents(self, root_lineage_envelope):
        assert root_lineage_envelope.envelope.parent_prov_ids == []

    def test_child_envelope_with_parents(self, sample_data, parent_prov_envelope):
        env = LineageEnvelope(
            data=sample_data,
            dataset_id="orders.enriched",
            producer_id="enricher",
            operation="TRANSFORM",
            parent_envelopes=[parent_prov_envelope],
        )
        assert env.envelope.parent_prov_ids == [parent_prov_envelope.prov_id]
        assert env.envelope.origin_id == parent_prov_envelope.origin_id

    def test_non_dict_data(self):
        env = LineageEnvelope(
            data=[1, 2, 3],
            dataset_id="numbers",
            producer_id="num-svc",
        )
        assert env.data == [1, 2, 3]
        # Schema should be empty for non-dict
        assert env.schema == {}

    def test_schema_id_derived_from_dataset_id(self, root_lineage_envelope):
        assert root_lineage_envelope.envelope.schema_id == "orders.raw.schema"


# --- Serialization tests ---

class TestLineageEnvelopeToJson:
    def test_returns_valid_json(self, root_lineage_envelope):
        result = root_lineage_envelope.to_json()
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_contains_lineage_and_data(self, root_lineage_envelope, sample_data):
        parsed = json.loads(root_lineage_envelope.to_json())
        assert "lineage" in parsed
        assert "data" in parsed
        assert parsed["data"] == sample_data

    def test_lineage_has_all_fields(self, root_lineage_envelope):
        parsed = json.loads(root_lineage_envelope.to_json())
        lineage = parsed["lineage"]

        required_fields = [
            "prov_id", "origin_id", "parent_prov_ids", "producer_id",
            "producer_hash", "schema_id", "schema_hash", "transform_ops",
            "chain_hash", "ingested_at", "topic", "partition", "offset",
        ]
        for field in required_fields:
            assert field in lineage, f"Missing field: {field}"

    def test_producer_id_in_lineage(self, root_lineage_envelope):
        parsed = json.loads(root_lineage_envelope.to_json())
        assert parsed["lineage"]["producer_id"] == "order-service-v1"

    def test_child_serialization_includes_parents(self, sample_data, parent_prov_envelope):
        env = LineageEnvelope(
            data=sample_data,
            dataset_id="d",
            producer_id="p",
            parent_envelopes=[parent_prov_envelope],
        )
        parsed = json.loads(env.to_json())
        assert parsed["lineage"]["parent_prov_ids"] == [parent_prov_envelope.prov_id]


# --- Deserialization tests ---

class TestLineageEnvelopeFromJson:
    def test_roundtrip(self, root_lineage_envelope, sample_data):
        json_str = root_lineage_envelope.to_json()
        data, envelope = LineageEnvelope.from_json(json_str)

        assert data == sample_data
        assert envelope is not None
        assert envelope.prov_id == root_lineage_envelope.envelope.prov_id
        assert envelope.producer_id == "order-service-v1"

    def test_no_lineage_key_returns_none(self):
        json_str = json.dumps({"foo": "bar"})
        data, envelope = LineageEnvelope.from_json(json_str)
        assert data == {"foo": "bar"}
        assert envelope is None

    def test_roundtrip_preserves_chain_hash(self, root_lineage_envelope):
        json_str = root_lineage_envelope.to_json()
        _, envelope = LineageEnvelope.from_json(json_str)
        assert envelope.chain_hash == root_lineage_envelope.envelope.chain_hash

    def test_roundtrip_preserves_parent_ids(self, sample_data, parent_prov_envelope):
        env = LineageEnvelope(
            data=sample_data,
            dataset_id="d",
            producer_id="p",
            parent_envelopes=[parent_prov_envelope],
        )
        json_str = env.to_json()
        _, restored = LineageEnvelope.from_json(json_str)
        assert restored.parent_prov_ids == [parent_prov_envelope.prov_id]

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            LineageEnvelope.from_json("not-json")
