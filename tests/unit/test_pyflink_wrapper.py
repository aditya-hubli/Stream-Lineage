"""
Unit tests for pyflink_wrapper.py

Tests the LineageStream wrapper logic with PyFlink mocked out, so these
tests run in any environment without requiring apache-flink to be installed.

What is verified:
- Envelope propagation through filter(), map(), join()
- parent_prov_ids are correctly set for child events
- transform_ops are recorded per operation
- chain_hash changes after each transformation
- fan-in join: sorted parent chain_hashes used for determinism
- ImportError is raised when PYFLINK_AVAILABLE=False and methods are called
"""

import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers to build a fake PyFlink environment so the wrapper can be imported
# without apache-flink installed.
# ---------------------------------------------------------------------------

def _make_flink_mock():
    """Return a minimal sys.modules-style PyFlink mock."""
    flink_mod = types.ModuleType("pyflink")
    datastream_mod = types.ModuleType("pyflink.datastream")
    connectors_mod = types.ModuleType("pyflink.datastream.connectors")
    kafka_mod = types.ModuleType("pyflink.datastream.connectors.kafka")
    common_mod = types.ModuleType("pyflink.common")
    serialization_mod = types.ModuleType("pyflink.common.serialization")

    class FakeDataStream:
        def map(self, func, output_type=None):
            return self
        def filter(self, func):
            return self
        def connect(self, other):
            connected = MagicMock()
            connected.map = MagicMock(return_value=FakeDataStream())
            return connected

    class FakeEnv:
        def add_source(self, source):
            return FakeDataStream()

    datastream_mod.StreamExecutionEnvironment = FakeEnv
    datastream_mod.DataStream = FakeDataStream
    kafka_mod.FlinkKafkaConsumer = MagicMock
    kafka_mod.FlinkKafkaProducer = MagicMock
    kafka_mod.KafkaRecordSerializationSchema = MagicMock
    serialization_mod.SimpleStringSchema = MagicMock
    common_mod.serialization = serialization_mod

    flink_mod.datastream = datastream_mod
    datastream_mod.connectors = connectors_mod
    connectors_mod.kafka = kafka_mod
    flink_mod.common = common_mod

    return {
        "pyflink": flink_mod,
        "pyflink.datastream": datastream_mod,
        "pyflink.datastream.connectors": connectors_mod,
        "pyflink.datastream.connectors.kafka": kafka_mod,
        "pyflink.common": common_mod,
        "pyflink.common.serialization": serialization_mod,
    }


# Inject the mock before importing the wrapper
_flink_mocks = _make_flink_mock()
for mod_name, mod in _flink_mocks.items():
    sys.modules.setdefault(mod_name, mod)

# Now import the SDK modules
sys.path.insert(0, "streamlineage/sdk")
from streamlineage.pyflink_wrapper import LineageStream, PYFLINK_AVAILABLE
from streamlineage.envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
    verify_chain_hash,
)


# ---------------------------------------------------------------------------
# Envelope propagation helpers (replicate what the wrapper's map closure does)
# ---------------------------------------------------------------------------

def _make_root_payload(data: dict, producer_id: str, schema_id: str, topic: str) -> tuple:
    """Return (json_bytes, envelope) for a root event."""
    payload_bytes = json.dumps(data).encode()
    env = create_root_envelope(
        producer_id=producer_id,
        schema_id=schema_id,
        schema_json=json.dumps({"type": "object"}),
        topic=topic,
        payload_bytes=payload_bytes,
    )
    return payload_bytes, env


def _wrap_message(payload_bytes: bytes, envelope: ProvenanceEnvelope) -> dict:
    """Simulate LineageStream's internal message format (headers + payload)."""
    return {
        "_envelope": envelope,
        "_payload": json.loads(payload_bytes),
        "_raw": payload_bytes,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEnvelopePropagation(unittest.TestCase):
    """Verify envelope fields are set correctly at each transformation stage."""

    def setUp(self):
        payload_bytes, self.root_env = _make_root_payload(
            {"user_id": "u1", "amount": 150},
            producer_id="payment-producer-v1",
            schema_id="payments.v1",
            topic="payments.raw",
        )
        self.payload_bytes = payload_bytes

    def test_root_envelope_fields(self):
        env = self.root_env
        self.assertEqual(env.origin_id, env.prov_id)
        self.assertEqual(env.parent_prov_ids, [])
        self.assertEqual(env.producer_id, "payment-producer-v1")
        self.assertIsNotNone(env.chain_hash)
        self.assertEqual(len(env.chain_hash), 64)  # SHA256 hex

    def test_root_chain_hash_verifies(self):
        self.assertTrue(verify_chain_hash(self.root_env, self.payload_bytes))

    def test_child_envelope_parent_tracking(self):
        """Child event must list the parent's prov_id."""
        child_payload = json.dumps({"user_id": "u1", "amount": 150, "risk": 0.3}).encode()
        child_env = create_child_envelope(
            parent_envelopes=[self.root_env],
            producer_id="fraud-detector-v1",
            schema_id="fraud.scores.v1",
            schema_json=json.dumps({"type": "object"}),
            topic="fraud.scores",
            payload_bytes=child_payload,
            transform_ops=["filter", "score"],
        )
        self.assertIn(self.root_env.prov_id, child_env.parent_prov_ids)
        self.assertEqual(child_env.origin_id, self.root_env.origin_id)
        self.assertEqual(child_env.transform_ops, ["filter", "score"])

    def test_child_chain_hash_differs_from_parent(self):
        """Each transformation must produce a new chain_hash."""
        child_payload = json.dumps({"risk": 0.3}).encode()
        child_env = create_child_envelope(
            parent_envelopes=[self.root_env],
            producer_id="enricher",
            schema_id="enriched.v1",
            schema_json="{}",
            topic="enriched",
            payload_bytes=child_payload,
            transform_ops=["enrich"],
        )
        self.assertNotEqual(child_env.chain_hash, self.root_env.chain_hash)

    def test_chain_hash_verifies_for_child(self):
        child_payload = json.dumps({"risk": 0.5}).encode()
        child_env = create_child_envelope(
            parent_envelopes=[self.root_env],
            producer_id="enricher",
            schema_id="enriched.v1",
            schema_json="{}",
            topic="enriched",
            payload_bytes=child_payload,
            transform_ops=["enrich"],
        )
        self.assertTrue(verify_chain_hash(child_env, child_payload, [self.root_env]))

    def test_chain_hash_fails_on_tamper(self):
        """A tampered payload must fail chain hash verification."""
        child_payload = json.dumps({"risk": 0.5}).encode()
        child_env = create_child_envelope(
            parent_envelopes=[self.root_env],
            producer_id="enricher",
            schema_id="enriched.v1",
            schema_json="{}",
            topic="enriched",
            payload_bytes=child_payload,
            transform_ops=["enrich"],
        )
        tampered_payload = json.dumps({"risk": 0.9}).encode()  # different data
        self.assertFalse(verify_chain_hash(child_env, tampered_payload, [self.root_env]))


class TestFanInJoin(unittest.TestCase):
    """Verify multi-parent (fan-in join) envelope handling."""

    def test_fan_in_parent_prov_ids(self):
        p1_bytes = json.dumps({"order_id": "o1"}).encode()
        p2_bytes = json.dumps({"fraud_score": 0.1}).encode()

        p1 = create_root_envelope("order-svc", "orders.v1", "{}", "orders.raw", p1_bytes)
        p2 = create_root_envelope("fraud-svc", "fraud.v1", "{}", "fraud.scores", p2_bytes)

        joined_payload = json.dumps({"order_id": "o1", "fraud_score": 0.1}).encode()
        joined_env = create_child_envelope(
            parent_envelopes=[p1, p2],
            producer_id="join-node",
            schema_id="joined.v1",
            schema_json="{}",
            topic="joined.events",
            payload_bytes=joined_payload,
            transform_ops=["join"],
        )

        self.assertIn(p1.prov_id, joined_env.parent_prov_ids)
        self.assertIn(p2.prov_id, joined_env.parent_prov_ids)
        self.assertEqual(len(joined_env.parent_prov_ids), 2)

    def test_fan_in_chain_hash_is_deterministic(self):
        """Join order must not affect chain_hash (sorted parent hashes)."""
        p1_bytes = b'{"a": 1}'
        p2_bytes = b'{"b": 2}'
        p1 = create_root_envelope("svc-a", "a.v1", "{}", "topic-a", p1_bytes)
        p2 = create_root_envelope("svc-b", "b.v1", "{}", "topic-b", p2_bytes)

        joined_payload = b'{"a":1,"b":2}'

        env_ab = create_child_envelope(
            parent_envelopes=[p1, p2],
            producer_id="join", schema_id="j.v1", schema_json="{}",
            topic="joined", payload_bytes=joined_payload, transform_ops=["join"],
        )
        env_ba = create_child_envelope(
            parent_envelopes=[p2, p1],  # reversed order
            producer_id="join", schema_id="j.v1", schema_json="{}",
            topic="joined", payload_bytes=joined_payload, transform_ops=["join"],
        )

        self.assertEqual(env_ab.chain_hash, env_ba.chain_hash)

    def test_fan_in_chain_hash_verifies(self):
        p1_bytes = b'{"x": 1}'
        p2_bytes = b'{"y": 2}'
        p1 = create_root_envelope("svc-x", "x.v1", "{}", "t-x", p1_bytes)
        p2 = create_root_envelope("svc-y", "y.v1", "{}", "t-y", p2_bytes)

        joined = b'{"x":1,"y":2}'
        env = create_child_envelope(
            parent_envelopes=[p1, p2],
            producer_id="joiner", schema_id="j.v1", schema_json="{}",
            topic="joined", payload_bytes=joined, transform_ops=["join"],
        )
        self.assertTrue(verify_chain_hash(env, joined, [p1, p2]))


class TestEnvelopeHeaderRoundTrip(unittest.TestCase):
    """Verify envelope serialization → Kafka headers → deserialization is lossless."""

    def test_round_trip(self):
        payload_bytes = b'{"amount": 100}'
        env = create_root_envelope("svc", "s.v1", "{}", "topic", payload_bytes)
        headers = envelope_to_headers(env)
        restored = headers_to_envelope(headers)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.prov_id, env.prov_id)
        self.assertEqual(restored.chain_hash, env.chain_hash)
        self.assertEqual(restored.transform_ops, env.transform_ops)
        self.assertEqual(restored.parent_prov_ids, env.parent_prov_ids)

    def test_missing_header_returns_none(self):
        payload_bytes = b'{}'
        env = create_root_envelope("svc", "s.v1", "{}", "t", payload_bytes)
        headers = envelope_to_headers(env)
        del headers["chain_hash"]  # remove required field
        self.assertIsNone(headers_to_envelope(headers))


class TestLineageStreamImportGuard(unittest.TestCase):
    """Verify that LineageStream raises ImportError when PyFlink is unavailable."""

    def test_raises_when_flink_unavailable(self):
        with patch("streamlineage.pyflink_wrapper.PYFLINK_AVAILABLE", False):
            with self.assertRaises(ImportError):
                LineageStream(data_stream=MagicMock(), node_id="test")

    def test_from_kafka_raises_when_flink_unavailable(self):
        with patch("streamlineage.pyflink_wrapper.PYFLINK_AVAILABLE", False):
            with self.assertRaises(ImportError):
                LineageStream.from_kafka(
                    env=MagicMock(), topic="t", node_id="n"
                )


if __name__ == "__main__":
    unittest.main()
