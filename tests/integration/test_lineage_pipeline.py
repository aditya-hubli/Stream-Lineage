"""
Integration tests for the StreamLineage pipeline.

These tests exercise the full stack end-to-end using real DuckDB (in-memory)
and real LineageGraphBuilder — no mocking of core components.

Kafka is NOT required: envelopes are injected directly into the graph builder
and storage, simulating what the ingester would do after consuming from Kafka.

Test coverage:
  1. Envelope → ingester (graph + storage) → DAG round-trip
  2. Impact analysis end-to-end (source corruption → blast radius)
  3. Schema drift detection + GET /api/v1/schema/drift equivalent
  4. Chain hash verification (valid + tampered cases)
  5. Audit report generation
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

import pytest

# Path setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "lineage-ingester"))

from streamlineage.envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    create_child_envelope,
    verify_chain_hash,
)
from storage import LineageStorage
from graph_builder import LineageGraphBuilder
from schema_detector import SchemaDriftDetector


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "integration.duckdb")


@pytest.fixture
def storage(db_path):
    s = LineageStorage(db_path)
    yield s
    s.close()


@pytest.fixture
def graph(storage):
    return LineageGraphBuilder(storage)


@pytest.fixture
def schema_detector(storage):
    return SchemaDriftDetector(storage)


# ---------------------------------------------------------------------------
# Helper: build a 3-stage linear pipeline
#
#   producer-A  →  transformer-B  →  consumer-C
# ---------------------------------------------------------------------------

def build_linear_pipeline(storage, graph_builder, topic_prefix="int"):
    """
    Produce 3 linked events through a linear pipeline and insert into
    storage + graph. Returns (env_a, env_b, env_c, payloads).
    """
    payload_a = json.dumps({"user_id": "u1", "amount": 200}).encode()
    env_a = create_root_envelope(
        producer_id="producer-A",
        schema_id="payments.v1",
        schema_json='{"fields":[{"name":"user_id","type":"string"},{"name":"amount","type":"int"}]}',
        topic=f"{topic_prefix}.raw",
        payload_bytes=payload_a,
        partition=0, offset=100,
    )

    payload_b = json.dumps({"user_id": "u1", "amount": 200, "risk": 0.2}).encode()
    env_b = create_child_envelope(
        parent_envelopes=[env_a],
        producer_id="transformer-B",
        schema_id="enriched.v1",
        schema_json='{"fields":[{"name":"user_id","type":"string"},{"name":"amount","type":"int"},{"name":"risk","type":"float"}]}',
        topic=f"{topic_prefix}.enriched",
        payload_bytes=payload_b,
        transform_ops=["enrich"],
        partition=0, offset=200,
    )

    payload_c = json.dumps({"user_id": "u1", "verdict": "pass"}).encode()
    env_c = create_child_envelope(
        parent_envelopes=[env_b],
        producer_id="consumer-C",
        schema_id="verdicts.v1",
        schema_json='{"fields":[{"name":"user_id","type":"string"},{"name":"verdict","type":"string"}]}',
        topic=f"{topic_prefix}.verdicts",
        payload_bytes=payload_c,
        transform_ops=["filter", "classify"],
        partition=0, offset=300,
    )

    for env in (env_a, env_b, env_c):
        graph_builder.process_envelope(env)

    return env_a, env_b, env_c, {
        env_a.prov_id: payload_a,
        env_b.prov_id: payload_b,
        env_c.prov_id: payload_c,
    }


# ---------------------------------------------------------------------------
# 1. DAG round-trip
# ---------------------------------------------------------------------------

class TestDagRoundTrip:

    def test_nodes_added_to_graph(self, storage, graph):
        env_a, env_b, env_c, _ = build_linear_pipeline(storage, graph)
        assert graph.graph.has_node("producer-A")
        assert graph.graph.has_node("transformer-B")
        assert graph.graph.has_node("consumer-C")

    def test_edges_reflect_lineage(self, storage, graph):
        env_a, env_b, env_c, _ = build_linear_pipeline(storage, graph)
        assert graph.graph.has_edge("producer-A", "transformer-B")
        assert graph.graph.has_edge("transformer-B", "consumer-C")

    def test_events_persisted_to_duckdb(self, storage, graph):
        env_a, env_b, env_c, _ = build_linear_pipeline(storage, graph)
        for prov_id in (env_a.prov_id, env_b.prov_id, env_c.prov_id):
            rows = storage.query_lineage_events({"prov_id": prov_id})
            assert len(rows) == 1, f"Event {prov_id} not found in storage"

    def test_edge_event_count_increments(self, storage, graph):
        build_linear_pipeline(storage, graph, topic_prefix="p1")
        # Second pipeline produces more events on the same edge
        build_linear_pipeline(storage, graph, topic_prefix="p2")
        edge_data = graph.graph.edges["producer-A", "transformer-B"]
        assert edge_data["event_count"] == 2

    def test_graph_json_serialization(self, storage, graph):
        build_linear_pipeline(storage, graph)
        graph_json = graph.get_graph_json()
        data = json.loads(graph_json)
        node_ids = {n["id"] for n in data["nodes"]}
        assert "producer-A" in node_ids

    def test_fan_in_join(self, storage, graph):
        """Two parents → one child (join node)."""
        p1 = create_root_envelope("svc-x", "x.v1", "{}", "t-x", b'{"x":1}', 0, 1)
        p2 = create_root_envelope("svc-y", "y.v1", "{}", "t-y", b'{"y":2}', 0, 2)
        child = create_child_envelope(
            parent_envelopes=[p1, p2],
            producer_id="join-node",
            schema_id="joined.v1",
            schema_json="{}",
            topic="joined",
            payload_bytes=b'{"x":1,"y":2}',
            transform_ops=["join"],
        )
        for env in (p1, p2, child):
            graph.process_envelope(env)

        assert graph.graph.has_edge("svc-x", "join-node")
        assert graph.graph.has_edge("svc-y", "join-node")


# ---------------------------------------------------------------------------
# 2. Impact analysis
# ---------------------------------------------------------------------------

class TestImpactAnalysis:

    def test_blast_radius_includes_downstream(self, storage, graph):
        env_a, env_b, env_c, _ = build_linear_pipeline(storage, graph)

        ingested_dt = datetime.fromtimestamp(env_a.ingested_at / 1000.0)
        corrupted_from = (ingested_dt - timedelta(seconds=5)).isoformat()
        corrupted_until = (ingested_dt + timedelta(seconds=5)).isoformat()

        result = graph.compute_impact("producer-A", corrupted_from, corrupted_until)
        downstream_nodes = {n["node"] for n in result["affected_downstream_nodes"]}
        assert "transformer-B" in downstream_nodes or "consumer-C" in downstream_nodes

    def test_impact_unknown_producer_raises(self, storage, graph):
        with pytest.raises(ValueError, match="not found"):
            graph.compute_impact(
                "nonexistent", "2020-01-01T00:00:00", "2020-01-02T00:00:00"
            )

    def test_impact_result_fields(self, storage, graph):
        env_a, _, _, _ = build_linear_pipeline(storage, graph)
        ingested_dt = datetime.fromtimestamp(env_a.ingested_at / 1000.0)
        result = graph.compute_impact(
            "producer-A",
            (ingested_dt - timedelta(seconds=5)).isoformat(),
            (ingested_dt + timedelta(seconds=5)).isoformat(),
        )
        assert "total_affected_events" in result
        assert "replay_kafka_offsets" in result
        assert "affected_downstream_nodes" in result


# ---------------------------------------------------------------------------
# 3. Schema drift detection
# ---------------------------------------------------------------------------

class TestSchemaDrift:

    def _send_event(self, graph, schema_detector, producer_id, schema_id, schema_json):
        payload = json.dumps({"data": "value"}).encode()
        env = create_root_envelope(
            producer_id=producer_id,
            schema_id=schema_id,
            schema_json=schema_json,
            topic="drift-topic",
            payload_bytes=payload,
        )
        graph.process_envelope(env)
        return schema_detector.check_envelope(env, schema_json)

    def test_first_schema_no_drift(self, storage, graph, schema_detector):
        result = self._send_event(
            graph, schema_detector, "p1", "s.v1",
            '{"fields":[{"name":"a","type":"string"}]}'
        )
        assert result is None  # No drift on first occurrence

    def test_additive_field_detected(self, storage, graph, schema_detector):
        self._send_event(
            graph, schema_detector, "p1", "s.v1",
            '{"fields":[{"name":"a","type":"string"}]}'
        )
        result = self._send_event(
            graph, schema_detector, "p1", "s.v1",
            '{"fields":[{"name":"a","type":"string"},{"name":"b","type":"int","nullable":true}]}'
        )
        assert result is not None
        assert result["severity"] == "ADDITIVE"

    def test_breaking_field_removal_detected(self, storage, graph, schema_detector):
        self._send_event(
            graph, schema_detector, "p2", "s.v2",
            '{"fields":[{"name":"a","type":"string"},{"name":"b","type":"int"}]}'
        )
        result = self._send_event(
            graph, schema_detector, "p2", "s.v2",
            '{"fields":[{"name":"a","type":"string"}]}'  # b removed
        )
        assert result is not None
        assert result["severity"] == "BREAKING"

    def test_drift_event_persisted(self, storage, graph, schema_detector):
        self._send_event(graph, schema_detector, "p3", "s.v3",
                         '{"fields":[{"name":"x","type":"int"}]}')
        self._send_event(graph, schema_detector, "p3", "s.v3",
                         '{"fields":[{"name":"x","type":"string"}]}')  # type change
        rows = storage.conn.execute(
            "SELECT * FROM schema_drift_events WHERE schema_id = 's.v3'"
        ).fetchall()
        assert len(rows) >= 1

    def test_no_drift_same_schema_hash(self, storage, graph, schema_detector):
        schema_json = '{"fields":[{"name":"x","type":"int"}]}'
        self._send_event(graph, schema_detector, "p4", "s.v4", schema_json)
        result = self._send_event(graph, schema_detector, "p4", "s.v4", schema_json)
        assert result is None  # Same hash, no drift


# ---------------------------------------------------------------------------
# 4. Chain hash verification
# ---------------------------------------------------------------------------

class TestChainHashVerification:

    def test_root_event_verifies(self):
        payload = b'{"amount": 100}'
        env = create_root_envelope("svc", "s.v1", "{}", "t", payload)
        assert verify_chain_hash(env, payload) is True

    def test_root_event_tampered_payload_fails(self):
        env = create_root_envelope("svc", "s.v1", "{}", "t", b'original')
        assert verify_chain_hash(env, b'tampered') is False

    def test_child_event_verifies(self):
        parent_payload = b'{"x": 1}'
        parent = create_root_envelope("svc-a", "a.v1", "{}", "t-a", parent_payload)
        child_payload = b'{"x": 1, "y": 2}'
        child = create_child_envelope(
            parent_envelopes=[parent],
            producer_id="svc-b", schema_id="b.v1", schema_json="{}",
            topic="t-b", payload_bytes=child_payload, transform_ops=["enrich"],
        )
        assert verify_chain_hash(child, child_payload, [parent]) is True

    def test_child_event_tampered_fails(self):
        parent_payload = b'{"x": 1}'
        parent = create_root_envelope("svc-a", "a.v1", "{}", "t-a", parent_payload)
        child_payload = b'{"x": 1, "y": 2}'
        child = create_child_envelope(
            parent_envelopes=[parent],
            producer_id="svc-b", schema_id="b.v1", schema_json="{}",
            topic="t-b", payload_bytes=child_payload, transform_ops=["enrich"],
        )
        assert verify_chain_hash(child, b'tampered payload', [parent]) is False

    def test_missing_parents_raises_value_error(self):
        parent = create_root_envelope("svc", "s.v1", "{}", "t", b'x')
        child = create_child_envelope(
            parent_envelopes=[parent],
            producer_id="svc-b", schema_id="b.v1", schema_json="{}",
            topic="t-b", payload_bytes=b'y', transform_ops=[],
        )
        with pytest.raises(ValueError, match="parent_envelopes required"):
            verify_chain_hash(child, b'y', parent_envelopes=None)

    def test_chain_depth_three(self):
        """Verify a 3-hop chain: root → child → grandchild."""
        pa = b'a'
        pb = b'ab'
        pc = b'abc'
        e_a = create_root_envelope("a", "s", "{}", "t", pa)
        e_b = create_child_envelope([e_a], "b", "s", "{}", "t", pb, [])
        e_c = create_child_envelope([e_b], "c", "s", "{}", "t", pc, [])

        assert verify_chain_hash(e_a, pa)
        assert verify_chain_hash(e_b, pb, [e_a])
        assert verify_chain_hash(e_c, pc, [e_b])


# ---------------------------------------------------------------------------
# 5. DAG snapshot & diff
# ---------------------------------------------------------------------------

class TestDagSnapshotAndDiff:

    def test_snapshot_saved_and_loadable(self, storage, graph):
        build_linear_pipeline(storage, graph)
        snap_id = graph.take_snapshot()
        assert snap_id is not None

        result = storage.conn.execute(
            "SELECT node_count, edge_count FROM dag_snapshots WHERE snapshot_id = ?",
            (snap_id,)
        ).fetchone()
        assert result[0] == 3  # A, B, C
        assert result[1] == 2  # A→B, B→C

    def test_diff_detects_new_node(self, storage, graph):
        build_linear_pipeline(storage, graph)
        snap_a = graph.get_graph_json()

        # Add a new node
        extra = create_root_envelope("producer-D", "d.v1", "{}", "t-d", b'd')
        graph.process_envelope(extra)
        snap_b = graph.get_graph_json()

        diff = graph.diff_snapshots(snap_a, snap_b)
        assert "producer-D" in diff["nodes_added"]
        assert diff["node_delta"] == 1

    def test_diff_empty_graphs(self, storage, graph):
        snap = graph.get_graph_json()
        diff = graph.diff_snapshots(snap, snap)
        assert diff["nodes_added"] == []
        assert diff["nodes_removed"] == []
        assert diff["node_delta"] == 0

    def test_load_from_snapshot_restores_nodes(self, storage, graph):
        build_linear_pipeline(storage, graph)
        snap_json = graph.get_graph_json()

        fresh_graph = LineageGraphBuilder(storage)
        # Manually clear (bypass _restore_graph_state which reads from storage)
        import networkx as nx
        fresh_graph.graph = nx.DiGraph()

        assert fresh_graph.load_from_snapshot(snap_json)
        assert fresh_graph.graph.has_node("producer-A")
        assert fresh_graph.graph.has_edge("producer-A", "transformer-B")
