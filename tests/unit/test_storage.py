"""Tests for LineageStorage (DuckDB persistence layer)."""

import json
import os
import tempfile
import time
from datetime import datetime, timedelta

import pytest

# Add service paths so imports resolve
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "lineage-ingester"))

from storage import LineageStorage
from streamlineage.envelope import ProvenanceEnvelope, create_root_envelope, create_child_envelope


# --- Fixtures ---

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_lineage.duckdb")


@pytest.fixture
def storage(db_path):
    s = LineageStorage(db_path)
    yield s
    s.close()


@pytest.fixture
def sample_envelope():
    return create_root_envelope(
        producer_id="test-producer",
        schema_id="test.v1",
        schema_json='{"type": "object"}',
        topic="test-topic",
        payload_bytes=b"test-payload",
        partition=0,
        offset=100,
    )


def _make_envelope(producer_id="p", topic="t", offset=0):
    """Helper to quickly create envelopes with custom fields."""
    return create_root_envelope(
        producer_id=producer_id,
        schema_id="s",
        schema_json="{}",
        topic=topic,
        payload_bytes=f"payload-{offset}".encode(),
        partition=0,
        offset=offset,
    )


# --- Schema initialization tests ---

class TestStorageInit:
    def test_creates_tables(self, storage):
        tables = storage.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {row[0] for row in tables}
        assert "lineage_events" in table_names
        assert "schema_snapshots" in table_names
        assert "dag_snapshots" in table_names
        assert "schema_drift_events" in table_names

    def test_idempotent_init(self, db_path):
        s1 = LineageStorage(db_path)
        s1.close()
        s2 = LineageStorage(db_path)  # should not raise
        s2.close()

    def test_close(self, db_path):
        s = LineageStorage(db_path)
        s.close()
        assert s.conn is None


# --- Lineage event insert/query tests ---

class TestLineageEvents:
    def test_insert_and_query(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        events = storage.query_lineage_events({"producer_id": "test-producer"})
        assert len(events) == 1
        assert events[0]["prov_id"] == sample_envelope.prov_id

    def test_insert_idempotent(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        storage.insert_lineage_event(sample_envelope)  # duplicate
        events = storage.query_lineage_events({"producer_id": "test-producer"})
        assert len(events) == 1

    def test_batch_insert(self, storage):
        envelopes = [_make_envelope(offset=i) for i in range(10)]
        storage.insert_lineage_events_batch(envelopes)
        events = storage.query_lineage_events({"producer_id": "p"})
        assert len(events) == 10

    def test_batch_insert_empty(self, storage):
        storage.insert_lineage_events_batch([])  # should not raise

    def test_query_by_topic(self, storage):
        storage.insert_lineage_event(_make_envelope(topic="topic-a"))
        storage.insert_lineage_event(_make_envelope(topic="topic-b"))
        events = storage.query_lineage_events({"topic": "topic-a"})
        assert len(events) == 1
        assert events[0]["topic"] == "topic-a"

    def test_query_by_origin_id(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        events = storage.query_lineage_events({"origin_id": sample_envelope.origin_id})
        assert len(events) == 1

    def test_query_no_filters_returns_all(self, storage):
        storage.insert_lineage_event(_make_envelope(producer_id="a", offset=1))
        storage.insert_lineage_event(_make_envelope(producer_id="b", offset=2))
        events = storage.query_lineage_events({})
        assert len(events) == 2

    def test_query_by_schema_id(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        events = storage.query_lineage_events({"schema_id": "test.v1"})
        assert len(events) == 1

    def test_stored_fields(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        events = storage.query_lineage_events({"prov_id": sample_envelope.prov_id})
        e = events[0]
        assert e["producer_id"] == sample_envelope.producer_id
        assert e["topic"] == sample_envelope.topic
        assert e["kafka_offset"] == sample_envelope.offset
        assert e["kafka_partition"] == sample_envelope.partition
        assert e["chain_hash"] == sample_envelope.chain_hash


# --- Impact events tests ---

class TestImpactEvents:
    def test_get_impact_events(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        ingested_dt = datetime.fromtimestamp(sample_envelope.ingested_at / 1000.0)
        # Use naive ISO format (no Z suffix) to match DuckDB's naive timestamps
        from_ts = (ingested_dt - timedelta(seconds=10)).isoformat()
        until_ts = (ingested_dt + timedelta(seconds=10)).isoformat()

        events = storage.get_impact_events("test-producer", from_ts, until_ts)
        assert len(events) == 1

    def test_get_impact_events_empty_window(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        # Query a time window far in the past
        events = storage.get_impact_events(
            "test-producer", "2000-01-01T00:00:00Z", "2000-01-02T00:00:00Z"
        )
        assert len(events) == 0

    def test_get_impact_events_wrong_producer(self, storage, sample_envelope):
        storage.insert_lineage_event(sample_envelope)
        ingested_dt = datetime.fromtimestamp(sample_envelope.ingested_at / 1000.0)
        from_ts = (ingested_dt - timedelta(seconds=10)).isoformat() + "Z"
        until_ts = (ingested_dt + timedelta(seconds=10)).isoformat() + "Z"

        events = storage.get_impact_events("nonexistent-producer", from_ts, until_ts)
        assert len(events) == 0


# --- Schema snapshot tests ---

class TestSchemaSnapshots:
    def test_upsert_new_schema(self, storage):
        storage.upsert_schema_snapshot("test.v1", "hash123", '{"field": "value"}')
        result = storage.conn.execute(
            "SELECT * FROM schema_snapshots WHERE schema_id = 'test.v1'"
        ).fetchall()
        assert len(result) == 1

    def test_upsert_updates_last_seen(self, storage):
        storage.upsert_schema_snapshot("test.v1", "hash123", '{"field": "value"}')
        first = storage.conn.execute(
            "SELECT last_seen_at FROM schema_snapshots WHERE schema_id = 'test.v1'"
        ).fetchone()[0]

        storage.upsert_schema_snapshot("test.v1", "hash123", '{"field": "value"}')
        second = storage.conn.execute(
            "SELECT last_seen_at FROM schema_snapshots WHERE schema_id = 'test.v1'"
        ).fetchone()[0]

        assert second >= first


# --- DAG snapshot tests ---

class TestDagSnapshots:
    def test_save_and_retrieve(self, storage):
        graph_json = '{"nodes": [], "edges": []}'
        snapshot_id = storage.save_dag_snapshot(graph_json, 0, 0)
        assert snapshot_id is not None

        result = storage.conn.execute(
            "SELECT * FROM dag_snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        assert result is not None

    def test_get_dag_snapshot_at(self, storage):
        graph_json = '{"nodes": [1], "edges": []}'
        storage.save_dag_snapshot(graph_json, 1, 0)

        # Query for a time slightly in the future
        future = (datetime.utcnow() + timedelta(seconds=5)).isoformat() + "Z"
        snapshot = storage.get_dag_snapshot_at(future)
        assert snapshot is not None
        assert snapshot["graph_json"] == graph_json
        assert snapshot["node_count"] == 1

    def test_get_dag_snapshot_at_before_any(self, storage):
        storage.save_dag_snapshot("{}", 0, 0)
        snapshot = storage.get_dag_snapshot_at("2000-01-01T00:00:00Z")
        assert snapshot is None


# --- Drift event tests ---

class TestDriftEvents:
    def test_insert_drift_event(self, storage):
        drift_event = {
            "drift_id": "drift-001",
            "schema_id": "test.v1",
            "old_schema_hash": "aaa",
            "new_schema_hash": "bbb",
            "old_schema_json": '{"old": true}',
            "new_schema_json": '{"new": true}',
            "severity": "BREAKING",
            "detected_at": datetime.utcnow().isoformat() + "Z",
            "affected_producer_id": "producer-1",
        }
        storage.insert_drift_event(drift_event)

        result = storage.conn.execute(
            "SELECT * FROM schema_drift_events WHERE drift_id = 'drift-001'"
        ).fetchone()
        assert result is not None
