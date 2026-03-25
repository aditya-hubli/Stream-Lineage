"""Tests for LineageGraphBuilder (in-memory DAG)."""

import json
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "lineage-ingester"))

from graph_builder import LineageGraphBuilder, LRUCache
from storage import LineageStorage
from streamlineage.envelope import create_root_envelope, create_child_envelope


# --- Fixtures ---

@pytest.fixture
def storage(tmp_path):
    s = LineageStorage(str(tmp_path / "test.duckdb"))
    yield s
    s.close()


@pytest.fixture
def builder(storage):
    return LineageGraphBuilder(storage)


def _root(producer_id="producer-a", topic="topic-a", payload=b"data"):
    return create_root_envelope(producer_id, "s", "{}", topic, payload)


def _child(parents, producer_id="consumer-b", topic="topic-b", ops=None):
    return create_child_envelope(
        parent_envelopes=parents,
        producer_id=producer_id,
        schema_id="s",
        schema_json="{}",
        topic=topic,
        payload_bytes=b"derived",
        transform_ops=ops or ["transform"],
    )


# --- LRU Cache tests ---

class TestLRUCache:
    def test_put_and_get(self):
        cache = LRUCache(capacity=10)
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_returns_none(self):
        cache = LRUCache(capacity=10)
        assert cache.get("nonexistent") is None

    def test_eviction_at_capacity(self):
        cache = LRUCache(capacity=3)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.put("c", "3")
        cache.put("d", "4")  # should evict "a"
        assert cache.get("a") is None
        assert cache.get("b") == "2"

    def test_access_refreshes_entry(self):
        cache = LRUCache(capacity=3)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.put("c", "3")
        cache.get("a")  # refresh "a"
        cache.put("d", "4")  # should evict "b" (oldest after refresh)
        assert cache.get("a") == "1"
        assert cache.get("b") is None

    def test_update_existing_key(self):
        cache = LRUCache(capacity=10)
        cache.put("k", "old")
        cache.put("k", "new")
        assert cache.get("k") == "new"
        assert len(cache.cache) == 1


# --- Graph builder node tests ---

class TestGraphBuilderNodes:
    def test_process_envelope_adds_node(self, builder):
        env = _root("producer-a")
        builder.process_envelope(env)
        assert builder.graph.has_node("producer-a")

    def test_node_metadata(self, builder):
        env = _root("producer-a", topic="my-topic")
        builder.process_envelope(env)
        data = builder.graph.nodes["producer-a"]
        assert data["producer_id"] == "producer-a"
        assert data["event_count"] == 1
        assert "my-topic" in data["topics"]

    def test_duplicate_envelope_increments_count(self, builder):
        builder.process_envelope(_root("producer-a", payload=b"1"))
        builder.process_envelope(_root("producer-a", payload=b"2"))
        assert builder.graph.nodes["producer-a"]["event_count"] == 2

    def test_multiple_topics_tracked(self, builder):
        builder.process_envelope(_root("producer-a", topic="t1"))
        builder.process_envelope(_root("producer-a", topic="t2"))
        topics = builder.graph.nodes["producer-a"]["topics"]
        assert "t1" in topics
        assert "t2" in topics


# --- Graph builder edge tests ---

class TestGraphBuilderEdges:
    def test_child_creates_edge(self, builder):
        root = _root("producer-a")
        builder.process_envelope(root)

        child = _child([root], "consumer-b")
        builder.process_envelope(child)

        assert builder.graph.has_edge("producer-a", "consumer-b")

    def test_edge_metadata(self, builder):
        root = _root("producer-a")
        builder.process_envelope(root)

        child = _child([root], "consumer-b")
        builder.process_envelope(child)

        edge = builder.graph.edges["producer-a", "consumer-b"]
        assert edge["event_count"] == 1
        assert edge["source_producer_id"] == "producer-a"
        assert edge["target_producer_id"] == "consumer-b"

    def test_repeated_edges_increment_count(self, builder):
        root1 = _root("producer-a", payload=b"1")
        builder.process_envelope(root1)
        child1 = _child([root1], "consumer-b")
        builder.process_envelope(child1)

        root2 = _root("producer-a", payload=b"2")
        builder.process_envelope(root2)
        child2 = _child([root2], "consumer-b")
        builder.process_envelope(child2)

        assert builder.graph.edges["producer-a", "consumer-b"]["event_count"] == 2

    def test_fan_in_creates_multiple_edges(self, builder):
        root_a = _root("source-a")
        root_b = _root("source-b")
        builder.process_envelope(root_a)
        builder.process_envelope(root_b)

        joined = _child([root_a, root_b], "joiner")
        builder.process_envelope(joined)

        assert builder.graph.has_edge("source-a", "joiner")
        assert builder.graph.has_edge("source-b", "joiner")

    def test_fan_out(self, builder):
        root = _root("source")
        builder.process_envelope(root)

        builder.process_envelope(_child([root], "consumer-1"))
        builder.process_envelope(_child([root], "consumer-2"))

        assert builder.graph.has_edge("source", "consumer-1")
        assert builder.graph.has_edge("source", "consumer-2")

    def test_missing_parent_in_cache_no_edge(self, builder):
        """If parent's prov_id isn't in cache, edge won't be created."""
        # Create a child envelope referencing a parent that was never processed
        fake_parent = _root("unknown-parent")
        child = _child([fake_parent], "consumer")
        builder.process_envelope(child)

        # Node should exist but no edge since parent wasn't processed
        assert builder.graph.has_node("consumer")
        assert builder.graph.number_of_edges() == 0


# --- Graph builder lineage event (batch) tests ---

class TestGraphBuilderLineageEvent:
    def test_simple_lineage_event(self, builder):
        builder.process_lineage_event({
            "producer_id": "dbt-model-v1",
            "dataset_id": "stg_orders",
            "operation": "TRANSFORM",
        })
        assert builder.graph.has_node("dbt-model-v1")

    def test_lineage_event_with_parents(self, builder):
        builder.process_lineage_event({
            "producer_id": "dbt-model-v1",
            "dataset_id": "stg_orders",
        })
        builder.process_lineage_event({
            "producer_id": "downstream-v1",
            "dataset_id": "mart_orders",
            "parent_datasets": ["dbt-model-v1"],
        })
        assert builder.graph.has_edge("dbt-model-v1", "downstream-v1")

    def test_lineage_event_missing_producer_id_raises(self, builder):
        with pytest.raises(ValueError, match="producer_id is required"):
            builder.process_lineage_event({"dataset_id": "d"})


# --- Serialization tests ---

class TestGraphSerialization:
    def test_get_graph_json(self, builder):
        builder.process_envelope(_root("node-a"))
        json_str = builder.get_graph_json()
        data = json.loads(json_str)
        assert "nodes" in data
        # NetworkX node_link_data uses "links" or "edges" depending on version
        assert "links" in data or "edges" in data

    def test_snapshot_roundtrip(self, builder):
        root = _root("producer-x")
        builder.process_envelope(root)
        builder.process_envelope(_child([root], "consumer-y"))

        snapshot = builder.get_graph_json()

        # Create a fresh builder and load snapshot
        builder2 = LineageGraphBuilder(builder.storage)
        assert builder2.load_from_snapshot(snapshot) is True
        assert builder2.graph.has_node("producer-x")
        assert builder2.graph.has_node("consumer-y")
        assert builder2.graph.has_edge("producer-x", "consumer-y")

    def test_load_invalid_snapshot(self, builder):
        assert builder.load_from_snapshot("not-json") is False

    def test_take_snapshot_persists(self, builder, storage):
        builder.process_envelope(_root("n1"))
        snapshot_id = builder.take_snapshot()
        assert snapshot_id is not None

        result = storage.conn.execute(
            "SELECT * FROM dag_snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        assert result is not None


# --- Diff tests ---

class TestGraphDiff:
    def test_diff_added_node(self, builder):
        snap_a = builder.get_graph_json()
        builder.process_envelope(_root("new-node"))
        snap_b = builder.get_graph_json()

        diff = builder.diff_snapshots(snap_a, snap_b)
        assert "new-node" in diff["nodes_added"]
        assert diff["nodes_removed"] == []

    def test_diff_removed_node(self, builder):
        builder.process_envelope(_root("old-node"))
        snap_a = builder.get_graph_json()

        # Simulate removal by using an empty graph snapshot
        builder2 = LineageGraphBuilder(builder.storage)
        snap_b = builder2.get_graph_json()

        diff = builder.diff_snapshots(snap_a, snap_b)
        assert "old-node" in diff["nodes_removed"]

    def test_diff_added_edge(self, builder):
        root = _root("a")
        builder.process_envelope(root)
        snap_a = builder.get_graph_json()

        builder.process_envelope(_child([root], "b"))
        snap_b = builder.get_graph_json()

        diff = builder.diff_snapshots(snap_a, snap_b)
        assert len(diff["edges_added"]) >= 1
        added_edge = diff["edges_added"][0]
        assert added_edge["source"] == "a"
        assert added_edge["target"] == "b"

    def test_diff_no_changes(self, builder):
        builder.process_envelope(_root("x"))
        snap = builder.get_graph_json()
        diff = builder.diff_snapshots(snap, snap)
        assert diff["nodes_added"] == []
        assert diff["nodes_removed"] == []
        assert diff["edges_added"] == []
        assert diff["edges_removed"] == []
        assert diff["node_delta"] == 0
        assert diff["edge_delta"] == 0


# --- Subscriber tests ---

class TestGraphSubscribers:
    def test_subscriber_called_on_new_node(self, builder):
        events = []
        builder.subscribe(lambda etype, data: events.append((etype, data)))

        builder.process_envelope(_root("new-node"))
        # Subscribers run in a daemon thread, give it a moment
        time.sleep(0.1)

        node_events = [e for e in events if e[0] == "dag.node.added"]
        assert len(node_events) == 1
        assert node_events[0][1]["producer_id"] == "new-node"

    def test_subscriber_called_on_new_edge(self, builder):
        events = []
        builder.subscribe(lambda etype, data: events.append((etype, data)))

        root = _root("src")
        builder.process_envelope(root)
        builder.process_envelope(_child([root], "dst"))
        time.sleep(0.1)

        edge_events = [e for e in events if e[0] == "dag.edge.added"]
        assert len(edge_events) == 1

    def test_subscriber_exception_doesnt_crash(self, builder):
        def bad_callback(etype, data):
            raise RuntimeError("boom")

        builder.subscribe(bad_callback)
        builder.process_envelope(_root("node"))  # should not raise
        time.sleep(0.1)


# --- Thread safety tests ---

class TestGraphThreadSafety:
    def test_concurrent_envelope_processing(self, builder):
        errors = []

        def produce_envelopes(prefix, count):
            try:
                for i in range(count):
                    env = _root(f"{prefix}-{i}", payload=f"p{i}".encode())
                    builder.process_envelope(env)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=produce_envelopes, args=("t1", 50)),
            threading.Thread(target=produce_envelopes, args=("t2", 50)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert builder.graph.number_of_nodes() == 100
