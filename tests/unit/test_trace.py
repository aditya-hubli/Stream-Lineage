"""Tests for the trace endpoint logic (ancestor/descendant traversal)."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdk"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "lineage-ingester"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "api"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "services", "api", "routers"))

from storage import LineageStorage
from streamlineage.envelope import create_root_envelope, create_child_envelope
from trace import _fetch_event, _fetch_children, _event_to_trace


# --- Fixtures ---

@pytest.fixture
def storage(tmp_path):
    s = LineageStorage(str(tmp_path / "trace_test.duckdb"))
    yield s
    s.close()


def _root(producer_id="src", topic="t", payload=b"data"):
    return create_root_envelope(producer_id, "s", "{}", topic, payload)


def _child(parents, producer_id="dst", topic="t-out", ops=None):
    return create_child_envelope(
        parent_envelopes=parents,
        producer_id=producer_id,
        schema_id="s",
        schema_json="{}",
        topic=topic,
        payload_bytes=b"derived",
        transform_ops=ops or ["transform"],
    )


# --- _fetch_event tests ---

class TestFetchEvent:
    def test_found(self, storage):
        env = _root()
        storage.insert_lineage_event(env)
        event = _fetch_event(storage, env.prov_id)
        assert event is not None
        assert event["prov_id"] == env.prov_id

    def test_not_found(self, storage):
        assert _fetch_event(storage, "nonexistent") is None


# --- _fetch_children tests ---

class TestFetchChildren:
    def test_finds_children(self, storage):
        root = _root()
        child = _child([root], "consumer")
        storage.insert_lineage_event(root)
        storage.insert_lineage_event(child)

        children = _fetch_children(storage, root.prov_id)
        assert len(children) == 1
        assert children[0]["prov_id"] == child.prov_id

    def test_no_children(self, storage):
        root = _root()
        storage.insert_lineage_event(root)
        assert _fetch_children(storage, root.prov_id) == []

    def test_multiple_children(self, storage):
        root = _root()
        child1 = _child([root], "consumer-1")
        child2 = _child([root], "consumer-2")
        storage.insert_lineage_event(root)
        storage.insert_lineage_event(child1)
        storage.insert_lineage_event(child2)

        children = _fetch_children(storage, root.prov_id)
        assert len(children) == 2
        child_ids = {c["prov_id"] for c in children}
        assert child1.prov_id in child_ids
        assert child2.prov_id in child_ids

    def test_no_false_positive_substring_match(self, storage):
        """A prov_id 'abc' should not match a parent 'abcdef'."""
        root_a = _root("a", payload=b"a")
        root_b = _root("b", payload=b"b")
        child = _child([root_b], "c")

        storage.insert_lineage_event(root_a)
        storage.insert_lineage_event(root_b)
        storage.insert_lineage_event(child)

        # root_a should not appear as parent of child
        children_of_a = _fetch_children(storage, root_a.prov_id)
        assert len(children_of_a) == 0


# --- _event_to_trace tests ---

class TestEventToTrace:
    def test_converts_root_event(self, storage):
        root = _root()
        storage.insert_lineage_event(root)
        event = _fetch_event(storage, root.prov_id)
        trace = _event_to_trace(event, depth=0)

        assert trace.prov_id == root.prov_id
        assert trace.depth == 0
        assert trace.parent_prov_ids == []
        assert trace.producer_id == "src"

    def test_converts_child_event(self, storage):
        root = _root()
        child = _child([root], ops=["filter", "enrich"])
        storage.insert_lineage_event(root)
        storage.insert_lineage_event(child)
        event = _fetch_event(storage, child.prov_id)
        trace = _event_to_trace(event, depth=1)

        assert trace.prov_id == child.prov_id
        assert trace.depth == 1
        assert root.prov_id in trace.parent_prov_ids
        assert trace.transform_ops == ["filter", "enrich"]

    def test_negative_depth_for_ancestors(self, storage):
        root = _root()
        storage.insert_lineage_event(root)
        event = _fetch_event(storage, root.prov_id)
        trace = _event_to_trace(event, depth=-2)
        assert trace.depth == -2


# --- End-to-end trace traversal tests ---

class TestTraceTraversal:
    """Test the full ancestor/descendant walk logic by simulating the endpoint."""

    def _trace(self, storage, prov_id, max_depth=50):
        """Simulate the trace endpoint logic without FastAPI."""
        event = _fetch_event(storage, prov_id)
        assert event is not None

        # Walk ancestors
        ancestors = []
        visited = {prov_id}
        parent_ids = json.loads(event["parent_prov_ids"]) if event["parent_prov_ids"] else []
        queue = [(pid, -1) for pid in parent_ids if pid not in visited]
        visited.update(parent_ids)

        while queue:
            current_id, depth = queue.pop(0)
            if abs(depth) > max_depth:
                continue
            ancestor = _fetch_event(storage, current_id)
            if not ancestor:
                continue
            ancestors.append(_event_to_trace(ancestor, depth))
            anc_parents = json.loads(ancestor["parent_prov_ids"]) if ancestor["parent_prov_ids"] else []
            for pid in anc_parents:
                if pid not in visited:
                    queue.append((pid, depth - 1))
                    visited.add(pid)

        ancestors.sort(key=lambda e: e.depth)

        # Walk descendants
        descendants = []
        visited_desc = {prov_id}
        desc_queue = [(prov_id, 1)]

        while desc_queue:
            current_id, depth = desc_queue.pop(0)
            if depth > max_depth:
                continue
            children = _fetch_children(storage, current_id)
            for child in children:
                cid = child["prov_id"]
                if cid not in visited_desc:
                    visited_desc.add(cid)
                    descendants.append(_event_to_trace(child, depth))
                    desc_queue.append((cid, depth + 1))

        descendants.sort(key=lambda e: e.depth)
        return ancestors, _event_to_trace(event, 0), descendants

    def test_root_event_no_ancestors(self, storage):
        root = _root()
        storage.insert_lineage_event(root)
        ancestors, event, descendants = self._trace(storage, root.prov_id)
        assert len(ancestors) == 0
        assert event.prov_id == root.prov_id

    def test_leaf_event_no_descendants(self, storage):
        root = _root()
        child = _child([root])
        storage.insert_lineage_event(root)
        storage.insert_lineage_event(child)

        ancestors, event, descendants = self._trace(storage, child.prov_id)
        assert len(ancestors) == 1
        assert ancestors[0].prov_id == root.prov_id
        assert len(descendants) == 0

    def test_middle_event_has_both(self, storage):
        root = _root("a")
        mid = _child([root], "b")
        leaf = _child([mid], "c")
        storage.insert_lineage_event(root)
        storage.insert_lineage_event(mid)
        storage.insert_lineage_event(leaf)

        ancestors, event, descendants = self._trace(storage, mid.prov_id)
        assert event.prov_id == mid.prov_id
        assert len(ancestors) == 1
        assert ancestors[0].prov_id == root.prov_id
        assert len(descendants) == 1
        assert descendants[0].prov_id == leaf.prov_id

    def test_three_level_chain(self, storage):
        a = _root("a")
        b = _child([a], "b")
        c = _child([b], "c")
        d = _child([c], "d")
        for env in [a, b, c, d]:
            storage.insert_lineage_event(env)

        ancestors, event, descendants = self._trace(storage, b.prov_id)
        assert len(ancestors) == 1  # just a
        assert len(descendants) == 2  # c and d
        assert ancestors[0].depth == -1
        assert descendants[0].depth == 1  # c
        assert descendants[1].depth == 2  # d

    def test_fan_in_trace(self, storage):
        a = _root("source-a", payload=b"a")
        b = _root("source-b", payload=b"b")
        joined = _child([a, b], "joiner")
        for env in [a, b, joined]:
            storage.insert_lineage_event(env)

        ancestors, event, descendants = self._trace(storage, joined.prov_id)
        assert len(ancestors) == 2
        ancestor_ids = {anc.prov_id for anc in ancestors}
        assert a.prov_id in ancestor_ids
        assert b.prov_id in ancestor_ids

    def test_fan_out_trace(self, storage):
        root = _root()
        child1 = _child([root], "consumer-1")
        child2 = _child([root], "consumer-2")
        for env in [root, child1, child2]:
            storage.insert_lineage_event(env)

        ancestors, event, descendants = self._trace(storage, root.prov_id)
        assert len(descendants) == 2
        desc_ids = {d.prov_id for d in descendants}
        assert child1.prov_id in desc_ids
        assert child2.prov_id in desc_ids

    def test_max_depth_limits_traversal(self, storage):
        a = _root("a")
        b = _child([a], "b")
        c = _child([b], "c")
        d = _child([c], "d")
        for env in [a, b, c, d]:
            storage.insert_lineage_event(env)

        # Trace from b with max_depth=1: should only get c, not d
        ancestors, event, descendants = self._trace(storage, b.prov_id, max_depth=1)
        assert len(descendants) == 1
        assert descendants[0].prov_id == c.prov_id
