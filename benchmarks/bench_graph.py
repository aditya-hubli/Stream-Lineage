"""
Benchmarks for LineageGraphBuilder operations.

Covers: process_envelope (node creation, edge creation, cache hits),
LRU cache operations, graph serialization, and snapshot diff.
"""

import os
import tempfile

from harness import bench, run_benchmarks, print_header

from streamlineage.envelope import create_root_envelope, create_child_envelope
from storage import LineageStorage
from graph_builder import LineageGraphBuilder, LRUCache

# ── setup ────────────────────────────────────────────────────────────────
_tmpdir = tempfile.mkdtemp()
_storage = LineageStorage(os.path.join(_tmpdir, "bench_graph.duckdb"))
_builder = LineageGraphBuilder(_storage)

_SCHEMA_JSON = '{"type":"object"}'
_PAYLOAD = b'{"key":"value"}'

# Pre-create envelopes
_root_envs = []
for i in range(100):
    env = create_root_envelope(f"producer-{i}", "s.v1", _SCHEMA_JSON, f"topic-{i}", _PAYLOAD, 0, i)
    _root_envs.append(env)

# Seed the builder with some nodes so we can benchmark edge creation
for env in _root_envs[:50]:
    _builder.process_envelope(env)

# Create child envelopes that link to existing nodes
_child_envs = []
for i in range(50):
    child = create_child_envelope(
        [_root_envs[i]], f"consumer-{i}", "s.v1", _SCHEMA_JSON,
        f"output-topic-{i}", _PAYLOAD, ["transform"], 0, 1000 + i,
    )
    _child_envs.append(child)

# LRU cache for isolated benchmarks
_cache = LRUCache(capacity=10_000)
for i in range(5000):
    _cache.put(f"key-{i}", f"val-{i}")

# Counter for unique envelopes
_counter = [200]


# ── benchmarks ──────────────────────────────────────────────────────────

@bench("LRU cache: put", iterations=50_000)
def _():
    _counter[0] += 1
    _cache.put(f"k-{_counter[0]}", f"v-{_counter[0]}")


@bench("LRU cache: get (hit)", iterations=50_000)
def _():
    _cache.get("key-1000")


@bench("LRU cache: get (miss)", iterations=50_000)
def _():
    _cache.get("nonexistent-key")


@bench("process_envelope (new node, root)", iterations=1_000)
def _():
    _counter[0] += 1
    env = create_root_envelope(
        f"dyn-producer-{_counter[0]}", "s.v1", _SCHEMA_JSON,
        "dyn-topic", _PAYLOAD, 0, _counter[0],
    )
    _builder.process_envelope(env)


@bench("process_envelope (existing node, update)", iterations=1_000)
def _():
    # Re-process an existing root envelope (updates event_count)
    _builder.process_envelope(_root_envs[0])


@bench("process_envelope (child, creates edge)", iterations=500)
def _():
    _counter[0] += 1
    parent = _root_envs[_counter[0] % 50]
    child = create_child_envelope(
        [parent], f"child-{_counter[0]}", "s.v1", _SCHEMA_JSON,
        "child-topic", _PAYLOAD, ["map"], 0, _counter[0],
    )
    _builder.process_envelope(child)


@bench("get_graph_json (serialization)", iterations=200)
def _():
    _builder.get_graph_json()


@bench("diff_snapshots (two small graphs)", iterations=500)
def _():
    snap_a = '{"directed":true,"multigraph":false,"graph":{},"nodes":[{"id":"A"},{"id":"B"}],"links":[{"source":"A","target":"B"}]}'
    snap_b = '{"directed":true,"multigraph":false,"graph":{},"nodes":[{"id":"A"},{"id":"C"}],"links":[{"source":"A","target":"C"}]}'
    _builder.diff_snapshots(snap_a, snap_b)


if __name__ == "__main__":
    print_header("Graph Builder Benchmarks")
    run_benchmarks(warmup=20)
    _storage.close()
