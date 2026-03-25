"""
Benchmarks for DuckDB storage operations.

Covers: single insert, batch insert, query by producer, query by time range,
impact events query, and snapshot save/load.
"""

import os
import tempfile
import uuid

from harness import bench, run_benchmarks, print_header

from streamlineage.envelope import create_root_envelope
from storage import LineageStorage

# ── setup: in-memory DuckDB ─────────────────────────────────────────────
_tmpdir = tempfile.mkdtemp()
_db_path = os.path.join(_tmpdir, "bench.duckdb")
_storage = LineageStorage(_db_path)

_SCHEMA_JSON = '{"type":"object"}'
_PRODUCER = "bench-producer"
_TOPIC = "bench-topic"
_PAYLOAD = b'{"key":"value"}'

# Pre-seed some data for query benchmarks
_seeded_envelopes = []
for i in range(500):
    env = create_root_envelope(_PRODUCER, "s.v1", _SCHEMA_JSON, _TOPIC, _PAYLOAD, 0, i)
    _seeded_envelopes.append(env)
_storage.insert_lineage_events_batch(_seeded_envelopes)

# Track offset for single-insert bench to avoid PK collisions
_insert_counter = [0]


def _fresh_envelope():
    return create_root_envelope(
        f"bench-prod-{_insert_counter[0]}", "s.v1", _SCHEMA_JSON, _TOPIC, _PAYLOAD, 0, _insert_counter[0]
    )


# ── benchmarks ──────────────────────────────────────────────────────────

@bench("storage: single insert", iterations=2_000)
def _():
    _insert_counter[0] += 1
    env = _fresh_envelope()
    _storage.insert_lineage_event(env)


@bench("storage: batch insert (100 envelopes)", iterations=200)
def _():
    batch = []
    for _ in range(100):
        _insert_counter[0] += 1
        batch.append(_fresh_envelope())
    _storage.insert_lineage_events_batch(batch)


@bench("storage: query by producer_id", iterations=500)
def _():
    _storage.query_lineage_events({"producer_id": _PRODUCER})


@bench("storage: query by time range", iterations=500)
def _():
    _storage.query_lineage_events({
        "producer_id": _PRODUCER,
        "ingested_after": "2020-01-01T00:00:00",
        "ingested_before": "2030-01-01T00:00:00",
    })


@bench("storage: get_impact_events", iterations=500)
def _():
    _storage.get_impact_events(_PRODUCER, "2020-01-01T00:00:00", "2030-01-01T00:00:00")


@bench("storage: save DAG snapshot (small)", iterations=500)
def _():
    _storage.save_dag_snapshot('{"nodes":[],"edges":[]}', 0, 0)


if __name__ == "__main__":
    print_header("DuckDB Storage Benchmarks")
    run_benchmarks(warmup=20)
    _storage.close()
