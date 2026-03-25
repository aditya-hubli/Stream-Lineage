"""
End-to-end pipeline throughput benchmark.

Simulates the full ingestion path:
  create envelope → serialize to headers → deserialize → graph builder → DuckDB
Measures sustained throughput at different pipeline widths (fan-out).
"""

import os
import tempfile
import time

from harness import print_header

from streamlineage.envelope import (
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
)
from storage import LineageStorage
from graph_builder import LineageGraphBuilder

_SCHEMA_JSON = '{"type":"object","properties":{"x":{"type":"integer"}}}'
_PAYLOAD = b'{"x":1}'


def run_e2e(num_events: int, fan_out: int = 1, label: str = ""):
    """
    Run a full end-to-end pipeline simulation.

    Args:
        num_events: Number of root events to produce.
        fan_out: Number of child events per root event.
        label: Display label.
    """
    tmpdir = tempfile.mkdtemp()
    storage = LineageStorage(os.path.join(tmpdir, "e2e.duckdb"))
    builder = LineageGraphBuilder(storage)

    total_envelopes = 0
    t_start = time.perf_counter()

    for i in range(num_events):
        # 1. Root: create → serialize → deserialize → ingest
        root = create_root_envelope(
            f"producer-{i % 10}", "s.v1", _SCHEMA_JSON,
            f"topic-{i % 5}", _PAYLOAD, i % 3, i,
        )
        headers = envelope_to_headers(root)
        recovered = headers_to_envelope(headers)
        builder.process_envelope(root)
        total_envelopes += 1

        # 2. Fan-out children
        for j in range(fan_out):
            child = create_child_envelope(
                [root], f"consumer-{j}", "s.v1", _SCHEMA_JSON,
                f"output-{j}", _PAYLOAD, ["transform"], j, i * fan_out + j,
            )
            child_headers = envelope_to_headers(child)
            headers_to_envelope(child_headers)
            builder.process_envelope(child)
            total_envelopes += 1

    t_end = time.perf_counter()
    elapsed = t_end - t_start
    throughput = total_envelopes / elapsed

    print(
        f"  {label:<42s}  "
        f"{total_envelopes:>8,} events  "
        f"{elapsed:>6.2f}s  "
        f"{throughput:>12,.0f} events/s  "
        f"nodes={builder.graph.number_of_nodes()}  "
        f"edges={builder.graph.number_of_edges()}"
    )

    storage.close()
    return throughput


if __name__ == "__main__":
    print_header("End-to-End Pipeline Throughput")
    print("  Simulates: envelope create -> header serde -> graph build -> DuckDB insert\n")

    run_e2e(2_000, fan_out=0, label="linear pipeline (2k roots)")
    run_e2e(2_000, fan_out=1, label="1:1 transform (2k roots -> 2k children)")
    run_e2e(1_000, fan_out=3, label="1:3 fan-out (1k roots -> 3k children)")
    run_e2e(500, fan_out=10, label="1:10 fan-out (500 roots -> 5k children)")
    run_e2e(5_000, fan_out=1, label="sustained 5k roots + 5k children")
