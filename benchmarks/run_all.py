#!/usr/bin/env python3
"""
StreamLineage Benchmark Suite — run all benchmarks and print a combined report.

Usage:
    cd streamlineage/benchmarks
    python run_all.py
"""

import importlib
import sys
import time

from harness import _benchmarks, run_benchmarks, print_header, BenchResult


def main():
    print("=" * 100)
    print("  StreamLineage Performance Benchmark Suite")
    print("  Python %s" % sys.version.split()[0])
    print("=" * 100)

    all_results: list[BenchResult] = []

    # ── 1. Envelope benchmarks ──────────────────────────────────────────
    _benchmarks.clear()
    print_header("SDK Envelope Operations (per-event hot path)  ")
    importlib.import_module("bench_envelope")
    results = run_benchmarks()
    all_results.extend(results)

    # ── 2. Storage benchmarks ───────────────────────────────────────────
    _benchmarks.clear()
    print_header("DuckDB Storage Operations")
    importlib.import_module("bench_storage")
    results = run_benchmarks(warmup=20)
    all_results.extend(results)

    # ── 3. Graph builder benchmarks ─────────────────────────────────────
    _benchmarks.clear()
    print_header("Graph Builder Operations")
    importlib.import_module("bench_graph")
    results = run_benchmarks(warmup=20)
    all_results.extend(results)

    # ── 4. End-to-end throughput ────────────────────────────────────────
    print_header("End-to-End Pipeline Throughput")
    print("  Simulates: envelope create -> header serde -> graph build -> DuckDB insert\n")
    from bench_e2e import run_e2e

    run_e2e(2_000, fan_out=0, label="linear pipeline (2k roots)")
    run_e2e(2_000, fan_out=1, label="1:1 transform (2k roots -> 2k children)")
    run_e2e(1_000, fan_out=3, label="1:3 fan-out (1k roots -> 3k children)")
    run_e2e(500, fan_out=10, label="1:10 fan-out (500 roots -> 5k children)")
    run_e2e(5_000, fan_out=1, label="sustained 5k roots + 5k children")

    # ── Summary ─────────────────────────────────────────────────────────
    print(f"\n{'=' * 100}")
    print("  SUMMARY")
    print(f"{'=' * 100}")

    # Find key metrics
    for r in all_results:
        if "create_root" in r.name:
            print(f"  Root envelope creation:  {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "create_child" in r.name and "1 parent" in r.name:
            print(f"  Child envelope creation: {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "roundtrip" in r.name:
            print(f"  Header serde roundtrip:  {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "verify_chain_hash (root)" in r.name:
            print(f"  Chain hash verification: {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "single insert" in r.name:
            print(f"  DuckDB single insert:    {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "batch insert" in r.name:
            print(f"  DuckDB batch insert:     {r.ops_per_sec:>12,.0f} batches/s (100 rows each)")
        elif "new node, root" in r.name:
            print(f"  Graph process (new):     {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")
        elif "existing node" in r.name:
            print(f"  Graph process (update):  {r.ops_per_sec:>12,.0f} ops/s  (p99={r.p99_us:.1f}us)")

    print(f"\n  Total benchmarks run: {len(all_results)}")
    print(f"{'=' * 100}")


if __name__ == "__main__":
    main()
