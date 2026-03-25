"""
Lightweight benchmark harness for StreamLineage.

Provides a simple decorator and runner that measures throughput (ops/sec),
latency (p50/p95/p99), and memory delta for each benchmark function.
No external dependencies beyond the stdlib.
"""

import gc
import os
import statistics
import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from typing import Callable

# ── path bootstrap so we can import SDK + services ──────────────────────
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_root, "sdk"))
sys.path.insert(0, os.path.join(_root, "services", "lineage-ingester"))


@dataclass
class BenchResult:
    name: str
    iterations: int
    total_sec: float
    ops_per_sec: float
    latency_ns: list[int] = field(repr=False, default_factory=list)
    p50_us: float = 0.0
    p95_us: float = 0.0
    p99_us: float = 0.0
    mem_peak_kb: float = 0.0

    def summary_line(self) -> str:
        return (
            f"  {self.name:<42s}  "
            f"{self.ops_per_sec:>12,.0f} ops/s  "
            f"p50={self.p50_us:>8.1f}us  "
            f"p95={self.p95_us:>8.1f}us  "
            f"p99={self.p99_us:>8.1f}us  "
            f"mem={self.mem_peak_kb:>8.1f}KB"
        )


# ── registry ────────────────────────────────────────────────────────────
_benchmarks: list[tuple[str, Callable, int]] = []


def bench(name: str, iterations: int = 10_000):
    """Decorator to register a benchmark function."""
    def decorator(fn: Callable):
        _benchmarks.append((name, fn, iterations))
        return fn
    return decorator


def run_benchmarks(warmup: int = 100) -> list[BenchResult]:
    """Run all registered benchmarks and return results."""
    results: list[BenchResult] = []

    for name, fn, iterations in _benchmarks:
        # Warmup
        for _ in range(warmup):
            fn()

        gc.collect()
        gc.disable()

        tracemalloc.start()
        latencies_ns: list[int] = []

        wall_start = time.perf_counter()
        for _ in range(iterations):
            t0 = time.perf_counter_ns()
            fn()
            t1 = time.perf_counter_ns()
            latencies_ns.append(t1 - t0)
        wall_end = time.perf_counter()

        _, mem_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        gc.enable()

        total_sec = wall_end - wall_start
        ops = iterations / total_sec if total_sec > 0 else float("inf")

        # Convert ns -> µs for percentiles
        sorted_lat = sorted(latencies_ns)
        p50 = sorted_lat[int(len(sorted_lat) * 0.50)] / 1000.0
        p95 = sorted_lat[int(len(sorted_lat) * 0.95)] / 1000.0
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)] / 1000.0

        r = BenchResult(
            name=name,
            iterations=iterations,
            total_sec=total_sec,
            ops_per_sec=ops,
            latency_ns=latencies_ns,
            p50_us=p50,
            p95_us=p95,
            p99_us=p99,
            mem_peak_kb=mem_peak / 1024,
        )
        results.append(r)
        print(r.summary_line())

    return results


def print_header(section: str):
    print(f"\n{'-' * 100}")
    print(f"  {section}")
    print(f"{'-' * 100}")
