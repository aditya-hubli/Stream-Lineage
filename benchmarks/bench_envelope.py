"""
Benchmarks for SDK envelope operations — the per-event hot path.

Covers: create_root_envelope, create_child_envelope, envelope_to_headers,
headers_to_envelope roundtrip, and verify_chain_hash.
"""

from harness import bench, run_benchmarks, print_header

from streamlineage.envelope import (
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
    verify_chain_hash,
)

# ── shared fixtures ─────────────────────────────────────────────────────
_PAYLOAD = b'{"user_id":"u-12345","amount":99.99,"currency":"USD","ts":"2025-01-15T12:00:00Z"}'
_SCHEMA_JSON = '{"type":"object","properties":{"user_id":{"type":"string"},"amount":{"type":"number"}}}'
_PRODUCER = "payment-service-v2.3"
_SCHEMA_ID = "payments.v4"
_TOPIC = "payments"

# Pre-create envelopes for child/verify benchmarks
_root = create_root_envelope(_PRODUCER, _SCHEMA_ID, _SCHEMA_JSON, _TOPIC, _PAYLOAD, 0, 42)
_child = create_child_envelope(
    [_root], "transform-svc", "payments-enriched.v1", _SCHEMA_JSON,
    "payments-enriched", b'{"enriched":true}', ["filter", "enrich"], 0, 100,
)
_root_headers = envelope_to_headers(_root)


# ── benchmarks ──────────────────────────────────────────────────────────

@bench("create_root_envelope", iterations=50_000)
def _():
    create_root_envelope(_PRODUCER, _SCHEMA_ID, _SCHEMA_JSON, _TOPIC, _PAYLOAD, 0, 42)


@bench("create_child_envelope (1 parent)", iterations=50_000)
def _():
    create_child_envelope(
        [_root], "transform-svc", "payments-enriched.v1", _SCHEMA_JSON,
        "payments-enriched", b'{"enriched":true}', ["filter", "enrich"],
    )


@bench("create_child_envelope (3 parents, fan-in)", iterations=20_000)
def _():
    create_child_envelope(
        [_root, _child, _root], "join-svc", "joined.v1", _SCHEMA_JSON,
        "joined-topic", b'{"joined":true}', ["join"],
    )


@bench("envelope_to_headers", iterations=100_000)
def _():
    envelope_to_headers(_root)


@bench("headers_to_envelope", iterations=100_000)
def _():
    headers_to_envelope(_root_headers)


@bench("envelope roundtrip (to_headers -> from_headers)", iterations=50_000)
def _():
    h = envelope_to_headers(_root)
    headers_to_envelope(h)


@bench("verify_chain_hash (root)", iterations=50_000)
def _():
    verify_chain_hash(_root, _PAYLOAD)


@bench("verify_chain_hash (child)", iterations=50_000)
def _():
    verify_chain_hash(_child, b'{"enriched":true}', [_root])


if __name__ == "__main__":
    print_header("SDK Envelope Benchmarks")
    run_benchmarks()
