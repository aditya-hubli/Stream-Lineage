# Stream-Lineage

**Real-time Kafka pipeline observability** — cryptographic provenance on every event, live DAG tracking, schema drift detection, and surgical replay by exact partition+offset pairs instead of replaying entire topics.

**[Try the interactive simulator →](https://stream-lineage.vercel.app)**
No install, no backend. Pick a pipeline topology, inject corruption, and watch the surgical replay plan generate in real time.

---

## The Problem

When bad data flows through a Kafka pipeline, the standard fix is brutal: replay the entire topic from a checkpoint. For a high-throughput pipeline that might mean reprocessing millions of events to fix a few hundred corrupted ones.

StreamLineage solves this by attaching a provenance envelope to every message. Each envelope records who produced the event, what its parent events were, and a cryptographic chain hash linking it to its full lineage. When corruption is detected, StreamLineage traces the exact `origin_id` chain through the DAG and returns precise `{topic → partition → [offset, ...]}` pairs — only the records that actually need replaying.

---

## How It Works

```
Your Producer (SDK)
  ↓  stamps every message with a ProvenanceEnvelope (Kafka headers)
  ↓  chain_hash = SHA256(parent_chain_hashes + payload_hash)
RedPanda / Kafka
  ↓
Lineage Ingester (Kafka consumer)
  ↓  extracts envelopes, builds in-memory NetworkX DAG
  ↓  persists events to DuckDB (topic, partition, offset, origin_id, chain_hash)
FastAPI (port 8000)
  ↓  REST endpoints + WebSocket live updates
React Frontend (port 5173)
     live DAG visualization, impact analysis, surgical replay UI
```

**The `origin_id` is the key.** Every event carries the UUID of its root ancestor. A corrupted root event and all its downstream derivatives share the same `origin_id`, making it trivial to find every contaminated record across every topic.

---

## Features

- **Provenance envelope** — UUID chain linking every event to its origin, with SHA256 chain hashes for tamper detection
- **Live DAG** — real-time visualization of which services produce/consume which topics, with per-edge latency tracking
- **Surgical replay** — exact partition+offset pairs for corrupted lineage chains, including join partner resolution for fan-in nodes
- **Schema drift detection** — detects BREAKING / ADDITIVE / COMPATIBLE schema changes per producer
- **Impact analysis** — BFS blast radius computation from any corrupted producer
- **Chain hash verification** — cryptographic audit of any event's full lineage
- **Interactive simulator** — runs entirely in-browser at [stream-lineage.vercel.app](https://stream-lineage.vercel.app), no backend required
- **Broker-agnostic SDK** — works via Kafka headers (confluent-kafka) or embedded JSON payload (any platform)

---

## Quick Start

```bash
git clone https://github.com/aditya-hubli/Stream-Lineage.git
cd Stream-Lineage
docker-compose up -d
```

| Service | URL |
|---|---|
| Frontend | http://localhost:5173 |
| API + Swagger docs | http://localhost:8000/docs |
| RedPanda Console | http://localhost:8080 |
| Grafana | http://localhost:3001 (admin/admin) |
| Prometheus | http://localhost:9090 |

Run a demo pipeline to generate lineage data:

```bash
curl -X POST http://localhost:8000/api/scenarios/simple_pipeline/start
```

---

## SDK Integration

### Wrap your Kafka producer (zero config)

```python
from confluent_kafka import Producer
from streamlineage.interceptor import ProvenanceInterceptor, WrappedProducer

raw_producer = Producer({'bootstrap.servers': 'localhost:19092'})
producer = WrappedProducer(raw_producer, ProvenanceInterceptor('payment-service-v2'))

# Use exactly like a normal producer — provenance headers injected automatically
producer.produce('payments', value=b'{"amount": 100}')
```

### Propagate lineage through transforms

```python
from streamlineage.interceptor import ThreadLocalParentEnvelope

# In your transform/consumer:
with ThreadLocalParentEnvelope(parent_envelope, transform_ops=['enrich', 'validate']):
    producer.produce('payments.enriched', value=enriched_payload)
# Child event automatically links to parent — origin_id chain preserved
```

### Broker-agnostic (embed in payload)

```python
from streamlineage.simple_envelope import LineageEnvelope

# Works with Flink, Pulsar, Kinesis, HTTP — no Kafka header support needed
env = LineageEnvelope('risk-engine', 'risk.v2', parent=parent_env)
payload = env.wrap({'score': 0.94, 'flags': []})
# Lineage metadata embedded inside the JSON body
```

---

## Surgical Replay

```bash
curl -X POST http://localhost:8000/api/v1/replay/plan \
  -H 'Content-Type: application/json' \
  -d '{
    "producer_id": "payment-service-v2",
    "corrupted_from": "2024-01-15T10:30:00Z",
    "corrupted_until": "2024-01-15T10:45:00Z"
  }'
```

Response:

```json
{
  "total_affected_records": 43,
  "total_events_in_window": 12847,
  "replay_efficiency_pct": 99.67,
  "partition_offsets": {
    "payments": {
      "0": { "exact_offsets": [1201, 1204, 1207], "optimized_ranges": [{"start": 1201, "end": 1208}] },
      "1": { "exact_offsets": [844, 845], "optimized_ranges": [{"start": 844, "end": 846}] }
    },
    "payments.enriched": {
      "0": { "exact_offsets": [2891, 2894, 2897], "optimized_ranges": [{"start": 2891, "end": 2898}] }
    }
  }
}
```

Instead of replaying 12,847 records, you seek to 43 exact offsets.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/lineage/ingest` | Ingest a provenance envelope via HTTP (non-Kafka pipelines) |
| `GET` | `/api/v1/dag` | Current DAG state (nodes + edges) |
| `POST` | `/api/v1/replay/plan` | Surgical replay plan with exact partition+offset pairs |
| `GET` | `/api/v1/trace/{prov_id}` | Full lineage trace — ancestors and descendants for one event |
| `POST` | `/api/v1/impact` | Blast radius analysis for a corrupted producer + time window |
| `GET` | `/api/v1/schema/drift` | Schema drift events (BREAKING / ADDITIVE / COMPATIBLE) |
| `GET` | `/api/v1/diff` | DAG diff between two snapshots |
| `GET` | `/api/v1/audit/{prov_id}` | Cryptographic audit of a full lineage chain |
| `GET` | `/api/v1/producers` | All known producers with status (active / stale / dead) |
| `WS` | `/ws/lineage` | Real-time DAG updates via WebSocket |

Full interactive docs at `/docs` (Swagger UI) and `/redoc`.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:19092` | Kafka/RedPanda broker address |
| `DUCKDB_PATH` | `/data/lineage.duckdb` | Path to DuckDB database file |
| `API_HOST` | `0.0.0.0` | API bind address |
| `API_PORT` | `8000` | API port |
| `REDIS_URL` | _(unset)_ | Redis URL for fast graph recovery on restart (optional) |
| `GRAPH_TTL_SECONDS` | `3600` | Evict DAG nodes not seen within this window (0 = disabled) |
| `KAFKA_CONSUMER_GROUP` | `lineage-ingester` | Kafka consumer group ID |

---

## Running Tests

```bash
cd sdk
pip install -e ".[dev]"
pytest ../tests/unit/ -v          # 190 unit tests
pytest ../tests/unit/ -m unit     # by marker
pytest -k test_envelope           # single test by name
```

---

## Benchmarks

Results from running on Windows 11 / Python 3.13 / DuckDB. Linux/Docker performance is higher.

```bash
python benchmarks/run_all.py
```

Full results in [`benchmarks/results/benchmark_results.json`](benchmarks/results/benchmark_results.json).

---

## Docker Compose Files

Two compose files are included for different environments:

| File | Use | Notes |
|---|---|---|
| `docker-compose.yml` | **Development** | Ports exposed locally, includes RedPanda Console UI, lower resource limits |
| `docker-compose.prod.yml` | **Production** | No exposed ports (security), higher memory/CPU limits, `restart: always`, configurable via `EXTERNAL_HOST` env var |

---

## Project Structure

```
├── sdk/                        # Pip-installable Python SDK
│   └── streamlineage/
│       ├── envelope.py         # ProvenanceEnvelope dataclass + hash utilities
│       ├── interceptor.py      # Kafka producer wrapper (auto-injects headers)
│       ├── simple_envelope.py  # Broker-agnostic lineage (embeds in JSON payload)
│       └── pyflink_wrapper.py  # Optional PyFlink integration
├── services/
│   ├── api/                    # FastAPI REST + WebSocket server
│   ├── lineage-ingester/       # Kafka consumer → builds DAG → DuckDB
│   ├── scenarios/              # Demo pipelines (simple, fan-in, fan-out, complex)
│   ├── simulator/              # Standalone event generator service
│   └── sidecar/                # Inject lineage into pipelines without code changes
├── frontend/                   # React + TypeScript UI (Vite, React Flow, Tailwind)
│   └── src/simulator/          # Browser-only interactive demo (no backend needed)
├── tests/                      # 190 unit + integration tests
├── benchmarks/                 # Performance benchmarks with results
├── infra/                      # Prometheus, Grafana, RedPanda console configs
├── examples/                   # Apache Flink + Kafka integration example
└── docker-compose.yml          # Full stack
```

---

## License

MIT
