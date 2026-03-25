# Apache Flink + StreamLineage Integration

## What This Is

This directory demonstrates **StreamLineage integration with Apache Flink** using a production-style Kafka pipeline.

The pipeline reads from Kafka, transforms data, and writes back to Kafka - **with full lineage tracking added in 3 lines of code**.

## Files

- `original_pipeline.py` — Official Apache Flink Kafka CSV example (unmodified)
- `lineage_enabled_pipeline.py` — Same pipeline with StreamLineage integration

## The 3-Line Integration

```python
# 1. Import StreamLineage
from streamlineage.pyflink_wrapper import LineageStream

# 2. Replace Kafka source
stream = LineageStream.from_kafka(
    env=env,
    topic="flink-input",
    node_id="flink-csv-processor-v1",
    bootstrap_servers="localhost:19092"
)

# 3. Replace Kafka sink
transformed.to_kafka(topic="flink-output", bootstrap_servers="localhost:19092")
```

**That's it**. No changes to business logic, transformations, or data processing.

## How to Run

### Prerequisites

```bash
# 1. Start infrastructure (from streamlineage/ directory)
docker-compose up -d redpanda

# 2. Start StreamLineage API
cd streamlineage/services/api
python main.py &

# 3. Verify services
curl http://localhost:8000/health
# Should return: {"status":"ok",...}
```

### Step 1: Generate Test Data

```bash
cd streamlineage/examples/apache-flink-kafka-example

python lineage_enabled_pipeline.py --generate-data
```

This creates 20 test events in the `flink-input` topic.

### Step 2: Run Flink Pipeline (Without Lineage)

```bash
# Run the original Apache Flink example
python original_pipeline.py
```

This runs vanilla Flink - **no lineage tracking**.

### Step 3: Run Flink Pipeline (With Lineage)

```bash
# Run the lineage-enabled version
python lineage_enabled_pipeline.py
```

This runs **the exact same pipeline** but with lineage tracking enabled.

### Step 4: Verify Lineage Tracking

```bash
# Check the DAG
curl http://localhost:8000/api/v1/dag | python -m json.tool

# You should see a node:
# {
#   "producer_id": "flink-csv-processor-v1",
#   "event_count": 20,
#   "topics": ["flink-output"]
# }
```

## What Gets Tracked

When you run the lineage-enabled pipeline, StreamLineage automatically captures:

1. **Provenance IDs**: Every event gets a unique `prov_id`
2. **Parent links**: Output events reference their input events via `parent_prov_ids`
3. **Origin chains**: Corrupted events can be traced back to their source via `origin_id`
4. **Kafka metadata**: Partition + offset for every event (enables surgical replay)
5. **DAG construction**: The Flink job appears as a node in the real-time lineage graph

## Surgical Replay Example

After running the pipeline, you can test surgical replay:

```bash
# Simulate corruption: 5 events were bad starting at timestamp T
curl -X POST http://localhost:8000/api/v1/replay/plan \
  -H "Content-Type: application/json" \
  -d '{
    "producer_id": "flink-csv-processor-v1",
    "corrupted_from": "2026-02-16T10:00:00Z",
    "corrupted_until": "2026-02-16T11:00:00Z"
  }'

# Response shows exact partition+offset pairs to replay:
# {
#   "total_affected_events": 5,
#   "partition_offsets": {
#     "flink-output": {
#       "0": [{"offset": 123}, {"offset": 456}, ...]
#     }
#   },
#   "consumer_commands": {
#     "python": "# Auto-generated Python consumer code...",
#     "cli": "kafka-console-consumer --partition 0 --offset 123 ..."
#   }
# }
```

Instead of replaying **all 20 events** (100%), you replay **only 5 events** (25%).

## Proof This Works With Production Code

- ✅ Based on official Apache Flink example
- ✅ Uses real Kafka broker (Red panda)
- ✅ Processes actual data (not hardcoded mocks)
- ✅ Full lineage chain: input topic → Flink transform → output topic
- ✅ Zero changes to business logic (3 wrapper lines only)

## Architecture

```
┌──────────────┐      ┌─────────────────────┐      ┌───────────────┐
│ flink-input  │─────▶│  Flink Pipeline     │─────▶│ flink-output  │
│  (Kafka)     │      │  + LineageStream    │      │   (Kafka)     │
└──────────────┘      └─────────────────────┘      └───────────────┘
                               │
                               │ POST lineage metadata
                               ▼
                      ┌──────────────────┐
                      │ StreamLineage API│
                      │  (port 8000)     │
                      └──────────────────┘
                               │
                               │ Stores in DuckDB
                               ▼
                      ┌──────────────────┐
                      │   DAG + Storage  │
                      │  origin_id chains│
                      └──────────────────┘
```

## Next Steps

**For production use**, you would:

1. Install PyFlink in your Flink cluster: `pip install apache-flink`
2. Add StreamLineage SDK to your deployment: `pip install streamlineage`
3. Wrap your Kafka sources/sinks with `LineageStream`
4. Deploy StreamLineage API as a service
5. Configure replay automation for corruption incidents

**Estimated integration time**: 1-2 hours for a typical Flink job.

## License

This example is based on the Apache Flink project (Apache License 2.0).
StreamLineage integration code is provided as-is for demonstration purposes.
