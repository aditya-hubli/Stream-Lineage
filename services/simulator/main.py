"""
StreamLineage Event Simulator

Generates realistic multi-stage streaming events with provenance tracking
to demonstrate the lineage system. Simulates a payment processing pipeline:

  payment-source -> fraud-checker -> enricher -> output

Each stage produces events with proper lineage envelopes so the ingester
can build the DAG automatically.
"""

import json
import os
import random
import signal
import sys
import time

from confluent_kafka import Consumer, Producer

# Add SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))
from streamlineage import LineageEnvelope

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
PRODUCE_RATE = int(os.getenv("PRODUCE_RATE", "10"))  # events per minute

# Topics
TOPIC_PAYMENTS = "sim-payments"
TOPIC_FRAUD_CHECKED = "sim-fraud-checked"
TOPIC_ENRICHED = "sim-enriched"
TOPIC_OUTPUT = "sim-output"

running = True


def signal_handler(sig, frame):
    global running
    print("[SIMULATOR] Shutting down...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "client.id": "simulator",
    })


def create_consumer(group_id: str, topics: list) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(topics)
    return consumer


# ---------------------------------------------------------------------------
# Stage 1: Payment source — produces root events
# ---------------------------------------------------------------------------
def produce_payment(producer: Producer, seq: int):
    """Produce a simulated payment event with lineage envelope."""
    amount = round(random.uniform(5.0, 5000.0), 2)
    user_id = f"user-{random.randint(1, 100):03d}"
    currency = random.choice(["USD", "EUR", "GBP", "INR"])

    event = {
        "payment_id": f"pay-{seq:06d}",
        "user_id": user_id,
        "amount": amount,
        "currency": currency,
        "merchant": random.choice(["Amazon", "Flipkart", "Swiggy", "Uber", "Netflix"]),
        "timestamp": time.time(),
    }

    envelope = LineageEnvelope(
        data=event,
        dataset_id="sim.payments",
        producer_id="sim-payment-source-v1",
        operation="CREATE",
    )

    producer.produce(
        topic=TOPIC_PAYMENTS,
        key=user_id.encode("utf-8"),
        value=envelope.to_json().encode("utf-8"),
    )
    producer.poll(0)
    return event


# ---------------------------------------------------------------------------
# Stage 2: Fraud checker — consumes payments, produces fraud-checked events
# ---------------------------------------------------------------------------
def run_fraud_checker(producer: Producer, consumer: Consumer):
    """Poll one message, apply fraud scoring, produce downstream."""
    msg = consumer.poll(0.5)
    if msg is None or msg.error():
        return

    message_str = msg.value().decode("utf-8")
    data, parent_envelope = LineageEnvelope.from_json(message_str)

    # Simple fraud heuristic
    risk_score = random.uniform(0.0, 1.0)
    if data.get("amount", 0) > 3000:
        risk_score = min(risk_score + 0.4, 1.0)

    enriched = {
        **data,
        "risk_score": round(risk_score, 3),
        "fraud_flag": risk_score > 0.8,
    }

    envelope = LineageEnvelope(
        data=enriched,
        dataset_id="sim.fraud-checked",
        producer_id="sim-fraud-checker-v1",
        operation="TRANSFORM",
        parent_envelopes=[parent_envelope] if parent_envelope else None,
    )

    producer.produce(
        topic=TOPIC_FRAUD_CHECKED,
        key=data.get("user_id", "").encode("utf-8"),
        value=envelope.to_json().encode("utf-8"),
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Stage 3: Enricher — consumes fraud-checked, adds geo/device info
# ---------------------------------------------------------------------------
def run_enricher(producer: Producer, consumer: Consumer):
    """Poll one message, enrich with metadata, produce downstream."""
    msg = consumer.poll(0.5)
    if msg is None or msg.error():
        return

    message_str = msg.value().decode("utf-8")
    data, parent_envelope = LineageEnvelope.from_json(message_str)

    enriched = {
        **data,
        "country": random.choice(["IN", "US", "UK", "DE", "JP"]),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "enriched_at": time.time(),
    }

    envelope = LineageEnvelope(
        data=enriched,
        dataset_id="sim.enriched",
        producer_id="sim-enricher-v1",
        operation="TRANSFORM",
        parent_envelopes=[parent_envelope] if parent_envelope else None,
    )

    producer.produce(
        topic=TOPIC_ENRICHED,
        key=data.get("user_id", "").encode("utf-8"),
        value=envelope.to_json().encode("utf-8"),
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Stage 4: Output sink — consumes enriched, writes to final topic
# ---------------------------------------------------------------------------
def run_output_sink(producer: Producer, consumer: Consumer):
    """Poll one message, write final output."""
    msg = consumer.poll(0.5)
    if msg is None or msg.error():
        return

    message_str = msg.value().decode("utf-8")
    data, parent_envelope = LineageEnvelope.from_json(message_str)

    final = {
        **data,
        "completed_at": time.time(),
        "status": "flagged" if data.get("fraud_flag") else "approved",
    }

    envelope = LineageEnvelope(
        data=final,
        dataset_id="sim.output",
        producer_id="sim-output-sink-v1",
        operation="AGGREGATE",
        parent_envelopes=[parent_envelope] if parent_envelope else None,
    )

    producer.produce(
        topic=TOPIC_OUTPUT,
        key=data.get("user_id", "").encode("utf-8"),
        value=envelope.to_json().encode("utf-8"),
    )
    producer.poll(0)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("StreamLineage Event Simulator")
    print(f"  Kafka: {KAFKA_BROKERS}")
    print(f"  Rate:  {PRODUCE_RATE} events/min")
    print("=" * 60)

    producer = create_producer()

    # Consumers for each downstream stage
    fraud_consumer = create_consumer("sim-fraud-checker", [TOPIC_PAYMENTS])
    enrich_consumer = create_consumer("sim-enricher", [TOPIC_FRAUD_CHECKED])
    output_consumer = create_consumer("sim-output-sink", [TOPIC_ENRICHED])

    interval = 60.0 / PRODUCE_RATE
    seq = 0

    print(f"[SIMULATOR] Producing every {interval:.1f}s — press Ctrl+C to stop")

    while running:
        # Stage 1: produce a new payment
        event = produce_payment(producer, seq)
        seq += 1
        print(f"[SIM] #{seq} payment {event['payment_id']}  "
              f"${event['amount']:.2f} {event['currency']}")

        producer.flush(timeout=2)

        # Run downstream stages (they process whatever is available)
        for _ in range(3):
            run_fraud_checker(producer, fraud_consumer)
            run_enricher(producer, enrich_consumer)
            run_output_sink(producer, output_consumer)

        producer.flush(timeout=2)

        time.sleep(interval)

    # Cleanup
    producer.flush(timeout=5)
    fraud_consumer.close()
    enrich_consumer.close()
    output_consumer.close()
    print("[SIMULATOR] Done.")


if __name__ == "__main__":
    main()
