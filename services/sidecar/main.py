"""
StreamLineage Sidecar Proxy

Consumes from a raw Kafka topic (produced by non-instrumented/legacy services),
injects a ProvenanceEnvelope as Kafka headers, and republishes to a shadow topic.

This enables lineage tracking for producers that cannot be instrumented directly
— zero changes required to the upstream service.

Configuration (env vars):
    SOURCE_TOPIC    Topic to consume from (default: raw.events)
    SHADOW_TOPIC    Topic to publish to (default: {SOURCE_TOPIC}.lineage)
    KAFKA_BROKERS   Broker addresses (default: localhost:9092)
    PRODUCER_ID     Logical name for this sidecar (default: sidecar-{SOURCE_TOPIC})
    SCHEMA_ID       Schema ID to attach to envelopes (default: unknown.v1)
    CONSUMER_GROUP  Kafka consumer group (default: streamlineage-sidecar-{SOURCE_TOPIC})
"""

import hashlib
import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, Producer, KafkaError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))

from streamlineage.envelope import create_root_envelope, envelope_to_headers


SOURCE_TOPIC = os.getenv('SOURCE_TOPIC', 'raw.events')
SHADOW_TOPIC = os.getenv('SHADOW_TOPIC', f'{SOURCE_TOPIC}.lineage')
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092')
PRODUCER_ID = os.getenv('PRODUCER_ID', f'sidecar-{SOURCE_TOPIC}')
SCHEMA_ID = os.getenv('SCHEMA_ID', 'unknown.v1')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', f'streamlineage-sidecar-{SOURCE_TOPIC}')


class SidecarProxy:
    """
    Consumes raw events from SOURCE_TOPIC, injects provenance envelopes,
    and republishes to SHADOW_TOPIC.
    """

    def __init__(self):
        self.running = True
        self.total_proxied = 0
        self.last_commit_time = time.time()
        self.last_metric_time = time.time()
        self.last_metric_count = 0

        print(f"[INIT] Sidecar proxy starting")
        print(f"  Source: {SOURCE_TOPIC}")
        print(f"  Shadow: {SHADOW_TOPIC}")
        print(f"  Brokers: {KAFKA_BROKERS}")
        print(f"  Producer ID: {PRODUCER_ID}")

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        self.consumer.subscribe([SOURCE_TOPIC])

        self.producer = Producer({'bootstrap.servers': KAFKA_BROKERS})

        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        print("[INIT] Sidecar proxy initialized")

    def _shutdown_handler(self, signum, frame):
        print(f"\n[SHUTDOWN] Signal {signum} received, shutting down...")
        self.running = False

    def run(self):
        """Main proxy loop."""
        print("[START] Sidecar proxy running...")
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    self._check_periodic()
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[ERROR] Consumer error: {msg.error()}")
                    continue

                self._proxy_message(msg)
                self._check_periodic()

        except KeyboardInterrupt:
            pass
        finally:
            self._shutdown()

    def _proxy_message(self, msg):
        """Inject envelope headers and republish to shadow topic."""
        payload_bytes = msg.value() or b''

        # Build a schema_json stub — real deployments should provide a schema registry
        schema_json = json.dumps({'schema_id': SCHEMA_ID})

        envelope = create_root_envelope(
            producer_id=PRODUCER_ID,
            schema_id=SCHEMA_ID,
            schema_json=schema_json,
            topic=SOURCE_TOPIC,
            payload_bytes=payload_bytes,
        )

        # Start with any existing headers from the source message
        existing_headers = list(msg.headers() or [])

        # Add provenance headers (overwrite any existing prov_* keys)
        prov_headers = [
            (k.encode() if isinstance(k, str) else k, v)
            for k, v in envelope_to_headers(envelope).items()
        ]

        self.producer.produce(
            topic=SHADOW_TOPIC,
            value=payload_bytes,
            key=msg.key(),
            headers=existing_headers + prov_headers,
        )
        self.producer.poll(0)
        self.total_proxied += 1

    def _check_periodic(self):
        now = time.time()

        # Commit offsets every 5 seconds
        if now - self.last_commit_time >= 5.0:
            self.consumer.commit(asynchronous=False)
            self.last_commit_time = now

        # Log metrics every 30 seconds
        if now - self.last_metric_time >= 30.0:
            elapsed = now - self.last_metric_time
            delta = self.total_proxied - self.last_metric_count
            rate = delta / elapsed if elapsed > 0 else 0
            print(f"[METRICS] Proxied: {self.total_proxied:,} | Rate: {rate:.1f}/sec")
            self.last_metric_time = now
            self.last_metric_count = self.total_proxied

    def _shutdown(self):
        print("\n[SHUTDOWN] Flushing producer...")
        self.producer.flush(timeout=5)
        print("[SHUTDOWN] Closing consumer...")
        self.consumer.close()
        print(f"[COMPLETE] Sidecar proxy shut down. Total proxied: {self.total_proxied:,}")


if __name__ == '__main__':
    SidecarProxy().run()
