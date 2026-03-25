"""
Main Lineage Ingester Service

Consumes from all Kafka topics, extracts provenance envelopes from message headers,
and feeds them to the LineageGraphBuilder for real-time lineage tracking.
"""

import os
import signal
import sys
import time
from datetime import datetime

import json

from confluent_kafka import Consumer, Producer, KafkaError

# Add SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))

from streamlineage.envelope import ProvenanceEnvelope, headers_to_envelope, verify_chain_hash
from storage import LineageStorage
from graph_builder import LineageGraphBuilder
from schema_detector import SchemaDriftDetector


class LineageIngester:
    """Main lineage ingester service."""
    
    def __init__(
        self,
        kafka_brokers: str,
        duckdb_path: str,
        consumer_group: str = "streamlineage-lineage-ingester"
    ):
        """
        Initialize the lineage ingester.
        
        Args:
            kafka_brokers: Kafka bootstrap servers
            duckdb_path: Path to DuckDB database file
            consumer_group: Kafka consumer group ID
        """
        self.kafka_brokers = kafka_brokers
        self.duckdb_path = duckdb_path
        self.consumer_group = consumer_group
        self.running = True
        
        # Performance metrics
        self.total_events = 0
        self.total_drifts = 0
        self.total_hash_failures = 0
        self.last_metric_time = time.time()
        self.last_metric_count = 0
        self.last_commit_time = time.time()
        self.last_snapshot_time = time.time()
        self.last_prune_time = time.time()
        
        # Initialize storage and graph builder
        print(f"[INIT] Initializing DuckDB storage at {duckdb_path}")
        self.storage = LineageStorage(duckdb_path)
        
        print("[INIT] Initializing lineage graph builder")
        self.graph_builder = LineageGraphBuilder(self.storage)
        
        print("[INIT] Initializing schema drift detector")
        self.schema_detector = SchemaDriftDetector(self.storage)
        
        # Initialize Kafka consumer
        print(f"[INIT] Creating Kafka consumer (brokers: {kafka_brokers})")
        self.consumer = Consumer({
            'bootstrap.servers': kafka_brokers,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for reliability
        })
        
        # Dead-letter producer for quarantined chain hash failures
        self.dl_producer = Producer({'bootstrap.servers': kafka_brokers})

        # Subscribe to all topics except internal ones
        # Using pattern subscription to match all topics except those starting with "__"
        self.consumer.subscribe(['^(?!__).*'])
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        print("[INIT] Lineage ingester initialized successfully")
    
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown on SIGINT/SIGTERM."""
        print(f"\n[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run(self):
        """
        Main consumption loop.
        
        Continuously polls Kafka for messages, extracts provenance envelopes,
        and processes them through the graph builder and drift detector.
        """
        print("[START] Starting lineage ingestion...")
        print(f"  Consumer Group: {self.consumer_group}")
        print(f"  Topic Pattern: ^(?!__).*  (all topics except internal)")
        print()
        
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message, check periodic tasks
                    self._check_periodic_tasks()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    else:
                        print(f"[ERROR] Consumer error: {msg.error()}")
                        continue
                
                # Process message
                self._process_message(msg)
                
                # Check periodic tasks (commits, snapshots, metrics)
                self._check_periodic_tasks()
        
        except KeyboardInterrupt:
            print("\n[SHUTDOWN] Keyboard interrupt received")
        
        finally:
            self._shutdown()
    
    def _process_message(self, msg):
        """
        Process a single Kafka message.

        Supports two lineage extraction methods:
        1. Kafka headers (ProvenanceInterceptor path — chain hash verified)
        2. JSON payload with embedded lineage (LineageEnvelope path — no hash verification)

        Args:
            msg: confluent_kafka.Message
        """
        envelope = None
        headers = {}
        from_headers = False

        # Method 1: Try extracting from Kafka headers first
        headers_list = msg.headers()
        if headers_list:
            headers = {k: v for k, v in headers_list}
            envelope = headers_to_envelope(headers)
            if envelope is not None:
                from_headers = True

        # Method 2: Try extracting from JSON payload (universal - works for any broker)
        if envelope is None:
            try:
                from streamlineage import LineageEnvelope
                message_str = msg.value().decode('utf-8')
                data, envelope = LineageEnvelope.from_json(message_str)
            except (json.JSONDecodeError, KeyError, AttributeError):
                # Not a lineage message, skip
                return

        if envelope is None:
            return

        # Verify chain hash BEFORE overwriting Kafka metadata.
        # The hash was computed at produce time using the partition/topic stored
        # in the envelope headers, so we must verify against those values.
        if from_headers:
            if not self._verify_chain_hash(envelope, msg.value()):
                self._handle_hash_failure(msg, envelope)
                return

        # Update envelope with actual Kafka metadata
        envelope.topic = msg.topic()
        envelope.partition = msg.partition()
        envelope.offset = msg.offset()

        # Process through graph builder
        self.graph_builder.process_envelope(envelope)

        # Check for schema drift
        schema_json = headers.get(b'schema_json', b'{}').decode('utf-8')
        drift_event = self.schema_detector.check_envelope(envelope, schema_json)

        if drift_event:
            severity = drift_event['severity']
            schema_id = drift_event['schema_id']
            print(f"[DRIFT] Schema drift detected: {schema_id} - {severity}")
            self.total_drifts += 1
            self.graph_builder._notify_subscribers('schema.drift.detected', drift_event)

        self.total_events += 1

    def _verify_chain_hash(self, envelope: ProvenanceEnvelope, payload_bytes: bytes) -> bool:
        """
        Verify an envelope's chain hash.

        For root events, verifies directly from payload.
        For child events, looks up parent chain_hashes from DuckDB first.
        Returns True on any verification error (fail-open) to avoid blocking
        legitimate events when parents haven't been ingested yet.
        """
        try:
            if not envelope.parent_prov_ids:
                return verify_chain_hash(envelope, payload_bytes)

            # Child event: fetch parent chain_hashes from storage
            parent_hashes = self._get_parent_chain_hashes(envelope.parent_prov_ids)
            if parent_hashes is None:
                # Parents not in storage yet (out-of-order delivery) — allow through
                return True
            if len(parent_hashes) != len(envelope.parent_prov_ids):
                return True  # Some parents missing, can't fully verify

            # Build minimal stubs with only the chain_hash field needed for verification
            parent_stubs = [
                ProvenanceEnvelope(
                    prov_id=pid, origin_id='', parent_prov_ids=[],
                    producer_id='', producer_hash='', schema_id='', schema_hash='',
                    transform_ops=[], chain_hash=parent_hashes[pid],
                    ingested_at=0, topic='',
                )
                for pid in envelope.parent_prov_ids
            ]
            return verify_chain_hash(envelope, payload_bytes, parent_stubs)
        except Exception as e:
            print(f"[WARN] Chain hash verification error for {envelope.prov_id}: {e}")
            return True  # Fail-open

    def _get_parent_chain_hashes(self, parent_prov_ids: list[str]) -> dict | None:
        """
        Fetch chain_hash for each parent prov_id from DuckDB.
        Returns None if no parents are found (signals out-of-order delivery).
        """
        placeholders = ','.join(['?' for _ in parent_prov_ids])
        rows = self.storage.conn.execute(
            f"SELECT prov_id, chain_hash FROM lineage_events WHERE prov_id IN ({placeholders})",
            parent_prov_ids
        ).fetchall()
        if not rows:
            return None
        return {row[0]: row[1] for row in rows}

    def _handle_hash_failure(self, msg, envelope: ProvenanceEnvelope):
        """
        Quarantine an event with a chain hash mismatch.

        - Emits a chain.hash.failure WebSocket event
        - Produces the original message to __lineage_deadletter with failure headers
        - Does NOT insert the event into the DAG or DuckDB
        """
        self.total_hash_failures += 1
        failure_data = {
            'prov_id': envelope.prov_id,
            'producer_id': envelope.producer_id,
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'stored_chain_hash': envelope.chain_hash,
            'detected_at': datetime.utcnow().isoformat() + 'Z',
        }
        print(f"[HASH FAILURE] Chain hash mismatch: prov_id={envelope.prov_id} "
              f"producer={envelope.producer_id} "
              f"topic={msg.topic()}:{msg.partition()}@{msg.offset()}")

        # Notify WebSocket subscribers (dashboard alert)
        self.graph_builder._notify_subscribers('chain.hash.failure', failure_data)

        # Produce to dead-letter topic with failure context headers
        try:
            dl_headers = list(msg.headers() or [])
            dl_headers.append((b'dl_reason', b'chain_hash_mismatch'))
            dl_headers.append((b'dl_original_topic', msg.topic().encode()))
            dl_headers.append((b'dl_detected_at', failure_data['detected_at'].encode()))
            dl_headers.append((b'dl_stored_hash', envelope.chain_hash.encode()))
            self.dl_producer.produce(
                topic='__lineage_deadletter',
                value=msg.value(),
                headers=dl_headers,
            )
            self.dl_producer.poll(0)  # Trigger non-blocking delivery
        except Exception as e:
            print(f"[ERROR] Failed to produce to __lineage_deadletter: {e}")
    
    def _check_periodic_tasks(self):
        """Check and execute periodic tasks (commits, snapshots, metrics)."""
        current_time = time.time()
        
        # Commit offsets every 5 seconds
        if current_time - self.last_commit_time >= 5.0:
            self.consumer.commit(asynchronous=False)
            self.last_commit_time = current_time
        
        # Take DAG snapshot every 60 seconds
        if current_time - self.last_snapshot_time >= 60.0:
            try:
                snapshot_id = self.graph_builder.take_snapshot()
                node_count = self.graph_builder.graph.number_of_nodes()
                edge_count = self.graph_builder.graph.number_of_edges()
                print(f"[SNAPSHOT] DAG snapshot taken: {snapshot_id} "
                      f"(nodes: {node_count}, edges: {edge_count})")
            except Exception as e:
                print(f"[ERROR] Failed to take snapshot: {e}")
            self.last_snapshot_time = current_time
        
        # Prune stale graph nodes/edges every 5 minutes
        if current_time - self.last_prune_time >= 300.0:
            try:
                result = self.graph_builder.prune_stale_nodes()
                if result['pruned_nodes'] > 0:
                    print(f"[PRUNE] Removed {result['pruned_nodes']} nodes, "
                          f"{result['pruned_edges']} edges from in-memory graph")
            except Exception as e:
                print(f"[ERROR] Graph pruning failed: {e}")
            self.last_prune_time = current_time

        # Log metrics every 10 seconds
        if current_time - self.last_metric_time >= 10.0:
            elapsed = current_time - self.last_metric_time
            events_processed = self.total_events - self.last_metric_count
            events_per_sec = events_processed / elapsed if elapsed > 0 else 0
            
            node_count = self.graph_builder.graph.number_of_nodes()
            edge_count = self.graph_builder.graph.number_of_edges()
            
            print(f"[METRICS] Events: {self.total_events:,} | "
                  f"Rate: {events_per_sec:.1f}/sec | "
                  f"Nodes: {node_count} | "
                  f"Edges: {edge_count} | "
                  f"Drifts: {self.total_drifts} | "
                  f"HashFailures: {self.total_hash_failures}")
            
            self.last_metric_time = current_time
            self.last_metric_count = self.total_events
    
    def _shutdown(self):
        """Perform graceful shutdown."""
        print("\n[SHUTDOWN] Stopping consumption...")
        
        # Flush dead-letter producer
        print("[SHUTDOWN] Flushing dead-letter producer...")
        self.dl_producer.flush(timeout=5)

        # Close consumer
        print("[SHUTDOWN] Closing Kafka consumer...")
        self.consumer.close()
        
        # Take final snapshot
        print("[SHUTDOWN] Taking final DAG snapshot...")
        try:
            snapshot_id = self.graph_builder.take_snapshot()
            print(f"[SHUTDOWN] Final snapshot: {snapshot_id}")
        except Exception as e:
            print(f"[ERROR] Failed to take final snapshot: {e}")
        
        # Close storage
        print("[SHUTDOWN] Closing DuckDB storage...")
        self.storage.close()
        
        # Print final statistics
        print("\n[COMPLETE] Lineage ingester shut down cleanly")
        print(f"  Total Events Processed: {self.total_events:,}")
        print(f"  Schema Drifts Detected: {self.total_drifts}")
        print(f"  Chain Hash Failures:    {self.total_hash_failures}")
        print(f"  Final DAG Nodes: {self.graph_builder.graph.number_of_nodes()}")
        print(f"  Final DAG Edges: {self.graph_builder.graph.number_of_edges()}")


def main():
    """Main entry point."""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/lineage.duckdb")
    
    print("=" * 60)
    print("StreamLineage - Lineage Ingester Service")
    print("=" * 60)
    print(f"Kafka Brokers: {kafka_brokers}")
    print(f"DuckDB Path: {duckdb_path}")
    print("=" * 60)
    print()
    
    ingester = LineageIngester(
        kafka_brokers=kafka_brokers,
        duckdb_path=duckdb_path
    )
    
    ingester.run()


if __name__ == "__main__":
    main()
