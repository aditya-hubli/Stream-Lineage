"""
DuckDB Storage Layer for StreamLineage

Manages all persistent storage for lineage events, schema snapshots, DAG snapshots,
and schema drift detection using DuckDB as the analytical engine.
"""

import json
from datetime import datetime
from typing import Optional

import duckdb

from streamlineage.envelope import ProvenanceEnvelope


class LineageStorage:
    """
    DuckDB-based storage for StreamLineage lineage data.
    
    DuckDB runs in-process without requiring a server, providing fast analytical
    queries over lineage events and metadata.
    """
    
    def __init__(self, db_path: str, read_only: bool = False):
        """
        Initialize LineageStorage with DuckDB connection.
        
        Args:
            db_path: Path to DuckDB database file
            read_only: If True, open database in read-only mode
        """
        self.db_path = db_path
        self.read_only = read_only
        self.conn = duckdb.connect(db_path, read_only=read_only)
        if not read_only:
            self._initialize_schema()
    
    def _initialize_schema(self):
        """Create database schema if it doesn't already exist."""
        # Create lineage_events table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS lineage_events (
                prov_id VARCHAR PRIMARY KEY,
                origin_id VARCHAR NOT NULL,
                parent_prov_ids VARCHAR,
                producer_id VARCHAR NOT NULL,
                producer_hash VARCHAR NOT NULL,
                schema_id VARCHAR NOT NULL,
                schema_hash VARCHAR NOT NULL,
                transform_ops VARCHAR,
                chain_hash VARCHAR NOT NULL,
                topic VARCHAR NOT NULL,
                kafka_offset BIGINT,
                kafka_partition INTEGER,
                ingested_at TIMESTAMP NOT NULL
            )
        """)
        
        # Create indexes for common queries
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lineage_origin_id 
            ON lineage_events(origin_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lineage_producer_id 
            ON lineage_events(producer_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lineage_schema_id 
            ON lineage_events(schema_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_lineage_ingested_at 
            ON lineage_events(ingested_at)
        """)
        
        # Create schema_snapshots table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_snapshots (
                schema_id VARCHAR,
                schema_hash VARCHAR,
                schema_json VARCHAR,
                first_seen_at TIMESTAMP,
                last_seen_at TIMESTAMP,
                PRIMARY KEY (schema_id, schema_hash)
            )
        """)
        
        # Create dag_snapshots table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dag_snapshots (
                snapshot_id VARCHAR PRIMARY KEY,
                snapshot_at TIMESTAMP NOT NULL,
                graph_json VARCHAR NOT NULL,
                node_count INTEGER,
                edge_count INTEGER
            )
        """)
        
        # Create schema_drift_events table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_drift_events (
                drift_id VARCHAR PRIMARY KEY,
                schema_id VARCHAR NOT NULL,
                old_schema_hash VARCHAR NOT NULL,
                new_schema_hash VARCHAR NOT NULL,
                old_schema_json VARCHAR,
                new_schema_json VARCHAR,
                severity VARCHAR NOT NULL,
                detected_at TIMESTAMP NOT NULL,
                affected_producer_id VARCHAR
            )
        """)
    
    def insert_lineage_event(self, envelope: ProvenanceEnvelope):
        """
        Insert a single lineage event from a ProvenanceEnvelope.
        
        Uses INSERT OR IGNORE for idempotency - duplicate prov_id will be skipped.
        
        Args:
            envelope: ProvenanceEnvelope to store
        """
        # Convert timestamp from milliseconds to datetime
        ingested_at = datetime.fromtimestamp(envelope.ingested_at / 1000.0)
        
        self.conn.execute("""
            INSERT OR IGNORE INTO lineage_events (
                prov_id, origin_id, parent_prov_ids, producer_id, producer_hash,
                schema_id, schema_hash, transform_ops, chain_hash, topic,
                kafka_offset, kafka_partition, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            envelope.prov_id,
            envelope.origin_id,
            json.dumps(envelope.parent_prov_ids),
            envelope.producer_id,
            envelope.producer_hash,
            envelope.schema_id,
            envelope.schema_hash,
            json.dumps(envelope.transform_ops),
            envelope.chain_hash,
            envelope.topic,
            envelope.offset,
            envelope.partition,
            ingested_at,
        ))
    
    def insert_lineage_events_batch(self, envelopes: list[ProvenanceEnvelope]):
        """
        Batch insert lineage events for better performance.
        
        Args:
            envelopes: List of ProvenanceEnvelope objects to store
        """
        if not envelopes:
            return
        
        # Prepare batch data
        batch_data = []
        for envelope in envelopes:
            ingested_at = datetime.fromtimestamp(envelope.ingested_at / 1000.0)
            batch_data.append((
                envelope.prov_id,
                envelope.origin_id,
                json.dumps(envelope.parent_prov_ids),
                envelope.producer_id,
                envelope.producer_hash,
                envelope.schema_id,
                envelope.schema_hash,
                json.dumps(envelope.transform_ops),
                envelope.chain_hash,
                envelope.topic,
                envelope.offset,
                envelope.partition,
                ingested_at,
            ))
        
        self.conn.executemany("""
            INSERT OR IGNORE INTO lineage_events (
                prov_id, origin_id, parent_prov_ids, producer_id, producer_hash,
                schema_id, schema_hash, transform_ops, chain_hash, topic,
                kafka_offset, kafka_partition, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
    
    def upsert_schema_snapshot(self, schema_id: str, schema_hash: str, schema_json: str):
        """
        Insert new schema snapshot or update last_seen_at if already exists.
        
        Args:
            schema_id: Schema identifier
            schema_hash: SHA256 hash of schema
            schema_json: JSON string of schema
        """
        now = datetime.utcnow()
        
        # Try to update existing record
        result = self.conn.execute("""
            UPDATE schema_snapshots 
            SET last_seen_at = ? 
            WHERE schema_id = ? AND schema_hash = ?
        """, (now, schema_id, schema_hash))
        
        # If no rows updated, insert new record
        if result.fetchone()[0] == 0:
            self.conn.execute("""
                INSERT INTO schema_snapshots (
                    schema_id, schema_hash, schema_json, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?)
            """, (schema_id, schema_hash, schema_json, now, now))
    
    def insert_drift_event(self, drift_event: dict):
        """
        Insert a schema drift detection event.
        
        Args:
            drift_event: Dictionary with drift event data
        """
        self.conn.execute("""
            INSERT INTO schema_drift_events (
                drift_id, schema_id, old_schema_hash, new_schema_hash,
                old_schema_json, new_schema_json, severity, detected_at,
                affected_producer_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            drift_event["drift_id"],
            drift_event["schema_id"],
            drift_event["old_schema_hash"],
            drift_event["new_schema_hash"],
            drift_event.get("old_schema_json"),
            drift_event.get("new_schema_json"),
            drift_event["severity"],
            datetime.fromisoformat(drift_event["detected_at"].replace("Z", "+00:00")),
            drift_event.get("affected_producer_id"),
        ))
    
    def save_dag_snapshot(self, graph_json: str, node_count: int, edge_count: int) -> str:
        """
        Save a DAG snapshot and return its ID.
        
        Args:
            graph_json: JSON-serialized NetworkX graph
            node_count: Number of nodes in graph
            edge_count: Number of edges in graph
        
        Returns:
            snapshot_id (UUID)
        """
        import uuid
        snapshot_id = str(uuid.uuid4())
        snapshot_at = datetime.utcnow()
        
        self.conn.execute("""
            INSERT INTO dag_snapshots (
                snapshot_id, snapshot_at, graph_json, node_count, edge_count
            ) VALUES (?, ?, ?, ?, ?)
        """, (snapshot_id, snapshot_at, graph_json, node_count, edge_count))
        
        return snapshot_id
    
    def get_dag_snapshot_at(self, timestamp: str) -> Optional[dict]:
        """
        Get the closest DAG snapshot at or before the given timestamp.
        
        Args:
            timestamp: ISO8601 timestamp string
        
        Returns:
            Dictionary with snapshot data, or None if no snapshot found
        """
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        
        result = self.conn.execute("""
            SELECT snapshot_id, snapshot_at, graph_json, node_count, edge_count
            FROM dag_snapshots
            WHERE snapshot_at <= ?
            ORDER BY snapshot_at DESC
            LIMIT 1
        """, (dt,)).fetchone()
        
        if not result:
            return None
        
        return {
            "snapshot_id": result[0],
            "snapshot_at": result[1].isoformat() + "Z",
            "graph_json": result[2],
            "node_count": result[3],
            "edge_count": result[4],
        }
    
    def query_lineage_events(self, filters: dict) -> list[dict]:
        """
        Flexible query for lineage events with filtering.
        
        Args:
            filters: Dictionary of filters:
                - origin_id: Filter by origin_id
                - producer_id: Filter by producer_id
                - schema_id: Filter by schema_id
                - topic: Filter by topic
                - ingested_after: ISO8601 timestamp (inclusive)
                - ingested_before: ISO8601 timestamp (exclusive)
        
        Returns:
            List of dictionaries representing lineage events
        """
        conditions = []
        params = []
        
        if "origin_id" in filters:
            conditions.append("origin_id = ?")
            params.append(filters["origin_id"])
        
        if "producer_id" in filters:
            conditions.append("producer_id = ?")
            params.append(filters["producer_id"])
        
        if "schema_id" in filters:
            conditions.append("schema_id = ?")
            params.append(filters["schema_id"])
        
        if "topic" in filters:
            conditions.append("topic = ?")
            params.append(filters["topic"])
        
        if "ingested_after" in filters:
            dt = datetime.fromisoformat(filters["ingested_after"].replace("Z", "+00:00"))
            conditions.append("ingested_at >= ?")
            params.append(dt)
        
        if "ingested_before" in filters:
            dt = datetime.fromisoformat(filters["ingested_before"].replace("Z", "+00:00"))
            conditions.append("ingested_at < ?")
            params.append(dt)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT * FROM lineage_events WHERE {where_clause} ORDER BY ingested_at DESC"
        
        results = self.conn.execute(query, params).fetchall()
        
        # Convert to list of dicts
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in results]
    
    def get_impact_events(
        self,
        producer_id: str,
        corrupted_from: str,
        corrupted_until: str
    ) -> list[dict]:
        """
        Get all lineage events from a producer within a time range.
        
        These are the "blast" events that potentially caused downstream corruption.
        
        Args:
            producer_id: Producer identifier
            corrupted_from: ISO8601 timestamp (inclusive)
            corrupted_until: ISO8601 timestamp (exclusive)
        
        Returns:
            List of dictionaries representing lineage events
        """
        from_dt = datetime.fromisoformat(corrupted_from.replace("Z", "+00:00"))
        until_dt = datetime.fromisoformat(corrupted_until.replace("Z", "+00:00"))
        
        results = self.conn.execute("""
            SELECT *
            FROM lineage_events
            WHERE producer_id = ?
              AND ingested_at >= ?
              AND ingested_at < ?
            ORDER BY ingested_at ASC
        """, (producer_id, from_dt, until_dt)).fetchall()
        
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in results]
    
    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
