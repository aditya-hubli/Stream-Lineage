"""
StreamLineage Provenance Envelope Data Model

This module defines the core data model for provenance tracking in StreamLineage.
Every Kafka event carries a provenance envelope as message headers, enabling
end-to-end lineage tracking across the streaming pipeline.
"""

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ProvenanceEnvelope:
    """
    Core provenance envelope that tracks lineage metadata for streaming events.
    
    Attributes:
        prov_id: Unique identifier for this specific event instance (UUID4)
        origin_id: ID of the original root event (same as prov_id for root events)
        parent_prov_ids: List of parent event IDs (empty for root events, supports fan-in)
        producer_id: Human-readable service identifier (e.g., "payment-service-v2.3")
        producer_hash: SHA256 hash of producer_id for integrity verification
        schema_id: Schema identifier (e.g., "payments.v4")
        schema_hash: SHA256 hash of the schema JSON string
        transform_ops: List of transformation operations applied (e.g., ["filter", "enrich"])
        chain_hash: Cryptographic chain hash linking this event to its lineage
        ingested_at: Unix timestamp in milliseconds when event was created
        topic: Kafka topic this event belongs to
        partition: Kafka partition number (-1 if unknown)
        offset: Kafka offset (-1 if unknown)
    """
    prov_id: str
    origin_id: str
    parent_prov_ids: list[str]
    producer_id: str
    producer_hash: str
    schema_id: str
    schema_hash: str
    transform_ops: list[str]
    chain_hash: str
    ingested_at: int
    topic: str
    partition: int = -1
    offset: int = -1


def _compute_sha256(data: str) -> str:
    """Compute SHA256 hash of a string and return hex digest."""
    return hashlib.sha256(data.encode('utf-8')).hexdigest()


def _compute_sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hash of bytes and return hex digest."""
    return hashlib.sha256(data).hexdigest()


def create_root_envelope(
    producer_id: str,
    schema_id: str,
    schema_json: str,
    topic: str,
    payload_bytes: bytes,
    partition: int = -1,
    offset: int = -1
) -> ProvenanceEnvelope:
    """
    Create a root provenance envelope for an original event.
    
    Root events are the starting point of a lineage chain and have no parent events.
    The prov_id and origin_id are identical for root events.
    
    Args:
        producer_id: Identifier for the producing service (e.g., "payment-service-v2.3")
        schema_id: Schema identifier (e.g., "payments.v4")
        schema_json: JSON string representation of the schema
        topic: Kafka topic name
        payload_bytes: Raw message payload bytes
        partition: Kafka partition number (default: -1)
        offset: Kafka offset (default: -1)
    
    Returns:
        ProvenanceEnvelope with populated fields for a root event
    """
    prov_id = str(uuid.uuid4())
    producer_hash = _compute_sha256(producer_id)
    schema_hash = _compute_sha256(schema_json)
    payload_hash = _compute_sha256_bytes(payload_bytes)
    
    # Chain hash for root events: SHA256(topic + partition + payload_hash)
    chain_input = f"{topic}{partition}{payload_hash}"
    chain_hash = _compute_sha256(chain_input)
    
    ingested_at = int(time.time() * 1000)  # Unix timestamp in milliseconds
    
    return ProvenanceEnvelope(
        prov_id=prov_id,
        origin_id=prov_id,  # For root events, origin_id == prov_id
        parent_prov_ids=[],
        producer_id=producer_id,
        producer_hash=producer_hash,
        schema_id=schema_id,
        schema_hash=schema_hash,
        transform_ops=[],
        chain_hash=chain_hash,
        ingested_at=ingested_at,
        topic=topic,
        partition=partition,
        offset=offset
    )


def create_child_envelope(
    parent_envelopes: list[ProvenanceEnvelope],
    producer_id: str,
    schema_id: str,
    schema_json: str,
    topic: str,
    payload_bytes: bytes,
    transform_ops: list[str],
    partition: int = -1,
    offset: int = -1
) -> ProvenanceEnvelope:
    """
    Create a child provenance envelope derived from one or more parent events.
    
    Child events maintain lineage by referencing their parent events and inherit
    the origin_id from their parents. Supports fan-in scenarios where multiple
    parent events are joined or merged.
    
    Args:
        parent_envelopes: List of parent ProvenanceEnvelope objects
        producer_id: Identifier for the producing service
        schema_id: Schema identifier
        schema_json: JSON string representation of the schema
        topic: Kafka topic name
        payload_bytes: Raw message payload bytes
        transform_ops: List of transformation operations applied
        partition: Kafka partition number (default: -1)
        offset: Kafka offset (default: -1)
    
    Returns:
        ProvenanceEnvelope with lineage linked to parent events
    """
    if not parent_envelopes:
        raise ValueError("Child envelope requires at least one parent envelope")
    
    prov_id = str(uuid.uuid4())
    # Inherit origin_id from the first parent (all parents should have the same origin)
    origin_id = parent_envelopes[0].origin_id
    parent_prov_ids = [p.prov_id for p in parent_envelopes]
    
    producer_hash = _compute_sha256(producer_id)
    schema_hash = _compute_sha256(schema_json)
    payload_hash = _compute_sha256_bytes(payload_bytes)
    
    # Chain hash for child events: SHA256(sorted_parent_chain_hashes + payload_hash)
    # Sorting ensures determinism regardless of join order
    parent_chain_hashes = [p.chain_hash for p in parent_envelopes]
    sorted_parent_hashes = sorted(parent_chain_hashes)
    parent_hashes_joined = "|".join(sorted_parent_hashes)
    chain_input = f"{parent_hashes_joined}{payload_hash}"
    chain_hash = _compute_sha256(chain_input)
    
    ingested_at = int(time.time() * 1000)
    
    return ProvenanceEnvelope(
        prov_id=prov_id,
        origin_id=origin_id,
        parent_prov_ids=parent_prov_ids,
        producer_id=producer_id,
        producer_hash=producer_hash,
        schema_id=schema_id,
        schema_hash=schema_hash,
        transform_ops=transform_ops,
        chain_hash=chain_hash,
        ingested_at=ingested_at,
        topic=topic,
        partition=partition,
        offset=offset
    )


def envelope_to_headers(envelope: ProvenanceEnvelope) -> dict[str, bytes]:
    """
    Convert a ProvenanceEnvelope to Kafka message headers.
    
    All header values are UTF-8 encoded bytes. Lists are JSON-encoded first.
    
    Args:
        envelope: ProvenanceEnvelope to convert
    
    Returns:
        Dictionary mapping header names to UTF-8 encoded byte values
    """
    return {
        "prov_id": envelope.prov_id.encode('utf-8'),
        "origin_id": envelope.origin_id.encode('utf-8'),
        "parent_prov_ids": json.dumps(envelope.parent_prov_ids).encode('utf-8'),
        "producer_id": envelope.producer_id.encode('utf-8'),
        "producer_hash": envelope.producer_hash.encode('utf-8'),
        "schema_id": envelope.schema_id.encode('utf-8'),
        "schema_hash": envelope.schema_hash.encode('utf-8'),
        "transform_ops": json.dumps(envelope.transform_ops).encode('utf-8'),
        "chain_hash": envelope.chain_hash.encode('utf-8'),
        "ingested_at": str(envelope.ingested_at).encode('utf-8'),
        "topic": envelope.topic.encode('utf-8'),
        "partition": str(envelope.partition).encode('utf-8'),
        "offset": str(envelope.offset).encode('utf-8'),
    }


def headers_to_envelope(headers: dict[str, bytes]) -> Optional[ProvenanceEnvelope]:
    """
    Convert Kafka message headers to a ProvenanceEnvelope.
    
    Inverse of envelope_to_headers. Returns None if required headers are missing,
    enabling graceful degradation for messages without provenance metadata.
    
    Args:
        headers: Dictionary of header names to byte values
    
    Returns:
        ProvenanceEnvelope if all required headers present, None otherwise
    """
    required_fields = [
        "prov_id", "origin_id", "parent_prov_ids", "producer_id",
        "producer_hash", "schema_id", "schema_hash", "transform_ops",
        "chain_hash", "ingested_at", "topic", "partition", "offset"
    ]
    
    # Check if all required headers are present
    if not all(field in headers for field in required_fields):
        return None
    
    try:
        return ProvenanceEnvelope(
            prov_id=headers["prov_id"].decode('utf-8'),
            origin_id=headers["origin_id"].decode('utf-8'),
            parent_prov_ids=json.loads(headers["parent_prov_ids"].decode('utf-8')),
            producer_id=headers["producer_id"].decode('utf-8'),
            producer_hash=headers["producer_hash"].decode('utf-8'),
            schema_id=headers["schema_id"].decode('utf-8'),
            schema_hash=headers["schema_hash"].decode('utf-8'),
            transform_ops=json.loads(headers["transform_ops"].decode('utf-8')),
            chain_hash=headers["chain_hash"].decode('utf-8'),
            ingested_at=int(headers["ingested_at"].decode('utf-8')),
            topic=headers["topic"].decode('utf-8'),
            partition=int(headers["partition"].decode('utf-8')),
            offset=int(headers["offset"].decode('utf-8')),
        )
    except (ValueError, KeyError, json.JSONDecodeError):
        return None


def verify_chain_hash(
    envelope: ProvenanceEnvelope,
    payload_bytes: bytes,
    parent_envelopes: Optional[list[ProvenanceEnvelope]] = None
) -> bool:
    """
    Verify the integrity of an envelope's chain hash.
    
    Recomputes the chain hash from the envelope's metadata and payload, then
    compares it to the stored chain_hash. This detects tampering or corruption.
    
    Args:
        envelope: ProvenanceEnvelope to verify
        payload_bytes: Raw message payload bytes
        parent_envelopes: List of parent envelopes (required for child events)
    
    Returns:
        True if chain_hash is valid, False if tampered or corrupted
    """
    payload_hash = _compute_sha256_bytes(payload_bytes)
    
    # For root events (no parents)
    if not envelope.parent_prov_ids:
        chain_input = f"{envelope.topic}{envelope.partition}{payload_hash}"
        expected_chain_hash = _compute_sha256(chain_input)
    else:
        # For child events
        if parent_envelopes is None:
            raise ValueError("parent_envelopes required to verify child event chain hash")
        
        parent_chain_hashes = [p.chain_hash for p in parent_envelopes]
        sorted_parent_hashes = sorted(parent_chain_hashes)
        parent_hashes_joined = "|".join(sorted_parent_hashes)
        chain_input = f"{parent_hashes_joined}{payload_hash}"
        expected_chain_hash = _compute_sha256(chain_input)
    
    return envelope.chain_hash == expected_chain_hash
