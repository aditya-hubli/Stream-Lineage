"""
Verification Router

Endpoints for verifying chain hash integrity of lineage events.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from streamlineage.envelope import ProvenanceEnvelope, verify_chain_hash
from dependencies import get_storage
from storage import LineageStorage


router = APIRouter()


# Pydantic models
class VerifyRequest(BaseModel):
    """Request to verify an event's chain hash."""
    payload_bytes_hex: str  # Hex-encoded original payload


class VerifyResponse(BaseModel):
    """Verification result."""
    prov_id: str
    verified: bool
    stored_chain_hash: str
    computed_chain_hash: str
    verdict: str  # "VALID", "TAMPERED", or "PARENTS_MISSING"


@router.post("/{prov_id}", response_model=VerifyResponse)
async def verify_event(
    prov_id: str,
    request: VerifyRequest,
    storage: LineageStorage = Depends(get_storage)
):
    """
    Verify the chain hash integrity of a lineage event.
    
    Reconstructs the chain hash from the stored envelope and parent envelopes,
    then compares it to the original chain hash to detect tampering.
    
    Args:
        prov_id: Provenance ID of the event to verify
        request: Verification request with original payload (hex-encoded)
    
    Returns:
        Verification result with verdict
    
    Raises:
        HTTPException: 404 if event not found
    """
    # Fetch lineage event from storage
    results = storage.conn.execute("""
        SELECT 
            prov_id, origin_id, parent_prov_ids, producer_id, producer_hash,
            schema_id, schema_hash, transform_ops, chain_hash, topic,
            kafka_offset, kafka_partition, ingested_at
        FROM lineage_events
        WHERE prov_id = ?
    """, (prov_id,)).fetchone()
    
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Event with prov_id '{prov_id}' not found"
        )
    
    # Reconstruct ProvenanceEnvelope
    import json
    envelope = ProvenanceEnvelope(
        prov_id=results[0],
        origin_id=results[1],
        parent_prov_ids=json.loads(results[2]),
        producer_id=results[3],
        producer_hash=results[4],
        schema_id=results[5],
        schema_hash=results[6],
        transform_ops=json.loads(results[7]),
        chain_hash=results[8],
        topic=results[9],
        offset=results[10],
        partition=results[11],
        ingested_at=int(results[12].timestamp() * 1000),
    )
    
    # Decode payload from hex
    try:
        payload_bytes = bytes.fromhex(request.payload_bytes_hex)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid hex string for payload_bytes_hex"
        )
    
    # Fetch parent envelopes if needed
    parent_envelopes = None
    if envelope.parent_prov_ids:
        parent_envelopes = []
        for parent_prov_id in envelope.parent_prov_ids:
            parent_results = storage.conn.execute("""
                SELECT 
                    prov_id, origin_id, parent_prov_ids, producer_id, producer_hash,
                    schema_id, schema_hash, transform_ops, chain_hash, topic,
                    kafka_offset, kafka_partition, ingested_at
                FROM lineage_events
                WHERE prov_id = ?
            """, (parent_prov_id,)).fetchone()
            
            if not parent_results:
                # Parent not found
                return VerifyResponse(
                    prov_id=prov_id,
                    verified=False,
                    stored_chain_hash=envelope.chain_hash,
                    computed_chain_hash="N/A",
                    verdict="PARENTS_MISSING",
                )
            
            parent_envelope = ProvenanceEnvelope(
                prov_id=parent_results[0],
                origin_id=parent_results[1],
                parent_prov_ids=json.loads(parent_results[2]),
                producer_id=parent_results[3],
                producer_hash=parent_results[4],
                schema_id=parent_results[5],
                schema_hash=parent_results[6],
                transform_ops=json.loads(parent_results[7]),
                chain_hash=parent_results[8],
                topic=parent_results[9],
                offset=parent_results[10],
                partition=parent_results[11],
                ingested_at=int(parent_results[12].timestamp() * 1000),
            )
            parent_envelopes.append(parent_envelope)
    
    # Verify chain hash
    is_valid = verify_chain_hash(envelope, payload_bytes, parent_envelopes)
    
    # Recompute chain hash for comparison
    import hashlib
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()
    
    if not envelope.parent_prov_ids:
        # Root event
        chain_input = f"{envelope.topic}{envelope.partition}{payload_hash}"
        computed_hash = hashlib.sha256(chain_input.encode('utf-8')).hexdigest()
    else:
        # Child event
        parent_hashes = sorted([p.chain_hash for p in parent_envelopes])
        parent_hashes_joined = "|".join(parent_hashes)
        chain_input = f"{parent_hashes_joined}{payload_hash}"
        computed_hash = hashlib.sha256(chain_input.encode('utf-8')).hexdigest()
    
    return VerifyResponse(
        prov_id=prov_id,
        verified=is_valid,
        stored_chain_hash=envelope.chain_hash,
        computed_chain_hash=computed_hash,
        verdict="VALID" if is_valid else "TAMPERED",
    )
