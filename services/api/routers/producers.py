"""
Producers Router

Endpoints for listing and querying producer information.
"""

import os
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_graph_builder
from graph_builder import LineageGraphBuilder


router = APIRouter()


# Health thresholds (seconds). Configurable via env vars.
_ACTIVE_THRESHOLD = int(os.getenv('PRODUCER_ACTIVE_SECONDS', '60'))
_STALE_THRESHOLD = int(os.getenv('PRODUCER_STALE_SECONDS', '300'))


def _compute_health(last_seen_at: str) -> str:
    """Return 'active', 'stale', or 'dead' based on last_seen_at ISO string."""
    if not last_seen_at:
        return 'dead'
    try:
        last_seen = datetime.fromisoformat(last_seen_at.replace('Z', '+00:00'))
        age_seconds = (datetime.now(timezone.utc) - last_seen).total_seconds()
        if age_seconds <= _ACTIVE_THRESHOLD:
            return 'active'
        if age_seconds <= _STALE_THRESHOLD:
            return 'stale'
        return 'dead'
    except (ValueError, TypeError):
        return 'dead'


# Pydantic models
class ProducerInfo(BaseModel):
    """Producer/processor information."""
    producer_id: str
    event_count: int
    topics: List[str]
    first_seen_at: str
    last_seen_at: str
    last_schema_id: str
    status: str  # 'active' | 'stale' | 'dead'


@router.get("", response_model=List[ProducerInfo])
async def list_producers(
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder)
):
    """
    List all producer nodes in the current graph.
    
    Returns information about all producers/processors that have been
    observed in the lineage graph, sorted by event count descending.
    
    Returns:
        List of producer information
    """
    with graph_builder.lock:
        producers = []
        
        for node_id, node_data in graph_builder.graph.nodes(data=True):
            # Get the most recent schema_id for this producer from storage
            last_schema_id = "unknown"
            try:
                result = graph_builder.storage.conn.execute("""
                    SELECT schema_id FROM lineage_events
                    WHERE producer_id = ?
                    ORDER BY ingested_at DESC
                    LIMIT 1
                """, (node_id,)).fetchone()
                
                if result:
                    last_schema_id = result[0]
            except Exception:
                pass
            
            last_seen_at = node_data.get('last_seen_at', '')
            producers.append(ProducerInfo(
                producer_id=node_data.get('producer_id', node_id),
                event_count=node_data.get('event_count', 0),
                topics=list(node_data.get('topics', set())),
                first_seen_at=node_data.get('first_seen_at', ''),
                last_seen_at=last_seen_at,
                last_schema_id=last_schema_id,
                status=_compute_health(last_seen_at),
            ))
        
        # Sort by event_count descending
        producers.sort(key=lambda p: p.event_count, reverse=True)
    
    return producers


@router.get("/{producer_id}/latest_prov_id")
async def get_latest_prov_id(
    producer_id: str,
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder),
):
    """Return the most recent prov_id for a producer (seeds the Trace panel)."""
    try:
        row = graph_builder.storage.conn.execute(
            "SELECT prov_id FROM lineage_events WHERE producer_id = ? ORDER BY ingested_at DESC LIMIT 1",
            (producer_id,),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        raise HTTPException(status_code=404, detail=f"No events for producer {producer_id}")
    return {"producer_id": producer_id, "prov_id": row[0]}
