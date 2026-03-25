"""
Impact Analysis Router

Endpoints for computing blast radius and downstream impact of corrupted events.
"""

import time
from datetime import datetime
from typing import List, Dict

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_graph_builder
from graph_builder import LineageGraphBuilder


router = APIRouter()


# Pydantic models
class ImpactRequest(BaseModel):
    """Request for impact analysis."""
    producer_id: str
    corrupted_from: str  # ISO8601 timestamp
    corrupted_until: str  # ISO8601 timestamp


class AffectedNode(BaseModel):
    """Downstream node affected by corruption."""
    node: str
    affected_events: int
    topic: str
    distance_from_source: int


class ImpactResponse(BaseModel):
    """Impact analysis results."""
    producer_id: str
    corrupted_from: str
    corrupted_until: str
    total_affected_events: int
    affected_downstream_nodes: List[AffectedNode]
    replay_kafka_offsets: Dict[str, List[int]]  # topic -> [min_offset, max_offset]
    computed_at: str


@router.post("", response_model=ImpactResponse)
async def compute_impact(
    request: ImpactRequest,
    response: Response,
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder)
):
    """
    Compute the blast radius of corrupted events from a producer.
    
    Performs graph traversal to find all downstream nodes affected by
    corrupted events in the specified time window, and provides Kafka
    offset ranges for replaying the affected data.
    
    Args:
        request: Impact analysis request with producer and time window
    
    Returns:
        Impact analysis results with affected nodes and replay offsets
    
    Raises:
        HTTPException: 404 if producer_id not found in graph
    """
    start_time = time.time()
    
    try:
        # Compute impact using graph builder
        result = graph_builder.compute_impact(
            producer_id=request.producer_id,
            corrupted_from=request.corrupted_from,
            corrupted_until=request.corrupted_until,
        )
        
        # Convert to response model
        affected_nodes = [
            AffectedNode(**node) for node in result['affected_downstream_nodes']
        ]
        
        impact_response = ImpactResponse(
            producer_id=result['producer_id'],
            corrupted_from=result['corrupted_from'],
            corrupted_until=result['corrupted_until'],
            total_affected_events=result['total_affected_events'],
            affected_downstream_nodes=affected_nodes,
            replay_kafka_offsets=result['replay_kafka_offsets'],
            computed_at=result['computed_at'],
        )
        
        # Add computation time to response header
        computation_time_ms = int((time.time() - start_time) * 1000)
        response.headers["X-Computation-Time-Ms"] = str(computation_time_ms)
        
        return impact_response
    
    except ValueError as e:
        # Producer not found in graph
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    except Exception as e:
        # Other errors
        raise HTTPException(
            status_code=500,
            detail=f"Impact analysis failed: {str(e)}"
        )
