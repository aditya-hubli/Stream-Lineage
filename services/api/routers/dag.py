"""
DAG Router

Endpoints for accessing and querying the lineage DAG (Directed Acyclic Graph).
"""

import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_storage, get_graph_builder
from storage import LineageStorage
from graph_builder import LineageGraphBuilder


router = APIRouter()


# Pydantic models
class NodeData(BaseModel):
    """DAG node representing a producer/processor."""
    id: str
    producer_id: str
    event_count: int
    topics: List[str]
    first_seen_at: str
    last_seen_at: str


class EdgeData(BaseModel):
    """DAG edge representing data flow between producers."""
    source: str
    target: str
    event_count: int
    schema_ids: List[str]
    last_seen_at: str


class DagResponse(BaseModel):
    """Complete DAG response."""
    nodes: List[NodeData]
    edges: List[EdgeData]
    node_count: int
    edge_count: int
    generated_at: str


class SnapshotMetadata(BaseModel):
    """DAG snapshot metadata."""
    snapshot_id: str
    snapshot_at: str
    node_count: int
    edge_count: int


class DagSnapshotResponse(DagResponse):
    """DAG snapshot with metadata."""
    snapshot_id: str
    snapshot_at: str


@router.get("", response_model=DagResponse)
async def get_current_dag(
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder)
):
    """
    Get the current live lineage DAG.
    
    Returns the complete directed acyclic graph showing all producers and
    the data flow between them.
    
    Returns:
        Current DAG with nodes and edges
    """
    # Get graph JSON from builder
    graph_json_str = graph_builder.get_graph_json()
    graph_data = json.loads(graph_json_str)
    
    # Extract nodes and edges
    nodes = []
    for node in graph_data.get('nodes', []):
        nodes.append(NodeData(
            id=node['id'],
            producer_id=node.get('producer_id', node['id']),
            event_count=node.get('event_count', 0),
            topics=node.get('topics', []),
            first_seen_at=node.get('first_seen_at', ''),
            last_seen_at=node.get('last_seen_at', ''),
        ))
    
    edges = []
    for link in graph_data.get('links', []):
        edges.append(EdgeData(
            source=link['source'],
            target=link['target'],
            event_count=link.get('event_count', 0),
            schema_ids=link.get('schema_ids', []),
            last_seen_at=link.get('last_seen_at', ''),
        ))
    
    return DagResponse(
        nodes=nodes,
        edges=edges,
        node_count=len(nodes),
        edge_count=len(edges),
        generated_at=datetime.utcnow().isoformat() + 'Z',
    )


@router.get("/snapshot", response_model=DagSnapshotResponse)
async def get_dag_snapshot(
    at: str,
    storage: LineageStorage = Depends(get_storage)
):
    """
    Get a historical DAG snapshot at a specific point in time.
    
    Args:
        at: ISO8601 timestamp (returns snapshot at or before this time)
    
    Returns:
        DAG snapshot with metadata
    
    Raises:
        HTTPException: 404 if no snapshot found
    """
    # Query storage for snapshot
    snapshot = storage.get_dag_snapshot_at(at)
    
    if not snapshot:
        raise HTTPException(
            status_code=404,
            detail=f"No snapshot found at or before {at}"
        )
    
    # Parse graph JSON
    graph_data = json.loads(snapshot['graph_json'])
    
    # Extract nodes and edges
    nodes = []
    for node in graph_data.get('nodes', []):
        nodes.append(NodeData(
            id=node['id'],
            producer_id=node.get('producer_id', node['id']),
            event_count=node.get('event_count', 0),
            topics=node.get('topics', []),
            first_seen_at=node.get('first_seen_at', ''),
            last_seen_at=node.get('last_seen_at', ''),
        ))
    
    edges = []
    for link in graph_data.get('links', []):
        edges.append(EdgeData(
            source=link['source'],
            target=link['target'],
            event_count=link.get('event_count', 0),
            schema_ids=link.get('schema_ids', []),
            last_seen_at=link.get('last_seen_at', ''),
        ))
    
    return DagSnapshotResponse(
        nodes=nodes,
        edges=edges,
        node_count=snapshot['node_count'],
        edge_count=snapshot['edge_count'],
        snapshot_id=snapshot['snapshot_id'],
        snapshot_at=snapshot['snapshot_at'],
        generated_at=datetime.utcnow().isoformat() + 'Z',
    )


@router.get("/snapshots", response_model=List[SnapshotMetadata])
async def list_snapshots(
    limit: int = 100,
    storage: LineageStorage = Depends(get_storage)
):
    """
    List available DAG snapshots (metadata only).
    
    Args:
        limit: Maximum number of snapshots to return (default 100)
    
    Returns:
        List of snapshot metadata, sorted by snapshot_at descending
    """
    results = storage.conn.execute("""
        SELECT snapshot_id, snapshot_at, node_count, edge_count
        FROM dag_snapshots
        ORDER BY snapshot_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    
    snapshots = []
    for row in results:
        snapshots.append(SnapshotMetadata(
            snapshot_id=row[0],
            snapshot_at=row[1].isoformat() + 'Z' if hasattr(row[1], 'isoformat') else row[1],
            node_count=row[2],
            edge_count=row[3],
        ))
    
    return snapshots
