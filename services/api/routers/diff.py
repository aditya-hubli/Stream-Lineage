"""
Diff Router

Endpoints for comparing DAG snapshots and detecting changes over time.
"""

import json
from datetime import datetime
from typing import List, Optional

import networkx as nx
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
class EdgeChange(BaseModel):
    """Edge that was added or removed."""
    source: str
    target: str


class SnapshotInfo(BaseModel):
    """Snapshot identification."""
    snapshot_id: str
    snapshot_at: str


class TransformChange(BaseModel):
    """Per-node change in transform operations between snapshots."""
    node: str
    old_ops: List[str]
    new_ops: List[str]


class SchemaChange(BaseModel):
    """Schema version change detected on an edge between snapshots."""
    edge_source: str
    edge_target: str
    schemas_added: List[str]   # schema_ids present in B but not A
    schemas_removed: List[str] # schema_ids present in A but not B
    breaking: Optional[bool] = None  # True if any drift event marked BREAKING


class DiffResponse(BaseModel):
    """DAG snapshot diff results."""
    snapshot_a: SnapshotInfo
    snapshot_b: SnapshotInfo
    nodes_added: List[str]
    nodes_removed: List[str]
    edges_added: List[EdgeChange]
    edges_removed: List[EdgeChange]
    transform_changes: List[TransformChange]
    schema_changes: List[SchemaChange]
    node_delta: int
    edge_delta: int
    computed_at: str


@router.get("/snapshots")
async def list_snapshots(storage: LineageStorage = Depends(get_storage)):
    """List available DAG snapshots for the diff picker UI."""
    try:
        rows = storage.conn.execute("""
            SELECT snapshot_id, snapshot_at, node_count, edge_count
            FROM dag_snapshots
            ORDER BY snapshot_at DESC
            LIMIT 50
        """).fetchall()
        return [
            {
                "snapshot_id": str(r[0]),
                "snapshot_at": r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1]),
                "node_count": r[2],
                "edge_count": r[3],
            }
            for r in rows
        ]
    except Exception:
        return []


@router.get("", response_model=DiffResponse)
async def diff_snapshots(
    snapshot_a: str,
    snapshot_b: str,
    storage: LineageStorage = Depends(get_storage),
    graph_builder: LineageGraphBuilder = Depends(get_graph_builder)
):
    """
    Compare two DAG snapshots and compute the differences.
    
    Shows which nodes and edges were added or removed between two points in time,
    enabling evolution analysis and change detection.
    
    Args:
        snapshot_a: ISO8601 timestamp for first snapshot
        snapshot_b: ISO8601 timestamp for second snapshot
    
    Returns:
        Diff results showing added/removed nodes and edges
    
    Raises:
        HTTPException: 404 if either snapshot not found
    """
    # Fetch snapshot A
    snap_a = storage.get_dag_snapshot_at(snapshot_a)
    if not snap_a:
        raise HTTPException(
            status_code=404,
            detail=f"No snapshot found at or before {snapshot_a}"
        )
    
    # Fetch snapshot B
    snap_b = storage.get_dag_snapshot_at(snapshot_b)
    if not snap_b:
        raise HTTPException(
            status_code=404,
            detail=f"No snapshot found at or before {snapshot_b}"
        )
    
    # Compute structural diff using graph builder
    diff = graph_builder.diff_snapshots(
        snap_a['graph_json'],
        snap_b['graph_json']
    )

    edges_added = [EdgeChange(**edge) for edge in diff['edges_added']]
    edges_removed = [EdgeChange(**edge) for edge in diff['edges_removed']]

    # --- transform_changes: per-node op delta for nodes present in both snapshots ---
    graph_a = nx.node_link_graph(json.loads(snap_a['graph_json']), edges='links')
    graph_b = nx.node_link_graph(json.loads(snap_b['graph_json']), edges='links')
    common_nodes = set(graph_a.nodes()) & set(graph_b.nodes())

    transform_changes = []
    for node_id in common_nodes:
        # Query most recent transform_ops for this producer at each snapshot time
        ops_at_a = _get_latest_transform_ops(storage, node_id, snap_a['snapshot_at'])
        ops_at_b = _get_latest_transform_ops(storage, node_id, snap_b['snapshot_at'])
        if ops_at_a != ops_at_b:
            transform_changes.append(TransformChange(
                node=node_id,
                old_ops=ops_at_a,
                new_ops=ops_at_b,
            ))

    # --- schema_changes: schema_ids delta on common edges ---
    schema_changes = []
    common_edges = set(graph_a.edges()) & set(graph_b.edges())
    for src, tgt in common_edges:
        ids_a = set(graph_a.edges[src, tgt].get('schema_ids', []))
        ids_b = set(graph_b.edges[src, tgt].get('schema_ids', []))
        added = list(ids_b - ids_a)
        removed = list(ids_a - ids_b)
        if added or removed:
            # Check if any relevant drift events were BREAKING
            breaking = _check_breaking_drift(storage, added + removed,
                                             snap_a['snapshot_at'], snap_b['snapshot_at'])
            schema_changes.append(SchemaChange(
                edge_source=src,
                edge_target=tgt,
                schemas_added=added,
                schemas_removed=removed,
                breaking=breaking,
            ))

    return DiffResponse(
        snapshot_a=SnapshotInfo(
            snapshot_id=snap_a['snapshot_id'],
            snapshot_at=snap_a['snapshot_at']
        ),
        snapshot_b=SnapshotInfo(
            snapshot_id=snap_b['snapshot_id'],
            snapshot_at=snap_b['snapshot_at']
        ),
        nodes_added=diff['nodes_added'],
        nodes_removed=diff['nodes_removed'],
        edges_added=edges_added,
        edges_removed=edges_removed,
        transform_changes=transform_changes,
        schema_changes=schema_changes,
        node_delta=diff['node_delta'],
        edge_delta=diff['edge_delta'],
        computed_at=datetime.utcnow().isoformat() + 'Z',
    )


def _get_latest_transform_ops(storage, producer_id: str, before_iso: str) -> list:
    """Return the most recent transform_ops list for a producer at or before a timestamp."""
    try:
        dt = datetime.fromisoformat(before_iso.replace('Z', '+00:00'))
        row = storage.conn.execute("""
            SELECT transform_ops FROM lineage_events
            WHERE producer_id = ? AND ingested_at <= ?
            ORDER BY ingested_at DESC LIMIT 1
        """, (producer_id, dt)).fetchone()
        if row and row[0]:
            return json.loads(row[0]) if isinstance(row[0], str) else list(row[0])
    except Exception:
        pass
    return []


def _check_breaking_drift(storage, schema_ids: list, from_iso: str, until_iso: str) -> Optional[bool]:
    """Return True if any BREAKING drift event exists for these schema_ids in the window."""
    if not schema_ids:
        return None
    try:
        from_dt = datetime.fromisoformat(from_iso.replace('Z', '+00:00'))
        until_dt = datetime.fromisoformat(until_iso.replace('Z', '+00:00'))
        placeholders = ','.join(['?' for _ in schema_ids])
        row = storage.conn.execute(
            f"""SELECT COUNT(*) FROM schema_drift_events
                WHERE schema_id IN ({placeholders})
                AND detected_at BETWEEN ? AND ?
                AND severity = 'BREAKING'""",
            schema_ids + [from_dt, until_dt]
        ).fetchone()
        if row:
            return row[0] > 0
    except Exception:
        pass
    return None
