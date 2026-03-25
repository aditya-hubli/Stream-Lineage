"""
Trace Router

Endpoint for tracing the full lineage of a single event — from root ancestor
through all transformations to leaf descendants.
"""

import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_storage
from storage import LineageStorage


router = APIRouter()


# --- Pydantic models ---

class TraceEvent(BaseModel):
    """A single event in the lineage trace."""
    prov_id: str
    origin_id: str
    parent_prov_ids: List[str]
    producer_id: str
    schema_id: str
    topic: str
    partition: int
    offset: int
    transform_ops: List[str]
    chain_hash: str
    ingested_at: str
    depth: int  # 0 = the queried event, negative = ancestors, positive = descendants


class TraceResponse(BaseModel):
    """Full lineage trace for a single event."""
    prov_id: str
    origin_id: str
    ancestors: List[TraceEvent]
    event: TraceEvent
    descendants: List[TraceEvent]
    total_depth: int  # max ancestor depth + max descendant depth
    trace_computed_at: str


# --- Helpers ---

def _row_to_dict(row, columns) -> dict:
    return dict(zip(columns, row))


def _fetch_event(storage: LineageStorage, prov_id: str) -> Optional[dict]:
    """Fetch a single lineage event by prov_id."""
    result = storage.conn.execute(
        "SELECT * FROM lineage_events WHERE prov_id = ?", (prov_id,)
    ).fetchone()
    if not result:
        return None
    columns = [desc[0] for desc in storage.conn.description]
    return _row_to_dict(result, columns)


def _fetch_children(storage: LineageStorage, prov_id: str) -> list[dict]:
    """Fetch events that list prov_id in their parent_prov_ids."""
    # parent_prov_ids is stored as JSON array string, so use LIKE for matching
    results = storage.conn.execute(
        "SELECT * FROM lineage_events WHERE parent_prov_ids LIKE ?",
        (f"%{prov_id}%",)
    ).fetchall()
    if not results:
        return []
    columns = [desc[0] for desc in storage.conn.description]
    # Filter for exact prov_id match within the JSON array (not substring)
    children = []
    for row in results:
        d = _row_to_dict(row, columns)
        parent_ids = json.loads(d["parent_prov_ids"]) if d["parent_prov_ids"] else []
        if prov_id in parent_ids:
            children.append(d)
    return children


def _event_to_trace(event: dict, depth: int) -> TraceEvent:
    """Convert a storage row dict to a TraceEvent."""
    parent_prov_ids = event.get("parent_prov_ids", "[]")
    if isinstance(parent_prov_ids, str):
        parent_prov_ids = json.loads(parent_prov_ids)

    transform_ops = event.get("transform_ops", "[]")
    if isinstance(transform_ops, str):
        transform_ops = json.loads(transform_ops)

    ingested_at = event.get("ingested_at", "")
    if hasattr(ingested_at, "isoformat"):
        ingested_at = ingested_at.isoformat() + "Z"

    return TraceEvent(
        prov_id=event["prov_id"],
        origin_id=event["origin_id"],
        parent_prov_ids=parent_prov_ids,
        producer_id=event["producer_id"],
        schema_id=event["schema_id"],
        topic=event["topic"],
        partition=event.get("kafka_partition", -1),
        offset=event.get("kafka_offset", -1),
        transform_ops=transform_ops,
        chain_hash=event["chain_hash"],
        ingested_at=ingested_at,
        depth=depth,
    )


# --- Endpoint ---

@router.get("/{prov_id}", response_model=TraceResponse)
async def trace_event(
    prov_id: str,
    max_depth: int = Query(default=50, ge=1, le=500, description="Max traversal depth"),
    storage: LineageStorage = Depends(get_storage),
):
    """
    Trace the full lineage of a single event.

    Walks backward through parent_prov_ids to find all ancestors (root events),
    then walks forward to find all descendants. Returns the complete provenance
    chain for the event.

    Args:
        prov_id: Provenance ID of the event to trace
        max_depth: Maximum traversal depth in each direction (default 50)

    Returns:
        Full trace with ancestors, the event itself, and descendants
    """
    # Fetch the target event
    event = _fetch_event(storage, prov_id)
    if not event:
        raise HTTPException(status_code=404, detail=f"Event '{prov_id}' not found")

    # --- Walk ancestors (backward through parent_prov_ids) ---
    ancestors = []
    visited = {prov_id}
    queue = []  # (prov_id, depth)

    parent_ids = json.loads(event["parent_prov_ids"]) if event["parent_prov_ids"] else []
    for pid in parent_ids:
        if pid not in visited:
            queue.append((pid, -1))
            visited.add(pid)

    while queue:
        current_id, depth = queue.pop(0)
        if abs(depth) > max_depth:
            continue

        ancestor = _fetch_event(storage, current_id)
        if not ancestor:
            continue

        ancestors.append(_event_to_trace(ancestor, depth))

        # Continue walking parents
        ancestor_parents = json.loads(ancestor["parent_prov_ids"]) if ancestor["parent_prov_ids"] else []
        for pid in ancestor_parents:
            if pid not in visited:
                queue.append((pid, depth - 1))
                visited.add(pid)

    # Sort ancestors by depth (most distant ancestor first)
    ancestors.sort(key=lambda e: e.depth)

    # --- Walk descendants (forward through children) ---
    descendants = []
    visited_desc = {prov_id}
    desc_queue = [(prov_id, 1)]

    while desc_queue:
        current_id, depth = desc_queue.pop(0)
        if depth > max_depth:
            continue

        children = _fetch_children(storage, current_id)
        for child in children:
            child_prov_id = child["prov_id"]
            if child_prov_id not in visited_desc:
                visited_desc.add(child_prov_id)
                descendants.append(_event_to_trace(child, depth))
                desc_queue.append((child_prov_id, depth + 1))

    # Sort descendants by depth (closest first)
    descendants.sort(key=lambda e: e.depth)

    # Compute total depth
    min_ancestor_depth = min((a.depth for a in ancestors), default=0)
    max_descendant_depth = max((d.depth for d in descendants), default=0)
    total_depth = max_descendant_depth - min_ancestor_depth

    return TraceResponse(
        prov_id=prov_id,
        origin_id=event["origin_id"],
        ancestors=ancestors,
        event=_event_to_trace(event, 0),
        descendants=descendants,
        total_depth=total_depth,
        trace_computed_at=datetime.utcnow().isoformat() + "Z",
    )
