"""
Schema Router

Endpoints for schema drift detection and version tracking.
"""

from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lineage-ingester'))

from dependencies import get_storage
from storage import LineageStorage


router = APIRouter()


# Pydantic models
class FieldChange(BaseModel):
    """Field that was added, removed, or changed."""
    name: str
    type: Optional[str] = None
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    nullable: Optional[bool] = None


class SchemaDiff(BaseModel):
    """Schema difference details."""
    fields_added: List[FieldChange]
    fields_removed: List[FieldChange]
    fields_changed: List[FieldChange]


class DriftEvent(BaseModel):
    """Schema drift event."""
    drift_id: str
    schema_id: str
    severity: str
    detected_at: str
    old_schema_hash: str
    new_schema_hash: str
    affected_producer_id: Optional[str]
    diff: Optional[SchemaDiff] = None


class SchemaField(BaseModel):
    """Schema field definition."""
    name: str
    type: str


class SchemaFieldChange(BaseModel):
    """Schema field that changed type."""
    name: str
    old_type: str
    new_type: str


class SchemaDiffResponse(BaseModel):
    """Side-by-side schema diff result."""
    schema_id: str
    old_hash: str
    new_hash: str
    severity: str
    fields_added: List[SchemaField]
    fields_removed: List[SchemaField]
    fields_changed: List[SchemaFieldChange]
    all_fields_old: List[SchemaField]
    all_fields_new: List[SchemaField]


class SchemaVersion(BaseModel):
    """Schema version information."""
    schema_hash: str
    schema_json: str
    first_seen_at: str
    last_seen_at: str


@router.get("/drift", response_model=List[DriftEvent])
async def get_drift_events(
    since: Optional[str] = Query(None, description="ISO8601 timestamp (default: last 24h)"),
    schema_id: Optional[str] = Query(None, description="Filter by schema ID"),
    storage: LineageStorage = Depends(get_storage)
):
    """
    Get schema drift events.
    
    Returns all detected schema drift events, optionally filtered by
    time window and schema ID.
    
    Args:
        since: ISO8601 timestamp to filter events after (default: last 24h)
        schema_id: Optional schema ID filter
    
    Returns:
        List of drift events sorted by detected_at descending
    """
    import json
    
    # Default to last 24 hours if not specified
    if since is None:
        since_dt = datetime.utcnow() - timedelta(hours=24)
    else:
        since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
    
    # Build query
    if schema_id:
        query = """
            SELECT 
                drift_id, schema_id, old_schema_hash, new_schema_hash,
                old_schema_json, new_schema_json, severity, detected_at,
                affected_producer_id
            FROM schema_drift_events
            WHERE schema_id = ? AND detected_at >= ?
            ORDER BY detected_at DESC
        """
        params = (schema_id, since_dt)
    else:
        query = """
            SELECT 
                drift_id, schema_id, old_schema_hash, new_schema_hash,
                old_schema_json, new_schema_json, severity, detected_at,
                affected_producer_id
            FROM schema_drift_events
            WHERE detected_at >= ?
            ORDER BY detected_at DESC
        """
        params = (since_dt,)
    
    results = storage.conn.execute(query, params).fetchall()
    
    # Convert to response model
    drift_events = []
    for row in results:
        # Parse diff if we have both old and new schemas
        diff = None
        if row[4] and row[5]:  # old_schema_json and new_schema_json
            try:
                from schema_detector import SchemaDriftDetector
                detector = SchemaDriftDetector(storage)
                diff_data = detector.compute_schema_diff(row[4], row[5])
                
                diff = SchemaDiff(
                    fields_added=[FieldChange(**f) for f in diff_data['fields_added']],
                    fields_removed=[FieldChange(**f) for f in diff_data['fields_removed']],
                    fields_changed=[FieldChange(**f) for f in diff_data['fields_changed']],
                )
            except Exception as e:
                print(f"Error computing diff: {e}")
        
        drift_events.append(DriftEvent(
            drift_id=row[0],
            schema_id=row[1],
            old_schema_hash=row[2],
            new_schema_hash=row[3],
            severity=row[6],
            detected_at=row[7].isoformat() + 'Z' if hasattr(row[7], 'isoformat') else row[7],
            affected_producer_id=row[8],
            diff=diff,
        ))
    
    return drift_events


@router.get("/diff", response_model=SchemaDiffResponse)
async def get_schema_diff(
    schema_id: str = Query(..., description="Schema ID to diff"),
    old_hash: str = Query(..., description="Old schema hash"),
    new_hash: str = Query(..., description="New schema hash"),
    storage: LineageStorage = Depends(get_storage)
):
    """
    Compute a side-by-side diff between two versions of a schema.

    Looks up both schema versions by (schema_id, hash) from schema_snapshots,
    then classifies the change severity and returns a full field-level diff.
    """
    import json
    from fastapi import HTTPException

    rows = storage.conn.execute("""
        SELECT schema_hash, schema_json
        FROM schema_snapshots
        WHERE schema_id = ? AND schema_hash IN (?, ?)
    """, (schema_id, old_hash, new_hash)).fetchall()

    schemas = {row[0]: row[1] for row in rows}

    if old_hash not in schemas:
        raise HTTPException(status_code=404, detail=f"Schema not found: {schema_id}@{old_hash}")
    if new_hash not in schemas:
        raise HTTPException(status_code=404, detail=f"Schema not found: {schema_id}@{new_hash}")

    old_schema_json = schemas[old_hash]
    new_schema_json = schemas[new_hash]

    from schema_detector import SchemaDriftDetector
    detector = SchemaDriftDetector(storage)

    severity = detector.classify_drift(old_schema_json, new_schema_json)
    diff = detector.compute_schema_diff(old_schema_json, new_schema_json)

    old_fields_map = detector._extract_fields(json.loads(old_schema_json))
    new_fields_map = detector._extract_fields(json.loads(new_schema_json))

    all_fields_old = [SchemaField(name=n, type=f.get('type', 'unknown')) for n, f in old_fields_map.items()]
    all_fields_new = [SchemaField(name=n, type=f.get('type', 'unknown')) for n, f in new_fields_map.items()]

    return SchemaDiffResponse(
        schema_id=schema_id,
        old_hash=old_hash,
        new_hash=new_hash,
        severity=severity,
        fields_added=[SchemaField(name=f['name'], type=f.get('type', 'unknown')) for f in diff['fields_added']],
        fields_removed=[SchemaField(name=f['name'], type=f.get('type', 'unknown')) for f in diff['fields_removed']],
        fields_changed=[SchemaFieldChange(name=f['name'], old_type=f['old_type'], new_type=f['new_type']) for f in diff['fields_changed']],
        all_fields_old=all_fields_old,
        all_fields_new=all_fields_new,
    )


@router.get("/{schema_id}/versions", response_model=List[SchemaVersion])
async def get_schema_versions(
    schema_id: str,
    storage: LineageStorage = Depends(get_storage)
):
    """
    Get all known versions of a schema.
    
    Returns version history for a specific schema, showing when each
    version was first and last seen.
    
    Args:
        schema_id: Schema identifier
    
    Returns:
        List of schema versions sorted by first_seen_at
    """
    results = storage.conn.execute("""
        SELECT schema_hash, schema_json, first_seen_at, last_seen_at
        FROM schema_snapshots
        WHERE schema_id = ?
        ORDER BY first_seen_at ASC
    """, (schema_id,)).fetchall()
    
    versions = []
    for row in results:
        versions.append(SchemaVersion(
            schema_hash=row[0],
            schema_json=row[1],
            first_seen_at=row[2].isoformat() + 'Z' if hasattr(row[2], 'isoformat') else row[2],
            last_seen_at=row[3].isoformat() + 'Z' if hasattr(row[3], 'isoformat') else row[3],
        ))
    
    return versions
