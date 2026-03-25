"""
Schema Drift Detector

Detects and classifies schema changes between events. Monitors schema evolution
and identifies breaking changes, additive changes, and compatible modifications.
"""

import json
import uuid
from datetime import datetime
from typing import Optional

from streamlineage.envelope import ProvenanceEnvelope
from storage import LineageStorage


class SchemaDriftDetector:
    """
    Detects schema drift by comparing schema hashes and classifying severity.
    
    Maintains an in-memory cache of known schemas and persists schema snapshots
    and drift events to storage.
    """
    
    def __init__(self, storage: LineageStorage):
        """
        Initialize the schema drift detector.
        
        Args:
            storage: LineageStorage instance for persistence
        """
        self.storage = storage
        # In-memory cache: schema_id -> (schema_hash, schema_json)
        self.known_schemas = {}
        
        # Load existing schema snapshots from storage on init
        self._load_existing_schemas()
    
    def _load_existing_schemas(self):
        """Load existing schema snapshots from storage into memory cache."""
        # Query all schema snapshots from storage
        try:
            results = self.storage.conn.execute("""
                SELECT schema_id, schema_hash, schema_json
                FROM schema_snapshots
                ORDER BY last_seen_at DESC
            """).fetchall()
            
            # Build cache with most recent schema per schema_id
            for schema_id, schema_hash, schema_json in results:
                if schema_id not in self.known_schemas:
                    self.known_schemas[schema_id] = (schema_hash, schema_json)
        except Exception as e:
            # If table doesn't exist yet or other error, start with empty cache
            print(f"Warning: Could not load existing schemas: {e}")
    
    def check_envelope(
        self,
        envelope: ProvenanceEnvelope,
        schema_json: str
    ) -> Optional[dict]:
        """
        Check if envelope represents a schema drift and classify it.
        
        Args:
            envelope: ProvenanceEnvelope to check
            schema_json: Current schema JSON string
        
        Returns:
            Drift event dictionary if drift detected, None otherwise
        """
        schema_id = envelope.schema_id
        schema_hash = envelope.schema_hash
        
        # Check if this is the first time seeing this schema_id
        if schema_id not in self.known_schemas:
            # First time seeing this schema - store it
            self.storage.upsert_schema_snapshot(schema_id, schema_hash, schema_json)
            self.known_schemas[schema_id] = (schema_hash, schema_json)
            return None  # No drift on first occurrence
        
        # Get the last known schema
        last_schema_hash, last_schema_json = self.known_schemas[schema_id]
        
        # Check if schema hash matches
        if schema_hash == last_schema_hash:
            # No change - just update last_seen_at
            self.storage.upsert_schema_snapshot(schema_id, schema_hash, schema_json)
            return None  # No drift
        
        # Schema has changed - classify the drift
        severity = self.classify_drift(last_schema_json, schema_json)
        
        # Compute detailed diff
        diff = self.compute_schema_diff(last_schema_json, schema_json)
        
        # Create drift event
        drift_event = {
            'drift_id': str(uuid.uuid4()),
            'schema_id': schema_id,
            'old_schema_hash': last_schema_hash,
            'new_schema_hash': schema_hash,
            'old_schema_json': last_schema_json,
            'new_schema_json': schema_json,
            'severity': severity,
            'detected_at': datetime.utcnow().isoformat() + 'Z',
            'affected_producer_id': envelope.producer_id,
            'diff': diff,
        }
        
        # Store drift event
        self.storage.insert_drift_event(drift_event)
        
        # Update known schemas cache
        self.storage.upsert_schema_snapshot(schema_id, schema_hash, schema_json)
        self.known_schemas[schema_id] = (schema_hash, schema_json)
        
        return drift_event
    
    def classify_drift(self, old_schema_json: str, new_schema_json: str) -> str:
        """
        Classify the severity of schema drift.
        
        Args:
            old_schema_json: Previous schema JSON string
            new_schema_json: New schema JSON string
        
        Returns:
            "BREAKING", "ADDITIVE", or "COMPATIBLE"
        """
        try:
            old_schema = json.loads(old_schema_json)
            new_schema = json.loads(new_schema_json)
        except json.JSONDecodeError:
            # If we can't parse, assume breaking change
            return "BREAKING"
        
        # Get field dictionaries for comparison
        old_fields = self._extract_fields(old_schema)
        new_fields = self._extract_fields(new_schema)
        
        # Check for removed fields (BREAKING)
        removed_fields = set(old_fields.keys()) - set(new_fields.keys())
        if removed_fields:
            return "BREAKING"
        
        # Check for type changes (BREAKING)
        for field_name in old_fields.keys() & new_fields.keys():
            old_type = old_fields[field_name].get('type')
            new_type = new_fields[field_name].get('type')
            
            if old_type != new_type:
                return "BREAKING"
        
        # Check for added fields
        added_fields = set(new_fields.keys()) - set(old_fields.keys())
        if added_fields:
            # Check if all added fields are nullable/optional
            all_nullable = all(
                new_fields[field].get('nullable', True) or 
                'default' in new_fields[field]
                for field in added_fields
            )
            
            if all_nullable:
                return "ADDITIVE"
            else:
                # Added required field is BREAKING
                return "BREAKING"
        
        # Only metadata/description changes
        return "COMPATIBLE"
    
    def compute_schema_diff(self, old_schema_json: str, new_schema_json: str) -> dict:
        """
        Compute a detailed diff between two schemas.
        
        Args:
            old_schema_json: Previous schema JSON string
            new_schema_json: New schema JSON string
        
        Returns:
            Dictionary with fields_added, fields_removed, fields_changed
        """
        try:
            old_schema = json.loads(old_schema_json)
            new_schema = json.loads(new_schema_json)
        except json.JSONDecodeError:
            return {
                'fields_added': [],
                'fields_removed': [],
                'fields_changed': [],
            }
        
        old_fields = self._extract_fields(old_schema)
        new_fields = self._extract_fields(new_schema)
        
        # Find added fields
        added_field_names = set(new_fields.keys()) - set(old_fields.keys())
        fields_added = [
            {
                'name': field_name,
                'type': new_fields[field_name].get('type', 'unknown'),
                'nullable': new_fields[field_name].get('nullable', True),
            }
            for field_name in added_field_names
        ]
        
        # Find removed fields
        removed_field_names = set(old_fields.keys()) - set(new_fields.keys())
        fields_removed = [
            {
                'name': field_name,
                'type': old_fields[field_name].get('type', 'unknown'),
            }
            for field_name in removed_field_names
        ]
        
        # Find changed fields (type changes)
        fields_changed = []
        for field_name in old_fields.keys() & new_fields.keys():
            old_type = old_fields[field_name].get('type')
            new_type = new_fields[field_name].get('type')
            
            if old_type != new_type:
                fields_changed.append({
                    'name': field_name,
                    'old_type': old_type,
                    'new_type': new_type,
                })
        
        return {
            'fields_added': fields_added,
            'fields_removed': fields_removed,
            'fields_changed': fields_changed,
        }
    
    def _extract_fields(self, schema: dict) -> dict:
        """
        Extract field definitions from a schema.
        
        Supports multiple schema formats:
        - {"fields": [...]}  (explicit fields array)
        - {"properties": {...}}  (JSON Schema format)
        - {"type": "object", "properties": {...}}  (JSON Schema with type)
        
        Args:
            schema: Schema dictionary
        
        Returns:
            Dictionary mapping field_name -> field_definition
        """
        # Format 1: {"fields": [{"name": "...", "type": "..."}]}
        if 'fields' in schema and isinstance(schema['fields'], list):
            return {
                field['name']: field
                for field in schema['fields']
                if 'name' in field
            }
        
        # Format 2: {"properties": {"field1": {"type": "..."}, ...}}
        if 'properties' in schema and isinstance(schema['properties'], dict):
            return {
                field_name: {
                    'name': field_name,
                    'type': field_def.get('type', 'unknown'),
                    'nullable': field_def.get('nullable', True),
                    **field_def
                }
                for field_name, field_def in schema['properties'].items()
            }
        
        # Empty or unknown format
        return {}
    
    def get_drift_history(self, schema_id: str) -> list[dict]:
        """
        Get drift event history for a specific schema.
        
        Args:
            schema_id: Schema identifier
        
        Returns:
            List of drift event dictionaries, sorted by detected_at descending
        """
        results = self.storage.conn.execute("""
            SELECT 
                drift_id, schema_id, old_schema_hash, new_schema_hash,
                old_schema_json, new_schema_json, severity, detected_at,
                affected_producer_id
            FROM schema_drift_events
            WHERE schema_id = ?
            ORDER BY detected_at DESC
        """, (schema_id,)).fetchall()
        
        drift_events = []
        for row in results:
            drift_event = {
                'drift_id': row[0],
                'schema_id': row[1],
                'old_schema_hash': row[2],
                'new_schema_hash': row[3],
                'old_schema_json': row[4],
                'new_schema_json': row[5],
                'severity': row[6],
                'detected_at': row[7].isoformat() + 'Z' if hasattr(row[7], 'isoformat') else row[7],
                'affected_producer_id': row[8],
            }
            
            # Compute diff for this event
            if row[4] and row[5]:  # If we have both old and new schemas
                drift_event['diff'] = self.compute_schema_diff(row[4], row[5])
            
            drift_events.append(drift_event)
        
        return drift_events
