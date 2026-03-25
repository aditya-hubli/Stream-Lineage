"""
Simple wrapper for easy lineage tracking.

This provides a simplified API for users who want to quickly add lineage
without dealing with the lower-level create_root_envelope API.
"""

import json
from typing import Optional, Any, Dict, List
from .envelope import create_root_envelope, create_child_envelope, ProvenanceEnvelope


class LineageEnvelope:
    """
    Simplified wrapper for creating lineage envelopes.
    
    Usage:
        envelope = LineageEnvelope(
            data={'order_id': '123'},
            dataset_id='orders.raw',
            producer_id='order-service-v1',
            operation='CREATE'
        )
        
        json_str = envelope.to_json()
        producer.produce('orders', value=json_str.encode('utf-8'))
    """
    
    def __init__(
        self,
        data: Any,
        dataset_id: str,
        producer_id: str,
        operation: str = "TRANSFORM",
        parent_envelopes: Optional[List[ProvenanceEnvelope]] = None,
        parents: Optional[List[str]] = None,  # Simple parent dataset IDs
        schema: Optional[Dict[str, str]] = None
    ):
        """
        Create a lineage envelope.
        
        Args:
            data: The actual data payload (will be JSON serialized)
            dataset_id: Dataset/topic identifier (e.g., 'orders.valid')
            producer_id: Service identifier (e.g., 'fraud-scorer-v1')
            operation: Operation type (e.g., 'TRANSFORM', 'CREATE', 'FILTER')
            parent_envelopes: Optional list of parent ProvenanceEnvelope objects
            parents: Optional list of parent dataset IDs (simpler alternative)
            schema: Optional schema dict (e.g., {'order_id': 'string', 'amount': 'float'})
        """
        self.data = data
        self.dataset_id = dataset_id
        self.producer_id = producer_id
        self.operation = operation
        
        # Note: If parents (dataset IDs) are provided, we ignore them for now
        # In a full implementation, we'd look up their envelopes from storage
        self.parent_envelopes = parent_envelopes or []
        self.parent_datasets = parents or []
        
        self.schema = schema or {}
        
        # Generate schema from data if not provided
        if not self.schema and isinstance(data, dict):
            self.schema = {k: type(v).__name__ for k, v in data.items()}
        
        # Create the actual provenance envelope
        self._envelope = self._create_envelope()
    
    def _create_envelope(self) -> ProvenanceEnvelope:
        """Create the underlying ProvenanceEnvelope."""
        # Serialize data and schema
        data_json = json.dumps(self.data, default=str)
        schema_json = json.dumps(self.schema)
        
        if self.parent_envelopes:
            # Child envelope with lineage
            return create_child_envelope(
                parent_envelopes=self.parent_envelopes,
                producer_id=self.producer_id,
                schema_id=f"{self.dataset_id}.schema",
                schema_json=schema_json,
                topic=self.dataset_id,
                payload_bytes=data_json.encode('utf-8'),
                transform_ops=[self.operation]
            )
        else:
            # Root envelope
            return create_root_envelope(
                producer_id=self.producer_id,
                schema_id=f"{self.dataset_id}.schema",
                schema_json=schema_json,
                topic=self.dataset_id,
                payload_bytes=data_json.encode('utf-8')
            )
    
    def to_json(self) -> str:
        """
        Serialize to JSON string for Kafka.
        
        Returns complete message with both envelope metadata and data payload.
        """
        message = {
            'lineage': {
                'prov_id': self._envelope.prov_id,
                'origin_id': self._envelope.origin_id,
                'parent_prov_ids': self._envelope.parent_prov_ids,
                'producer_id': self._envelope.producer_id,
                'producer_hash': self._envelope.producer_hash,
                'schema_id': self._envelope.schema_id,
                'schema_hash': self._envelope.schema_hash,
                'transform_ops': self._envelope.transform_ops,
                'chain_hash': self._envelope.chain_hash,
                'ingested_at': self._envelope.ingested_at,
                'topic': self._envelope.topic,
                'partition': self._envelope.partition,
                'offset': self._envelope.offset
            },
            'data': self.data
        }
        return json.dumps(message, default=str)
    
    @staticmethod
    def from_json(json_str: str) -> tuple[Dict[str, Any], Optional[ProvenanceEnvelope]]:
        """
        Parse message and extract data + envelope.
        
        Returns:
            Tuple of (data_dict, envelope) or (data_dict, None) if no lineage
        """
        message = json.loads(json_str)
        
        if 'lineage' not in message:
            return message, None
        
        lineage = message['lineage']
        envelope = ProvenanceEnvelope(
            prov_id=lineage['prov_id'],
            origin_id=lineage['origin_id'],
            parent_prov_ids=lineage['parent_prov_ids'],
            producer_id=lineage['producer_id'],
            producer_hash=lineage['producer_hash'],
            schema_id=lineage['schema_id'],
            schema_hash=lineage['schema_hash'],
            transform_ops=lineage['transform_ops'],
            chain_hash=lineage['chain_hash'],
            ingested_at=lineage['ingested_at'],
            topic=lineage['topic'],
            partition=lineage.get('partition', -1),
            offset=lineage.get('offset', -1)
        )
        
        return message.get('data', message), envelope
    
    @property
    def envelope(self) -> ProvenanceEnvelope:
        """Get the underlying ProvenanceEnvelope."""
        return self._envelope
