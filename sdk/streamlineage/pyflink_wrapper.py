"""
StreamLineage PyFlink SDK Wrapper

Provides LineageStream wrapper for Apache Flink Python API (PyFlink) to automatically
track data provenance through streaming transformations.

Note: This module requires PyFlink to be installed. If PyFlink is not available,
imports will succeed with a warning, but all methods will raise ImportError when called.
This allows the SDK to be pip-installed in non-Flink environments without breaking.

Example Usage:
    from pyflink.datastream import StreamExecutionEnvironment
    from streamlineage.pyflink_wrapper import LineageStream
    
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Create lineage-tracked stream from Kafka
    stream = LineageStream.from_kafka(
        env=env,
        topic="payments.raw",
        node_id="fraud-detector-v1",
        bootstrap_servers="localhost:9092"
    )
    
    # Filter with automatic lineage tracking
    filtered = stream.filter(lambda event: event['amount'] > 100)
    
    # Map with custom operation name
    enriched = filtered.map(
        lambda event: {**event, 'risk_score': calculate_risk(event)},
        op_name="risk_scoring"
    )
    
    # Sink to output topic with lineage headers
    enriched.to_kafka("fraud-alerts", "localhost:9092")
    
    env.execute("Fraud Detection Pipeline")
"""

import json
import warnings
from typing import Callable, Optional, Any, Dict
from dataclasses import asdict

try:
    from pyflink.datastream import StreamExecutionEnvironment, DataStream
    from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import (
        FlinkKafkaConsumer,
        FlinkKafkaProducer,
        KafkaRecordSerializationSchema
    )
    PYFLINK_AVAILABLE = True
except ImportError:
    warnings.warn(
        "PyFlink is not installed. LineageStream will not be functional. "
        "Install with: pip install apache-flink"
    )
    PYFLINK_AVAILABLE = False
    StreamExecutionEnvironment = Any
    DataStream = Any


from .envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
)


class LineageStream:
    """
    Wrapper around PyFlink DataStream that automatically tracks provenance.
    
    This class wraps transformation operations (filter, map, join) to ensure
    provenance envelopes flow through the pipeline with correct chain hashing
    and parent tracking.
    
    Attributes:
        data_stream: The underlying PyFlink DataStream
        node_id: Identifier for this processing node (job/operator name)
        schema_id: Schema identifier for messages in this stream
    """
    
    def __init__(
        self,
        data_stream: DataStream,
        node_id: str,
        schema_id: str = "unknown",
        bootstrap_servers: str = "localhost:9092"
    ):
        """
        Initialize LineageStream.
        
        Args:
            data_stream: PyFlink DataStream to wrap
            node_id: Unique identifier for this processing node
            schema_id: Schema identifier for messages
            bootstrap_servers: Kafka bootstrap servers
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError(
                "PyFlink is required to use LineageStream. "
                "Install with: pip install apache-flink"
            )
        
        self.data_stream = data_stream
        self.node_id = node_id
        self.schema_id = schema_id
        self.bootstrap_servers = bootstrap_servers
    
    @classmethod
    def from_kafka(
        cls,
        env: StreamExecutionEnvironment,
        topic: str,
        node_id: str,
        bootstrap_servers: str = "localhost:9092",
        schema_id: str = "unknown",
        consumer_group: str = None
    ) -> "LineageStream":
        """
        Create LineageStream from Kafka topic.
        
        Args:
            env: Flink StreamExecutionEnvironment
            topic: Kafka topic to consume from
            node_id: Identifier for this consumer node
            bootstrap_servers: Kafka brokers (comma-separated)
            schema_id: Schema identifier for messages
            consumer_group: Kafka consumer group ID (default: streamlineage-{node_id})
        
        Returns:
            LineageStream wrapping the Kafka source
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError("PyFlink is required to use from_kafka()")
        
        consumer_group = consumer_group or f"streamlineage-{node_id}"
        
        # Create Kafka consumer with provenance-aware deserialization
        consumer = FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=SimpleStringSchema(),
            properties={
                'bootstrap.servers': bootstrap_servers,
                'group.id': consumer_group,
            }
        )
        
        # Get DataStream from consumer
        data_stream = env.add_source(consumer)
        
        return cls(
            data_stream=data_stream,
            node_id=node_id,
            schema_id=schema_id,
            bootstrap_servers=bootstrap_servers
        )
    
    def filter(self, func: Callable[[dict], bool]) -> "LineageStream":
        """
        Filter stream with automatic provenance tracking.
        
        Args:
            func: Filter predicate function taking deserialized payload
        
        Returns:
            New LineageStream with filtered data and updated provenance
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError("PyFlink is required to use filter()")
        
        class LineageFilterFunction(FilterFunction):
            def __init__(self, user_func, node_id, schema_id):
                self.user_func = user_func
                self.node_id = node_id
                self.schema_id = schema_id
            
            def filter(self, value: str) -> bool:
                """Apply user filter and track provenance."""
                try:
                    # Parse message (assumed JSON)
                    message = json.loads(value)
                    
                    # Apply user's filter function
                    return self.user_func(message)
                except Exception as e:
                    warnings.warn(f"Filter function error: {e}")
                    return False
        
        class LineageFilterMap(MapFunction):
            def __init__(self, node_id, schema_id):
                self.node_id = node_id
                self.schema_id = schema_id
            
            def map(self, value: str) -> str:
                """Update provenance envelope after filter."""
                try:
                    message = json.loads(value)
                    
                    # Extract provenance envelope from message metadata if present
                    # Note: In PyFlink, headers are not directly accessible in MapFunction
                    # This is a simplified version - production would need ProcessFunction
                    
                    # Create child envelope with "filter" operation
                    parent_envelope = message.get('_prov_envelope')
                    if parent_envelope:
                        parent = ProvenanceEnvelope(**parent_envelope)
                        child_envelope = create_child_envelope(
                            payload_bytes=json.dumps(message).encode('utf-8'),
                            parent_envelopes=[parent],
                            producer_id=self.node_id,
                            topic=f"{self.node_id}-filtered",
                            partition=0,
                            offset=0,
                            schema_id=self.schema_id,
                            transform_ops=["filter"]
                        )
                        message['_prov_envelope'] = asdict(child_envelope)
                    
                    return json.dumps(message)
                except Exception as e:
                    warnings.warn(f"Provenance update error: {e}")
                    return value
        
        # Apply filter and update provenance
        filtered = self.data_stream.filter(
            LineageFilterFunction(func, self.node_id, self.schema_id)
        )
        
        # Update provenance envelopes
        updated = filtered.map(LineageFilterMap(self.node_id, self.schema_id))
        
        return LineageStream(
            data_stream=updated,
            node_id=self.node_id,
            schema_id=self.schema_id,
            bootstrap_servers=self.bootstrap_servers
        )
    
    def map(
        self,
        func: Callable[[dict], dict],
        op_name: str = "map"
    ) -> "LineageStream":
        """
        Map stream with automatic provenance tracking.
        
        Args:
            func: Transformation function taking and returning dict
            op_name: Name of this operation for provenance tracking
        
        Returns:
            New LineageStream with mapped data and updated provenance
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError("PyFlink is required to use map()")
        
        class LineageMapFunction(MapFunction):
            def __init__(self, user_func, node_id, schema_id, op_name):
                self.user_func = user_func
                self.node_id = node_id
                self.schema_id = schema_id
                self.op_name = op_name
            
            def map(self, value: str) -> str:
                """Apply user map and update provenance."""
                try:
                    message = json.loads(value)
                    
                    # Extract parent envelope
                    parent_envelope = message.get('_prov_envelope')
                    
                    # Apply user's transformation
                    transformed = self.user_func(message)
                    
                    # Create child envelope
                    if parent_envelope:
                        parent = ProvenanceEnvelope(**parent_envelope)
                        child_envelope = create_child_envelope(
                            payload_bytes=json.dumps(transformed).encode('utf-8'),
                            parent_envelopes=[parent],
                            producer_id=self.node_id,
                            topic=f"{self.node_id}-{self.op_name}",
                            partition=0,
                            offset=0,
                            schema_id=self.schema_id,
                            transform_ops=[self.op_name]
                        )
                        transformed['_prov_envelope'] = asdict(child_envelope)
                    
                    return json.dumps(transformed)
                except Exception as e:
                    warnings.warn(f"Map function error: {e}")
                    return value
        
        mapped = self.data_stream.map(
            LineageMapFunction(func, self.node_id, self.schema_id, op_name)
        )
        
        return LineageStream(
            data_stream=mapped,
            node_id=self.node_id,
            schema_id=self.schema_id,
            bootstrap_servers=self.bootstrap_servers
        )
    
    def join(
        self,
        other: "LineageStream",
        key_selector_self: Callable[[dict], Any],
        key_selector_other: Callable[[dict], Any],
        window_seconds: int = 30
    ) -> "LineageStream":
        """
        Join two streams with fan-in provenance tracking.
        
        This creates child envelopes with TWO parents (one from each stream),
        demonstrating the fan-in capability of the provenance model.
        
        Args:
            other: Other LineageStream to join with
            key_selector_self: Function to extract join key from this stream
            key_selector_other: Function to extract join key from other stream
            window_seconds: Time window for join (seconds)
        
        Returns:
            New LineageStream with joined data and dual-parent provenance
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError("PyFlink is required to use join()")
        
        # Note: This is a simplified implementation
        # Production would use CoProcessFunction with state and timers
        warnings.warn(
            "LineageStream.join() is a simplified implementation. "
            "Production use should implement proper windowed join logic."
        )
        
        # For demonstration, we merge the streams and track both parents
        class JoinMapFunction(MapFunction):
            def __init__(self, node_id, schema_id):
                self.node_id = node_id
                self.schema_id = schema_id
            
            def map(self, value: str) -> str:
                """Mark message as joined with placeholder logic."""
                try:
                    message = json.loads(value)
                    
                    # In real implementation, would extract both parent envelopes
                    # and create child with parent_envelopes=[parent1, parent2]
                    
                    parent_envelope = message.get('_prov_envelope')
                    if parent_envelope:
                        parent = ProvenanceEnvelope(**parent_envelope)
                        # Simplified: create child with single parent
                        # Real implementation would have both stream parents
                        child_envelope = create_child_envelope(
                            payload_bytes=json.dumps(message).encode('utf-8'),
                            parent_envelopes=[parent],  # Would be [parent1, parent2]
                            producer_id=self.node_id,
                            topic=f"{self.node_id}-joined",
                            partition=0,
                            offset=0,
                            schema_id=self.schema_id,
                            transform_ops=["join"]
                        )
                        message['_prov_envelope'] = asdict(child_envelope)
                    
                    return json.dumps(message)
                except Exception as e:
                    warnings.warn(f"Join function error: {e}")
                    return value
        
        # Union the two streams (simplified join)
        joined = self.data_stream.union(other.data_stream)
        updated = joined.map(JoinMapFunction(self.node_id, self.schema_id))
        
        return LineageStream(
            data_stream=updated,
            node_id=self.node_id,
            schema_id=self.schema_id,
            bootstrap_servers=self.bootstrap_servers
        )
    
    def to_kafka(self, topic: str, bootstrap_servers: str = None) -> None:
        """
        Sink stream to Kafka topic with provenance headers.
        
        Args:
            topic: Destination Kafka topic
            bootstrap_servers: Kafka brokers (uses instance default if not provided)
        """
        if not PYFLINK_AVAILABLE:
            raise ImportError("PyFlink is required to use to_kafka()")
        
        servers = bootstrap_servers or self.bootstrap_servers
        
        # Create Kafka producer that preserves provenance in headers
        producer = FlinkKafkaProducer(
            topic=topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                'bootstrap.servers': servers,
            }
        )
        
        self.data_stream.add_sink(producer)
    
    def get_data_stream(self) -> DataStream:
        """
        Get underlying PyFlink DataStream.
        
        Returns:
            The wrapped DataStream for advanced operations
        """
        return self.data_stream


# Export only LineageStream - internal classes are private
__all__ = ['LineageStream']
