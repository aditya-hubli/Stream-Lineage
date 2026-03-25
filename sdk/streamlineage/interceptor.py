"""
Kafka Producer Interceptor for automatic provenance tracking.

This module provides a transparent wrapper around Kafka producers that
automatically injects provenance envelopes into every message, enabling
end-to-end lineage tracking across streaming pipelines.

Example usage:
    >>> from confluent_kafka import Producer
    >>> from streamlineage.interceptor import ProvenanceInterceptor
    >>> 
    >>> # Create interceptor
    >>> interceptor = ProvenanceInterceptor(
    ...     producer_id="payment-service-v2.3",
    ...     schema_id="payments.v4",
    ...     schema_json='{"type": "object", "properties": {...}}'
    ... )
    >>> 
    >>> # Wrap existing producer
    >>> base_producer = Producer({'bootstrap.servers': 'localhost:9092'})
    >>> producer = interceptor.wrap_producer(base_producer)
    >>> 
    >>> # Produce messages - provenance is automatically injected
    >>> producer.produce(
    ...     topic='payments',
    ...     value=b'{"amount": 100.0, "user_id": "abc123"}',
    ...     key=b'abc123'
    ... )
    >>> producer.flush()
    >>> 
    >>> # For derived events (transformations), set parent context
    >>> from streamlineage.interceptor import ThreadLocalParentEnvelope
    >>> from streamlineage.envelope import headers_to_envelope
    >>> 
    >>> # When consuming a message with provenance
    >>> consumed_msg = consumer.poll(1.0)
    >>> parent_env = headers_to_envelope(dict(consumed_msg.headers()))
    >>> 
    >>> # Set parent context for downstream production
    >>> with ThreadLocalParentEnvelope(parent_env):
    ...     producer.produce(
    ...         topic='payments-processed',
    ...         value=b'{"amount": 100.0, "processed": true}',
    ...     )
"""

import threading
from typing import Optional, Any

from .envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
)


# Thread-local storage for parent envelope context
_thread_local = threading.local()


class ThreadLocalParentEnvelope:
    """
    Context manager for setting parent envelope in thread-local storage.
    
    This allows producers to automatically create child envelopes when producing
    derived/transformed events, maintaining lineage across transformations.
    
    Example:
        >>> parent_env = headers_to_envelope(consumed_message.headers())
        >>> with ThreadLocalParentEnvelope(parent_env):
        ...     producer.produce(topic='output', value=transformed_data)
    """
    
    def __init__(self, envelope: Optional[ProvenanceEnvelope], transform_ops: list[str] = None):
        """
        Initialize parent envelope context.
        
        Args:
            envelope: Parent ProvenanceEnvelope to set as context
            transform_ops: Optional list of transformation operations being applied
        """
        self.envelope = envelope
        self.transform_ops = transform_ops or []
        self.previous_envelope = None
        self.previous_ops = None
    
    def __enter__(self):
        """Set parent envelope in thread-local storage."""
        self.previous_envelope = getattr(_thread_local, 'parent_envelope', None)
        self.previous_ops = getattr(_thread_local, 'transform_ops', None)
        _thread_local.parent_envelope = self.envelope
        _thread_local.transform_ops = self.transform_ops
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore previous parent envelope context."""
        _thread_local.parent_envelope = self.previous_envelope
        _thread_local.transform_ops = self.previous_ops


class ProvenanceInterceptor:
    """
    Kafka Producer Interceptor that automatically injects provenance envelopes.
    
    This interceptor wraps Kafka producers and transparently adds lineage metadata
    to every message produced, creating root envelopes for original events and
    child envelopes for derived/transformed events.
    """
    
    def __init__(
        self,
        producer_id: str,
        schema_id: str = "unknown",
        schema_json: str = "{}"
    ):
        """
        Initialize the provenance interceptor.
        
        Args:
            producer_id: Identifier for this service (e.g., "payment-service-v2.3")
            schema_id: Schema identifier for produced messages (e.g., "payments.v4")
            schema_json: JSON string representation of the message schema
        """
        self.producer_id = producer_id
        self.schema_id = schema_id
        self.schema_json = schema_json
    
    def on_send(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[list] = None,
        partition: int = -1,
        **kwargs
    ) -> tuple:
        """
        Intercept message production and inject provenance envelope.
        
        This method is called before every message is produced. It:
        1. Checks thread-local storage for a parent envelope context
        2. If no thread-local parent, checks message headers for parent envelope
        3. Creates child envelope if parent exists, otherwise creates root envelope
        4. Merges provenance headers with existing headers
        
        Args:
            topic: Kafka topic name
            value: Message payload bytes
            key: Optional message key bytes
            headers: Optional list of (header_name, header_value) tuples
            partition: Kafka partition number
            **kwargs: Additional arguments to pass through
        
        Returns:
            Tuple of (topic, value, key, merged_headers, kwargs)
        """
        # Get transform ops from thread-local storage if available
        transform_ops = getattr(_thread_local, 'transform_ops', [])
        
        # Check for parent envelope in thread-local storage first
        parent_envelope = getattr(_thread_local, 'parent_envelope', None)
        
        # If no thread-local parent, check message headers
        if parent_envelope is None and headers:
            headers_dict = {k: v for k, v in headers}
            parent_envelope = headers_to_envelope(headers_dict)
        
        # Create appropriate envelope
        if parent_envelope:
            # This is a derived event - create child envelope
            envelope = create_child_envelope(
                parent_envelopes=[parent_envelope],
                producer_id=self.producer_id,
                schema_id=self.schema_id,
                schema_json=self.schema_json,
                topic=topic,
                payload_bytes=value,
                transform_ops=transform_ops,
                partition=partition
            )
        else:
            # This is a root event - create root envelope
            envelope = create_root_envelope(
                producer_id=self.producer_id,
                schema_id=self.schema_id,
                schema_json=self.schema_json,
                topic=topic,
                payload_bytes=value,
                partition=partition
            )
        
        # Convert envelope to headers
        prov_headers = envelope_to_headers(envelope)
        
        # Merge with existing headers (provenance headers take precedence)
        existing_headers_dict = {k: v for k, v in headers} if headers else {}
        existing_headers_dict.update(prov_headers)
        merged_headers = list(existing_headers_dict.items())
        
        return topic, value, key, merged_headers, kwargs
    
    def wrap_producer(self, producer):
        """
        Wrap a Kafka producer with provenance injection.
        
        Args:
            producer: confluent_kafka.Producer instance
        
        Returns:
            WrappedProducer that automatically injects provenance
        """
        return WrappedProducer(producer, self)


class WrappedProducer:
    """
    Wrapper around confluent_kafka.Producer that injects provenance envelopes.
    
    This class intercepts produce() calls to inject lineage metadata while
    delegating all other operations directly to the underlying producer.
    """
    
    def __init__(self, producer, interceptor: ProvenanceInterceptor):
        """
        Initialize wrapped producer.
        
        Args:
            producer: confluent_kafka.Producer instance to wrap
            interceptor: ProvenanceInterceptor instance for envelope creation
        """
        self._producer = producer
        self._interceptor = interceptor
    
    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        headers: Optional[list] = None,
        partition: int = -1,
        **kwargs
    ):
        """
        Produce a message with automatic provenance injection.
        
        Args:
            topic: Kafka topic name
            value: Message payload bytes
            key: Optional message key bytes
            headers: Optional list of (header_name, header_value) tuples
            partition: Kafka partition number
            **kwargs: Additional arguments (e.g., on_delivery callback, timestamp)
        """
        # Intercept and inject provenance
        topic, value, key, headers, kwargs = self._interceptor.on_send(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            **kwargs
        )
        
        # Delegate to real producer
        return self._producer.produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            **kwargs
        )
    
    def flush(self, timeout: Optional[float] = None) -> int:
        """Flush pending messages. Delegates to underlying producer."""
        if timeout is not None:
            return self._producer.flush(timeout)
        return self._producer.flush()
    
    def poll(self, timeout: Optional[float] = None) -> int:
        """Poll for events. Delegates to underlying producer."""
        if timeout is not None:
            return self._producer.poll(timeout)
        return self._producer.poll()
    
    def __len__(self) -> int:
        """Return number of messages waiting to be delivered."""
        return len(self._producer)
    
    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes to underlying producer."""
        return getattr(self._producer, name)
