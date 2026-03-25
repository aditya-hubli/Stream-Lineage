"""StreamLineage SDK for provenance tracking in streaming pipelines."""

__version__ = "0.1.0"

from .envelope import (
    ProvenanceEnvelope,
    create_root_envelope,
    create_child_envelope,
    envelope_to_headers,
    headers_to_envelope,
    verify_chain_hash,
)

from .interceptor import (
    ProvenanceInterceptor,
    WrappedProducer,
    ThreadLocalParentEnvelope,
)

from .simple_envelope import LineageEnvelope

# PyFlink wrapper is optional - only import if PyFlink is available
try:
    from .pyflink_wrapper import LineageStream
    __all__ = [
        "ProvenanceEnvelope",
        "create_root_envelope",
        "create_child_envelope",
        "envelope_to_headers",
        "headers_to_envelope",
        "verify_chain_hash",
        "ProvenanceInterceptor",
        "WrappedProducer",
        "ThreadLocalParentEnvelope",
        "LineageEnvelope",
        "LineageStream",
    ]
except ImportError:
    # PyFlink not available - export without LineageStream
    __all__ = [
        "ProvenanceEnvelope",
        "create_root_envelope",
        "create_child_envelope",
        "envelope_to_headers",
        "headers_to_envelope",
        "verify_chain_hash",
        "ProvenanceInterceptor",
        "WrappedProducer",
        "ThreadLocalParentEnvelope",
        "LineageEnvelope",
    ]
