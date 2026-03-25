"""
Dependency Injection for FastAPI

Provides shared instances of storage and graph builder for API endpoints.
"""

import os
from typing import Generator

from storage import LineageStorage
from graph_builder import LineageGraphBuilder


# Global instances (initialized at startup)
_storage: LineageStorage = None
_graph_builder: LineageGraphBuilder = None


def initialize_dependencies(duckdb_path: str):
    """
    Initialize global dependencies.
    
    Called during FastAPI startup event.
    
    Args:
        duckdb_path: Path to DuckDB database file
    """
    global _storage, _graph_builder
    
    # Open DuckDB in write mode (API handles both reads and writes for demo)
    _storage = LineageStorage(duckdb_path, read_only=False)
    _graph_builder = LineageGraphBuilder(_storage)
    
    # Load latest snapshot if available
    try:
        result = _storage.conn.execute("""
            SELECT graph_json FROM dag_snapshots 
            ORDER BY snapshot_at DESC LIMIT 1
        """).fetchone()
        
        if result:
            graph_json = result[0]
            if _graph_builder.load_from_snapshot(graph_json):
                print("[STARTUP] Loaded latest DAG snapshot successfully")
            else:
                print("[STARTUP] Failed to load DAG snapshot, starting with empty graph")
        else:
            print("[STARTUP] No previous snapshot found, starting with empty graph")
    except Exception as e:
        print(f"[STARTUP] Error loading snapshot: {e}, starting with empty graph")


def get_storage() -> Generator[LineageStorage, None, None]:
    """
    FastAPI dependency for LineageStorage.
    
    Yields:
        Shared LineageStorage instance
    """
    yield _storage


def get_graph_builder() -> Generator[LineageGraphBuilder, None, None]:
    """
    FastAPI dependency for LineageGraphBuilder.
    
    Yields:
        Shared LineageGraphBuilder instance
    """
    yield _graph_builder


def get_storage_instance() -> LineageStorage:
    """Get storage instance directly (for non-dependency use)."""
    return _storage


def get_graph_builder_instance() -> LineageGraphBuilder:
    """Get graph builder instance directly (for non-dependency use)."""
    return _graph_builder
