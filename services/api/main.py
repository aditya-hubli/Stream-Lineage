"""
StreamLineage FastAPI Application

REST API and WebSocket server for real-time lineage tracking and analysis.
"""

import os
import sys
from datetime import datetime
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Add parent directories to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'sdk'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lineage-ingester'))

from dependencies import initialize_dependencies, get_graph_builder_instance
from scenario_orchestrator import get_orchestrator

# Create FastAPI app
app = FastAPI(
    title="StreamLineage API",
    description="Real-Time Data Provenance & Lineage Engine",
    version="1.0.0",
)

# CORS middleware - allow all origins (dev/demo project)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connection manager
class ConnectionManager:
    """Manages active WebSocket connections with subscription filtering."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        # Per-connection subscriptions: ws -> set of event type prefixes
        # Empty set means "subscribe to all"
        self._subscriptions: dict[WebSocket, Set[str]] = {}

    async def connect(self, websocket: WebSocket):
        """Accept and store a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        self._subscriptions[websocket] = set()  # default: all events

    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        self._subscriptions.pop(websocket, None)

    def subscribe(self, websocket: WebSocket, event_types: list[str]):
        """Set subscription filter for a connection."""
        self._subscriptions[websocket] = set(event_types)

    def unsubscribe(self, websocket: WebSocket):
        """Remove subscription filter (receive all events)."""
        self._subscriptions[websocket] = set()

    def _matches(self, websocket: WebSocket, event_type: str) -> bool:
        """Check if a connection is subscribed to an event type."""
        subs = self._subscriptions.get(websocket, set())
        if not subs:
            return True  # empty set = all events
        return any(event_type.startswith(prefix) for prefix in subs)

    async def broadcast(self, event_type: str, data: dict):
        """
        Broadcast a message to subscribed clients.

        Args:
            event_type: Type of event (e.g., 'dag.node.added')
            data: Event data dictionary
        """
        import json
        message = json.dumps({
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        })

        disconnected = set()
        for connection in self.active_connections:
            if not self._matches(connection, event_type):
                continue
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.add(connection)

        for connection in disconnected:
            self.disconnect(connection)


manager = ConnectionManager()


@app.on_event("startup")
async def startup_event():
    """Initialize dependencies on startup."""
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/lineage.duckdb")
    
    print("=" * 60)
    print("StreamLineage API - Starting Up")
    print("=" * 60)
    print(f"DuckDB Path: {duckdb_path}")
    print("=" * 60)
    
    # Initialize storage and graph builder
    initialize_dependencies(duckdb_path)
    
    # Subscribe to graph builder events for WebSocket broadcasting
    graph_builder = get_graph_builder_instance()
    
    def on_graph_update(event_type: str, data: dict):
        """Callback for graph update events."""
        import asyncio
        # Schedule broadcast on the event loop
        try:
            loop = asyncio.get_event_loop()
            asyncio.run_coroutine_threadsafe(
                manager.broadcast(event_type, data),
                loop
            )
        except Exception as e:
            print(f"[ERROR] Failed to broadcast event: {e}")
    
    graph_builder.subscribe(on_graph_update)
    
    print("[STARTUP] API server ready")


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Status and timestamp
    """
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.post("/api/lineage/ingest")
async def ingest_lineage(lineage: dict):
    """
    HTTP endpoint for ingesting lineage metadata.
    
    Supports two formats:
    1. ProvenanceEnvelope format (from LineageEnvelope.to_json())
    2. Simple format (dataset_id, producer_id, parent_datasets)
    
    This works for ANY streaming platform - not just Kafka!
    
    Args:
        lineage: Lineage metadata dict
    
    Returns:
        Success status
    """
    try:
        # Get graph builder instance
        graph_builder = get_graph_builder_instance()
        
        # Check if this is a full ProvenanceEnvelope
        if 'prov_id' in lineage and 'parent_prov_ids' in lineage:
            # Full envelope - convert to ProvenanceEnvelope and process
            from streamlineage.envelope import ProvenanceEnvelope
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
            graph_builder.process_envelope(envelope)
            
            return {
                "status": "success",
                "prov_id": envelope.prov_id,
                "producer_id": envelope.producer_id,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        else:
            # Simple format - process as batch job lineage
            graph_builder.process_lineage_event(lineage)
            
            return {
                "status": "success",
                "dataset_id": lineage.get("dataset_id"),
                "producer_id": lineage.get("producer_id"),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
    except Exception as e:
        print(f"[ERROR] Failed to ingest lineage: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }


@app.websocket("/ws/lineage")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time lineage updates.
    
    Clients connect here to receive live DAG updates, schema drift notifications,
    and other real-time events.
    """
    await manager.connect(websocket)
    
    try:
        # Send current DAG state on connection
        graph_builder = get_graph_builder_instance()
        graph_json = graph_builder.get_graph_json()
        
        import json
        await websocket.send_text(json.dumps({
            "event_type": "dag.full",
            "data": json.loads(graph_json),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }))
        
        # Keep connection alive and handle incoming messages
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")

                if action == "subscribe":
                    # {"action": "subscribe", "event_types": ["dag", "scenario"]}
                    event_types = msg.get("event_types", [])
                    manager.subscribe(websocket, event_types)
                    await websocket.send_text(json.dumps({
                        "event_type": "subscription.updated",
                        "data": {"event_types": event_types},
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }))

                elif action == "unsubscribe":
                    # {"action": "unsubscribe"} — receive all events
                    manager.unsubscribe(websocket)
                    await websocket.send_text(json.dumps({
                        "event_type": "subscription.cleared",
                        "data": {},
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }))

                elif action == "ping":
                    await websocket.send_text(json.dumps({
                        "event_type": "pong",
                        "data": {},
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }))

                else:
                    await websocket.send_text(json.dumps({
                        "event_type": "error",
                        "data": {"message": f"Unknown action: {action}"},
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }))
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "event_type": "error",
                    "data": {"message": "Invalid JSON"},
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }))
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"[WS] Client disconnected. Active connections: {len(manager.active_connections)}")
    except Exception as e:
        print(f"[ERROR] WebSocket error: {e}")
        manager.disconnect(websocket)


@app.get("/api/scenarios/status")
async def get_scenarios_status():
    """
    Get status of all scenarios.
    
    Returns:
        Status of each scenario (running/stopped) with process details
    """
    orchestrator = get_orchestrator()
    return orchestrator.get_all_status()


@app.post("/api/scenarios/{scenario_id}/start")
async def start_scenario(scenario_id: str):
    """
    Start a scenario's Python processes.
    
    Args:
        scenario_id: 'ecommerce' or 'hybrid'
    
    Returns:
        Success status and process list
    """
    orchestrator = get_orchestrator()
    result = orchestrator.start_scenario(scenario_id)
    
    # Broadcast event to WebSocket clients
    if result.get("success"):
        await manager.broadcast("scenario.started", {
            "scenario_id": scenario_id,
            "processes": result.get("processes", [])
        })
    
    return result


@app.post("/api/scenarios/{scenario_id}/stop")
async def stop_scenario(scenario_id: str):
    """
    Stop a scenario's Python processes.
    
    Args:
        scenario_id: 'ecommerce' or 'hybrid'
    
    Returns:
        Success status
    """
    orchestrator = get_orchestrator()
    result = orchestrator.stop_scenario(scenario_id)
    
    # Broadcast event to WebSocket clients
    if result.get("success"):
        await manager.broadcast("scenario.stopped", {
            "scenario_id": scenario_id,
            "stopped": result.get("stopped", [])
        })
    
    return result


# Import and include routers
from routers import dag, impact, diff, verify, schema, producers, replay, trace, audit

app.include_router(dag.router, prefix="/api/v1/dag", tags=["DAG"])
app.include_router(impact.router, prefix="/api/v1/impact", tags=["Impact Analysis"])
app.include_router(diff.router, prefix="/api/v1/diff", tags=["Diff"])
app.include_router(verify.router, prefix="/api/v1/verify", tags=["Verification"])
app.include_router(schema.router, prefix="/api/v1/schema", tags=["Schema"])
app.include_router(producers.router, prefix="/api/v1/producers", tags=["Producers"])
app.include_router(replay.router, prefix="/api/v1/replay", tags=["Surgical Replay"])
app.include_router(trace.router, prefix="/api/v1/trace", tags=["Trace"])
app.include_router(audit.router, prefix="/api/v1/audit", tags=["Audit"])


def main():
    """Run the API server."""
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
