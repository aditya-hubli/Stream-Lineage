"""
Lineage Graph Builder

Maintains the live, in-memory lineage DAG using NetworkX. Receives ProvenanceEnvelope
objects and builds a directed graph where nodes represent producer/processor instances
and edges represent data flow between them.
"""

import json
import os
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Callable, Optional

try:
    import redis as _redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False


_REDIS_GRAPH_KEY = 'streamlineage:graph:snapshot'


class RedisGraphStore:
    """
    Thin wrapper around Redis for fast graph state persistence.

    On startup the ingester can recover the in-memory graph in milliseconds
    by reading the latest JSON from Redis instead of replaying all Kafka events
    or loading a potentially stale DuckDB snapshot.

    Falls back gracefully when Redis is unavailable — all operations are no-ops.
    """

    def __init__(self, redis_url: Optional[str] = None):
        self._client = None
        if not _REDIS_AVAILABLE:
            print("[REDIS] redis-py not installed — graph state will not be persisted to Redis")
            return
        url = redis_url or os.getenv('REDIS_URL', '')
        if not url:
            print("[REDIS] REDIS_URL not set — graph state will not be persisted to Redis")
            return
        try:
            self._client = _redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            self._client.ping()
            print(f"[REDIS] Connected to Redis at {url}")
        except Exception as e:
            print(f"[REDIS] Could not connect to Redis ({e}) — continuing without Redis")
            self._client = None

    @property
    def available(self) -> bool:
        return self._client is not None

    def save(self, graph_json: str) -> bool:
        """Persist graph JSON to Redis. Returns True on success."""
        if not self._client:
            return False
        try:
            self._client.set(_REDIS_GRAPH_KEY, graph_json)
            return True
        except Exception as e:
            print(f"[REDIS] Save failed: {e}")
            return False

    def load(self) -> Optional[str]:
        """Load graph JSON from Redis. Returns None if missing or on error."""
        if not self._client:
            return None
        try:
            data = self._client.get(_REDIS_GRAPH_KEY)
            return data.decode('utf-8') if data else None
        except Exception as e:
            print(f"[REDIS] Load failed: {e}")
            return None

import networkx as nx

from streamlineage.envelope import ProvenanceEnvelope
from storage import LineageStorage


class LRUCache:
    """Simple LRU cache for prov_id -> producer_id mapping."""
    
    def __init__(self, capacity: int = 100000):
        self.cache = OrderedDict()
        self.capacity = capacity
    
    def get(self, key: str) -> Optional[str]:
        """Get value from cache, moving to end if found."""
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]
    
    def put(self, key: str, value: str):
        """Put value in cache, evicting oldest if at capacity."""
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)  # Remove oldest


class LineageGraphBuilder:
    """
    Builds and maintains the live lineage DAG in memory.
    
    Graph structure:
    - Nodes: producer/processor instances (identified by producer_id)
    - Edges: data flow between producers (source -> target)
    
    Thread-safe for concurrent envelope processing.
    """
    
    def __init__(self, storage: LineageStorage):
        """
        Initialize the graph builder.
        
        Args:
            storage: LineageStorage instance for persistence
        """
        self.storage = storage
        self.graph = nx.DiGraph()
        self.lock = threading.Lock()
        self.subscribers = []

        # TTL for in-memory graph nodes/edges (seconds). 0 = disabled.
        self.ttl_seconds = int(os.getenv('GRAPH_TTL_SECONDS', '3600'))

        # Redis for fast graph state recovery on restart
        self.redis_store = RedisGraphStore()

        # LRU cache for prov_id -> (producer_id, ingested_at) lookups (max 100k entries)
        self.prov_cache = LRUCache(capacity=100000)

        # Attempt to restore graph state: Redis first, then latest DuckDB snapshot
        self._restore_graph_state()
    
    def _restore_graph_state(self):
        """
        Restore the in-memory graph on startup.

        Priority:
        1. Redis (fastest — milliseconds)
        2. Latest DuckDB dag_snapshot (fallback)
        3. Start empty (if neither available)
        """
        # Try Redis first
        graph_json = self.redis_store.load()
        if graph_json:
            if self.load_from_snapshot(graph_json):
                node_count = self.graph.number_of_nodes()
                edge_count = self.graph.number_of_edges()
                print(f"[INIT] Restored graph from Redis: {node_count} nodes, {edge_count} edges")
                return

        # Fall back to DuckDB latest snapshot
        try:
            result = self.storage.conn.execute("""
                SELECT graph_json, node_count, edge_count FROM dag_snapshots
                ORDER BY snapshot_at DESC LIMIT 1
            """).fetchone()
            if result:
                if self.load_from_snapshot(result[0]):
                    print(f"[INIT] Restored graph from DuckDB snapshot: "
                          f"{result[1]} nodes, {result[2]} edges")
                    return
        except Exception as e:
            print(f"[INIT] Could not load DuckDB snapshot: {e}")

        print("[INIT] Starting with empty graph (no snapshot found)")

    def process_envelope(self, envelope: ProvenanceEnvelope):
        """
        Process a provenance envelope and update the graph.
        
        Thread-safe method that:
        1. Adds/updates node for the envelope's producer
        2. Adds/updates edges from parent producers
        3. Stores envelope in persistent storage
        4. Notifies subscribers of graph updates
        
        Args:
            envelope: ProvenanceEnvelope to process
        """
        with self.lock:
            current_time = datetime.utcnow().isoformat() + "Z"
            producer_id = envelope.producer_id
            
            # Add or update node for current producer
            if self.graph.has_node(producer_id):
                # Update existing node
                node_data = self.graph.nodes[producer_id]
                node_data['last_seen_at'] = current_time
                node_data['event_count'] += 1
                node_data['topics'].add(envelope.topic)
            else:
                # Add new node
                self.graph.add_node(
                    producer_id,
                    producer_id=producer_id,
                    first_seen_at=current_time,
                    last_seen_at=current_time,
                    event_count=1,
                    topics={envelope.topic},
                )
                # Notify subscribers of new node
                self._notify_subscribers('dag.node.added', {
                    'producer_id': producer_id,
                    'first_seen_at': current_time,
                })
            
            # Process parent relationships (edges)
            for parent_prov_id in envelope.parent_prov_ids:
                # Look up parent's (producer_id, ingested_at) from cache
                cached = self.prov_cache.get(parent_prov_id)

                if cached:
                    parent_producer_id, parent_ingested_at = cached

                    # Compute latency between parent and child (milliseconds)
                    latency_ms = max(0, envelope.ingested_at - parent_ingested_at)

                    # Add or update edge from parent to current producer
                    if self.graph.has_edge(parent_producer_id, producer_id):
                        # Update existing edge with running average latency
                        edge_data = self.graph.edges[parent_producer_id, producer_id]
                        prev_count = edge_data['event_count']
                        prev_avg = edge_data['avg_latency_ms']
                        edge_data['avg_latency_ms'] = (
                            (prev_avg * prev_count + latency_ms) / (prev_count + 1)
                        )
                        edge_data['event_count'] += 1
                        edge_data['schema_ids'].add(envelope.schema_id)
                        edge_data['last_seen_at'] = current_time

                        # Notify subscribers of edge update
                        self._notify_subscribers('dag.edge.updated', {
                            'source': parent_producer_id,
                            'target': producer_id,
                            'event_count': edge_data['event_count'],
                            'avg_latency_ms': round(edge_data['avg_latency_ms'], 2),
                        })
                    else:
                        # Add new edge
                        self.graph.add_edge(
                            parent_producer_id,
                            producer_id,
                            source_producer_id=parent_producer_id,
                            target_producer_id=producer_id,
                            event_count=1,
                            schema_ids={envelope.schema_id},
                            last_seen_at=current_time,
                            avg_latency_ms=latency_ms,
                        )

                        # Notify subscribers of new edge
                        self._notify_subscribers('dag.edge.added', {
                            'source': parent_producer_id,
                            'target': producer_id,
                            'avg_latency_ms': latency_ms,
                        })

            # Store envelope in prov_id -> (producer_id, ingested_at) cache
            self.prov_cache.put(envelope.prov_id, (producer_id, envelope.ingested_at))
        
        # Store envelope in persistent storage (outside lock for better concurrency)
        self.storage.insert_lineage_event(envelope)
    
    def process_lineage_event(self, lineage: dict):
        """
        Process a simplified lineage event from HTTP POST (batch jobs).
        
        This method handles lineage events from non-Kafka sources like dbt,
        Airflow, or other batch processing systems.
        
        Args:
            lineage: Dictionary with keys:
                - dataset_id: Dataset identifier
                - producer_id: Producer/job identifier
                - operation: Operation type (TRANSFORM, AGGREGATE, etc.)
                - parent_datasets: List of parent dataset IDs (optional)
                - schema: Schema dict (optional)
                - timestamp: ISO timestamp (optional)
        """
        with self.lock:
            current_time = datetime.utcnow().isoformat() + "Z"
            producer_id = lineage.get("producer_id")
            dataset_id = lineage.get("dataset_id")
            
            if not producer_id:
                raise ValueError("producer_id is required")
            
            # Add or update node for current producer
            if self.graph.has_node(producer_id):
                # Update existing node
                node_data = self.graph.nodes[producer_id]
                node_data['last_seen_at'] = current_time
                node_data['event_count'] += 1
                if dataset_id:
                    node_data['topics'].add(dataset_id)
            else:
                # Add new node
                self.graph.add_node(
                    producer_id,
                    producer_id=producer_id,
                    first_seen_at=current_time,
                    last_seen_at=current_time,
                    event_count=1,
                    topics={dataset_id} if dataset_id else set(),
                )
                # Notify subscribers of new node
                self._notify_subscribers('dag.node.added', {
                    'producer_id': producer_id,
                    'first_seen_at': current_time,
                })
            
            # Process parent relationships (edges)
            parent_datasets = lineage.get("parent_datasets", [])
            for parent_dataset in parent_datasets:
                # Assume parent_dataset is a producer_id
                # (In batch systems, dataset often maps 1:1 with producer)
                if self.graph.has_edge(parent_dataset, producer_id):
                    # Update existing edge
                    edge_data = self.graph.edges[parent_dataset, producer_id]
                    edge_data['event_count'] += 1
                    edge_data['last_seen_at'] = current_time
                    
                    # Notify subscribers of edge update
                    self._notify_subscribers('dag.edge.updated', {
                        'source': parent_dataset,
                        'target': producer_id,
                        'event_count': edge_data['event_count'],
                    })
                else:
                    # Add new edge
                    self.graph.add_edge(
                        parent_dataset,
                        producer_id,
                        source_producer_id=parent_dataset,
                        target_producer_id=producer_id,
                        event_count=1,
                        schema_ids=set(),
                        last_seen_at=current_time,
                        avg_latency_ms=0,
                    )
                    
                    # Notify subscribers of new edge
                    self._notify_subscribers('dag.edge.added', {
                        'source': parent_dataset,
                        'target': producer_id,
                    })
    
    def compute_impact(
        self,
        producer_id: str,
        corrupted_from: str,
        corrupted_until: str
    ) -> dict:
        """
        Compute the blast radius of corrupted events from a producer.
        
        Performs BFS from the producer to find all downstream nodes and
        queries storage for event counts in the corrupted time window.
        
        Args:
            producer_id: Source producer that produced corrupted events
            corrupted_from: ISO8601 timestamp (inclusive)
            corrupted_until: ISO8601 timestamp (exclusive)
        
        Returns:
            Impact analysis response dictionary
        """
        with self.lock:
            # Verify producer exists in graph
            if not self.graph.has_node(producer_id):
                raise ValueError(f"Producer '{producer_id}' not found in lineage graph")
            
            # BFS to find all downstream nodes
            downstream_nodes = []
            visited = set()
            queue = [(producer_id, 0)]  # (node, distance)
            
            while queue:
                current_node, distance = queue.pop(0)
                
                if current_node in visited:
                    continue
                
                visited.add(current_node)
                
                # Add to downstream list (exclude source itself)
                if distance > 0:
                    downstream_nodes.append((current_node, distance))
                
                # Add successors to queue
                for successor in self.graph.successors(current_node):
                    if successor not in visited:
                        queue.append((successor, distance + 1))
        
        # Query storage for impact events (source producer)
        source_events = self.storage.get_impact_events(
            producer_id, corrupted_from, corrupted_until
        )
        
        # Build affected downstream nodes list
        affected_nodes = []
        replay_offsets = {}
        
        for node_id, distance in downstream_nodes:
            # Query events for this downstream node in the time window
            node_events = self.storage.query_lineage_events({
                'producer_id': node_id,
                'ingested_after': corrupted_from,
                'ingested_before': corrupted_until,
            })
            
            if node_events:
                # Get node metadata from graph
                with self.lock:
                    node_data = self.graph.nodes[node_id]
                    topics = list(node_data['topics'])
                
                affected_nodes.append({
                    'node': node_id,
                    'affected_events': len(node_events),
                    'topic': topics[0] if topics else 'unknown',
                    'distance_from_source': distance,
                })
                
                # Collect Kafka offsets for replay
                for event in node_events:
                    topic = event['topic']
                    offset = event['kafka_offset']
                    
                    if topic not in replay_offsets:
                        replay_offsets[topic] = [offset, offset]
                    else:
                        replay_offsets[topic][0] = min(replay_offsets[topic][0], offset)
                        replay_offsets[topic][1] = max(replay_offsets[topic][1], offset)
        
        # Sort affected nodes by affected_events descending
        affected_nodes.sort(key=lambda x: x['affected_events'], reverse=True)
        
        # Calculate total affected events
        total_affected = len(source_events) + sum(n['affected_events'] for n in affected_nodes)
        
        return {
            'producer_id': producer_id,
            'corrupted_from': corrupted_from,
            'corrupted_until': corrupted_until,
            'total_affected_events': total_affected,
            'affected_downstream_nodes': affected_nodes,
            'replay_kafka_offsets': replay_offsets,
            'computed_at': datetime.utcnow().isoformat() + 'Z',
        }
    
    def build_surgical_replay_plan(
        self,
        producer_id: str,
        corrupted_from: str,
        corrupted_until: str,
        include_join_partners: bool = True
    ) -> dict:
        """
        Build a surgical replay plan with exact partition+offset pairs.
        
        This is the core of provenance-based replay. Instead of replaying
        entire topics, this identifies the exact Kafka records affected by
        corruption using origin_id chains and parent lineage.
        
        Args:
            producer_id: Source producer that produced corrupted events
            corrupted_from: ISO8601 timestamp (inclusive)
            corrupted_until: ISO8601 timestamp (exclusive)
            include_join_partners: If True, includes clean events needed for joins
        
        Returns:
            Surgical replay plan with partition-specific offset lists and consumer commands
        """
        with self.lock:
            # Verify producer exists
            if not self.graph.has_node(producer_id):
                raise ValueError(f"Producer '{producer_id}' not found in lineage graph")
            
            # Get topologically sorted downstream nodes (process order)
            try:
                # Get all reachable nodes from producer
                reachable = nx.descendants(self.graph, producer_id)
                reachable.add(producer_id)
                
                # Create subgraph and topologically sort
                subgraph = self.graph.subgraph(reachable)
                topo_order = list(nx.topological_sort(subgraph))
            except nx.NetworkXError:
                # Graph might have cycles (shouldn't happen but handle gracefully)
                topo_order = [producer_id]
        
        # Query source corrupted events (with origin_id for chain tracking)
        source_events = self.storage.get_impact_events(
            producer_id, corrupted_from, corrupted_until
        )
        
        # Build origin_id set for chain tracking
        corrupted_origin_ids = set()
        for event in source_events:
            corrupted_origin_ids.add(event.get('origin_id', event['prov_id']))
        
        # Build partition-specific offset lists
        # Structure: {topic: {partition: [offset1, offset2, ...]}}
        partition_offsets = {}
        
        # Track affected events per node with full details
        affected_per_node = {}
        
        # Process nodes in topological order
        for node_id in topo_order:
            # Query events for this node in time window
            node_events = self.storage.query_lineage_events({
                'producer_id': node_id,
                'ingested_after': corrupted_from,
                'ingested_before': corrupted_until,
            })
            
            if not node_events:
                continue
            
            # Filter to only corrupted lineage (matching origin_id)
            affected_events = []
            for event in node_events:
                event_origin = event.get('origin_id', event['prov_id'])
                if event_origin in corrupted_origin_ids:
                    affected_events.append(event)
                    # Track new origin IDs (for downstream propagation)
                    corrupted_origin_ids.add(event['prov_id'])
            
            if not affected_events:
                continue
            
            # Collect partition+offset pairs
            for event in affected_events:
                topic = event['topic']
                partition = event.get('kafka_partition', -1)
                offset = event.get('kafka_offset', -1)
                
                if offset == -1:
                    continue  # Skip non-Kafka events
                
                # Initialize topic dict
                if topic not in partition_offsets:
                    partition_offsets[topic] = {}
                
                # Initialize partition list
                if partition not in partition_offsets[topic]:
                    partition_offsets[topic][partition] = []
                
                # Add offset (will deduplicate later)
                partition_offsets[topic][partition].append({
                    'offset': offset,
                    'prov_id': event['prov_id'],
                    'origin_id': event.get('origin_id'),
                    'producer_id': event['producer_id'],
                })
            
            # Track node stats
            affected_per_node[node_id] = {
                'affected_events': len(affected_events),
                'total_events_in_window': len(node_events),
                'selectivity': len(affected_events) / len(node_events) if node_events else 0,
            }
        
        # Join partner resolution: find clean events needed for corrupted joins
        join_partners = {}
        if include_join_partners:
            for node_id in topo_order:
                # Check if this is a join node (multiple incoming edges)
                with self.lock:
                    if not self.graph.has_node(node_id):
                        continue
                    predecessors = list(self.graph.predecessors(node_id))
                
                if len(predecessors) > 1:
                    # This is a join node - check if it has corrupted events
                    if node_id not in affected_per_node:
                        continue
                    
                    # Query all events for this join node to find parent relationships
                    join_events = self.storage.query_lineage_events({
                        'producer_id': node_id,
                        'ingested_after': corrupted_from,
                        'ingested_before': corrupted_until,
                    })
                    
                    # For each corrupted join event, find its partner events
                    for join_event in join_events:
                        if join_event.get('origin_id', join_event['prov_id']) not in corrupted_origin_ids:
                            continue
                        
                        # Parse parent_prov_ids to find partner events
                        parent_prov_ids = join_event.get('parent_prov_ids', [])
                        if isinstance(parent_prov_ids, str):
                            import json
                            try:
                                parent_prov_ids = json.loads(parent_prov_ids)
                            except:
                                parent_prov_ids = []
                        
                        # Find which parents are corrupted vs clean
                        for parent_prov_id in parent_prov_ids:
                            # Query the parent event
                            parent_events = self.storage.query_lineage_events({
                                'prov_id': parent_prov_id,
                            })
                            
                            if not parent_events:
                                continue
                            
                            parent_event = parent_events[0]
                            parent_origin = parent_event.get('origin_id', parent_event['prov_id'])
                            
                            # If parent is NOT corrupted, it's a join partner we need
                            if parent_origin not in corrupted_origin_ids:
                                topic = parent_event['topic']
                                partition = parent_event.get('kafka_partition', -1)
                                offset = parent_event.get('kafka_offset', -1)
                                
                                if offset == -1:
                                    continue
                                
                                # Add to join_partners tracking
                                if topic not in join_partners:
                                    join_partners[topic] = {}
                                if partition not in join_partners[topic]:
                                    join_partners[topic][partition] = []
                                
                                join_partners[topic][partition].append({
                                    'offset': offset,
                                    'prov_id': parent_event['prov_id'],
                                    'origin_id': parent_origin,
                                    'producer_id': parent_event['producer_id'],
                                    'reason': 'join_partner',
                                })
                                
                                # Also add to main partition_offsets
                                if topic not in partition_offsets:
                                    partition_offsets[topic] = {}
                                if partition not in partition_offsets[topic]:
                                    partition_offsets[topic][partition] = []
                                
                                partition_offsets[topic][partition].append({
                                    'offset': offset,
                                    'prov_id': parent_event['prov_id'],
                                    'origin_id': parent_origin,
                                    'producer_id': parent_event['producer_id'],
                                })
        
        # Sort offsets and find contiguous ranges for optimization
        optimized_seeks = {}
        total_records = 0
        
        for topic, partitions in partition_offsets.items():
            optimized_seeks[topic] = {}
            
            for partition, offset_records in partitions.items():
                # Sort by offset
                offset_records.sort(key=lambda x: x['offset'])
                
                # Deduplicate
                seen_offsets = set()
                unique_records = []
                for rec in offset_records:
                    if rec['offset'] not in seen_offsets:
                        seen_offsets.add(rec['offset'])
                        unique_records.append(rec)
                
                # Find contiguous ranges to minimize seeks
                # If offsets are close together (within 10 records), treat as range
                ranges = []
                if unique_records:
                    range_start = unique_records[0]['offset']
                    range_end = unique_records[0]['offset']
                    
                    for i in range(1, len(unique_records)):
                        curr_offset = unique_records[i]['offset']
                        if curr_offset - range_end <= 10:
                            # Extend range
                            range_end = curr_offset
                        else:
                            # Close current range, start new one
                            ranges.append({'start': range_start, 'end': range_end + 1})
                            range_start = curr_offset
                            range_end = curr_offset
                    
                    # Close final range
                    ranges.append({'start': range_start, 'end': range_end + 1})
                
                optimized_seeks[topic][partition] = {
                    'exact_offsets': [r['offset'] for r in unique_records],
                    'optimized_ranges': ranges,
                    'record_count': len(unique_records),
                }
                
                total_records += len(unique_records)
        
        # Calculate replay efficiency
        total_events_in_window = sum(n['total_events_in_window'] for n in affected_per_node.values())
        efficiency_pct = (total_records / total_events_in_window * 100) if total_events_in_window > 0 else 0
        
        # Count join partners
        join_partners_count = sum(
            len(offsets) 
            for topic_data in join_partners.values() 
            for offsets in topic_data.values()
        )
        
        return {
            'producer_id': producer_id,
            'corrupted_from': corrupted_from,
            'corrupted_until': corrupted_until,
            'total_affected_records': total_records,
            'total_events_in_window': total_events_in_window,
            'replay_efficiency_pct': round(efficiency_pct, 2),
            'affected_nodes': affected_per_node,
            'partition_offsets': optimized_seeks,
            'topological_order': topo_order,
            'corrupted_origin_ids': list(corrupted_origin_ids),
            'join_partners_included': include_join_partners,
            'join_partners_count': join_partners_count,
            'computed_at': datetime.utcnow().isoformat() + 'Z',
        }
    
    def get_graph_json(self) -> str:
        """
        Serialize the current graph to JSON.
        
        Thread-safe method that converts the NetworkX graph to JSON format
        using node-link data structure.
        
        Returns:
            JSON string representing the graph
        """
        with self.lock:
            # Convert sets to lists for JSON serialization
            graph_copy = self.graph.copy()
            for node, data in graph_copy.nodes(data=True):
                if 'topics' in data and isinstance(data['topics'], set):
                    data['topics'] = list(data['topics'])
            
            for u, v, data in graph_copy.edges(data=True):
                if 'schema_ids' in data and isinstance(data['schema_ids'], set):
                    data['schema_ids'] = list(data['schema_ids'])
            
            # Serialize to node-link format (edges='links' for NX <3.4 compat)
            graph_data = nx.node_link_data(graph_copy, edges='links')
            return json.dumps(graph_data)
    
    def load_from_snapshot(self, graph_json: str):
        """
        Load graph state from a saved snapshot.
        
        Thread-safe method that replaces the current graph with a snapshot.
        Used for restoring state after restart.
        
        Args:
            graph_json: JSON string from snapshot
        """
        with self.lock:
            try:
                # Deserialize graph
                graph_data = json.loads(graph_json)
                restored_graph = nx.node_link_graph(graph_data, edges='links')
                
                # Convert lists back to sets
                for node, data in restored_graph.nodes(data=True):
                    if 'topics' in data and isinstance(data['topics'], list):
                        data['topics'] = set(data['topics'])
                
                for u, v, data in restored_graph.edges(data=True):
                    if 'schema_ids' in data and isinstance(data['schema_ids'], list):
                        data['schema_ids'] = set(data['schema_ids'])
                
                # Replace current graph
                self.graph = restored_graph
                
                return True
            except Exception as e:
                print(f"[ERROR] Failed to load snapshot: {e}")
                return False
    
    def diff_snapshots(self, snapshot_a_json: str, snapshot_b_json: str) -> dict:
        """
        Compute the difference between two graph snapshots.
        
        Args:
            snapshot_a_json: JSON of first graph snapshot
            snapshot_b_json: JSON of second graph snapshot
        
        Returns:
            Dictionary with nodes/edges added and removed
        """
        # Deserialize graphs
        graph_a_data = json.loads(snapshot_a_json)
        graph_b_data = json.loads(snapshot_b_json)
        
        graph_a = nx.node_link_graph(graph_a_data, edges='links')
        graph_b = nx.node_link_graph(graph_b_data, edges='links')
        
        # Compute node differences
        nodes_a = set(graph_a.nodes())
        nodes_b = set(graph_b.nodes())
        
        nodes_added = list(nodes_b - nodes_a)
        nodes_removed = list(nodes_a - nodes_b)
        
        # Compute edge differences
        edges_a = set(graph_a.edges())
        edges_b = set(graph_b.edges())
        
        edges_added = [{'source': u, 'target': v} for u, v in (edges_b - edges_a)]
        edges_removed = [{'source': u, 'target': v} for u, v in (edges_a - edges_b)]
        
        return {
            'nodes_added': nodes_added,
            'nodes_removed': nodes_removed,
            'edges_added': edges_added,
            'edges_removed': edges_removed,
            'node_delta': len(nodes_added) - len(nodes_removed),
            'edge_delta': len(edges_added) - len(edges_removed),
        }
    
    def take_snapshot(self) -> str:
        """
        Take a snapshot of the current graph and persist it to DuckDB and Redis.

        Returns:
            snapshot_id (UUID)
        """
        graph_json = self.get_graph_json()

        with self.lock:
            node_count = self.graph.number_of_nodes()
            edge_count = self.graph.number_of_edges()

        snapshot_id = self.storage.save_dag_snapshot(graph_json, node_count, edge_count)

        # Mirror to Redis for fast recovery on restart
        self.redis_store.save(graph_json)

        return snapshot_id
    
    def prune_stale_nodes(self) -> dict:
        """
        Evict nodes and edges that have not been seen within TTL_SECONDS.

        Stale nodes are archived by taking a DAG snapshot before removal so the
        historical record in DuckDB is preserved. Returns a summary of what was pruned.
        Does nothing if ttl_seconds == 0.
        """
        if self.ttl_seconds == 0:
            return {'pruned_nodes': 0, 'pruned_edges': 0}

        cutoff = datetime.utcnow() - timedelta(seconds=self.ttl_seconds)
        cutoff_iso = cutoff.isoformat() + 'Z'

        stale_nodes = []
        with self.lock:
            for node, data in list(self.graph.nodes(data=True)):
                last_seen = data.get('last_seen_at', '')
                if last_seen and last_seen < cutoff_iso:
                    stale_nodes.append(node)

            if not stale_nodes:
                return {'pruned_nodes': 0, 'pruned_edges': 0}

            # Count edges that will be removed with these nodes
            pruned_edges = sum(
                self.graph.degree(n) for n in stale_nodes if self.graph.has_node(n)
            )

            # Remove stale nodes (also removes their edges)
            self.graph.remove_nodes_from(stale_nodes)

        print(f"[TTL] Pruned {len(stale_nodes)} stale nodes, ~{pruned_edges} edges "
              f"(last_seen < {cutoff_iso})")

        # Take a snapshot after pruning so the post-prune state is recorded
        try:
            self.take_snapshot()
        except Exception as e:
            print(f"[TTL] Snapshot after prune failed: {e}")

        return {'pruned_nodes': len(stale_nodes), 'pruned_edges': pruned_edges}

    def subscribe(self, callback: Callable[[str, dict], None]):
        """
        Subscribe to graph update events.
        
        Args:
            callback: Function to call on graph updates.
                      Signature: callback(event_type: str, data: dict)
        """
        with self.lock:
            self.subscribers.append(callback)
    
    def _notify_subscribers(self, event_type: str, data: dict):
        """
        Notify all subscribers of a graph update event.
        
        Runs callbacks in a separate thread to avoid blocking graph updates.
        
        Args:
            event_type: Type of event (e.g., 'dag.node.added')
            data: Event data dictionary
        """
        # Create a copy of subscribers list to avoid issues with concurrent modification
        subscribers_copy = list(self.subscribers)
        
        def notify():
            for callback in subscribers_copy:
                try:
                    callback(event_type, data)
                except Exception as e:
                    # Log error but don't let one subscriber failure affect others
                    print(f"Error notifying subscriber: {e}")
        
        # Run notifications in separate thread (non-blocking)
        thread = threading.Thread(target=notify, daemon=True)
        thread.start()
