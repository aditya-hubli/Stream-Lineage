// --- DAG types ---
export interface DagNode {
  id: string;
  producer_id: string;
  event_count: number;
  topics: string[];
  first_seen_at: string;
  last_seen_at: string;
}

export interface DagEdge {
  source: string;
  target: string;
  event_count: number;
  schema_ids: string[];
  last_seen_at: string;
}

export interface DagResponse {
  nodes: DagNode[];
  edges: DagEdge[];
  node_count: number;
  edge_count: number;
  generated_at: string;
}

// --- Impact types ---
export interface AffectedNode {
  node: string;
  affected_events: number;
  topic: string;
  distance_from_source: number;
}

export interface ImpactResponse {
  producer_id: string;
  corrupted_from: string;
  corrupted_until: string;
  total_affected_events: number;
  affected_downstream_nodes: AffectedNode[];
  replay_kafka_offsets: Record<string, [number, number]>;
  computed_at: string;
}

// --- Trace types ---
export interface TraceEvent {
  prov_id: string;
  origin_id: string;
  parent_prov_ids: string[];
  producer_id: string;
  schema_id: string;
  topic: string;
  partition: number;
  offset: number;
  transform_ops: string[];
  chain_hash: string;
  ingested_at: string;
  depth: number;
}

export interface TraceResponse {
  prov_id: string;
  origin_id: string;
  ancestors: TraceEvent[];
  event: TraceEvent;
  descendants: TraceEvent[];
  total_depth: number;
  trace_computed_at: string;
}

// --- Schema Drift types ---
export interface DriftEvent {
  drift_id: string;
  schema_id: string;
  old_schema_hash: string;
  new_schema_hash: string;
  severity: 'BREAKING' | 'ADDITIVE' | 'COMPATIBLE';
  detected_at: string;
  affected_producer_id: string;
  diff?: {
    fields_added: { name: string; type: string }[];
    fields_removed: { name: string; type: string }[];
    fields_changed: { name: string; old_type: string; new_type: string }[];
  };
}

// --- Schema Diff types ---
export interface SchemaField {
  name: string;
  type: string;
}

export interface SchemaFieldChange {
  name: string;
  old_type: string;
  new_type: string;
}

export interface SchemaDiffResponse {
  schema_id: string;
  old_hash: string;
  new_hash: string;
  severity: 'BREAKING' | 'ADDITIVE' | 'COMPATIBLE';
  fields_added: SchemaField[];
  fields_removed: SchemaField[];
  fields_changed: SchemaFieldChange[];
  all_fields_old: SchemaField[];
  all_fields_new: SchemaField[];
}

// --- Snapshot type ---
export interface SnapshotInfo {
  snapshot_id: string;
  snapshot_at: string;
  node_count: number;
  edge_count: number;
}

// --- Diff types ---
export interface EdgeChange {
  source: string;
  target: string;
}

export interface TransformChange {
  node: string;
  old_ops: string[];
  new_ops: string[];
}

export interface SchemaChange {
  edge_source: string;
  edge_target: string;
  schemas_added: string[];
  schemas_removed: string[];
  breaking?: boolean;
}

export interface DiffResponse {
  snapshot_a: { snapshot_id: string; snapshot_at: string };
  snapshot_b: { snapshot_id: string; snapshot_at: string };
  nodes_added: string[];
  nodes_removed: string[];
  edges_added: EdgeChange[];
  edges_removed: EdgeChange[];
  transform_changes?: TransformChange[];
  schema_changes?: SchemaChange[];
  node_delta: number;
  edge_delta: number;
  computed_at: string;
}

// --- Audit types ---
export interface AuditNode {
  prov_id: string;
  origin_id: string;
  producer_id: string;
  topic: string;
  partition: number;
  offset: number;
  ingested_at: string;
  transform_ops: string[];
  chain_hash: string;
  chain_hash_verified: boolean;
  verdict: 'VALID' | 'TAMPERED' | 'UNVERIFIABLE';
}

export interface AuditReport {
  report_id: string;
  generated_at: string;
  root_prov_id: string;
  queried_prov_id: string;
  total_nodes_audited: number;
  valid_count: number;
  tampered_count: number;
  unverifiable_count: number;
  overall_verdict: 'ALL_VALID' | 'TAMPERED' | 'PARTIAL';
  report_hash: string;
  chain: AuditNode[];
}

// --- Producer types ---
export interface ProducerInfo {
  producer_id: string;
  event_count: number;
  topics: string[];
  first_seen_at: string;
  last_seen_at: string;
  last_schema_id: string;
  status: 'active' | 'stale' | 'dead';
}

// --- WebSocket event types ---
export interface WsEvent {
  event_type: string;
  data: Record<string, unknown>;
  timestamp: string;
}
