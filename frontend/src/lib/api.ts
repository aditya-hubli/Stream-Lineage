import type {
  DagResponse,
  ImpactResponse,
  TraceResponse,
  DriftEvent,
  DiffResponse,
  SchemaDiffResponse,
  AuditReport,
  SnapshotInfo,
  ProducerInfo,
} from '../types/api';

const BASE = '/api';

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

// --- DAG ---
export const fetchDag = () => fetchJson<DagResponse>(`${BASE}/v1/dag`);

// --- Impact ---
export const fetchImpact = (producerId: string, from: string, until: string) =>
  fetchJson<ImpactResponse>(`${BASE}/v1/impact`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ producer_id: producerId, corrupted_from: from, corrupted_until: until }),
  });

// --- Trace ---
export const fetchTrace = (provId: string) =>
  fetchJson<TraceResponse>(`${BASE}/v1/trace/${provId}`);

// --- Schema Drift ---
export const fetchDriftEvents = (schemaId?: string) =>
  fetchJson<DriftEvent[]>(
    `${BASE}/v1/schema/drift${schemaId ? `?schema_id=${schemaId}` : ''}`
  );

// --- Schema Diff ---
export const fetchSchemaDiff = (schemaId: string, oldHash: string, newHash: string) =>
  fetchJson<SchemaDiffResponse>(
    `${BASE}/v1/schema/diff?schema_id=${encodeURIComponent(schemaId)}&old_hash=${encodeURIComponent(oldHash)}&new_hash=${encodeURIComponent(newHash)}`
  );

// --- Diff ---
export const fetchDiff = (snapshotA: string, snapshotB: string) =>
  fetchJson<DiffResponse>(
    `${BASE}/v1/diff?snapshot_a=${encodeURIComponent(snapshotA)}&snapshot_b=${encodeURIComponent(snapshotB)}`
  );

export const fetchSnapshots = () =>
  fetchJson<SnapshotInfo[]>(`${BASE}/v1/diff/snapshots`);

// --- Producers ---
export const fetchProducers = () =>
  fetchJson<ProducerInfo[]>(`${BASE}/v1/producers`);

export const fetchLatestProvId = (producerId: string) =>
  fetchJson<{ producer_id: string; prov_id: string }>(
    `${BASE}/v1/producers/${encodeURIComponent(producerId)}/latest_prov_id`
  );

// --- Audit ---
export const fetchAudit = (provId: string) =>
  fetchJson<AuditReport>(`${BASE}/v1/audit/${encodeURIComponent(provId)}`);

// --- Health ---
export const fetchHealth = () =>
  fetchJson<{ status: string; timestamp: string }>('/health');
