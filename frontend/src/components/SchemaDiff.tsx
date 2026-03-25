import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Columns, Plus, Minus, RefreshCw, ArrowLeftRight, Zap } from 'lucide-react';
import { fetchSchemaDiff, fetchDriftEvents } from '../lib/api';
import type { SchemaDiffResponse, DriftEvent } from '../types/api';

export function SchemaDiff() {
  const [schemaId, setSchemaId] = useState('');
  const [hashA, setHashA] = useState('');
  const [hashB, setHashB] = useState('');
  const [diff, setDiff] = useState<SchemaDiffResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [driftEvents, setDriftEvents] = useState<DriftEvent[]>([]);

  // Load recent drift events on mount so the user can pick one to diff
  useEffect(() => {
    fetchDriftEvents()
      .then(setDriftEvents)
      .catch(() => {/* silently ignore — manual entry still works */});
  }, []);

  const selectDriftEvent = (evt: DriftEvent) => {
    setSchemaId(evt.schema_id);
    setHashA(evt.old_schema_hash);
    setHashB(evt.new_schema_hash);
    setDiff(null);
    setError(null);
  };

  const run = async () => {
    if (!schemaId || !hashA || !hashB) return;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchSchemaDiff(schemaId, hashA, hashB);
      setDiff(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load diff');
      setDiff(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="h-full flex flex-col bg-zinc-950">
      {/* Header */}
      <div className="px-6 py-5 border-b border-zinc-800">
        <div className="flex items-center gap-2.5 mb-1">
          <Columns size={18} className="text-blue-400" />
          <h2 className="text-base font-semibold text-zinc-100">Schema Diff</h2>
        </div>
        <p className="text-[13px] text-zinc-500">
          Side-by-side comparison of two schema versions.
        </p>
      </div>

      {/* Recent drift events — quick-select */}
      {driftEvents.length > 0 && (
        <div className="px-6 py-3 border-b border-zinc-800">
          <p className="text-[11px] text-zinc-500 mb-2 flex items-center gap-1.5">
            <Zap size={11} className="text-amber-400" />
            Recent drift events — click to auto-fill
          </p>
          <div className="flex flex-col gap-1 max-h-36 overflow-y-auto">
            {driftEvents.slice(0, 8).map((evt) => (
              <button
                key={evt.drift_id}
                onClick={() => selectDriftEvent(evt)}
                className="flex items-center gap-2 px-2.5 py-1.5 rounded-md text-left
                           bg-zinc-900 hover:bg-zinc-800 border border-zinc-800
                           hover:border-zinc-600 transition-colors group"
              >
                <span className={`text-[10px] font-medium px-1.5 py-0.5 rounded border
                  ${evt.severity === 'BREAKING'
                    ? 'text-red-400 bg-red-950/40 border-red-800/50'
                    : evt.severity === 'ADDITIVE'
                      ? 'text-amber-400 bg-amber-950/30 border-amber-800/40'
                      : 'text-emerald-400 bg-emerald-950/30 border-emerald-800/40'
                  }`}>
                  {evt.severity}
                </span>
                <span className="text-[12px] font-mono text-zinc-300 truncate flex-1">
                  {evt.schema_id}
                </span>
                <span className="text-[11px] text-zinc-600 shrink-0">
                  {new Date(evt.detected_at).toLocaleTimeString()}
                </span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Inputs */}
      <div className="px-6 py-4 border-b border-zinc-800 space-y-3">
        <div>
          <label className="block text-[11px] text-zinc-500 mb-1">Schema ID</label>
          <input
            type="text"
            placeholder="payments.v4"
            value={schemaId}
            onChange={(e) => setSchemaId(e.target.value)}
            className="w-full px-3 py-2 text-sm bg-zinc-900 border border-zinc-700 rounded-lg
                       text-zinc-200 placeholder-zinc-600 focus:border-blue-500 focus:outline-none"
          />
        </div>
        <div className="flex gap-3 items-end">
          <div className="flex-1">
            <label className="block text-[11px] text-zinc-500 mb-1">Old Schema Hash</label>
            <input
              type="text"
              placeholder="sha256:a3f9..."
              value={hashA}
              onChange={(e) => setHashA(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-zinc-900 border border-zinc-700 rounded-lg
                         text-zinc-200 placeholder-zinc-600 focus:border-blue-500 focus:outline-none font-mono"
            />
          </div>
          <div className="flex-1">
            <label className="block text-[11px] text-zinc-500 mb-1">New Schema Hash</label>
            <input
              type="text"
              placeholder="sha256:b1c2..."
              value={hashB}
              onChange={(e) => setHashB(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-zinc-900 border border-zinc-700 rounded-lg
                         text-zinc-200 placeholder-zinc-600 focus:border-blue-500 focus:outline-none font-mono"
            />
          </div>
          <button
            onClick={run}
            disabled={loading || !schemaId || !hashA || !hashB}
            className="px-4 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-500 text-white
                       disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            Diff
          </button>
        </div>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto px-6 py-4">
        {error && (
          <div className="px-3 py-2 rounded-lg bg-red-950/50 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {diff && (
          <div className="space-y-4">
            {/* Severity badge */}
            <div className="flex items-center gap-3">
              <span className={`px-2.5 py-1 text-xs font-medium rounded-md border ${severityColor(diff.severity)}`}>
                {diff.severity}
              </span>
              <span className="text-sm text-zinc-400">
                {diff.fields_added.length} added, {diff.fields_removed.length} removed, {diff.fields_changed.length} changed
              </span>
            </div>

            {/* Side-by-side view */}
            <div className="grid grid-cols-2 gap-4">
              {/* Old schema */}
              <div className="rounded-lg border border-zinc-800 bg-zinc-900 overflow-hidden">
                <div className="px-4 py-2.5 border-b border-zinc-800 text-sm font-medium text-zinc-400">
                  Old Schema
                  <span className="ml-2 text-[10px] font-mono text-zinc-600">{hashA.slice(0, 16)}...</span>
                </div>
                <div className="px-4 py-3 space-y-1">
                  {diff.all_fields_old.map((f) => {
                    const removed = diff.fields_removed.some((r) => r.name === f.name);
                    const changed = diff.fields_changed.some((c) => c.name === f.name);
                    return (
                      <motion.div
                        key={f.name}
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className={`flex items-center gap-2 text-[12px] px-2 py-1 rounded ${
                          removed
                            ? 'bg-red-950/30 border border-red-800/30'
                            : changed
                              ? 'bg-amber-950/20 border border-amber-800/30'
                              : ''
                        }`}
                      >
                        {removed && <Minus size={12} className="text-red-400 shrink-0" />}
                        {changed && <ArrowLeftRight size={12} className="text-amber-400 shrink-0" />}
                        {!removed && !changed && <span className="w-3 shrink-0" />}
                        <span className={`font-mono ${removed ? 'text-red-300 line-through' : changed ? 'text-amber-300' : 'text-zinc-400'}`}>
                          {f.name}
                        </span>
                        <span className="text-zinc-600">: {f.type}</span>
                      </motion.div>
                    );
                  })}
                </div>
              </div>

              {/* New schema */}
              <div className="rounded-lg border border-zinc-800 bg-zinc-900 overflow-hidden">
                <div className="px-4 py-2.5 border-b border-zinc-800 text-sm font-medium text-zinc-400">
                  New Schema
                  <span className="ml-2 text-[10px] font-mono text-zinc-600">{hashB.slice(0, 16)}...</span>
                </div>
                <div className="px-4 py-3 space-y-1">
                  {diff.all_fields_new.map((f) => {
                    const added = diff.fields_added.some((a) => a.name === f.name);
                    const changed = diff.fields_changed.some((c) => c.name === f.name);
                    return (
                      <motion.div
                        key={f.name}
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className={`flex items-center gap-2 text-[12px] px-2 py-1 rounded ${
                          added
                            ? 'bg-emerald-950/30 border border-emerald-800/30'
                            : changed
                              ? 'bg-amber-950/20 border border-amber-800/30'
                              : ''
                        }`}
                      >
                        {added && <Plus size={12} className="text-emerald-400 shrink-0" />}
                        {changed && <ArrowLeftRight size={12} className="text-amber-400 shrink-0" />}
                        {!added && !changed && <span className="w-3 shrink-0" />}
                        <span className={`font-mono ${added ? 'text-emerald-300' : changed ? 'text-amber-300' : 'text-zinc-400'}`}>
                          {f.name}
                        </span>
                        <span className="text-zinc-600">: {f.type}</span>
                      </motion.div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function severityColor(s: string) {
  switch (s) {
    case 'BREAKING': return 'text-red-400 bg-red-950/40 border-red-800/50';
    case 'ADDITIVE': return 'text-amber-400 bg-amber-950/30 border-amber-800/40';
    case 'COMPATIBLE': return 'text-emerald-400 bg-emerald-950/30 border-emerald-800/40';
    default: return 'text-zinc-400 bg-zinc-900 border-zinc-800';
  }
}
