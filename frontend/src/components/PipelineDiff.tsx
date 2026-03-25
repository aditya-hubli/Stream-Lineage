import { useState } from 'react';
import { motion } from 'framer-motion';
import { GitCompare, Plus, Minus, ArrowRight, RefreshCw } from 'lucide-react';
import { fetchDiff } from '../lib/api';
import type { DiffResponse } from '../types/api';
import { BorderBeam } from './ui/border-beam';

export function PipelineDiff() {
  const [snapshotA, setSnapshotA] = useState('');
  const [snapshotB, setSnapshotB] = useState('');
  const [diff, setDiff] = useState<DiffResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = async () => {
    if (!snapshotA || !snapshotB) return;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchDiff(snapshotA, snapshotB);
      setDiff(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Diff failed');
      setDiff(null);
    } finally {
      setLoading(false);
    }
  };

  const hasChanges = diff && (
    diff.nodes_added.length > 0 ||
    diff.nodes_removed.length > 0 ||
    diff.edges_added.length > 0 ||
    diff.edges_removed.length > 0
  );

  return (
    <div className="h-full flex flex-col bg-zinc-950">
      {/* Header */}
      <div className="px-6 py-5 border-b border-zinc-800">
        <div className="flex items-center gap-2.5 mb-1">
          <GitCompare size={18} className="text-violet-400" />
          <h2 className="text-base font-semibold text-zinc-100">Pipeline Diff</h2>
        </div>
        <p className="text-[13px] text-zinc-500">
          Compare two DAG snapshots to see what changed in data flow.
        </p>
      </div>

      {/* Inputs */}
      <div className="px-6 py-4 border-b border-zinc-800 flex items-end gap-3">
        <div className="flex-1">
          <label className="block text-[11px] text-zinc-500 mb-1">Snapshot A (ISO8601)</label>
          <input
            type="text"
            placeholder="2025-06-01T10:00:00Z"
            value={snapshotA}
            onChange={(e) => setSnapshotA(e.target.value)}
            className="w-full px-3 py-2 text-sm bg-zinc-900 border border-zinc-700 rounded-lg
                       text-zinc-200 placeholder-zinc-600 focus:border-violet-500 focus:outline-none"
          />
        </div>
        <div className="flex-1">
          <label className="block text-[11px] text-zinc-500 mb-1">Snapshot B (ISO8601)</label>
          <input
            type="text"
            placeholder="2025-06-02T10:00:00Z"
            value={snapshotB}
            onChange={(e) => setSnapshotB(e.target.value)}
            className="w-full px-3 py-2 text-sm bg-zinc-900 border border-zinc-700 rounded-lg
                       text-zinc-200 placeholder-zinc-600 focus:border-violet-500 focus:outline-none"
          />
        </div>
        <button
          onClick={run}
          disabled={loading || !snapshotA || !snapshotB}
          className="px-4 py-2 text-sm rounded-lg bg-violet-600 hover:bg-violet-500 text-white
                     disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          Compare
        </button>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {error && (
          <div className="px-3 py-2 rounded-lg bg-red-950/50 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {diff && !hasChanges && (
          <div className="text-center py-12 text-zinc-600 text-sm">
            No differences found between the two snapshots.
          </div>
        )}

        {diff && hasChanges && (
          <>
            {/* Summary */}
            <div className="grid grid-cols-4 gap-3">
              <SummaryCard label="Nodes Added" count={diff.nodes_added.length} color="emerald" />
              <SummaryCard label="Nodes Removed" count={diff.nodes_removed.length} color="red" />
              <SummaryCard label="Edges Added" count={diff.edges_added.length} color="emerald" />
              <SummaryCard label="Edges Removed" count={diff.edges_removed.length} color="red" />
            </div>

            {/* Node changes */}
            {diff.nodes_added.length > 0 && (
              <Section title="Nodes Added">
                {diff.nodes_added.map((n) => (
                  <motion.div
                    key={n}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center gap-2 text-sm"
                  >
                    <Plus size={14} className="text-emerald-400" />
                    <span className="font-mono text-emerald-300">{n}</span>
                  </motion.div>
                ))}
              </Section>
            )}

            {diff.nodes_removed.length > 0 && (
              <Section title="Nodes Removed">
                {diff.nodes_removed.map((n) => (
                  <motion.div
                    key={n}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center gap-2 text-sm"
                  >
                    <Minus size={14} className="text-red-400" />
                    <span className="font-mono text-red-300 line-through">{n}</span>
                  </motion.div>
                ))}
              </Section>
            )}

            {/* Edge changes */}
            {diff.edges_added.length > 0 && (
              <Section title="Edges Added">
                {diff.edges_added.map((e, i) => (
                  <motion.div
                    key={`a-${i}`}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center gap-2 text-sm"
                  >
                    <Plus size={14} className="text-emerald-400" />
                    <span className="font-mono text-zinc-300">{e.source}</span>
                    <ArrowRight size={12} className="text-zinc-600" />
                    <span className="font-mono text-zinc-300">{e.target}</span>
                  </motion.div>
                ))}
              </Section>
            )}

            {diff.edges_removed.length > 0 && (
              <Section title="Edges Removed">
                {diff.edges_removed.map((e, i) => (
                  <motion.div
                    key={`r-${i}`}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    className="flex items-center gap-2 text-sm"
                  >
                    <Minus size={14} className="text-red-400" />
                    <span className="font-mono text-zinc-500 line-through">{e.source}</span>
                    <ArrowRight size={12} className="text-zinc-700" />
                    <span className="font-mono text-zinc-500 line-through">{e.target}</span>
                  </motion.div>
                ))}
              </Section>
            )}

            {/* Transform changes */}
            {diff.transform_changes && diff.transform_changes.length > 0 && (
              <Section title="Transform Changes">
                {diff.transform_changes.map((tc, i) => (
                  <motion.div
                    key={`tc-${i}`}
                    initial={{ opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="relative rounded-lg border border-zinc-800 bg-zinc-900 p-3"
                  >
                    <BorderBeam size={60} duration={8} colorFrom="#8b5cf6" colorTo="#6d28d9" borderWidth={1} />
                    <div className="text-sm font-mono text-zinc-200 mb-2">{tc.node}</div>
                    <div className="flex items-center gap-3 text-[12px]">
                      <div className="flex gap-1 flex-wrap">
                        {tc.old_ops.map((op) => (
                          <span key={op} className="px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-500">
                            {op}
                          </span>
                        ))}
                      </div>
                      <ArrowRight size={12} className="text-zinc-600 shrink-0" />
                      <div className="flex gap-1 flex-wrap">
                        {tc.new_ops.map((op) => {
                          const isNew = !tc.old_ops.includes(op);
                          return (
                            <span
                              key={op}
                              className={`px-1.5 py-0.5 rounded ${
                                isNew
                                  ? 'bg-emerald-950/40 text-emerald-400 border border-emerald-800/40'
                                  : 'bg-zinc-800 text-zinc-400'
                              }`}
                            >
                              {op}
                            </span>
                          );
                        })}
                      </div>
                    </div>
                  </motion.div>
                ))}
              </Section>
            )}

            {/* Schema changes */}
            {diff.schema_changes && diff.schema_changes.length > 0 && (
              <Section title="Schema Changes">
                {diff.schema_changes.map((sc, i) => (
                  <motion.div
                    key={`sc-${i}`}
                    initial={{ opacity: 0, y: 4 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="flex items-center gap-3 text-sm"
                  >
                    <span className="font-mono text-zinc-400">{sc.edge_source} → {sc.edge_target}</span>
                    <ArrowRight size={12} className="text-zinc-600" />
                    <span className="font-mono text-zinc-200">{[...sc.schemas_added, ...sc.schemas_removed].join(', ')}</span>
                    {sc.breaking && (
                      <span className="px-1.5 py-0.5 text-[10px] rounded bg-red-950/40 text-red-400 border border-red-800/40">
                        BREAKING
                      </span>
                    )}
                  </motion.div>
                ))}
              </Section>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function SummaryCard({ label, count, color }: { label: string; count: number; color: 'emerald' | 'red' }) {
  const colorMap = {
    emerald: count > 0 ? 'text-emerald-400 border-emerald-800/40 bg-emerald-950/20' : 'text-zinc-600 border-zinc-800 bg-zinc-900',
    red: count > 0 ? 'text-red-400 border-red-800/40 bg-red-950/20' : 'text-zinc-600 border-zinc-800 bg-zinc-900',
  };
  return (
    <div className={`rounded-lg border p-3 text-center ${colorMap[color]}`}>
      <div className="text-2xl font-bold font-mono">{count}</div>
      <div className="text-[11px] text-zinc-500">{label}</div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900/50 overflow-hidden">
      <div className="px-4 py-2.5 border-b border-zinc-800 text-sm font-medium text-zinc-300">
        {title}
      </div>
      <div className="px-4 py-3 space-y-2">
        {children}
      </div>
    </div>
  );
}
