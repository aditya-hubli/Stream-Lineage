import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { AlertTriangle, Plus, Minus, RefreshCw } from 'lucide-react';
import { fetchDriftEvents } from '../lib/api';
import type { DriftEvent } from '../types/api';
import { BorderBeam } from './ui/border-beam';

export function DriftPanel() {
  const [drifts, setDrifts] = useState<DriftEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetchDriftEvents();
      setDrifts(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const severityColor = (s: string) => {
    switch (s) {
      case 'BREAKING': return 'text-red-400 bg-red-950/40 border-red-800/50';
      case 'ADDITIVE': return 'text-amber-400 bg-amber-950/30 border-amber-800/40';
      case 'COMPATIBLE': return 'text-emerald-400 bg-emerald-950/30 border-emerald-800/40';
      default: return 'text-zinc-400 bg-zinc-900 border-zinc-800';
    }
  };

  return (
    <div className="h-full flex flex-col bg-zinc-950">
      {/* Header */}
      <div className="px-6 py-5 border-b border-zinc-800 flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2.5 mb-1">
            <AlertTriangle size={18} className="text-amber-400" />
            <h2 className="text-base font-semibold text-zinc-100">Schema Drift</h2>
          </div>
          <p className="text-[13px] text-zinc-500">
            Schema changes detected across your pipeline.
          </p>
        </div>
        <button
          onClick={load}
          className="p-2 rounded-lg hover:bg-zinc-800 text-zinc-500 hover:text-zinc-300 transition-colors"
        >
          <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-3">
        {error && (
          <div className="px-3 py-2 rounded-lg bg-red-950/50 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {!loading && drifts.length === 0 && !error && (
          <div className="text-center py-12 text-zinc-600 text-sm">
            No schema drift events detected yet.
          </div>
        )}

        {drifts.map((drift) => (
          <motion.div
            key={drift.drift_id}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            className="relative rounded-lg border border-zinc-800 bg-zinc-900 overflow-hidden"
          >
            {/* MagicUI BorderBeam on BREAKING drift events */}
            {drift.severity === 'BREAKING' && (
              <BorderBeam size={80} duration={6} colorFrom="#ef4444" colorTo="#f97316" borderWidth={1.5} />
            )}
            {/* Drift header */}
            <div className="px-4 py-3 flex items-center gap-3">
              <span className={`px-2 py-0.5 text-[11px] font-medium rounded-md border ${severityColor(drift.severity)}`}>
                {drift.severity}
              </span>
              <span className="text-sm text-zinc-200 font-mono">{drift.schema_id}</span>
              <span className="ml-auto text-[11px] text-zinc-600">
                {new Date(drift.detected_at).toLocaleString()}
              </span>
            </div>

            {/* Diff details */}
            {drift.diff && (
              <div className="px-4 pb-3 space-y-1.5">
                {drift.diff.fields_added.map((f) => (
                  <div key={f.name} className="flex items-center gap-2 text-[12px]">
                    <Plus size={12} className="text-emerald-400" />
                    <span className="text-emerald-300 font-mono">{f.name}</span>
                    <span className="text-zinc-600">: {f.type}</span>
                  </div>
                ))}
                {drift.diff.fields_removed.map((f) => (
                  <div key={f.name} className="flex items-center gap-2 text-[12px]">
                    <Minus size={12} className="text-red-400" />
                    <span className="text-red-300 font-mono">{f.name}</span>
                    <span className="text-zinc-600">: {f.type}</span>
                  </div>
                ))}
                {drift.diff.fields_changed.map((f) => (
                  <div key={f.name} className="flex items-center gap-2 text-[12px]">
                    <RefreshCw size={12} className="text-amber-400" />
                    <span className="text-amber-300 font-mono">{f.name}</span>
                    <span className="text-zinc-600">
                      {f.old_type} → {f.new_type}
                    </span>
                  </div>
                ))}
                {drift.affected_producer_id && (
                  <div className="text-[11px] text-zinc-500 pt-1">
                    Producer: <span className="text-zinc-400">{drift.affected_producer_id}</span>
                  </div>
                )}
              </div>
            )}
          </motion.div>
        ))}
      </div>
    </div>
  );
}
