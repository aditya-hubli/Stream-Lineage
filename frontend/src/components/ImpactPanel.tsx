import { useState } from 'react';
import { motion } from 'framer-motion';
import { Target, AlertTriangle, ArrowRight } from 'lucide-react';
import { fetchImpact } from '../lib/api';
import type { ImpactResponse, DagNode } from '../types/api';
import { ShimmerButton } from './ui/shimmer-button';
import { BorderBeam } from './ui/border-beam';
import { NumberTicker } from './ui/number-ticker';
import { Meteors } from './ui/meteors';

interface ImpactPanelProps {
  nodes: DagNode[];
  onHighlightNodes: (nodeIds: string[]) => void;
  initialProducerId?: string;
}

export function ImpactPanel({ nodes, onHighlightNodes, initialProducerId }: ImpactPanelProps) {
  const [producerId, setProducerId] = useState(initialProducerId ?? '');
  const [fromTime, setFromTime] = useState('');
  const [untilTime, setUntilTime] = useState('');
  const [result, setResult] = useState<ImpactResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleRun = async () => {
    if (!producerId || !fromTime || !untilTime) return;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchImpact(producerId, fromTime, untilTime);
      setResult(data);
      // Highlight affected nodes on the DAG
      const affected = [producerId, ...data.affected_downstream_nodes.map((n) => n.node)];
      onHighlightNodes(affected);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="h-full flex flex-col bg-zinc-950">
      {/* Header */}
      <div className="px-6 py-5 border-b border-zinc-800">
        <div className="flex items-center gap-2.5 mb-1">
          <Target size={18} className="text-red-400" />
          <h2 className="text-base font-semibold text-zinc-100">Impact Analysis</h2>
        </div>
        <p className="text-[13px] text-zinc-500">
          Identify the blast radius of corrupted data from a producer.
        </p>
      </div>

      {/* Form */}
      <div className="px-6 py-4 space-y-3 border-b border-zinc-800">
        <div>
          <label className="text-[11px] text-zinc-500 uppercase tracking-wider">Producer</label>
          <select
            value={producerId}
            onChange={(e) => setProducerId(e.target.value)}
            className="mt-1 w-full px-3 py-2 text-sm rounded-lg bg-zinc-900 border border-zinc-700 text-zinc-200 focus:outline-none focus:border-violet-600"
          >
            <option value="">Select producer...</option>
            {nodes.map((n) => (
              <option key={n.id} value={n.producer_id}>
                {n.producer_id}
              </option>
            ))}
          </select>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="text-[11px] text-zinc-500 uppercase tracking-wider">From</label>
            <input
              type="datetime-local"
              value={fromTime}
              onChange={(e) => setFromTime(e.target.value)}
              className="mt-1 w-full px-3 py-2 text-sm rounded-lg bg-zinc-900 border border-zinc-700 text-zinc-200 focus:outline-none focus:border-violet-600"
            />
          </div>
          <div>
            <label className="text-[11px] text-zinc-500 uppercase tracking-wider">Until</label>
            <input
              type="datetime-local"
              value={untilTime}
              onChange={(e) => setUntilTime(e.target.value)}
              className="mt-1 w-full px-3 py-2 text-sm rounded-lg bg-zinc-900 border border-zinc-700 text-zinc-200 focus:outline-none focus:border-violet-600"
            />
          </div>
        </div>
        {/* MagicUI ShimmerButton */}
        <ShimmerButton
          onClick={handleRun}
          disabled={loading || !producerId || !fromTime || !untilTime}
          shimmerColor="#ef4444"
          shimmerSize="0.05em"
          background="rgba(127, 29, 29, 0.8)"
          borderRadius="8px"
          className="w-full text-sm font-medium"
        >
          <span className="relative z-10 text-red-100">
            {loading ? 'Analyzing...' : 'Run Impact Analysis'}
          </span>
        </ShimmerButton>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto px-6 py-4">
        {error && (
          <div className="px-3 py-2 rounded-lg bg-red-950/50 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {result && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-4"
          >
            {/* Summary — with MagicUI BorderBeam + NumberTicker + Aceternity Meteors */}
            <div className="grid grid-cols-2 gap-3">
              <div className="relative overflow-hidden px-3 py-3 rounded-lg bg-red-950/30 border border-red-900/40">
                <span className="text-[11px] text-zinc-500">Total Affected</span>
                <p className="text-xl font-bold text-red-300 font-mono">
                  <NumberTicker value={result.total_affected_events} className="text-red-300" />
                </p>
                <BorderBeam size={60} duration={8} colorFrom="#ef4444" colorTo="#991b1b" />
                <Meteors number={6} />
              </div>
              <div className="relative overflow-hidden px-3 py-3 rounded-lg bg-zinc-900 border border-zinc-800">
                <span className="text-[11px] text-zinc-500">Downstream Nodes</span>
                <p className="text-xl font-bold text-zinc-100 font-mono">
                  <NumberTicker value={result.affected_downstream_nodes.length} className="text-zinc-100" />
                </p>
                <BorderBeam size={60} duration={10} colorFrom="#8b5cf6" colorTo="#6d28d9" />
              </div>
            </div>

            {/* Affected nodes list */}
            <div>
              <h4 className="text-[11px] text-zinc-500 uppercase tracking-wider mb-2">
                Affected Downstream
              </h4>
              <div className="space-y-1.5">
                {result.affected_downstream_nodes.map((n) => (
                  <div
                    key={n.node}
                    className="flex items-center gap-2 px-3 py-2 rounded-lg bg-zinc-900 border border-zinc-800"
                  >
                    <AlertTriangle size={12} className="text-red-400 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <span className="text-sm text-zinc-200 block truncate">{n.node}</span>
                      <span className="text-[11px] text-zinc-500">{n.topic}</span>
                    </div>
                    <div className="text-right shrink-0">
                      <span className="text-sm font-mono text-red-300">
                        {n.affected_events.toLocaleString()}
                      </span>
                      <span className="block text-[10px] text-zinc-600">
                        hop {n.distance_from_source}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Replay offsets */}
            {Object.keys(result.replay_kafka_offsets).length > 0 && (
              <div>
                <h4 className="text-[11px] text-zinc-500 uppercase tracking-wider mb-2">
                  Replay Offsets
                </h4>
                {Object.entries(result.replay_kafka_offsets).map(([topic, range]) => (
                  <div
                    key={topic}
                    className="flex items-center gap-2 px-3 py-2 rounded-lg bg-zinc-900 border border-zinc-800 text-sm"
                  >
                    <span className="text-zinc-400 truncate flex-1">{topic}</span>
                    <ArrowRight size={12} className="text-zinc-600" />
                    <span className="font-mono text-amber-300 text-[12px]">
                      {range[0]}..{range[1]}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </motion.div>
        )}
      </div>
    </div>
  );
}
