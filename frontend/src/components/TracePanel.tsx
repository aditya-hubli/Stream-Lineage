import { useState } from 'react';
import { motion } from 'framer-motion';
import { Route, Search, ArrowDown, ArrowUp, Hash } from 'lucide-react';
import { fetchTrace } from '../lib/api';
import type { TraceResponse, TraceEvent } from '../types/api';
import { ShimmerButton } from './ui/shimmer-button';
import { BorderBeam } from './ui/border-beam';
import { NumberTicker } from './ui/number-ticker';

interface TracePanelProps {
  initialProvId?: string;
}

export function TracePanel({ initialProvId }: TracePanelProps) {
  const [provId, setProvId] = useState(initialProvId ?? '');
  const [result, setResult] = useState<TraceResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleTrace = async () => {
    if (!provId.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const data = await fetchTrace(provId.trim());
      setResult(data);
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
          <Route size={18} className="text-violet-400" />
          <h2 className="text-base font-semibold text-zinc-100">Event Trace</h2>
        </div>
        <p className="text-[13px] text-zinc-500">
          Trace the full lineage of any event — root to leaf.
        </p>
      </div>

      {/* Search */}
      <div className="px-6 py-4 border-b border-zinc-800">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500" />
            <input
              type="text"
              placeholder="Enter prov_id..."
              value={provId}
              onChange={(e) => setProvId(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleTrace()}
              className="w-full pl-9 pr-3 py-2 text-sm rounded-lg bg-zinc-900 border border-zinc-700 text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-violet-600 font-mono"
            />
          </div>
          {/* MagicUI ShimmerButton */}
          <ShimmerButton
            onClick={handleTrace}
            disabled={loading || !provId.trim()}
            shimmerColor="#8b5cf6"
            background="rgba(91, 33, 182, 0.8)"
            borderRadius="8px"
            className="px-4 text-sm font-medium"
          >
            <span className="relative z-10 text-violet-100">
              {loading ? '...' : 'Trace'}
            </span>
          </ShimmerButton>
        </div>
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
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="space-y-1"
          >
            {/* Summary bar — with MagicUI NumberTicker + BorderBeam */}
            <div className="relative overflow-hidden flex items-center gap-4 px-3 py-2 mb-3 rounded-lg bg-zinc-900 border border-zinc-800 text-[12px]">
              <span className="text-zinc-500">
                Depth: <NumberTicker value={result.total_depth} className="text-zinc-300 font-mono text-[12px]" />
              </span>
              <span className="text-zinc-500">
                Ancestors: <NumberTicker value={result.ancestors.length} className="text-zinc-300 font-mono text-[12px]" />
              </span>
              <span className="text-zinc-500">
                Descendants: <NumberTicker value={result.descendants.length} className="text-zinc-300 font-mono text-[12px]" />
              </span>
              <BorderBeam size={80} duration={12} colorFrom="#8b5cf6" colorTo="#3b82f6" borderWidth={1} />
            </div>

            {/* Ancestors */}
            {result.ancestors.map((evt) => (
              <TraceEventCard key={evt.prov_id} event={evt} type="ancestor" />
            ))}

            {/* Current event */}
            <TraceEventCard event={result.event} type="current" />

            {/* Descendants */}
            {result.descendants.map((evt) => (
              <TraceEventCard key={evt.prov_id} event={evt} type="descendant" />
            ))}
          </motion.div>
        )}
      </div>
    </div>
  );
}

function TraceEventCard({
  event,
  type,
}: {
  event: TraceEvent;
  type: 'ancestor' | 'current' | 'descendant';
}) {
  const borderColor =
    type === 'current'
      ? 'border-violet-600/60 bg-violet-950/20'
      : type === 'ancestor'
        ? 'border-blue-800/40 bg-zinc-900'
        : 'border-emerald-800/40 bg-zinc-900';

  const depthIcon =
    type === 'ancestor' ? (
      <ArrowUp size={12} className="text-blue-400" />
    ) : type === 'descendant' ? (
      <ArrowDown size={12} className="text-emerald-400" />
    ) : null;

  return (
    <div className={`relative overflow-hidden px-3 py-2.5 rounded-lg border ${borderColor}`}>
      {/* MagicUI BorderBeam on the current (searched) event */}
      {type === 'current' && (
        <BorderBeam size={60} duration={8} colorFrom="#8b5cf6" colorTo="#6d28d9" borderWidth={1} />
      )}
      <div className="flex items-center gap-2 mb-1.5">
        {depthIcon}
        <span className="text-sm font-medium text-zinc-200">{event.producer_id}</span>
        <span className="ml-auto text-[10px] text-zinc-600 font-mono">
          depth {event.depth}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px]">
        <span className="text-zinc-500 flex items-center gap-1">
          <Hash size={10} />
          <span className="font-mono text-zinc-400 truncate">{event.prov_id.slice(0, 12)}...</span>
        </span>
        <span className="text-zinc-500">
          topic: <span className="text-zinc-400">{event.topic}</span>
        </span>
        {event.transform_ops.length > 0 && (
          <span className="col-span-2 text-zinc-500">
            ops:{' '}
            {event.transform_ops.map((op) => (
              <span
                key={op}
                className="inline-block px-1.5 py-0 mr-1 rounded bg-zinc-800 text-zinc-300 text-[10px]"
              >
                {op}
              </span>
            ))}
          </span>
        )}
      </div>
    </div>
  );
}
