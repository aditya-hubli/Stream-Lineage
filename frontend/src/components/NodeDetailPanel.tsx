import { motion, AnimatePresence } from 'framer-motion';
import { X, Zap, Clock, Hash, Layers } from 'lucide-react';
import type { DagNode } from '../types/api';
import { NumberTicker } from './ui/number-ticker';
import { ShimmerButton } from './ui/shimmer-button';
import { BorderBeam } from './ui/border-beam';

interface NodeDetailPanelProps {
  node: DagNode | null;
  onClose: () => void;
  onRunImpact: (producerId: string) => void;
  onTrace: (producerId: string) => void;
}

export function NodeDetailPanel({ node, onClose, onRunImpact, onTrace }: NodeDetailPanelProps) {
  return (
    <AnimatePresence>
      {node && (
        <motion.div
          initial={{ x: 320, opacity: 0 }}
          animate={{ x: 0, opacity: 1 }}
          exit={{ x: 320, opacity: 0 }}
          transition={{ type: 'spring', duration: 0.4, bounce: 0.1 }}
          className="absolute top-0 right-0 w-80 h-full border-l border-zinc-800 bg-zinc-950/95 backdrop-blur-md z-20 flex flex-col"
        >
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
            <h3 className="text-sm font-semibold text-zinc-100 truncate">{node.producer_id}</h3>
            <button
              onClick={onClose}
              className="p-1 rounded-md hover:bg-zinc-800 text-zinc-400 hover:text-zinc-200 transition-colors"
            >
              <X size={16} />
            </button>
          </div>

          {/* Stats */}
          <div className="px-4 py-4 space-y-3 border-b border-zinc-800">
            <div className="grid grid-cols-2 gap-3">
              <StatCard icon={Zap} label="Events" value={node.event_count.toLocaleString()} />
              <StatCard icon={Layers} label="Topics" value={node.topics.length.toString()} />
            </div>

            <div className="space-y-2">
              <DetailRow icon={Clock} label="First seen" value={formatTime(node.first_seen_at)} />
              <DetailRow icon={Clock} label="Last seen" value={formatTime(node.last_seen_at)} />
              <DetailRow icon={Hash} label="ID" value={node.id} mono />
            </div>

            {/* Topics */}
            <div>
              <span className="text-[11px] text-zinc-500 uppercase tracking-wider">Topics</span>
              <div className="mt-1.5 flex flex-wrap gap-1.5">
                {node.topics.map((t) => (
                  <span
                    key={t}
                    className="px-2 py-0.5 text-[11px] rounded-md bg-zinc-800 text-zinc-300 border border-zinc-700"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </div>
          </div>

          {/* Actions — with MagicUI ShimmerButton */}
          <div className="px-4 py-4 space-y-2">
            <ShimmerButton
              onClick={() => onRunImpact(node.producer_id)}
              shimmerColor="#ef4444"
              background="rgba(127, 29, 29, 0.6)"
              borderRadius="8px"
              className="w-full text-sm"
            >
              <span className="relative z-10 text-red-200">Run Impact Analysis</span>
            </ShimmerButton>
            <ShimmerButton
              onClick={() => onTrace(node.producer_id)}
              shimmerColor="#8b5cf6"
              background="rgba(91, 33, 182, 0.6)"
              borderRadius="8px"
              className="w-full text-sm"
            >
              <span className="relative z-10 text-violet-200">Trace Events</span>
            </ShimmerButton>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

function StatCard({ icon: Icon, label, value }: { icon: typeof Zap; label: string; value: string }) {
  const numericValue = parseInt(value.replace(/,/g, ''), 10);
  const isNumeric = !isNaN(numericValue);

  return (
    <div className="relative overflow-hidden px-3 py-2.5 rounded-lg bg-zinc-900 border border-zinc-800">
      <div className="flex items-center gap-1.5 mb-1">
        <Icon size={12} className="text-zinc-500" />
        <span className="text-[11px] text-zinc-500">{label}</span>
      </div>
      {/* MagicUI NumberTicker for animated stat values */}
      {isNumeric ? (
        <NumberTicker value={numericValue} className="text-lg font-semibold text-zinc-100 font-mono" />
      ) : (
        <span className="text-lg font-semibold text-zinc-100 font-mono">{value}</span>
      )}
      <BorderBeam size={40} duration={10} colorFrom="#8b5cf6" colorTo="#3f3f46" borderWidth={1} />
    </div>
  );
}

function DetailRow({
  icon: Icon,
  label,
  value,
  mono,
}: {
  icon: typeof Clock;
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="flex items-center justify-between text-[12px]">
      <span className="flex items-center gap-1.5 text-zinc-500">
        <Icon size={12} />
        {label}
      </span>
      <span className={`text-zinc-300 truncate max-w-[160px] ${mono ? 'font-mono text-[11px]' : ''}`}>
        {value}
      </span>
    </div>
  );
}

function formatTime(iso: string): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}
