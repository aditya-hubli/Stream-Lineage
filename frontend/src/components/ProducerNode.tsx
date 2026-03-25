import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { Box, Zap, AlertTriangle } from 'lucide-react';
import { BorderBeam } from './ui/border-beam';

export interface ProducerNodeData {
  label: string;
  eventCount: number;
  topics: string[];
  lastSeenAt: string;
  isSelected: boolean;
  isImpacted: boolean;
  status?: 'active' | 'stale' | 'dead';
}

export const ProducerNode = memo(({ data }: NodeProps & { data: ProducerNodeData }) => {
  const d = data as ProducerNodeData;

  return (
    <div
      className={`
        relative px-4 py-3 rounded-xl border backdrop-blur-sm
        min-w-[180px] transition-all duration-300
        ${d.isImpacted
          ? 'border-red-500/60 bg-red-950/40 shadow-[0_0_20px_rgba(239,68,68,0.15)]'
          : d.isSelected
            ? 'border-violet-500/60 bg-violet-950/30 shadow-[0_0_20px_rgba(139,92,246,0.15)]'
            : 'border-zinc-700/60 bg-zinc-900/80 hover:border-zinc-600'
        }
      `}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-2 !h-2 !bg-zinc-500 !border-zinc-700"
      />

      {/* Status indicator */}
      <div className="absolute -top-1 -right-1">
        <span
          className={`block w-2.5 h-2.5 rounded-full ${
            d.status === 'active'
              ? 'bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.6)]'
              : d.status === 'stale'
                ? 'bg-amber-400 shadow-[0_0_6px_rgba(251,191,36,0.6)]'
                : d.status === 'dead'
                  ? 'bg-red-600'
                  : 'bg-zinc-600'
          }`}
        />
      </div>

      {/* Header */}
      <div className="flex items-center gap-2 mb-1.5">
        {d.isImpacted ? (
          <AlertTriangle size={14} className="text-red-400 shrink-0" />
        ) : (
          <Box size={14} className="text-violet-400 shrink-0" />
        )}
        <span className="text-sm font-medium text-zinc-100 truncate">{d.label}</span>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-3 text-[11px] text-zinc-400">
        <span className="flex items-center gap-1">
          <Zap size={10} className="text-amber-400" />
          {d.eventCount.toLocaleString()}
        </span>
        <span className="truncate max-w-[100px]">{d.topics[0] || 'no topic'}</span>
      </div>

      {/* MagicUI BorderBeam on impacted or selected nodes */}
      {d.isImpacted && (
        <BorderBeam size={100} duration={4} colorFrom="#ef4444" colorTo="#f97316" borderWidth={1.5} />
      )}
      {d.isSelected && !d.isImpacted && (
        <BorderBeam size={100} duration={6} colorFrom="#8b5cf6" colorTo="#6d28d9" borderWidth={1} />
      )}

      <Handle
        type="source"
        position={Position.Right}
        className="!w-2 !h-2 !bg-zinc-500 !border-zinc-700"
      />
    </div>
  );
});

ProducerNode.displayName = 'ProducerNode';
