import { motion } from 'framer-motion';
import {
  Network,
  Target,
  Route,
  AlertTriangle,
  GitCompare,
  Columns,
  Activity,
  ChevronRight,
  ShieldAlert,
  ShieldCheck,
} from 'lucide-react';
import { NumberTicker } from './ui/number-ticker';
import { AnimatedShinyText } from './ui/animated-shiny-text';
import { BorderBeam } from './ui/border-beam';

export type View = 'dag' | 'impact' | 'trace' | 'drift' | 'diff' | 'schema-diff' | 'audit';

interface SidebarProps {
  currentView: View;
  onViewChange: (view: View) => void;
  wsStatus: 'connecting' | 'connected' | 'disconnected';
  eventCount: number;
  nodeCount: number;
  edgeCount: number;
  hashFailureCount: number;
}

const NAV_ITEMS: { id: View; label: string; icon: typeof Network }[] = [
  { id: 'dag', label: 'Live DAG', icon: Network },
  { id: 'impact', label: 'Impact Analysis', icon: Target },
  { id: 'trace', label: 'Event Trace', icon: Route },
  { id: 'drift', label: 'Schema Drift', icon: AlertTriangle },
  { id: 'diff', label: 'Pipeline Diff', icon: GitCompare },
  { id: 'schema-diff', label: 'Schema Diff', icon: Columns },
  { id: 'audit', label: 'Chain Audit', icon: ShieldCheck },
];

export function Sidebar({
  currentView,
  onViewChange,
  wsStatus,
  eventCount,
  nodeCount,
  edgeCount,
  hashFailureCount,
}: SidebarProps) {
  return (
    <div className="w-60 h-full border-r border-zinc-800 bg-zinc-950 flex flex-col">
      {/* Logo — with MagicUI BorderBeam */}
      <div className="relative px-5 py-5 border-b border-zinc-800 overflow-hidden rounded-br-lg">
        <div className="flex items-center gap-2.5">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-violet-600 to-purple-800 flex items-center justify-center">
            <Activity size={16} className="text-white" />
          </div>
          <div>
            <h1 className="text-sm font-semibold text-zinc-100 leading-tight">StreamLineage</h1>
            <p className="text-[10px] text-zinc-500 leading-tight">Provenance Engine</p>
          </div>
        </div>
        <BorderBeam size={80} duration={12} colorFrom="#8b5cf6" colorTo="#6d28d9" borderWidth={1} />
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-3 space-y-0.5">
        {NAV_ITEMS.map((item) => {
          const active = currentView === item.id;
          return (
            <button
              key={item.id}
              onClick={() => onViewChange(item.id)}
              className={`
                w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm
                transition-colors relative
                ${active
                  ? 'text-zinc-100 bg-zinc-800/80'
                  : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/40'
                }
              `}
            >
              {active && (
                <motion.div
                  layoutId="sidebar-active"
                  className="absolute inset-0 rounded-lg bg-zinc-800/80"
                  transition={{ type: 'spring', duration: 0.4, bounce: 0.15 }}
                />
              )}
              <item.icon size={16} className="relative z-10" />
              <span className="relative z-10">{item.label}</span>
              {active && (
                <ChevronRight size={14} className="relative z-10 ml-auto text-zinc-500" />
              )}
            </button>
          );
        })}
      </nav>

      {/* Status — with MagicUI AnimatedShinyText + NumberTicker */}
      <div className="px-4 py-4 border-t border-zinc-800 space-y-2">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-zinc-500">Status</span>
          <span className="flex items-center gap-1.5">
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                wsStatus === 'connected'
                  ? 'bg-emerald-400'
                  : wsStatus === 'connecting'
                    ? 'bg-amber-400 animate-pulse'
                    : 'bg-zinc-600'
              }`}
            />
            {wsStatus === 'connected' ? (
              <AnimatedShinyText className="text-[11px] text-emerald-400" shimmerWidth={60}>
                connected
              </AnimatedShinyText>
            ) : (
              <span className="text-zinc-500 text-[11px]">{wsStatus}</span>
            )}
          </span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-zinc-500">Nodes / Edges</span>
          <span className="text-zinc-300 font-mono">
            <NumberTicker value={nodeCount} className="text-zinc-300" /> / <NumberTicker value={edgeCount} className="text-zinc-300" />
          </span>
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-zinc-500">WS Events</span>
          <NumberTicker value={eventCount} className="text-zinc-300 font-mono text-[11px]" />
        </div>
        <div className="flex items-center justify-between text-[11px]">
          <span className="flex items-center gap-1 text-zinc-500">
            <ShieldAlert size={11} className={hashFailureCount > 0 ? 'text-red-400' : 'text-zinc-500'} />
            Hash Failures
          </span>
          <span className={`font-mono text-[11px] ${hashFailureCount > 0 ? 'text-red-400' : 'text-zinc-500'}`}>
            {hashFailureCount}
          </span>
        </div>
      </div>
    </div>
  );
}
