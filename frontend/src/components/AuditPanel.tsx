import { useState } from 'react';
import { motion } from 'framer-motion';
import {
  ShieldCheck,
  ShieldAlert,
  ShieldX,
  Search,
  Hash,
  Download,
  CheckCircle2,
  XCircle,
  HelpCircle,
} from 'lucide-react';
import { fetchAudit } from '../lib/api';
import type { AuditReport, AuditNode } from '../types/api';
import { ShimmerButton } from './ui/shimmer-button';
import { BorderBeam } from './ui/border-beam';
import { NumberTicker } from './ui/number-ticker';

function formatTime(iso: string): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  const diff = Date.now() - d.getTime();
  if (diff < 60_000) return `${Math.floor(diff / 1000)}s ago`;
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`;
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

const VERDICT_STYLES = {
  VALID: {
    pill: 'bg-emerald-950/60 text-emerald-400 border border-emerald-800/50',
    icon: <CheckCircle2 size={11} className="text-emerald-400" />,
  },
  TAMPERED: {
    pill: 'bg-red-950/60 text-red-400 border border-red-800/50',
    icon: <XCircle size={11} className="text-red-400" />,
  },
  UNVERIFIABLE: {
    pill: 'bg-amber-950/60 text-amber-400 border border-amber-800/50',
    icon: <HelpCircle size={11} className="text-amber-400" />,
  },
};

const OVERALL_STYLES = {
  ALL_VALID: {
    bg: 'bg-emerald-950/30 border-emerald-800/50',
    text: 'text-emerald-300',
    label: 'ALL VALID',
    icon: <ShieldCheck size={18} className="text-emerald-400" />,
    beam: { from: '#10b981', to: '#065f46' },
  },
  TAMPERED: {
    bg: 'bg-red-950/30 border-red-800/50',
    text: 'text-red-300',
    label: 'TAMPERED',
    icon: <ShieldX size={18} className="text-red-400" />,
    beam: { from: '#ef4444', to: '#7f1d1d' },
  },
  PARTIAL: {
    bg: 'bg-amber-950/30 border-amber-800/50',
    text: 'text-amber-300',
    label: 'PARTIAL',
    icon: <ShieldAlert size={18} className="text-amber-400" />,
    beam: { from: '#f59e0b', to: '#78350f' },
  },
};

function AuditNodeCard({ node, index }: { node: AuditNode; index: number }) {
  const style = VERDICT_STYLES[node.verdict];
  return (
    <motion.div
      initial={{ opacity: 0, x: -8 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: index * 0.03, duration: 0.2 }}
      className="flex items-start gap-3 px-3 py-2.5 rounded-lg bg-zinc-900 border border-zinc-800 hover:border-zinc-700 transition-colors"
    >
      {/* Verdict pill */}
      <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0 mt-0.5 ${style.pill}`}>
        {style.icon}
        {node.verdict}
      </span>

      {/* Main content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-medium text-zinc-200">{node.producer_id}</span>
          <span className="text-[11px] text-zinc-500">{node.topic}</span>
        </div>
        <div className="flex items-center gap-3 mt-1 flex-wrap">
          <span className="font-mono text-[11px] text-zinc-500">
            p{node.partition}:o{node.offset}
          </span>
          <span className="flex items-center gap-1 font-mono text-[11px] text-zinc-600">
            <Hash size={9} />
            {node.chain_hash.slice(0, 16)}…
          </span>
          {node.transform_ops.length > 0 && (
            <span className="flex items-center gap-1 flex-wrap">
              {node.transform_ops.map((op) => (
                <span
                  key={op}
                  className="px-1.5 py-0 rounded bg-zinc-800 text-zinc-400 text-[10px]"
                >
                  {op}
                </span>
              ))}
            </span>
          )}
        </div>
      </div>

      {/* Time */}
      <span className="text-[10px] text-zinc-600 shrink-0 mt-0.5">{formatTime(node.ingested_at)}</span>
    </motion.div>
  );
}

export function AuditPanel() {
  const [provId, setProvId] = useState('');
  const [result, setResult] = useState<AuditReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleAudit = async () => {
    if (!provId.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const data = await fetchAudit(provId.trim());
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Audit failed');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = () => {
    if (!result) return;
    const blob = new Blob([JSON.stringify(result, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `audit-${result.queried_prov_id.slice(0, 8)}-${result.report_id.slice(0, 8)}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const overall = result ? OVERALL_STYLES[result.overall_verdict] : null;

  return (
    <div className="h-full flex flex-col bg-zinc-950">
      {/* Header */}
      <div className="px-6 py-5 border-b border-zinc-800">
        <div className="flex items-center gap-2.5 mb-1">
          <ShieldCheck size={18} className="text-violet-400" />
          <h2 className="text-base font-semibold text-zinc-100">Chain Audit</h2>
        </div>
        <p className="text-[13px] text-zinc-500">
          Cryptographically verify the integrity of any event's lineage chain.
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
              onKeyDown={(e) => e.key === 'Enter' && handleAudit()}
              className="w-full pl-9 pr-3 py-2 text-sm rounded-lg bg-zinc-900 border border-zinc-700 text-zinc-200 placeholder-zinc-600 focus:outline-none focus:border-violet-600 font-mono"
            />
          </div>
          <ShimmerButton
            onClick={handleAudit}
            disabled={loading || !provId.trim()}
            shimmerColor="#8b5cf6"
            background="rgba(91, 33, 182, 0.8)"
            borderRadius="8px"
            className="px-4 text-sm font-medium"
          >
            <span className="relative z-10 text-violet-100">
              {loading ? '...' : 'Audit'}
            </span>
          </ShimmerButton>
        </div>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {error && (
          <div className="px-3 py-2 rounded-lg bg-red-950/50 border border-red-800/50 text-red-300 text-sm">
            {error}
          </div>
        )}

        {result && overall && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-4"
          >
            {/* Overall verdict banner */}
            <div className={`relative overflow-hidden px-4 py-3 rounded-lg border ${overall.bg}`}>
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  {overall.icon}
                  <span className={`text-sm font-bold tracking-widest ${overall.text}`}>
                    {overall.label}
                  </span>
                </div>
                <button
                  onClick={handleExport}
                  className="flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-zinc-800/60 hover:bg-zinc-700/60 text-zinc-400 hover:text-zinc-200 text-[11px] transition-colors"
                >
                  <Download size={11} />
                  Export JSON
                </button>
              </div>
              <div className="flex items-center gap-1.5 mt-2">
                <Hash size={10} className="text-zinc-600" />
                <span className="font-mono text-[11px] text-zinc-500 truncate">
                  {result.report_hash}
                </span>
              </div>
              <BorderBeam size={80} duration={10} colorFrom={overall.beam.from} colorTo={overall.beam.to} borderWidth={1} />
            </div>

            {/* Stats row */}
            <div className="grid grid-cols-3 gap-2">
              <div className="relative overflow-hidden px-3 py-2.5 rounded-lg bg-emerald-950/20 border border-emerald-900/40">
                <span className="text-[10px] text-zinc-500 uppercase tracking-wider block">Valid</span>
                <p className="text-lg font-bold text-emerald-300 font-mono">
                  <NumberTicker value={result.valid_count} className="text-emerald-300" />
                </p>
              </div>
              <div className="relative overflow-hidden px-3 py-2.5 rounded-lg bg-red-950/20 border border-red-900/40">
                <span className="text-[10px] text-zinc-500 uppercase tracking-wider block">Tampered</span>
                <p className="text-lg font-bold text-red-300 font-mono">
                  <NumberTicker value={result.tampered_count} className="text-red-300" />
                </p>
              </div>
              <div className="relative overflow-hidden px-3 py-2.5 rounded-lg bg-amber-950/20 border border-amber-900/40">
                <span className="text-[10px] text-zinc-500 uppercase tracking-wider block">Unknown</span>
                <p className="text-lg font-bold text-amber-300 font-mono">
                  <NumberTicker value={result.unverifiable_count} className="text-amber-300" />
                </p>
              </div>
            </div>

            {/* Chain */}
            <div>
              <h4 className="text-[11px] text-zinc-500 uppercase tracking-wider mb-2">
                Lineage Chain — {result.total_nodes_audited} nodes
              </h4>
              <div className="space-y-1.5">
                {result.chain.map((node, i) => (
                  <AuditNodeCard key={node.prov_id} node={node} index={i} />
                ))}
              </div>
            </div>
          </motion.div>
        )}
      </div>
    </div>
  );
}
