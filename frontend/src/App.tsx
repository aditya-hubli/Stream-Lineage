import { useState, useEffect, useRef } from 'react';
import { ShieldAlert, X } from 'lucide-react';
import { Sidebar, type View } from './components/Sidebar';
import { DagView } from './components/DagView';
import { ImpactPanel } from './components/ImpactPanel';
import { TracePanel } from './components/TracePanel';
import { DriftPanel } from './components/DriftPanel';
import { PipelineDiff } from './components/PipelineDiff';
import { SchemaDiff } from './components/SchemaDiff';
import { AuditPanel } from './components/AuditPanel';
import { useDag } from './hooks/useDag';
import { useWebSocket } from './hooks/useWebSocket';
import { SimulatorPage } from './simulator/SimulatorPage';

interface HashFailure {
  id: string;
  prov_id: string;
  producer_id: string;
  topic: string;
  detected_at: string;
}

// Show demo mode when ?demo in URL, OR when no ?live param (default for GitHub Pages)
function isDemoMode(): boolean {
  const params = new URLSearchParams(window.location.search);
  if (params.has('demo')) return true;
  if (params.has('live')) return false;
  return true; // default: demo
}

export default function App() {
  const [demoMode, setDemoMode] = useState(isDemoMode);
  const [view, setView] = useState<View>('dag');
  const [impactedNodes, setImpactedNodes] = useState<string[]>([]);
  const [hashFailures, setHashFailures] = useState<HashFailure[]>([]);
  const [failedProducers, setFailedProducers] = useState<string[]>([]);
  const [bannerFailure, setBannerFailure] = useState<HashFailure | null>(null);
  const bannerTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const { dag, refresh } = useDag(5000);
  const wsUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/lineage`;
  const { status: wsStatus, eventCount, lastEvent } = useWebSocket(wsUrl);

  if (lastEvent?.event_type?.startsWith('dag.')) {
    refresh();
  }

  useEffect(() => {
    if (lastEvent?.event_type !== 'chain.hash.failure') return;
    const data = lastEvent.data as Record<string, string>;
    const failure: HashFailure = {
      id: crypto.randomUUID(),
      prov_id: data.prov_id ?? '',
      producer_id: data.producer_id ?? '',
      topic: data.topic ?? '',
      detected_at: data.detected_at ?? new Date().toISOString(),
    };
    setHashFailures((prev) => [failure, ...prev].slice(0, 100));
    setFailedProducers((prev) => [...new Set([...prev, failure.producer_id])]);
    setBannerFailure(failure);
    clearTimeout(bannerTimer.current);
    bannerTimer.current = setTimeout(() => setBannerFailure(null), 8000);
  }, [lastEvent]);

  if (demoMode) {
    return <SimulatorPage onExitDemo={() => setDemoMode(false)} />;
  }

  const renderMainContent = () => {
    switch (view) {
      case 'dag':
        return <DagView dag={dag} impactedNodes={impactedNodes} failedNodes={failedProducers} />;
      case 'impact':
        return (
          <ImpactPanel
            nodes={dag?.nodes ?? []}
            onHighlightNodes={(ids) => {
              setImpactedNodes(ids);
              setView('dag');
            }}
          />
        );
      case 'trace':
        return <TracePanel />;
      case 'drift':
        return <DriftPanel />;
      case 'diff':
        return <PipelineDiff />;
      case 'schema-diff':
        return <SchemaDiff />;
      case 'audit':
        return <AuditPanel />;
    }
  };

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-zinc-950">
      <Sidebar
        currentView={view}
        onViewChange={setView}
        wsStatus={wsStatus}
        eventCount={eventCount}
        nodeCount={dag?.node_count ?? 0}
        edgeCount={dag?.edge_count ?? 0}
        hashFailureCount={hashFailures.length}
      />
      <main className="flex-1 overflow-hidden relative">
        {bannerFailure && (
          <div className="absolute top-3 left-1/2 -translate-x-1/2 z-50 w-[480px]
                          flex items-start gap-3 px-4 py-3 rounded-xl
                          bg-red-950/90 border border-red-700/60 shadow-2xl backdrop-blur-sm">
            <ShieldAlert size={18} className="text-red-400 shrink-0 mt-0.5" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-semibold text-red-300">Chain Hash Failure Detected</p>
              <p className="text-[12px] text-red-400/80 mt-0.5 truncate">
                <span className="font-mono">{bannerFailure.producer_id}</span>
                {' → '}{bannerFailure.topic}
              </p>
              <p className="text-[11px] text-red-600 font-mono mt-0.5 truncate">
                prov_id: {bannerFailure.prov_id}
              </p>
            </div>
            <button onClick={() => setBannerFailure(null)} className="text-red-500 hover:text-red-300 transition-colors shrink-0">
              <X size={16} />
            </button>
          </div>
        )}
        {/* Demo mode button */}
        <button
          onClick={() => setDemoMode(true)}
          style={{
            position: 'absolute', bottom: 16, right: 16, zIndex: 50,
            fontSize: 10, color: '#334155', background: '#0d1a2e',
            border: '1px solid #1e3a5f', padding: '6px 12px', borderRadius: 4,
            cursor: 'pointer', fontFamily: 'monospace', letterSpacing: '0.06em',
          }}
        >
          TRY DEMO →
        </button>
        {renderMainContent()}
      </main>
    </div>
  );
}
