import { useState, useEffect, useRef, useCallback, memo } from 'react';
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
  BaseEdge,
  getSmoothStepPath,
  Handle,
  Position,
  type NodeProps,
  type EdgeProps,
  type Node,
  type Edge,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { motion, AnimatePresence } from 'framer-motion';
import { Play, Square, Zap, Shield, RotateCcw, X } from 'lucide-react';
import { SimEngine, TOPOLOGIES, type SimState, type SimNodeState, type SimEdgeState, type TopologyDef } from './engine';

// ─── Custom Node ────────────────────────────────────────────────────────────

interface SimNodeData extends Record<string, unknown> {
  label: string;
  topic: string;
  status: SimNodeState['status'];
  eventCount: number;
  taintedCount: number;
}

const statusConfig = {
  idle:      { border: '#1a2540', bg: 'rgba(6,9,20,0.95)', dot: '#1e3a5f', text: '#3a5070', countColor: '#2a4060' },
  active:    { border: '#0e6680', bg: 'rgba(4,12,22,0.97)', dot: '#22d3ee', text: '#94d8e8', countColor: '#0e9ab8' },
  corrupted: { border: '#9b1a30', bg: 'rgba(18,4,8,0.98)', dot: '#f43f5e', text: '#fca5b5', countColor: '#f43f5e' },
  tainted:   { border: '#92400e', bg: 'rgba(16,8,2,0.98)', dot: '#fb923c', text: '#fcd0a0', countColor: '#fb923c' },
};

const SimNodeComponent = memo(({ data }: NodeProps) => {
  const d = data as SimNodeData;
  const cfg = statusConfig[d.status];
  const isHot = d.status === 'corrupted' || d.status === 'tainted';

  return (
    <div style={{
      background: cfg.bg,
      border: `1px solid ${cfg.border}`,
      borderRadius: 6,
      padding: '10px 14px 10px 12px',
      minWidth: 148,
      fontFamily: "'Azeret Mono', 'Courier New', monospace",
      position: 'relative',
      boxShadow: d.status === 'corrupted'
        ? `0 0 28px rgba(244,63,94,0.35), 0 0 8px rgba(244,63,94,0.2)`
        : d.status === 'tainted'
        ? `0 0 20px rgba(251,146,60,0.25)`
        : d.status === 'active'
        ? `0 0 14px rgba(34,211,238,0.12)`
        : 'none',
      transition: 'all 0.3s',
    }}>
      <Handle type="target" position={Position.Left} style={{ width: 6, height: 6, background: cfg.dot, border: 'none', left: -3 }} />

      {/* Status pulse for hot nodes */}
      {isHot && (
        <div style={{
          position: 'absolute', inset: -1, borderRadius: 6, pointerEvents: 'none',
          border: `1px solid ${cfg.border}`,
          animation: 'simPulse 1.8s ease-in-out infinite',
        }} />
      )}

      {/* Top row */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 11, fontWeight: 500, color: cfg.text, letterSpacing: '0.02em' }}>{d.label}</span>
        <div style={{ width: 6, height: 6, borderRadius: '50%', background: cfg.dot, flexShrink: 0,
          boxShadow: d.status !== 'idle' ? `0 0 6px ${cfg.dot}` : 'none' }} />
      </div>

      {/* Topic */}
      <div style={{ fontSize: 9, color: '#1e3a5f', marginBottom: 6, letterSpacing: '0.03em' }}>{d.topic}</div>

      {/* Stats */}
      <div style={{ display: 'flex', gap: 10, fontSize: 9 }}>
        <span style={{ color: cfg.countColor }}>{(d.eventCount as number).toLocaleString()}<span style={{ color: '#1e3a5f', marginLeft: 2 }}>ev</span></span>
        {(d.taintedCount as number) > 0 && (
          <span style={{ color: '#f43f5e' }}>⬡ {d.taintedCount as number}</span>
        )}
      </div>

      <Handle type="source" position={Position.Right} style={{ width: 6, height: 6, background: cfg.dot, border: 'none', right: -3 }} />
    </div>
  );
});
SimNodeComponent.displayName = 'SimNode';

// ─── Custom Edge ─────────────────────────────────────────────────────────────

function SimEdgeComponent({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps) {
  const edgeData = data as SimEdgeState | undefined;
  const [path] = getSmoothStepPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, borderRadius: 12 });
  const tainted = edgeData?.tainted ?? false;
  const active = edgeData?.active ?? false;
  const color = tainted ? '#f43f5e' : active ? '#22d3ee' : '#0d1f35';
  const width = active ? 2 : 1.5;
  const opacity = active ? 1 : 0.35;

  return (
    <>
      <BaseEdge id={id} path={path} style={{ stroke: color, strokeWidth: width, opacity, transition: 'stroke 0.4s, opacity 0.4s' }} />
      {active && (
        <circle r={tainted ? 4 : 3} fill={color} opacity={0.9}>
          <animateMotion dur={tainted ? '1.2s' : '1.8s'} repeatCount="indefinite" path={path} />
        </circle>
      )}
    </>
  );
}

const nodeTypes = { simNode: SimNodeComponent };
const edgeTypes = { simEdge: SimEdgeComponent };

// ─── Main Page ────────────────────────────────────────────────────────────────

export function SimulatorPage({ onExitDemo }: { onExitDemo?: () => void }) {
  const [topoIdx, setTopoIdx] = useState(1); // default: ecommerce
  const [simState, setSimState] = useState<SimState | null>(null);
  const [corruptTarget, setCorruptTarget] = useState<string>('');
  const [speed, setSpeed] = useState<'slow' | 'normal' | 'fast'>('normal');
  const [showReplay, setShowReplay] = useState(false);
  const mounted = useRef(true);
  const engineRef = useRef<SimEngine | null>(null);
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const eventListRef = useRef<HTMLDivElement>(null);

  const speedMs = speed === 'slow' ? 1200 : speed === 'fast' ? 350 : 700;

  const handleUpdate = useCallback((s: SimState) => {
    if (!mounted.current) return;
    setSimState(s);
    if (s.replayPlan.active && s.replayPlan.taintedEvents > 0) setShowReplay(true);
  }, []);

  // Init engine
  useEffect(() => {
    mounted.current = true;
    const topo: TopologyDef = TOPOLOGIES[topoIdx];
    const engine = new SimEngine(topo, handleUpdate);
    engineRef.current = engine;
    setCorruptTarget(topo.nodes[0].id);
    engine.start(speedMs);
    return () => {
      mounted.current = false;
      engine.destroy();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topoIdx]);

  // Speed changes
  useEffect(() => {
    const eng = engineRef.current;
    if (!eng || !eng.isRunning()) return;
    eng.stop();
    eng.start(speedMs);
  }, [speed, speedMs]);

  // Convert sim state → React Flow nodes/edges
  useEffect(() => {
    if (!simState) return;
    const rfNodes: Node[] = simState.nodes.map(n => ({
      id: n.id,
      type: 'simNode',
      position: { x: n.x, y: n.y },
      data: { label: n.label, topic: n.topic, status: n.status, eventCount: n.eventCount, taintedCount: n.taintedCount } as SimNodeData,
      draggable: false,
    }));
    const rfEdges: Edge[] = simState.edges.map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      type: 'simEdge',
      data: e as unknown as Record<string, unknown>,
    }));
    setNodes(rfNodes);
    setEdges(rfEdges);
  }, [simState, setNodes, setEdges]);

  const toggleRun = () => {
    const eng = engineRef.current;
    if (!eng) return;
    if (eng.isRunning()) eng.stop();
    else eng.start(speedMs);
    setSimState(eng.getState());
  };

  const handleInjectCorruption = () => {
    if (!corruptTarget || !engineRef.current) return;
    engineRef.current.injectCorruption(corruptTarget, 7000);
  };

  const handleTamper = () => {
    engineRef.current?.tamperLatestEvent();
  };

  const handleReset = () => {
    setShowReplay(false);
    const topo: TopologyDef = TOPOLOGIES[topoIdx];
    const engine = new SimEngine(topo, handleUpdate);
    engineRef.current?.destroy();
    engineRef.current = engine;
    engine.start(speedMs);
  };

  const plan = simState?.replayPlan;
  const totalEvents = simState?.nodes.reduce((s, n) => s + n.eventCount, 0) ?? 0;
  const taintedTotal = simState?.nodes.reduce((s, n) => s + n.taintedCount, 0) ?? 0;
  const tamperedCount = simState?.events.filter(e => e.tampered).length ?? 0;

  return (
    <div style={{
      width: '100vw', height: '100vh', background: '#03050e',
      fontFamily: "'Azeret Mono', 'Courier New', monospace",
      display: 'flex', flexDirection: 'column', overflow: 'hidden',
      color: '#e2e8f0',
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Azeret+Mono:wght@300;400;500;600&display=swap');
        @keyframes simPulse { 0%, 100% { opacity: 0.3; transform: scale(1); } 50% { opacity: 0.7; transform: scale(1.02); } }
        @keyframes scanline { 0% { transform: translateY(-100%); } 100% { transform: translateY(100vh); } }
        @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
        .sim-btn { cursor: pointer; transition: all 0.2s; border: none; font-family: inherit; }
        .sim-btn:hover { filter: brightness(1.2); }
        .sim-btn:active { transform: scale(0.97); }
        .event-row { border-bottom: 1px solid #0a1020; padding: 5px 0; animation: fadeInRow 0.3s ease; }
        @keyframes fadeInRow { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: none; } }
      `}</style>

      {/* ── HEADER ── */}
      <div style={{
        height: 52, borderBottom: '1px solid #0d1a2e', background: '#020408',
        display: 'flex', alignItems: 'center', padding: '0 20px', gap: 24, flexShrink: 0,
      }}>
        {/* Logo */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#22d3ee', boxShadow: '0 0 8px #22d3ee' }} />
          <span style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0', letterSpacing: '0.12em' }}>STREAM<span style={{ color: '#22d3ee' }}>LINEAGE</span></span>
          <span style={{ fontSize: 9, color: '#1e4060', marginLeft: 4, letterSpacing: '0.1em' }}>SIMULATOR</span>
          <span style={{ fontSize: 10, color: '#22d3ee', animation: 'blink 1s step-end infinite', marginLeft: 2 }}>▋</span>
        </div>

        {/* Topology tabs */}
        <div style={{ display: 'flex', gap: 4 }}>
          {TOPOLOGIES.map((t, i) => (
            <button
              key={t.id}
              className="sim-btn"
              onClick={() => { setTopoIdx(i); setShowReplay(false); }}
              style={{
                padding: '4px 12px', borderRadius: 4, fontSize: 10, letterSpacing: '0.06em',
                background: topoIdx === i ? 'rgba(34,211,238,0.1)' : 'transparent',
                border: `1px solid ${topoIdx === i ? '#22d3ee' : '#0d1a2e'}`,
                color: topoIdx === i ? '#22d3ee' : '#334155',
              }}
            >
              {t.name.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Speed */}
        <div style={{ display: 'flex', gap: 3, marginLeft: 8 }}>
          {(['slow', 'normal', 'fast'] as const).map(s => (
            <button
              key={s}
              className="sim-btn"
              onClick={() => setSpeed(s)}
              style={{
                padding: '3px 9px', borderRadius: 3, fontSize: 9, letterSpacing: '0.06em',
                background: speed === s ? '#0e2540' : 'transparent',
                border: `1px solid ${speed === s ? '#1e4060' : '#0a1422'}`,
                color: speed === s ? '#7dd3fc' : '#1e3a5f',
              }}
            >
              {s.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Status */}
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 16 }}>
          <span style={{ fontSize: 9, color: '#1e3a5f' }}>
            <span style={{ color: '#22d3ee' }}>{totalEvents.toLocaleString()}</span> EVENTS
          </span>
          <span style={{ fontSize: 9, color: '#1e3a5f' }}>
            <span style={{ color: taintedTotal > 0 ? '#f43f5e' : '#1e3a5f' }}>{taintedTotal}</span> TAINTED
          </span>
          <span style={{ fontSize: 9, color: '#1e3a5f' }}>
            <span style={{ color: tamperedCount > 0 ? '#f43f5e' : '#1e3a5f' }}>{tamperedCount}</span> TAMPERED
          </span>
          {onExitDemo && (
            <button
              className="sim-btn"
              onClick={onExitDemo}
              style={{ fontSize: 9, color: '#334155', background: 'transparent', border: '1px solid #0d1a2e', padding: '3px 10px', borderRadius: 3, letterSpacing: '0.06em' }}
            >
              ← LIVE MODE
            </button>
          )}
        </div>
      </div>

      {/* ── BODY ── */}
      <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '220px 1fr 280px', overflow: 'hidden' }}>

        {/* ── LEFT: CONTROLS ── */}
        <div style={{ borderRight: '1px solid #0d1a2e', background: '#020408', padding: 16, display: 'flex', flexDirection: 'column', gap: 12, overflow: 'hidden' }}>

          {/* Run / Stop */}
          <button
            className="sim-btn"
            onClick={toggleRun}
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
              padding: '10px', borderRadius: 6, fontSize: 11, fontWeight: 500, letterSpacing: '0.08em',
              background: simState?.running ? 'rgba(244,63,94,0.08)' : 'rgba(34,211,238,0.08)',
              border: `1px solid ${simState?.running ? '#9b1a30' : '#0e6680'}`,
              color: simState?.running ? '#f43f5e' : '#22d3ee',
            }}
          >
            {simState?.running ? <><Square size={12} /> STOP</> : <><Play size={12} /> RUN</>}
          </button>

          {/* Reset */}
          <button
            className="sim-btn"
            onClick={handleReset}
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
              padding: '7px', borderRadius: 5, fontSize: 10, letterSpacing: '0.08em',
              background: 'transparent', border: '1px solid #0d1a2e', color: '#334155',
            }}
          >
            <RotateCcw size={10} /> RESET
          </button>

          <div style={{ borderTop: '1px solid #0a1020', paddingTop: 12 }}>
            <div style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.1em', marginBottom: 8 }}>INJECT CORRUPTION</div>

            {/* Node selector */}
            <select
              value={corruptTarget}
              onChange={e => setCorruptTarget(e.target.value)}
              style={{
                width: '100%', padding: '6px 8px', borderRadius: 4, fontSize: 10,
                background: '#060a14', border: '1px solid #0d1a2e', color: '#7dd3fc',
                fontFamily: 'inherit', marginBottom: 8, outline: 'none',
              }}
            >
              {TOPOLOGIES[topoIdx].nodes.map(n => (
                <option key={n.id} value={n.id}>{n.label}</option>
              ))}
            </select>

            <button
              className="sim-btn"
              onClick={handleInjectCorruption}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
                padding: '9px', borderRadius: 5, fontSize: 10, fontWeight: 500, letterSpacing: '0.08em',
                background: 'rgba(244,63,94,0.1)', border: '1px solid #7b1a2a', color: '#f43f5e',
              }}
            >
              <Zap size={11} /> INJECT
            </button>
          </div>

          <div style={{ borderTop: '1px solid #0a1020', paddingTop: 12 }}>
            <div style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.1em', marginBottom: 8 }}>TAMPER DETECTION</div>
            <button
              className="sim-btn"
              onClick={handleTamper}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
                padding: '9px', borderRadius: 5, fontSize: 10, letterSpacing: '0.08em',
                background: 'rgba(251,146,60,0.06)', border: '1px solid #7a3a0a', color: '#fb923c',
              }}
            >
              <Shield size={11} /> TAMPER PAYLOAD
            </button>
            {tamperedCount > 0 && (
              <div style={{ marginTop: 6, fontSize: 9, color: '#f43f5e', textAlign: 'center' }}>
                ✗ {tamperedCount} chain hash failure{tamperedCount > 1 ? 's' : ''} detected
              </div>
            )}
          </div>

          {/* Stats */}
          <div style={{ borderTop: '1px solid #0a1020', paddingTop: 12, marginTop: 'auto' }}>
            <div style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.1em', marginBottom: 10 }}>PIPELINE STATS</div>
            {[
              { label: 'TOTAL EVENTS', value: totalEvents.toLocaleString(), color: '#22d3ee' },
              { label: 'TAINTED', value: taintedTotal.toString(), color: taintedTotal > 0 ? '#f43f5e' : '#1e3a5f' },
              { label: 'TAMPERED', value: tamperedCount.toString(), color: tamperedCount > 0 ? '#fb923c' : '#1e3a5f' },
              { label: 'NODES', value: TOPOLOGIES[topoIdx].nodes.length.toString(), color: '#334155' },
              { label: 'EDGES', value: TOPOLOGIES[topoIdx].edges.length.toString(), color: '#334155' },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                <span style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.06em' }}>{label}</span>
                <span style={{ fontSize: 9, color, fontWeight: 500 }}>{value}</span>
              </div>
            ))}
          </div>

          <div style={{ fontSize: 8, color: '#0d1a2e', letterSpacing: '0.06em', marginTop: 4 }}>
            {TOPOLOGIES[topoIdx].description}
          </div>
        </div>

        {/* ── CENTER: DAG ── */}
        <div style={{ position: 'relative', background: '#030711' }}>
          {/* Corner decorations */}
          <div style={{ position: 'absolute', top: 12, left: 12, fontSize: 8, color: '#0d1a2e', letterSpacing: '0.1em', zIndex: 10, pointerEvents: 'none' }}>
            LIVE DAG — {TOPOLOGIES[topoIdx].name.toUpperCase()}
          </div>
          <div style={{ position: 'absolute', top: 12, right: 12, fontSize: 8, color: '#0d1a2e', letterSpacing: '0.1em', zIndex: 10, pointerEvents: 'none' }}>
            {simState?.running ? (
              <span style={{ color: '#22d3ee' }}>● STREAMING</span>
            ) : (
              <span style={{ color: '#334155' }}>○ PAUSED</span>
            )}
          </div>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            fitView
            fitViewOptions={{ padding: 0.25 }}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
            zoomOnScroll={false}
            panOnScroll={false}
            panOnDrag={false}
            proOptions={{ hideAttribution: true }}
          >
            <Background variant={BackgroundVariant.Dots} gap={28} size={1} color="#0a1428" />
          </ReactFlow>

          {/* Legend */}
          <div style={{
            position: 'absolute', bottom: 12, left: 12, display: 'flex', gap: 16,
            fontSize: 8, color: '#1e3a5f', letterSpacing: '0.08em', pointerEvents: 'none',
          }}>
            {[
              { dot: '#22d3ee', label: 'ACTIVE' },
              { dot: '#f43f5e', label: 'CORRUPTED' },
              { dot: '#fb923c', label: 'TAINTED' },
              { dot: '#1e3a5f', label: 'IDLE' },
            ].map(({ dot, label }) => (
              <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                <div style={{ width: 5, height: 5, borderRadius: '50%', background: dot, boxShadow: dot !== '#1e3a5f' ? `0 0 4px ${dot}` : 'none' }} />
                {label}
              </div>
            ))}
          </div>
        </div>

        {/* ── RIGHT: EVENT STREAM ── */}
        <div style={{ borderLeft: '1px solid #0d1a2e', background: '#020408', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <div style={{ padding: '10px 14px 8px', borderBottom: '1px solid #0a1020', flexShrink: 0 }}>
            <span style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.12em' }}>EVENT STREAM</span>
          </div>
          <div ref={eventListRef} style={{ flex: 1, overflowY: 'auto', padding: '4px 0' }}>
            {simState?.events.map(ev => (
              <div key={ev.id} className="event-row" style={{ padding: '4px 14px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 2 }}>
                  <span style={{ fontSize: 8, color: '#0d1a2e', flexShrink: 0 }}>
                    {new Date(ev.timestamp).toISOString().slice(11, 22)}
                  </span>
                  {ev.tainted && (
                    <span style={{ fontSize: 7, color: '#f43f5e', border: '1px solid #7b1a2a', padding: '0px 4px', borderRadius: 2, letterSpacing: '0.06em', flexShrink: 0 }}>TAINTED</span>
                  )}
                  {ev.tampered && (
                    <span style={{ fontSize: 7, color: '#fb923c', border: '1px solid #7a3a0a', padding: '0px 4px', borderRadius: 2, letterSpacing: '0.06em', flexShrink: 0 }}>TAMPERED</span>
                  )}
                  {!ev.chainValid && (
                    <span style={{ fontSize: 7, color: '#ef4444', letterSpacing: '0.04em' }}>✗</span>
                  )}
                  {ev.chainValid && !ev.tainted && (
                    <span style={{ fontSize: 7, color: '#1e4060' }}>✓</span>
                  )}
                </div>
                <div style={{ fontSize: 9, color: ev.tainted ? '#7b2030' : '#1e3a5f', letterSpacing: '0.02em' }}>
                  <span style={{ color: ev.tainted ? '#e87090' : '#334155' }}>{ev.producerId}</span>
                  <span style={{ color: '#0d1a2e' }}> → </span>
                  <span style={{ color: '#0e3050' }}>{ev.topic}</span>
                </div>
                <div style={{ fontSize: 8, color: '#0d2040', fontFamily: 'inherit', marginTop: 1 }}>
                  <span style={{ color: '#0e2540' }}>p{ev.partition}:</span>{ev.chainHash}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── REPLAY DRAWER ── */}
      <AnimatePresence>
        {showReplay && plan && plan.taintedEvents > 0 && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.4, ease: 'easeOut' }}
            style={{
              borderTop: '1px solid #2a0a14',
              background: 'linear-gradient(180deg, #0a0210 0%, #06010e 100%)',
              flexShrink: 0, overflow: 'hidden',
            }}
          >
            <div style={{ padding: '14px 24px', display: 'flex', alignItems: 'center', gap: 32 }}>
              {/* Big efficiency number */}
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                <motion.span
                  key={plan.efficiencyPct}
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  style={{ fontSize: 36, fontWeight: 600, color: '#22d3ee', lineHeight: 1, letterSpacing: '-0.02em',
                    textShadow: '0 0 30px rgba(34,211,238,0.4)' }}
                >
                  {plan.efficiencyPct.toFixed(1)}%
                </motion.span>
                <span style={{ fontSize: 9, color: '#0e6680', letterSpacing: '0.1em' }}>DATA SAVED</span>
              </div>

              <div style={{ borderLeft: '1px solid #0d1a2e', paddingLeft: 24 }}>
                <div style={{ fontSize: 9, color: '#f43f5e', marginBottom: 4 }}>
                  ⬡ SURGICAL REPLAY: <span style={{ color: '#fca5b5' }}>{plan.taintedEvents} records</span>
                </div>
                <div style={{ fontSize: 9, color: '#1e3a5f' }}>
                  vs full replay: <span style={{ color: '#334155' }}>{plan.totalEventsInWindow.toLocaleString()} records in window</span>
                </div>
              </div>

              {/* Offset breakdown */}
              {plan.offsets.length > 0 && (
                <div style={{ borderLeft: '1px solid #0d1a2e', paddingLeft: 24, flex: 1 }}>
                  <div style={{ fontSize: 9, color: '#1e3a5f', letterSpacing: '0.08em', marginBottom: 6 }}>EXACT OFFSETS</div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {plan.offsets.slice(0, 4).map((o, i) => (
                      <div key={i} style={{
                        fontSize: 8, color: '#334155', background: '#060a14',
                        border: '1px solid #0d1a2e', borderRadius: 3, padding: '2px 8px',
                        fontFamily: 'inherit',
                      }}>
                        <span style={{ color: '#1e4060' }}>{o.topic.split('.')[0]}</span>
                        <span style={{ color: '#0d1a2e' }}>:</span>
                        <span style={{ color: '#1e3a5f' }}>p{o.partition}</span>
                        <span style={{ color: '#0d1a2e' }}>[</span>
                        <span style={{ color: '#f43f5e' }}>{o.offsets.slice(0, 3).join(',')}
                        {o.offsets.length > 3 ? `+${o.offsets.length - 3}` : ''}</span>
                        <span style={{ color: '#0d1a2e' }}>]</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              <button
                className="sim-btn"
                onClick={() => setShowReplay(false)}
                style={{ marginLeft: 'auto', color: '#1e3a5f', background: 'transparent', border: 'none', padding: 4 }}
              >
                <X size={12} />
              </button>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
