import { useEffect, useRef, useState, useCallback } from 'react';
import type { WsEvent } from '../types/api';

type WsStatus = 'connecting' | 'connected' | 'disconnected';

export function useWebSocket(url: string) {
  const wsRef = useRef<WebSocket | null>(null);
  const [status, setStatus] = useState<WsStatus>('disconnected');
  const [lastEvent, setLastEvent] = useState<WsEvent | null>(null);
  const [eventCount, setEventCount] = useState(0);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    setStatus('connecting');
    const ws = new WebSocket(url);

    ws.onopen = () => setStatus('connected');

    ws.onmessage = (e) => {
      try {
        const event: WsEvent = JSON.parse(e.data);
        setLastEvent(event);
        setEventCount((c) => c + 1);
      } catch {
        // ignore non-JSON messages
      }
    };

    ws.onclose = () => {
      setStatus('disconnected');
      // Auto-reconnect after 3s
      reconnectTimer.current = setTimeout(connect, 3000);
    };

    ws.onerror = () => ws.close();

    wsRef.current = ws;
  }, [url]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return { status, lastEvent, eventCount };
}
