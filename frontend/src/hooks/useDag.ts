import { useState, useEffect, useCallback } from 'react';
import type { DagResponse } from '../types/api';
import { fetchDag } from '../lib/api';

export function useDag(pollInterval = 5000) {
  const [dag, setDag] = useState<DagResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await fetchDag();
      setDag(data);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to fetch DAG');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, pollInterval);
    return () => clearInterval(id);
  }, [refresh, pollInterval]);

  return { dag, loading, error, refresh };
}
