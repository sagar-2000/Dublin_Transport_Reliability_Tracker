import { useEffect, useState } from 'react';
import { fetchStop } from '../api.js';

// Matches the server's LIVE_FEED_CACHE_MS default — same reasoning as
// useLiveVehicles: polling faster than that just re-fetches the same
// cached NTA response.
const POLL_INTERVAL_MS = 30_000;

export function useStop(stopId) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!stopId) {
      setData(null);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);

    async function poll() {
      try {
        const result = await fetchStop(stopId);
        if (cancelled) return;
        setData(result);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    poll();
    const timer = setInterval(poll, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [stopId]);

  return { data, error, loading };
}
