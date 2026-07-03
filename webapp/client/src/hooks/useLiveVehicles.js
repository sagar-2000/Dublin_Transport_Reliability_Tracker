import { useEffect, useRef, useState } from 'react';
import { fetchLiveVehicles } from '../api.js';

// Matches the server's LIVE_FEED_CACHE_MS default — polling faster than that
// would just re-fetch the same cached response.
const POLL_INTERVAL_MS = 30_000;

export function useLiveVehicles() {
  const [vehicles, setVehicles] = useState([]);
  const [feedTimestamp, setFeedTimestamp] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const timerRef = useRef(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await fetchLiveVehicles();
        if (cancelled) return;
        setVehicles(data.entities);
        setFeedTimestamp(data.feed_timestamp);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    poll();
    timerRef.current = setInterval(poll, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(timerRef.current);
    };
  }, []);

  return { vehicles, feedTimestamp, error, loading };
}
