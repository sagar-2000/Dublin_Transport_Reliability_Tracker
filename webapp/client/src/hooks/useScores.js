import { useEffect, useState } from 'react';
import { fetchScores } from '../api.js';

export function useScores() {
  const [scores, setScores] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    fetchScores()
      .then((data) => {
        if (cancelled) return;
        setScores(data.scores);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return { scores, error, loading };
}
