import { useMemo } from 'react';
import { useStop } from '../hooks/useStop.js';
import { ScoreBadge } from './ScoreBadge.jsx';
import { formatEta, formatDelay } from '../lib/arrivals.js';
import { getCurrentBucket, DAY_TYPE_LABELS, HOUR_BUCKET_LABELS } from '../lib/timeBuckets.js';

export function StopPanel({ stopId }) {
  const { data, error, loading } = useStop(stopId);
  const { dayType, hourBucket } = useMemo(() => getCurrentBucket(), []);

  if (!stopId) {
    return <p className="bucket-label">Search for a stop above to see its live arrivals and average delay.</p>;
  }

  if (loading && !data) {
    return <div className="banner">Loading stop…</div>;
  }

  if (error) {
    return <div className="banner error">{error}</div>;
  }

  if (!data) return null;

  const currentReliability = data.reliability.find(
    (r) => r.day_type === dayType && r.hour_bucket === hourBucket,
  );

  return (
    <div className="stop-panel">
      <h3>{data.stop.stop_name}</h3>

      <div className="stop-reliability-summary">
        <ScoreBadge score={currentReliability?.score_1_to_5 ?? null} />
        <span>
          {currentReliability
            ? `Avg delay ${currentReliability.avg_delay_minutes} min · ${currentReliability.pct_on_time}% on-time (${DAY_TYPE_LABELS[dayType]} ${HOUR_BUCKET_LABELS[hourBucket]})`
            : `No historical data for ${DAY_TYPE_LABELS[dayType]} ${HOUR_BUCKET_LABELS[hourBucket]} yet`}
        </span>
      </div>

      <h4>Live arrivals</h4>
      {data.arrivals.length === 0 ? (
        <p className="bucket-label">No buses currently reporting arrivals at this stop — check back closer to service hours.</p>
      ) : (
        <ul className="arrivals-list">
          {data.arrivals.map((a) => (
            <li key={`${a.trip_id}-${a.route_id}`}>
              <span className="arrival-route">{a.route_id}</span>
              <span className="arrival-eta">{formatEta(a.predicted_time) ?? '—'}</span>
              <span className="arrival-delay">{formatDelay(a.delay_sec) ?? 'delay unknown'}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
