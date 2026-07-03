import { useMemo, useState } from 'react';
import { ScoreBadge } from './ScoreBadge.jsx';

const COLUMNS = [
  { key: 'route_id', label: 'Route' },
  { key: 'score_1_to_5', label: 'Score' },
  { key: 'pct_on_time', label: 'On-time %' },
  { key: 'avg_delay_minutes', label: 'Avg delay (min)' },
  { key: 'sample_size', label: 'Samples' },
];

export function ReliabilityTable({ scores, dayType, hourBucket, routeFilter }) {
  const [sort, setSort] = useState({ key: 'score_1_to_5', direction: 'asc' });

  const rows = useMemo(() => {
    let filtered = scores.filter((r) => r.day_type === dayType && r.hour_bucket === hourBucket);
    if (routeFilter) {
      const needle = routeFilter.toLowerCase();
      filtered = filtered.filter((r) => r.route_id.toLowerCase().includes(needle));
    }

    const sorted = [...filtered].sort((a, b) => {
      const aVal = a[sort.key];
      const bVal = b[sort.key];
      // Nulls (insufficient-data scores) always sort last, regardless of direction.
      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      if (aVal < bVal) return sort.direction === 'asc' ? -1 : 1;
      if (aVal > bVal) return sort.direction === 'asc' ? 1 : -1;
      return 0;
    });

    return sorted;
  }, [scores, dayType, hourBucket, routeFilter, sort]);

  function toggleSort(key) {
    setSort((prev) =>
      prev.key === key
        ? { key, direction: prev.direction === 'asc' ? 'desc' : 'asc' }
        : { key, direction: 'asc' },
    );
  }

  return (
    <table className="reliability-table">
      <thead>
        <tr>
          {COLUMNS.map((col) => (
            <th key={col.key} onClick={() => toggleSort(col.key)}>
              {col.label}
              {sort.key === col.key ? (sort.direction === 'asc' ? ' ↑' : ' ↓') : ''}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.route_id}|${row.day_type}|${row.hour_bucket}`}>
            <td>{row.route_id}</td>
            <td><ScoreBadge score={row.score_1_to_5} /></td>
            <td>{row.pct_on_time}%</td>
            <td>{row.avg_delay_minutes}</td>
            <td>{row.sample_size}</td>
          </tr>
        ))}
        {rows.length === 0 && (
          <tr>
            <td colSpan={COLUMNS.length} className="empty-row">No routes match this bucket/filter.</td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
