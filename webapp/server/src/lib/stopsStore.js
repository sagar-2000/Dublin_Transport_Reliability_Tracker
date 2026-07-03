import { createCsvLoader } from './csvCache.js';
import { STOP_GOLD_CSV_PATH } from '../config.js';

const NUMERIC_FIELDS = [
  'stop_lat',
  'stop_lon',
  'score_1_to_5',
  'pct_on_time',
  'avg_delay_minutes',
  'delay_stddev_minutes',
  'sample_size',
];

export const loadStopReliability = createCsvLoader(STOP_GOLD_CSV_PATH, NUMERIC_FIELDS);

// One row per stop_id (day_type/hour_bucket rows collapsed), for search results.
export function searchStops(query, limit = 20) {
  const rows = loadStopReliability();
  const needle = query.trim().toLowerCase();

  const seen = new Map(); // stop_id -> stop summary
  for (const row of rows) {
    if (seen.has(row.stop_id)) continue;
    if (needle && !row.stop_name.toLowerCase().includes(needle)) continue;
    seen.set(row.stop_id, {
      stop_id: row.stop_id,
      stop_name: row.stop_name,
      stop_lat: row.stop_lat,
      stop_lon: row.stop_lon,
    });
    if (seen.size >= limit) break;
  }

  return [...seen.values()];
}

export function getStop(stopId) {
  const rows = loadStopReliability().filter((row) => row.stop_id === stopId);
  if (rows.length === 0) return null;

  const { stop_id, stop_name, stop_lat, stop_lon } = rows[0];
  return {
    stop: { stop_id, stop_name, stop_lat, stop_lon },
    reliability: rows.map(({ day_type, hour_bucket, score_1_to_5, pct_on_time, avg_delay_minutes, delay_stddev_minutes, sample_size }) => ({
      day_type, hour_bucket, score_1_to_5, pct_on_time, avg_delay_minutes, delay_stddev_minutes, sample_size,
    })),
  };
}
