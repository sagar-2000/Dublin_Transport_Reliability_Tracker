import fs from 'node:fs';
import { parse } from 'csv-parse/sync';
import { GOLD_CSV_PATH } from '../config.js';

// Cache keyed on the file's mtime so a re-run of the scoring pipeline is
// picked up on the next request without restarting the server.
let cache = null; // { mtimeMs, rows }

const NUMERIC_FIELDS = [
  'score_1_to_5',
  'pct_on_time',
  'avg_delay_minutes',
  'delay_stddev_minutes',
  'cancellation_rate',
  'sample_size',
];

function toRow(record) {
  const row = { ...record };
  for (const field of NUMERIC_FIELDS) {
    row[field] = record[field] === '' || record[field] == null ? null : Number(record[field]);
  }
  return row;
}

// Throws ENOENT if the scoring pipeline hasn't produced a gold CSV yet —
// callers are expected to turn that into a 503, not a crash.
export function loadScores() {
  const stat = fs.statSync(GOLD_CSV_PATH);
  if (cache && cache.mtimeMs === stat.mtimeMs) {
    return cache.rows;
  }

  const csvText = fs.readFileSync(GOLD_CSV_PATH, 'utf-8');
  const records = parse(csvText, { columns: true, skip_empty_lines: true });
  const rows = records.map(toRow);

  cache = { mtimeMs: stat.mtimeMs, rows };
  return rows;
}
