import fs from 'node:fs';
import { parse } from 'csv-parse/sync';

function toRow(record, numericFields) {
  const row = { ...record };
  for (const field of numericFields) {
    row[field] = record[field] === '' || record[field] == null ? null : Number(record[field]);
  }
  return row;
}

// Returns a loader that re-parses the CSV only when its mtime changes, so a
// re-run of the scoring pipeline is picked up on the next request without
// restarting the server. Throws ENOENT if the file doesn't exist yet —
// callers are expected to turn that into a 503, not a crash.
export function createCsvLoader(csvPath, numericFields) {
  let cache = null; // { mtimeMs, rows }

  return function load() {
    const stat = fs.statSync(csvPath);
    if (cache && cache.mtimeMs === stat.mtimeMs) {
      return cache.rows;
    }

    const csvText = fs.readFileSync(csvPath, 'utf-8');
    const records = parse(csvText, { columns: true, skip_empty_lines: true });
    const rows = records.map((record) => toRow(record, numericFields));

    cache = { mtimeMs: stat.mtimeMs, rows };
    return rows;
  };
}
