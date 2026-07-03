export function buildScoreIndex(scores) {
  const index = new Map();
  for (const row of scores) {
    index.set(`${row.route_id}|${row.day_type}|${row.hour_bucket}`, row);
  }
  return index;
}

export function lookupScore(index, routeId, dayType, hourBucket) {
  return index.get(`${routeId}|${dayType}|${hourBucket}`) ?? null;
}

// Score bands used for both map marker colors and table badges.
export function scoreColor(score) {
  if (score == null) return '#9ca3af'; // gray — no confident score
  if (score >= 4) return '#22c55e'; // green
  if (score >= 3) return '#84cc16'; // lime
  if (score >= 2) return '#f59e0b'; // amber
  return '#ef4444'; // red
}

export function scoreLabel(score) {
  return score == null ? 'No data' : score.toFixed(1);
}
