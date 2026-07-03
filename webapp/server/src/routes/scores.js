import { Router } from 'express';
import { loadScores } from '../lib/scoresStore.js';

export const scoresRouter = Router();

scoresRouter.get('/', (req, res) => {
  let rows;
  try {
    rows = loadScores();
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.status(503).json({
        error: 'Gold reliability scores not found — run the scoring pipeline first.',
      });
    }
    throw err;
  }

  const { route_id, day_type, hour_bucket, confident_only } = req.query;

  let filtered = rows;
  if (route_id) filtered = filtered.filter((r) => r.route_id === route_id);
  if (day_type) filtered = filtered.filter((r) => r.day_type === day_type);
  if (hour_bucket) filtered = filtered.filter((r) => r.hour_bucket === hour_bucket);
  if (confident_only === 'true') filtered = filtered.filter((r) => r.score_1_to_5 !== null);

  res.json({ count: filtered.length, scores: filtered });
});
