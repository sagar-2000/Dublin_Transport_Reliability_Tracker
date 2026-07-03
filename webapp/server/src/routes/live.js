import { Router } from 'express';
import { getLiveFeed } from '../lib/liveFeed.js';

export const liveRouter = Router();

const VALID_FEEDS = new Set(['vehicle_positions', 'trip_updates']);

liveRouter.get('/', async (req, res, next) => {
  const feedType = req.query.feed || 'vehicle_positions';

  if (!VALID_FEEDS.has(feedType)) {
    return res.status(400).json({ error: `feed must be one of: ${[...VALID_FEEDS].join(', ')}` });
  }

  try {
    const data = await getLiveFeed(feedType);
    res.json(data);
  } catch (err) {
    if (err.code === 'NO_API_KEY') {
      return res.status(500).json({ error: 'Server is missing NTA_API_KEY' });
    }
    if (err.code === 'RATE_LIMITED') {
      return res.status(429).json({ error: 'Rate limited by NTA API — try again shortly' });
    }
    if (err.code === 'UPSTREAM_ERROR') {
      return res.status(502).json({ error: err.message });
    }
    next(err);
  }
});
