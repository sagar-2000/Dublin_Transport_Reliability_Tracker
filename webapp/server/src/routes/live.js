import { Router } from 'express';
import { getLiveFeed, handleLiveFeedError } from '../lib/liveFeed.js';

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
    if (!handleLiveFeedError(err, res)) next(err);
  }
});
