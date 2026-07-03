import { Router } from 'express';
import { searchStops, getStop } from '../lib/stopsStore.js';
import { getStopArrivals, handleLiveFeedError } from '../lib/liveFeed.js';

export const stopsRouter = Router();

function handleStoreError(err, res) {
  if (err.code === 'ENOENT') {
    res.status(503).json({ error: 'Stop reliability data not found — run the scoring pipeline first.' });
    return true;
  }
  return false;
}

stopsRouter.get('/', (req, res, next) => {
  const query = req.query.search ?? '';
  try {
    res.json({ stops: searchStops(query) });
  } catch (err) {
    if (!handleStoreError(err, res)) next(err);
  }
});

stopsRouter.get('/:stopId', async (req, res, next) => {
  let stopData;
  try {
    stopData = getStop(req.params.stopId);
  } catch (err) {
    if (!handleStoreError(err, res)) next(err);
    return;
  }

  if (!stopData) {
    return res.status(404).json({ error: `Unknown stop_id: ${req.params.stopId}` });
  }

  try {
    const live = await getStopArrivals(req.params.stopId);
    res.json({ ...stopData, ...live });
  } catch (err) {
    if (!handleLiveFeedError(err, res)) next(err);
  }
});
