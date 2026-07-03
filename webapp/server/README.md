# Dublin Transit API

Express API serving the gold reliability scores and a live GTFS-Realtime proxy.

## Setup

```bash
npm install
cp .env.example .env   # fill in NTA_API_KEY
npm start               # or `npm run dev` for auto-reload
```

By default it reads the gold CSV from `../../data/gold/route_reliability_scores.csv`
(same `SCORING_DATA_DIR` env var convention as `scorer/config.py`, so it stays in
sync with the PySpark pipeline without duplicating config).

## Endpoints

### `GET /api/scores`
Reads `data/gold/route_reliability_scores.csv`. Returns `503` if the pipeline
hasn't produced it yet.

Query params (all optional, combinable):
- `route_id`, `day_type` (`weekday`/`saturday`/`sunday`), `hour_bucket`
- `confident_only=true` — drop buckets with `score_1_to_5 = null` (insufficient sample size)

### `GET /api/live?feed=vehicle_positions|trip_updates`
Proxies and simplifies the NTA GTFS-Realtime feed (default `vehicle_positions`).
Responses are cached in memory for `LIVE_FEED_CACHE_MS` (default 30s) per feed
type to stay within NTA's fair-use rate limit.

### `GET /api/health`
Liveness check.
