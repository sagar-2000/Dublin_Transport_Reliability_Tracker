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

### `GET /api/stops?search=<query>`
Searches `data/gold/stop_reliability_scores.csv` by stop name (case-insensitive
substring match). Returns up to 20 `{stop_id, stop_name, stop_lat, stop_lon}` matches.

### `GET /api/stops/:stopId`
Returns `{ stop, reliability, feed_timestamp, arrivals }` for one stop:
- `reliability` — that stop's avg delay / on-time % / score per day_type × hour_bucket
- `arrivals` — buses currently reporting a stop_time_update for this stop in the
  live TripUpdates feed, with `predicted_time` (absolute epoch, when the feed
  provides one) and `delay_sec`. This stands in for a schedule without needing
  the ~230MB static GTFS stop_times table — the tradeoff is it only shows buses
  already actively reporting (typically the next ~1-2 hours of service), not a
  full day's timetable. `404` if the stop_id isn't in the gold data.

### `GET /api/health`
Liveness check.
