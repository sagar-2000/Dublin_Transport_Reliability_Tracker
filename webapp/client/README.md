# Dublin Transit Client

React + Vite frontend: a live vehicle map (like TFI Live) with reliability
scores joined onto it as the analytical layer TFI Live doesn't have.

## Setup

```bash
npm install
cp .env.example .env   # point VITE_API_BASE_URL at the running API
npm run dev
```

Requires `webapp/server` running (see its README) for `/api/scores` and `/api/live`.

## Features

- **Live map** — vehicle positions from `/api/live`, polled every 30s (matches
  the server's cache window). Markers are colored by the reliability score of
  that vehicle's route in the currently selected day-type/hour-bucket.
- **Reliability table** — sortable, filterable by route, for any
  day-type × hour-bucket combination (defaults to "now", resolved in
  Europe/Dublin time via `src/lib/timeBuckets.js`, mirroring the bucket
  rules in `scorer/silver.py`).
- **Route filter** — one search box filters both the map markers and the table.

## Structure

- `src/api.js` — fetch wrappers for the two API endpoints
- `src/hooks/` — `useLiveVehicles` (polling), `useScores` (one-shot fetch)
- `src/lib/timeBuckets.js` — client-side day_type/hour_bucket classification
- `src/lib/scoreLookup.js` — route/bucket → score index + color bands
- `src/components/` — `MapView`, `ReliabilityTable`, `RouteFilter`, `BucketSelector`, `ScoreBadge`
