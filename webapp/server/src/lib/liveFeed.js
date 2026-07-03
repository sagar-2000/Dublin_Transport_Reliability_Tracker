import GtfsRealtimeBindings from 'gtfs-realtime-bindings';
import { NTA_API_KEY, LIVE_FEED_CACHE_MS } from '../config.js';

// Same NTA endpoints used by airflow/dags/gtfs_ingest_lib.py, kept in sync.
const FEED_URLS = {
  trip_updates: 'https://api.nationaltransport.ie/gtfsr/v2/TripUpdates',
  vehicle_positions: 'https://api.nationaltransport.ie/gtfsr/v2/Vehicles',
};

// One cache slot per feed type so trip_updates and vehicle_positions poll
// independently of each other.
const cache = new Map(); // feedType -> { fetchedAt, data }

// protobufjs decodes int64 fields as `Long` objects (the 'long' package is
// installed) rather than plain numbers. A Long is an object, so it's truthy
// even when it represents 0 — and 0 is exactly what an *unset* GTFS-R time
// field decodes to. A real Unix timestamp is never 0 in this feed, so we
// treat 0 as "absent" here rather than as 1970-01-01.
function longToEpoch(value) {
  if (value == null) return null;
  const num = typeof value.toNumber === 'function' ? value.toNumber() : Number(value);
  return num > 0 ? num : null;
}

function simplifyVehiclePosition(entity) {
  const v = entity.vehicle;
  if (!v) return null;
  return {
    entity_id: entity.id,
    vehicle_id: v.vehicle?.id ?? null,
    trip_id: v.trip?.tripId ?? null,
    route_id: v.trip?.routeId ?? null,
    latitude: v.position?.latitude ?? null,
    longitude: v.position?.longitude ?? null,
    bearing: v.position?.bearing ?? null,
    speed: v.position?.speed ?? null,
    timestamp: longToEpoch(v.timestamp),
  };
}

function simplifyTripUpdate(entity) {
  const t = entity.tripUpdate;
  if (!t) return null;
  return {
    entity_id: entity.id,
    trip_id: t.trip?.tripId ?? null,
    route_id: t.trip?.routeId ?? null,
    stop_time_updates: (t.stopTimeUpdate ?? []).map((s) => ({
      stop_id: s.stopId ?? null,
      stop_sequence: s.stopSequence ?? null,
      arrival_delay_sec: s.arrival?.delay ?? null,
      arrival_time: longToEpoch(s.arrival?.time),
      departure_delay_sec: s.departure?.delay ?? null,
      departure_time: longToEpoch(s.departure?.time),
    })),
  };
}

async function fetchFeed(feedType) {
  if (!NTA_API_KEY) {
    const err = new Error('NTA_API_KEY is not set');
    err.code = 'NO_API_KEY';
    throw err;
  }

  const response = await fetch(FEED_URLS[feedType], {
    headers: { 'x-api-key': NTA_API_KEY },
  });

  if (response.status === 429) {
    const err = new Error('Rate limited by NTA API');
    err.code = 'RATE_LIMITED';
    throw err;
  }
  if (!response.ok) {
    const err = new Error(`NTA API returned ${response.status}`);
    err.code = 'UPSTREAM_ERROR';
    throw err;
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  const feed = GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buffer);

  const simplify = feedType === 'trip_updates' ? simplifyTripUpdate : simplifyVehiclePosition;
  const entities = feed.entity.map(simplify).filter(Boolean);

  return {
    feed_timestamp: longToEpoch(feed.header?.timestamp),
    entities,
  };
}

// Serves a cached copy within LIVE_FEED_CACHE_MS so page traffic doesn't
// translate 1:1 into NTA API calls — keeps us inside the fair-use rate limit.
export async function getLiveFeed(feedType) {
  const cached = cache.get(feedType);
  const now = Date.now();
  if (cached && now - cached.fetchedAt < LIVE_FEED_CACHE_MS) {
    return cached.data;
  }

  const data = await fetchFeed(feedType);
  cache.set(feedType, { fetchedAt: now, data });
  return data;
}

// Scans the (cached) trip_updates feed for any bus currently reporting a
// stop_time_update at this stop — this is what stands in for a "schedule"
// without needing the ~230MB static stop_times table. Only shows buses that
// are already actively reporting, typically the next ~1-2 hours of service.
// A trip's stop_time_update list can include stops it has already passed
// (its full remaining itinerary, not just the next stop) — those aren't
// "upcoming arrivals" so we drop anything more than a minute in the past.
const PAST_ARRIVAL_GRACE_SEC = 60;

export async function getStopArrivals(stopId) {
  const feed = await getLiveFeed('trip_updates');
  const nowSec = feed.feed_timestamp ?? Math.floor(Date.now() / 1000);

  const arrivals = [];
  for (const entity of feed.entities) {
    for (const stu of entity.stop_time_updates) {
      if (stu.stop_id !== stopId) continue;
      const predicted_time = stu.arrival_time ?? stu.departure_time ?? null;
      if (predicted_time != null && predicted_time < nowSec - PAST_ARRIVAL_GRACE_SEC) continue;
      arrivals.push({
        trip_id: entity.trip_id,
        route_id: entity.route_id,
        predicted_time,
        delay_sec: stu.arrival_delay_sec ?? stu.departure_delay_sec ?? null,
      });
    }
  }

  arrivals.sort((a, b) => (a.predicted_time ?? Infinity) - (b.predicted_time ?? Infinity));
  return { feed_timestamp: feed.feed_timestamp, arrivals };
}

// Shared NTA-feed error → HTTP status mapping, used by any route that calls
// getLiveFeed/getStopArrivals. Returns true if it handled (and responded to)
// the error, false if the caller should pass it to next().
export function handleLiveFeedError(err, res) {
  if (err.code === 'NO_API_KEY') {
    res.status(500).json({ error: 'Server is missing NTA_API_KEY' });
    return true;
  }
  if (err.code === 'RATE_LIMITED') {
    res.status(429).json({ error: 'Rate limited by NTA API — try again shortly' });
    return true;
  }
  if (err.code === 'UPSTREAM_ERROR') {
    res.status(502).json({ error: err.message });
    return true;
  }
  return false;
}
