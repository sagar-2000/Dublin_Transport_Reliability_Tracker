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
    timestamp: v.timestamp ? Number(v.timestamp) : null,
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
      arrival_delay_sec: s.arrival?.delay ?? null,
      departure_delay_sec: s.departure?.delay ?? null,
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
    feed_timestamp: feed.header?.timestamp ? Number(feed.header.timestamp) : null,
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
