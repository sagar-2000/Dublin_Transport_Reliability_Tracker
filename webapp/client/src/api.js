const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

async function getJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`);
  const body = await response.json();
  if (!response.ok) {
    throw new Error(body.error || `Request to ${path} failed with ${response.status}`);
  }
  return body;
}

export function fetchScores() {
  return getJson('/api/scores');
}

export function fetchLiveVehicles() {
  return getJson('/api/live?feed=vehicle_positions');
}

export function searchStops(query) {
  return getJson(`/api/stops?search=${encodeURIComponent(query)}`);
}

export function fetchStop(stopId) {
  return getJson(`/api/stops/${encodeURIComponent(stopId)}`);
}
