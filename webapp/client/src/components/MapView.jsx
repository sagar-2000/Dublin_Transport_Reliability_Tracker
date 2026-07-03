import { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import markerIcon2x from 'leaflet/dist/images/marker-icon-2x.png';
import markerIcon from 'leaflet/dist/images/marker-icon.png';
import markerShadow from 'leaflet/dist/images/marker-shadow.png';
import { lookupScore, scoreColor, scoreLabel } from '../lib/scoreLookup.js';

// Vite doesn't resolve Leaflet's default marker icon's relative asset URLs —
// a well-known Leaflet+bundler issue, fixed by pointing it at the bundled copies.
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIcon2x,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

const DUBLIN_CENTER = [53.3498, -6.2603];
const STOP_ZOOM = 16;

function FlyToStop({ stop }) {
  const map = useMap();
  useEffect(() => {
    if (stop) map.flyTo([stop.stop_lat, stop.stop_lon], STOP_ZOOM);
  }, [stop, map]);
  return null;
}

export function MapView({ vehicles, scoreIndex, dayType, hourBucket, selectedStop }) {
  return (
    <MapContainer center={DUBLIN_CENTER} zoom={12} className="map-view">
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FlyToStop stop={selectedStop} />
      {selectedStop && (
        <Marker position={[selectedStop.stop_lat, selectedStop.stop_lon]}>
          <Popup>{selectedStop.stop_name}</Popup>
        </Marker>
      )}
      {vehicles.map((vehicle) => {
        if (vehicle.latitude == null || vehicle.longitude == null) return null;

        const scoreRow = lookupScore(scoreIndex, vehicle.route_id, dayType, hourBucket);
        const score = scoreRow?.score_1_to_5 ?? null;

        return (
          <CircleMarker
            key={vehicle.entity_id}
            center={[vehicle.latitude, vehicle.longitude]}
            radius={7}
            pathOptions={{ color: '#1f2937', weight: 1, fillColor: scoreColor(score), fillOpacity: 0.9 }}
          >
            <Popup>
              <strong>Route {vehicle.route_id ?? 'unknown'}</strong>
              <br />
              Trip: {vehicle.trip_id ?? 'n/a'}
              <br />
              Reliability score: {scoreLabel(score)}
              {scoreRow && (
                <>
                  <br />
                  On-time: {scoreRow.pct_on_time}% · avg delay {scoreRow.avg_delay_minutes}min
                </>
              )}
            </Popup>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
