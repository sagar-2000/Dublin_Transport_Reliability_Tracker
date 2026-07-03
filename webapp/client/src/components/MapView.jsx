import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import { lookupScore, scoreColor, scoreLabel } from '../lib/scoreLookup.js';

const DUBLIN_CENTER = [53.3498, -6.2603];

export function MapView({ vehicles, scoreIndex, dayType, hourBucket }) {
  return (
    <MapContainer center={DUBLIN_CENTER} zoom={12} className="map-view">
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
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
