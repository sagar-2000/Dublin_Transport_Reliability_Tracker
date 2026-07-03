import { useMemo, useState } from 'react';
import { MapView } from './components/MapView.jsx';
import { RouteFilter } from './components/RouteFilter.jsx';
import { BucketSelector } from './components/BucketSelector.jsx';
import { ReliabilityTable } from './components/ReliabilityTable.jsx';
import { useLiveVehicles } from './hooks/useLiveVehicles.js';
import { useScores } from './hooks/useScores.js';
import { buildScoreIndex } from './lib/scoreLookup.js';
import { getCurrentBucket, DAY_TYPE_LABELS, HOUR_BUCKET_LABELS } from './lib/timeBuckets.js';
import './App.css';

export default function App() {
  const { vehicles, feedTimestamp, error: liveError, loading: liveLoading } = useLiveVehicles();
  const { scores, error: scoresError, loading: scoresLoading } = useScores();

  const initialBucket = useMemo(() => getCurrentBucket(), []);
  const [dayType, setDayType] = useState(initialBucket.dayType);
  const [hourBucket, setHourBucket] = useState(initialBucket.hourBucket);
  const [routeFilter, setRouteFilter] = useState('');

  const scoreIndex = useMemo(() => buildScoreIndex(scores), [scores]);

  const visibleVehicles = useMemo(() => {
    if (!routeFilter) return vehicles;
    const needle = routeFilter.toLowerCase();
    return vehicles.filter((v) => v.route_id?.toLowerCase().includes(needle));
  }, [vehicles, routeFilter]);

  function resetToNow() {
    const now = getCurrentBucket();
    setDayType(now.dayType);
    setHourBucket(now.hourBucket);
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Dublin Transit Reliability Tracker</h1>
        <RouteFilter value={routeFilter} onChange={setRouteFilter} />
      </header>

      <main className="app-main">
        <section className="map-section">
          {liveError && <div className="banner error">Live feed error: {liveError}</div>}
          {liveLoading && <div className="banner">Loading live vehicles…</div>}
          <MapView
            vehicles={visibleVehicles}
            scoreIndex={scoreIndex}
            dayType={dayType}
            hourBucket={hourBucket}
          />
          <div className="map-footer">
            {visibleVehicles.length} vehicles on screen
            {feedTimestamp && ` · feed updated ${new Date(feedTimestamp * 1000).toLocaleTimeString('en-IE')}`}
          </div>
        </section>

        <aside className="analytics-section">
          <h2>Reliability scores</h2>
          <p className="bucket-label">
            Showing: {DAY_TYPE_LABELS[dayType]} · {HOUR_BUCKET_LABELS[hourBucket]}
          </p>
          <BucketSelector
            dayType={dayType}
            hourBucket={hourBucket}
            onChangeDayType={setDayType}
            onChangeHourBucket={setHourBucket}
            onResetToNow={resetToNow}
          />
          {scoresError && <div className="banner error">Scores error: {scoresError}</div>}
          {scoresLoading ? (
            <div className="banner">Loading scores…</div>
          ) : (
            <ReliabilityTable
              scores={scores}
              dayType={dayType}
              hourBucket={hourBucket}
              routeFilter={routeFilter}
            />
          )}
        </aside>
      </main>
    </div>
  );
}
