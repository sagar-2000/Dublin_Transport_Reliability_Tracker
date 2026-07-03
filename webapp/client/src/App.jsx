import { useMemo, useState } from 'react';
import { MapView } from './components/MapView.jsx';
import { RouteFilter } from './components/RouteFilter.jsx';
import { BucketSelector } from './components/BucketSelector.jsx';
import { ReliabilityTable } from './components/ReliabilityTable.jsx';
import { StopSearch } from './components/StopSearch.jsx';
import { StopPanel } from './components/StopPanel.jsx';
import { useLiveVehicles } from './hooks/useLiveVehicles.js';
import { useScores } from './hooks/useScores.js';
import { buildScoreIndex } from './lib/scoreLookup.js';
import { getCurrentBucket, DAY_TYPE_LABELS, HOUR_BUCKET_LABELS } from './lib/timeBuckets.js';
import './App.css';

const TABS = { STOPS: 'stops', ROUTES: 'routes' };

export default function App() {
  const { vehicles, feedTimestamp, error: liveError, loading: liveLoading } = useLiveVehicles();
  const { scores, error: scoresError, loading: scoresLoading } = useScores();

  const initialBucket = useMemo(() => getCurrentBucket(), []);
  const [dayType, setDayType] = useState(initialBucket.dayType);
  const [hourBucket, setHourBucket] = useState(initialBucket.hourBucket);
  const [routeFilter, setRouteFilter] = useState('');
  const [tab, setTab] = useState(TABS.STOPS);
  const [selectedStop, setSelectedStop] = useState(null);

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
        {tab === TABS.ROUTES && <RouteFilter value={routeFilter} onChange={setRouteFilter} />}
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
            selectedStop={selectedStop}
          />
          <div className="map-footer">
            {visibleVehicles.length} vehicles on screen
            {feedTimestamp && ` · feed updated ${new Date(feedTimestamp * 1000).toLocaleTimeString('en-IE')}`}
          </div>
        </section>

        <aside className="analytics-section">
          <div className="tab-toggle">
            <button
              type="button"
              className={tab === TABS.STOPS ? 'active' : ''}
              onClick={() => setTab(TABS.STOPS)}
            >
              Find a stop
            </button>
            <button
              type="button"
              className={tab === TABS.ROUTES ? 'active' : ''}
              onClick={() => setTab(TABS.ROUTES)}
            >
              Routes
            </button>
          </div>

          {tab === TABS.STOPS ? (
            <>
              <StopSearch onSelect={setSelectedStop} />
              <StopPanel stopId={selectedStop?.stop_id ?? null} />
            </>
          ) : (
            <>
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
            </>
          )}
        </aside>
      </main>
    </div>
  );
}
