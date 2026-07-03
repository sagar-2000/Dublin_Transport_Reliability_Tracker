import { useEffect, useState } from 'react';
import { searchStops } from '../api.js';

const DEBOUNCE_MS = 250;

export function StopSearch({ onSelect }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (query.trim().length < 2) {
      setResults([]);
      return;
    }

    let cancelled = false;
    const timer = setTimeout(() => {
      searchStops(query).then((data) => {
        if (!cancelled) setResults(data.stops);
      });
    }, DEBOUNCE_MS);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [query]);

  function handleSelect(stop) {
    setQuery(stop.stop_name);
    setOpen(false);
    onSelect(stop);
  }

  return (
    <div className="stop-search">
      <input
        type="text"
        placeholder="Search for a bus stop (e.g. Harcourt)"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
      />
      {open && results.length > 0 && (
        <ul className="stop-search-results">
          {results.map((stop) => (
            <li key={stop.stop_id}>
              {/* onMouseDown (not onClick) fires before the input's onBlur closes the list */}
              <button type="button" onMouseDown={() => handleSelect(stop)}>
                {stop.stop_name}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
