export function RouteFilter({ value, onChange }) {
  return (
    <input
      type="text"
      className="route-filter"
      placeholder="Filter by route (e.g. 46A)"
      value={value}
      onChange={(e) => onChange(e.target.value.trim())}
    />
  );
}
