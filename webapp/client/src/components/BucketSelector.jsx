import { DAY_TYPE_LABELS, HOUR_BUCKET_LABELS } from '../lib/timeBuckets.js';

export function BucketSelector({ dayType, hourBucket, onChangeDayType, onChangeHourBucket, onResetToNow }) {
  return (
    <div className="bucket-selector">
      <select value={dayType} onChange={(e) => onChangeDayType(e.target.value)}>
        {Object.entries(DAY_TYPE_LABELS).map(([value, label]) => (
          <option key={value} value={value}>{label}</option>
        ))}
      </select>
      <select value={hourBucket} onChange={(e) => onChangeHourBucket(e.target.value)}>
        {Object.entries(HOUR_BUCKET_LABELS).map(([value, label]) => (
          <option key={value} value={value}>{label}</option>
        ))}
      </select>
      <button type="button" onClick={onResetToNow}>Now</button>
    </div>
  );
}
