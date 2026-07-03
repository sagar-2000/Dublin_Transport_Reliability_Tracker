// Mirrors the bucket rules in scorer/silver.py exactly, so "current bucket"
// on the frontend lines up with the buckets the gold scores were computed for.
// Always resolved in Europe/Dublin local time regardless of the viewer's timezone.

const DUBLIN_TZ = 'Europe/Dublin';

function dublinParts(date) {
  const formatter = new Intl.DateTimeFormat('en-IE', {
    timeZone: DUBLIN_TZ,
    hour: 'numeric',
    hourCycle: 'h23',
    weekday: 'long',
  });
  const parts = formatter.formatToParts(date);
  const hour = Number(parts.find((p) => p.type === 'hour').value);
  const weekday = parts.find((p) => p.type === 'weekday').value;
  return { hour, weekday };
}

export function getHourBucket(hour) {
  if (hour >= 7 && hour < 10) return 'am_peak';
  if (hour >= 10 && hour < 16) return 'midday';
  if (hour >= 16 && hour < 19) return 'pm_peak';
  if (hour >= 19 && hour < 23) return 'evening';
  return 'night';
}

export function getDayType(weekday) {
  if (weekday === 'Saturday') return 'saturday';
  if (weekday === 'Sunday') return 'sunday';
  return 'weekday';
}

export function getCurrentBucket(date = new Date()) {
  const { hour, weekday } = dublinParts(date);
  return { dayType: getDayType(weekday), hourBucket: getHourBucket(hour) };
}

export const HOUR_BUCKET_LABELS = {
  am_peak: 'AM Peak',
  midday: 'Midday',
  pm_peak: 'PM Peak',
  evening: 'Evening',
  night: 'Night',
};

export const DAY_TYPE_LABELS = {
  weekday: 'Weekday',
  saturday: 'Saturday',
  sunday: 'Sunday',
};
