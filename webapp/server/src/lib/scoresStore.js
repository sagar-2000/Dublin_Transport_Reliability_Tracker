import { createCsvLoader } from './csvCache.js';
import { GOLD_CSV_PATH } from '../config.js';

const NUMERIC_FIELDS = [
  'score_1_to_5',
  'pct_on_time',
  'avg_delay_minutes',
  'delay_stddev_minutes',
  'cancellation_rate',
  'sample_size',
];

export const loadScores = createCsvLoader(GOLD_CSV_PATH, NUMERIC_FIELDS);
