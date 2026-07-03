import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Mirrors scorer/config.py: same SCORING_DATA_DIR env var and the same
// repo-root-relative default, so the API and the PySpark pipeline always
// agree on where gold data lives without duplicating the setting.
const REPO_ROOT = path.resolve(__dirname, '..', '..', '..');
const DATA_DIR = process.env.SCORING_DATA_DIR || path.join(REPO_ROOT, 'data');

export const GOLD_CSV_PATH = path.join(DATA_DIR, 'gold', 'route_reliability_scores.csv');

export const NTA_API_KEY = process.env.NTA_API_KEY;

export const PORT = Number(process.env.PORT || 3000);

// How long to serve a cached NTA feed response before re-polling, in ms.
// Keeps us within the NTA API's fair-use rate limit even under bursty traffic.
export const LIVE_FEED_CACHE_MS = Number(process.env.LIVE_FEED_CACHE_MS || 30_000);
