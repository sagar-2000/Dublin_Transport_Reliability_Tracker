import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { PORT } from './config.js';
import { scoresRouter } from './routes/scores.js';
import { liveRouter } from './routes/live.js';

const app = express();
app.use(cors());

app.get('/api/health', (req, res) => res.json({ status: 'ok' }));
app.use('/api/scores', scoresRouter);
app.use('/api/live', liveRouter);

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

app.listen(PORT, () => {
  console.log(`Dublin transit API listening on http://localhost:${PORT}`);
});
