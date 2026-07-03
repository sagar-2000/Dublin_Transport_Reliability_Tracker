# Deploying to Render (free tier)

Two services, defined in [render.yaml](render.yaml):

- **dublin-transit-api** — Express API (`webapp/server`), reads
  `data/gold/route_reliability_scores.csv` from the repo checkout and proxies
  the live NTA feed.
- **dublin-transit-client** — static React build (`webapp/client`).

## One-time setup

1. Push this repo to GitHub (already has a remote: `sagar-2000/Dublin_Transport_Reliability_Tracker`).
2. In the Render dashboard: **New > Blueprint**, connect the repo. Render reads
   `render.yaml` and creates both services.
3. On `dublin-transit-api`, set the env var **NTA_API_KEY** (not committed —
   same key from `webapp/server/.env`).
4. Once `dublin-transit-api` deploys, copy its URL (e.g.
   `https://dublin-transit-api.onrender.com`) and set it as
   **VITE_API_BASE_URL** on `dublin-transit-client`, then trigger a manual
   deploy of the client (env var changes require a rebuild since Vite bakes
   them in at build time).

Both services are on Render's free plan — the API will spin down after 15
minutes of inactivity and take ~30-60s to wake back up on the next request
(cold start). This is a known free-tier tradeoff, not a bug.

## Refreshing data after a pipeline run

There's no database and no persistent disk — the gold CSV is committed to
git, and Render serves whatever is in the last-deployed commit. After running
the scoring pipeline on the Oracle VM:

```bash
scp your_vm_user@your_vm_ip:/path/to/Public_transport_tracker/data/gold/route_reliability_scores.csv \
  data/gold/route_reliability_scores.csv
git add data/gold/route_reliability_scores.csv
git commit -m "Refresh gold reliability scores"
git push
```

Pushing to `main` auto-deploys `dublin-transit-api` with the new data. The
client doesn't need a redeploy — it just calls the API at request time.
