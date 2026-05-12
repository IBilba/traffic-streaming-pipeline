# serving/

FastAPI service that exposes the aggregated MongoDB collections over HTTP. Bonus deliverable.

## Responsibility

A thin read layer over `traffic.stats`. The package owns one `MongoClient` for the lifetime of the process (via the FastAPI `lifespan`), wraps it in a `TrafficRepository`, and injects the repository into HTTP handlers with `Depends`.

## Files

| File | What it provides |
|---|---|
| `repository.py` | `TrafficRepository` (pure PyMongo, no FastAPI imports). Methods: `ping`, `avg_speed_per_link`, `top_n_congested(n)`, `link_detail(link_id)`. Safe to use from non-HTTP contexts (notebooks, scripts) if you want. |
| `api.py` | FastAPI app. Endpoints: `GET /health`, `GET /links/avg-speed`, `GET /links/top-congested?n=...`, `GET /links/{link_id}`. Reads `MONGO_URI`, `MONGO_DATABASE`, `MONGO_STATS_COLLECTION`, `API_HOST`, `API_PORT` from environment. |

## Endpoints

| Method | Path | Returns |
|---|---|---|
| `GET` | `/health` | `{ "status": "ok", "mongo_ok": bool }`. Swallows Mongo errors so an external healthcheck can still parse JSON and distinguish "process alive" from "process alive but Mongo down". |
| `GET` | `/links/avg-speed` | List of `{ link, avg_speed, samples }`, sorted by `avg_speed` ascending. |
| `GET` | `/links/top-congested?n=5` | Top `n` snapshots by `vcount`. |
| `GET` | `/links/{link_id}` | Summary block (`avg_speed`, `min_speed`, `max_speed`, `peak_vcount`, `first_t`, `last_t`) plus the full ordered time series. `404` if the link is unknown. |

## Running

In Docker, enable the `api` profile:

```powershell
docker compose --profile api up -d traffic-api
curl http://localhost:8000/health
```

On the host, with an active venv and the rest of the stack running:

```powershell
$env:MONGO_URI = "mongodb://localhost:27018"
uvicorn traffic_pipeline.serving.api:app --reload
```

## Interactions with other directories

- Reads from the same `traffic.stats` collection that `scripts/run_consumer.py` writes through `models/link_stats.py`. The document shape is documented in `models/README.md`.
- Intentionally does **not** import `evaluation/sample_queries.py`: the two layers share intent, not code, so each can evolve independently.

## Design notes

- One `MongoClient` per process, owned by the FastAPI lifespan. Handlers receive a `TrafficRepository` through `Depends(get_repository)`; never open a new client per request.
- `/links/{link_id}` returns the full ordered series bounded by the simulation length (one row per `(t, link)`). If `SIMULATION_TMAX` grows materially, add a `?since=` / `?limit=` query parameter rather than paginating eagerly.
- The container shares the same Python 3.12 slim base as the producer to keep the dependency surface small. There are no native dependencies.
