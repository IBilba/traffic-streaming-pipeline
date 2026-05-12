# Traffic Streaming Pipeline

End-to-end streaming pipeline that turns simulated vehicle trajectories into queryable, real-time traffic statistics.

## What it does

The pipeline produces synthetic vehicle traffic with [UXSIM](https://github.com/toruseo/UXsim), streams the resulting events through a [Redpanda](https://redpanda.com/) (Kafka-compatible) broker, processes them with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), and stores both the raw events and aggregated per-link statistics in [MongoDB](https://www.mongodb.com/). Everything runs in Docker — `docker compose up -d` is the only command you need to bring the whole stack online.

This is the course project for **Big Data Management Systems (CEID_NE4348)**, University of Patras, Spring 2026.

## Architecture

```
 ┌─────────────────┐     JSON     ┌──────────────┐     Kafka      ┌──────────────┐
 │  UXSIM producer │ ───────────▶│  Redpanda    │ ─────────────▶ │ Spark        │
 │  (container)    │              │  topic:      │                │ Structured   │
 └─────────────────┘              │  uxsim       │                │ Streaming    │
                                  └──────────────┘                └──────┬───────┘
                                                                         │ foreachBatch
                                                              ┌──────────┴────────────┐
                                                              ▼                       ▼
                                                  ┌────────────────────┐  ┌────────────────────┐
                                                  │ MongoDB            │  │ MongoDB            │
                                                  │ traffic.raw_data   │  │ traffic.stats      │
                                                  │ (every event)      │  │ (vcount + vspeed   │
                                                  │                    │  │  per (t, link))    │
                                                  └────────────────────┘  └────────────────────┘
                                                                                      ▲
                                                                                      │ HTTP
                                                                              ┌───────┴───────┐
                                                                              │  FastAPI      │
                                                                              │  (bonus)      │
                                                                              └───────────────┘
```

Six containers run by default — `redpanda`, `topic-init` (one-shot), `spark-master`, `spark-worker`, `spark-job`, `mongo`, plus the `uxsim-producer` — and an optional `traffic-api` when the `api` profile is enabled.

## Installation

**Prerequisites:**
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows / macOS) **or** Docker Engine + Compose plugin (Linux). Verify with `docker ps`.
- ~4 GB of free RAM allocated to Docker.
- No local Python install is required to run the pipeline. To run unit tests or scripts on the host you also need Python 3.11+ and `pip install -r requirements.txt`.

**Clone and launch:**

```powershell
git clone https://github.com/IBilba/traffic-streaming-pipeline.git
cd traffic-streaming-pipeline
copy .env.example .env          # adjust if needed
docker compose up -d
```

Within ~60 seconds, the `traffic.raw_data` and `traffic.stats` collections will start filling.

## Configuration

All knobs live in `.env` (copied from `.env.example`) and are passed into containers by `docker-compose.yml`. CLI arguments override env vars where supported.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `redpanda:9092` | Broker address used by services inside `dsnet`. |
| `KAFKA_BOOTSTRAP_SERVERS_HOST` | `localhost:19092` | External listener for host-side scripts / `rpk`. |
| `KAFKA_TOPIC` | `uxsim` | Topic that producer writes to and consumer subscribes to. |
| `PRODUCE_INTERVAL_SEC` | `1` | Wall-clock pause between simulation time steps in the producer. |
| `SIMULATION_TMAX` | `3600` | Total simulated seconds. |
| `SIMULATION_SEED` | *(empty)* | Optional integer seed for reproducible UXSIM runs. |
| `MONGO_URI` | `mongodb://mongo:27017` | URI for in-network services. |
| `MONGO_URI_HOST` | `mongodb://localhost:27017` | URI for `mongosh` on the host. |
| `MONGO_DATABASE` | `traffic` | Target database. |
| `MONGO_RAW_COLLECTION` | `raw_data` | Per-event sink. |
| `MONGO_STATS_COLLECTION` | `stats` | Per-(t, link) aggregated sink. |
| `MONGO_WINDOWED_COLLECTION` | `stats_windowed` | 30-second windowed aggregations (bonus). |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | Master URL used by `spark-submit`. |
| `SPARK_DEBUG_CONSOLE` | `0` | If `1`, also stream stats to stdout for debugging. |
| `API_HOST` | `0.0.0.0` | Bind interface for the FastAPI service. |
| `API_PORT` | `8000` | API port (mapped to host). |

## Usage

### Bring the stack up

```powershell
docker compose up -d
docker compose ps                  # all services should be Up / healthy
```

### Inspect the broker

```powershell
docker exec -it redpanda rpk topic consume uxsim --num 5
```

### Inspect Spark progress

```powershell
docker logs -f spark-job           # look for "Batch processed"
```

### Inspect MongoDB

```powershell
docker exec -it mongo mongosh
```

```javascript
use traffic
db.raw_data.countDocuments()
db.stats.find().sort({_id: -1}).limit(5)
// avg speed per link (the assignment's required query)
db.stats.aggregate([
  { $group: { _id: "$link", avg_speed: { $avg: "$vspeed" } } },
  { $sort: { avg_speed: 1 } }
])
```

### Enable the REST API (bonus)

```powershell
docker compose --profile api up -d traffic-api
curl http://localhost:8000/health
curl http://localhost:8000/links/avg-speed
curl http://localhost:8000/links/top-congested?n=5
```

### Tear down

```powershell
docker compose down                # keeps MongoDB data (named volume)
docker compose down -v             # also wipes MongoDB data
```

## Data

There is no external dataset. All events are generated by UXSIM on demand from a deterministic 4×4 grid topology defined in `src/traffic_pipeline/ingestion/uxsim_network.py`. Each event carries the columns returned by `UXSIM.World.analyzer.vehicles_to_pandas()` (`name`, `dn`, `orig`, `dest`, `t`, `link`, `x`, `s`, `v`). Aggregated outputs and their schemas are documented in [`src/traffic_pipeline/models/README.md`](src/traffic_pipeline/models/README.md).

## Project structure

```
traffic-streaming-pipeline/
├── docker-compose.yml          # Stack definition (6 services + api profile)
├── Dockerfile.producer         # Image for UXSIM producer
├── Dockerfile.api              # Image for FastAPI service
├── config/                     # YAML configuration files
├── src/traffic_pipeline/       # Python source — one subpackage per concern
│   ├── ingestion/              # UXSIM simulation + Kafka producer
│   ├── preprocessing/          # Spark schemas + JSON parsing
│   ├── models/                 # Aggregations (link stats, windowed stats)
│   ├── evaluation/             # MongoDB inspection + sample queries
│   └── serving/                # FastAPI REST API (bonus)
├── scripts/                    # CLI entry points (run_producer, run_consumer, …)
├── tests/                      # pytest: unit/ + integration/
├── docs/                       # Architecture diagram, docker cheatsheet, report
├── notebooks/                  # Exploratory notebooks (not source of truth)
└── Code/                       # Original course-provided skeletons (read-only reference)
```

## Testing

```powershell
pip install -r requirements.txt
pytest tests/unit/                       # fast, no Docker needed
pytest tests/integration/ -m integration # requires a running compose stack
```

End-to-end smoke test:

```powershell
python scripts/verify_pipeline.py
```

## License

[MIT](LICENSE).
