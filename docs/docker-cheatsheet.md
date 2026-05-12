# Docker cheatsheet

Everyday commands for poking at the running stack. Assumes you are in
the repo root with `docker-compose.yml` next to you.

## Lifecycle

```powershell
docker compose up -d                       # bring everything up in the background
docker compose --profile api up -d         # also start traffic-api
docker compose ps                          # list services + health status
docker compose stop                        # stop containers, keep volumes
docker compose down                        # stop and remove containers, keep volumes
docker compose down -v                     # also drop the mongo_data volume (destroys traffic.* data)
```

## Logs

```powershell
docker logs -f spark-job                   # follow the consumer driver
docker logs --tail 200 uxsim-producer      # last 200 lines from the producer
docker compose logs -f redpanda topic-init # multi-service follow
```

Common log markers to look for:

- `spark-job`: `Mongo sink: writing N rows to traffic.raw_data (batch K)`
- `uxsim-producer`: `Producing t=... link=... v=...`
- `topic-init`: `TOPIC uxsim OK` then exits 0
- `redpanda`: `cluster health is GREEN`

## Exec into a container

```powershell
docker exec -it redpanda rpk cluster info
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic consume uxsim --num 5
docker exec -it redpanda rpk topic delete uxsim     # destructive, only when iterating
docker exec -it mongo mongosh
docker exec -it spark-job bash               # poke around inside the Spark image
```

Inside `mongosh`:

```javascript
use traffic
db.raw_data.countDocuments()
db.stats.find().sort({ _id: -1 }).limit(5)
db.stats.aggregate([
  { $group: { _id: "$link", avg_speed: { $avg: "$vspeed" } } },
  { $sort: { avg_speed: 1 } }
])
```

## Volume inspection

```powershell
docker volume ls | findstr mongo_data
docker volume inspect traffic-streaming-pipeline_mongo_data
```

## Disk / image hygiene

```powershell
docker system df                            # how much is Docker using?
docker image prune                          # remove dangling images
docker builder prune                        # remove old build cache (recovers GBs)
docker system prune -af --volumes           # nuclear option, deletes everything not currently in use
```

## Common gotchas

- **Broker listeners**: in-network services use `redpanda:9092`. Host-side scripts (`rpk` on the host, ad hoc Python) use `localhost:19092`. The wrong one gives "connection refused".
- **MongoDB host port**: the container's `27017` is mapped to host `27018` to avoid clashing with a host MongoDB. Compass / `mongosh` on the host should target `mongodb://localhost:27018`.
- **`down -v` wipes data**. The named volume `mongo_data` holds `traffic.raw_data`, `traffic.stats`, `traffic.stats_windowed`. Plain `down` keeps them; `down -v` deletes them.
- **`topic-init` is a one-shot service**. It exits 0 after creating the topic. `docker compose ps` will show it as `Exited (0)`; that is the intended state, not a failure.
- **`spark-job` may fail to install PyYAML**. The Spark base image runs as a non-root user with a read-only home, so any `pip install` in the entrypoint will be skipped silently. The consumer treats PyYAML as optional and reads env vars instead.

## Building images

```powershell
docker compose build                        # all services with a Dockerfile
docker compose build uxsim-producer         # just the producer
docker compose build --no-cache traffic-api # force a clean rebuild
```

## Resetting from a known state

```powershell
docker compose down -v
docker compose up -d
python scripts/verify_pipeline.py
```

Wait ~60 seconds between `up -d` and `verify_pipeline.py` to let Spark
land its first batch.
