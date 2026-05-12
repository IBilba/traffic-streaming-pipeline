# config/

YAML defaults that every component of the pipeline reads at startup.

## Responsibility

This directory is the single source of truth for non-secret default values (simulation parameters, broker address, MongoDB URI, Spark settings). The configuration layering is, from lowest to highest precedence:

1. `default.yaml` (this file): baseline values committed to the repo.
2. Environment variables, passed in by `docker-compose.yml` or set on the host before running a script. Documented in `.env.example`.
3. CLI flags on `scripts/run_*.py` for ad hoc overrides.

A higher layer always wins. Code in `src/` and `scripts/` reads `default.yaml` through `traffic_pipeline.ingestion.kafka_publisher.load_config()` (and the mirror logic in `scripts/run_consumer.py`).

## Files

| File | Purpose |
|---|---|
| `default.yaml` | Baseline values for the simulation, the Kafka broker, MongoDB collections, and Spark streaming knobs. |

## Layout of `default.yaml`

The file is organized into four top-level sections:

- `network`: UXSIM grid topology (size, signal cycle, demand).
- `simulation`: `tmax`, seed, produce interval.
- `kafka`: bootstrap servers (in-network), topic name, retry budget.
- `mongo`: URI, database, three collection names (`raw_data`, `stats`, `stats_windowed`).
- `spark`: app name, master URL, checkpoint root, window length, watermark.

## Gotchas

- The broker address listed here (`redpanda:9092`) is only valid from inside the `dsnet` Docker network. Host-side scripts must use `localhost:19092` (env var `KAFKA_BOOTSTRAP_SERVERS_HOST`). Confusing the two is the most common cause of "connection refused" errors.
- The container running `scripts/run_consumer.py` (Spark base image) has a read-only home, so `PyYAML` cannot be pip-installed at startup. The consumer therefore treats `default.yaml` as optional and falls back to environment variables. Keep every config key reachable through an env var.
- Never put secrets here. The repo has none today, but the policy stays.
