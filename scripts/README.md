# scripts/

CLI entry points that orchestrate the modules in `src/traffic_pipeline/`. These scripts are the runnable surface of the project; they wire pure logic together with side effects.

## Files

| File | What it does | How to run |
|---|---|---|
| `run_consumer.py` | Builds a `SparkSession`, opens the Kafka source, parses events, and starts three concurrent `foreachBatch` sinks: raw events (append), per-(t, link) stats (update), 30-second windowed stats (append). | `spark-submit run_consumer.py` inside the `spark-job` container, or `python scripts/run_consumer.py` on the host with PySpark installed. |
| `verify_pipeline.py` | Host-side smoke test. Runs four checks: containers up, topic exists, `raw_data` non-empty, `stats` non-empty. Exits 0 on full pass, 1 on any failure. | `python scripts/verify_pipeline.py [--min-raw N] [--min-stats N]` |

## `run_consumer.py`

Layered configuration: YAML (when `PyYAML` is available) overridden by environment variables overridden by CLI flags. Inside the Spark base image PyYAML cannot be installed (read-only home), so environment variables are authoritative there.

Main env vars: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `SPARK_MASTER_URL`, `MONGO_URI`, `MONGO_DATABASE`, `MONGO_RAW_COLLECTION`, `MONGO_STATS_COLLECTION`, `MONGO_WINDOWED_COLLECTION`, `SPARK_DEBUG_CONSOLE`.

Selected CLI flags: `--debug` enables an extra console sink that prints each `link_stats` micro-batch to stdout, useful when troubleshooting in a laptop terminal.

Checkpoints live under `/tmp/checkpoints/{raw,stats,windowed}` inside the container. They are separate per query because Spark Structured Streaming requires a unique checkpoint location per running query.

Exit codes: `0` on `KeyboardInterrupt`, non-zero if any `StreamingQuery.awaitTermination()` raises.

## `verify_pipeline.py`

Reads connection defaults from environment (`MONGO_URI_HOST` for the host-side Mongo, `KAFKA_TOPIC` for the topic name) and falls back to the project defaults. Each check prints `[OK]` or `[FAIL]` on stdout; a final summary line shows `[OK] N/4 checks passed`.

Typical run:

```powershell
python scripts/verify_pipeline.py
python scripts/verify_pipeline.py --min-raw 1000 --min-stats 50
```

## Interactions with other directories

- Imports everything from `src/traffic_pipeline/`. The `scripts/` package itself contains no domain logic; if a script grows logic worth testing in isolation, that logic moves into `src/traffic_pipeline/` first.
- Reads defaults from `config/default.yaml` when PyYAML is available.
