# preprocessing/

Pure Spark code that turns the raw Kafka payload into typed, queryable columns. No I/O lives here; the Kafka source itself is wired up in `ingestion/kafka_source.py`.

## Responsibility

Define the canonical schema of a UXSIM event and parse the JSON payload from Kafka into a Spark `DataFrame` that downstream aggregations (`models/`) can consume.

## Files

| File | What it does |
|---|---|
| `schemas.py` | `UXSIM_EVENT_SCHEMA: StructType` with the nine fields produced by `UXSIM.World.analyzer.vehicles_to_pandas()`: `name`, `dn`, `orig`, `dest`, `t`, `link`, `x`, `s`, `v`. |
| `stream_parser.py` | `parse_kafka_stream(df_raw, schema) -> DataFrame`. Pure function: takes the binary `value` column from the Kafka source, casts it to string, decodes via `from_json`, and returns the flattened columns. |

## Conventions

- The simulator's column name for speed is `v`, not `speed`. The assignment text and several queries use the word "speed"; this codebase follows the simulator. Aggregations that produce a friendlier name (e.g. `vspeed`) do so in `models/`, not here.
- The parser **does not** filter UXSIM status rows (`link` = `"waiting_at_origin_node"` or `"trip_end"`). That filtering lives in `models/link_stats.py`, where it is a parameter rather than a hardcoded constant. The parser's job is to faithfully type the payload.

## Interactions with other directories

- Consumed by `models/link_stats.py` and `models/windowed_stats.py`.
- Called from `scripts/run_consumer.py` right after the Kafka source is opened.
- The schema must mirror the JSON written by `ingestion/kafka_publisher.py`. Changing one without the other breaks the pipeline silently (Spark fills missing columns with `null`).

## Testing notes

The unit tests in `tests/unit/test_schemas.py` build input rows entirely in Catalyst (`spark.range(...).select(expr("CAST('...' AS BINARY) AS value"))`) instead of `spark.createDataFrame([...])`. The latter spawns a Python worker process whose handshake intermittently fails on Windows (`SocketTimeoutException: Accept timed out`). Stick to the Catalyst pattern for any new test in this package.
