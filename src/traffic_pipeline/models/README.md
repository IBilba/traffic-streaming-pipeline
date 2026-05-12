# models/

Pure aggregation functions over the parsed UXSIM event stream. These are the "analytical views" of the domain. No I/O lives here.

## Responsibility

Take a parsed `DataFrame` (matching `preprocessing.schemas.UXSIM_EVENT_SCHEMA`) and return another `DataFrame` ready to be written to MongoDB. The functions are pure: same input, same output, no side effects, no connections opened.

## Files

| File | What it computes |
|---|---|
| `link_stats.py` | `compute_link_stats(parsed, status_links=DEFAULT_STATUS_LINKS)`. Groups by `(t, link)` and emits `vcount` (number of vehicles on the link) and `vspeed` (mean speed). Drops UXSIM status rows (`waiting_at_origin_node`, `trip_end`) before aggregating. |
| `windowed_stats.py` | `compute_windowed_stats(parsed, window_seconds=30, watermark="60 seconds")`. Tumbling window over the simulation-time column, emitting `vcount`, `vspeed_avg`, `vspeed_min`, `vspeed_max` per `(window, link)`. The `window` struct is flattened to `window_start` / `window_end` `TimestampType` columns. Implements the bonus deliverables. |

## Output schemas

`compute_link_stats` returns:

| Column | Type | Notes |
|---|---|---|
| `t` | double | UXSIM simulation time, in seconds. |
| `link` | string | Edge identifier (e.g. `I1W1`). |
| `vcount` | long | Number of vehicles currently on the link. |
| `vspeed` | double | Mean speed (km/h) across vehicles on the link. |

`compute_windowed_stats` returns:

| Column | Type | Notes |
|---|---|---|
| `window_start` | timestamp | Window opening (event time). |
| `window_end` | timestamp | Window closing (event time). |
| `link` | string | Edge identifier. |
| `vcount` | long | Count of events in the window. |
| `vspeed_avg` | double | Mean speed. |
| `vspeed_min` | double | Minimum speed. |
| `vspeed_max` | double | Maximum speed. |

## Design choices

- The status-row filter is a **parameter** (`status_links: frozenset[str] = DEFAULT_STATUS_LINKS`), not a hardcoded constant. That keeps `compute_link_stats` testable with `status_links=frozenset()` and lets callers extend the sentinel set without editing this module.
- `compute_windowed_stats` flattens the Spark `window` struct into two `TimestampType` columns before returning, because the downstream Mongo sink and the REST API both prefer flat documents.
- Watermarking: `link_stats` is **unbounded** and uses `outputMode("update")` upstream. `windowed_stats` requires a watermark (`60 seconds` by default) and uses `outputMode("append")` upstream. Do not change the watermark here without rereading the Spark Structured Streaming "Output Modes" section in the official docs.

## Interactions with other directories

- Inputs: any `DataFrame` matching `preprocessing.schemas.UXSIM_EVENT_SCHEMA`.
- Outputs: written by `scripts/run_consumer.py` via the closure from `ingestion/mongo_sink.py`. The MongoDB collections they land in are configurable: `MONGO_STATS_COLLECTION` (default `stats`) and `MONGO_WINDOWED_COLLECTION` (default `stats_windowed`).
- Read back by `evaluation/sample_queries.py` and `serving/repository.py`.
