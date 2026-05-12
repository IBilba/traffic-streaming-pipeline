# evaluation/

PyMongo helpers for inspecting the data that the pipeline lands in MongoDB, plus a small library of canned aggregation queries used to verify the pipeline and to feed the course report.

## Responsibility

Read-only access to `traffic.raw_data`, `traffic.stats`, and `traffic.stats_windowed`. None of these functions write back to Mongo; none of them open the connection themselves either. Callers pass in a `pymongo.database.Database` handle, which keeps the connection lifecycle out of the helpers and makes them trivially mockable in tests.

## Files

| File | What it provides |
|---|---|
| `mongo_inspector.py` | Counters and sampling helpers: `count_raw`, `count_stats`, `count_windowed`, `latest_n`, `distinct_links`, `sample_document`, `collection_handles`. |
| `sample_queries.py` | PyMongo aggregation pipelines: `avg_speed_per_link`, `top_n_congested(n=5)`, `max_speed_delta(n=5)`. |

## Query summaries

- `avg_speed_per_link(db)` answers the assignment's required question ("average vehicle speed per link"). Sorts ascending so that the slowest links surface first.
- `top_n_congested(db, n=5)` ranks `(t, link)` snapshots by `vcount` descending; surfaces the worst instantaneous congestion.
- `max_speed_delta(db, n=5)` returns the links with the largest spread between max and min speed across the run. It is implemented as a derived range over `stats` so the result is interesting even on a short run; the assignment's bonus ("max/min speed per link") is covered both by this query and by the columns produced in `models.windowed_stats.compute_windowed_stats`.

## Interactions with other directories

- Reads from the collections written by `models/*` through `scripts/run_consumer.py`. Schema definitions live in `models/README.md`.
- The host-side smoke test in `scripts/verify_pipeline.py` uses the counters here.
- `serving/repository.py` deliberately **mirrors** these queries in intent without importing them. The two layers evolve independently: the report uses absolute query shapes, the REST API uses projections shaped for JSON responses.

## Conventions

- Every function takes the `Database` handle as its first argument and a collection name as a keyword argument with a sensible default (`DEFAULT_RAW_COLLECTION`, `DEFAULT_STATS_COLLECTION`, `DEFAULT_WINDOWED_COLLECTION`).
- All functions are read-only and synchronous. Streaming back from a cursor is fine; do not buffer entire collections in memory.
