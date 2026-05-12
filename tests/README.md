# tests/

Pytest suite. Unit tests run without Docker; integration tests require a running compose stack.

## Layout

| Directory | Marker | Requires Docker | Runtime |
|---|---|---|---|
| `tests/unit/` | none | no | ~45s on a cold start (mostly the Spark fixture warmup) |
| `tests/integration/` | `@pytest.mark.integration` | yes | ~1s once the stack is healthy |

## Running

```powershell
pip install -r requirements.txt
pytest tests/unit/                       # 25 tests, no Docker
pytest tests/integration/ -m integration # requires `docker compose up -d`
```

`pytest -m integration` is safe to run with the stack down: the test skips cleanly on `PyMongoError` instead of failing.

## Files

| File | What it covers |
|---|---|
| `tests/conftest.py` | Session-scoped `local[1]` `SparkSession` fixture used by every test that touches Spark. |
| `tests/unit/test_schemas.py` (3) | Field list of `UXSIM_EVENT_SCHEMA`, JSON round-trip through `parse_kafka_stream`, status-row preservation. |
| `tests/unit/test_link_stats.py` (4) | Aggregation correctness, custom `status_links`, empty input, schema of the output. |
| `tests/unit/test_windowed_stats.py` (4) | Window boundaries, watermark behaviour, flattened columns, `vspeed_min` / `vspeed_max` against a known input. |
| `tests/unit/test_kafka_publisher.py` (6) | Numpy coercion in `_to_json_safe`, dry-run path, real-path producer construction (bootstrap, serializer, retries), retry-then-succeed, retry-budget exhaustion, context-manager flush. Uses `unittest.mock.patch` on `traffic_pipeline.ingestion.kafka_publisher.KafkaProducer`. |
| `tests/unit/test_repository.py` (8) | `TrafficRepository` against a `MagicMock` PyMongo database. Asserts pipeline shape (group keys, sort direction, `$limit`), `n <= 0` guard, `link_detail` summary+series composition, 404 path, custom-collection routing. |
| `tests/integration/test_end_to_end.py` (1) | Polls Mongo on `MONGO_URI_HOST` for up to `INTEGRATION_WARMUP_SECONDS` and asserts that `raw_data` and `stats` are non-empty. Skips on connection failure. |

## Conventions

- **No `spark.createDataFrame([...])` from Python lists.** It spawns a Python worker whose handshake intermittently fails on Windows with `SocketTimeoutException: Accept timed out`. Build inputs with `spark.range(...)` + `expr(...)` or `spark.sql("VALUES ...")` so that the JVM does all the work. Same rule applies to UDFs; avoid them in tests.
- **Patch at the import site, not the source.** For `traffic_pipeline.ingestion.kafka_publisher`, patch `traffic_pipeline.ingestion.kafka_publisher.KafkaProducer`, not `kafka.KafkaProducer`. Patching the source after the module has already bound the name does nothing.
- **No `mongomock`.** Repository tests assert on the pipeline shape using `MagicMock`. Re-running aggregations in an in-memory Mongo would add a dependency for value the assertions do not need.
- New pure logic in `models/` is the easiest test target. Side-effect-heavy code in `scripts/` is harder to unit-test and is covered by the integration test instead.
