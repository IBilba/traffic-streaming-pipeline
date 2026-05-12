# ingestion/

Produces UXSIM traffic events and pushes them to Redpanda. Also hosts the edge I/O helpers used by the Spark driver script (`scripts/run_consumer.py`): Spark session builder, Kafka source reader, and MongoDB foreachBatch sink.

## Responsibility

This package is where the pipeline meets the outside world on the input side. It owns the three side-effectful lifecycle objects allowed by the project paradigm rules (`KafkaProducer`, `SparkSession`, `MongoClient`). Pure logic does **not** live here; it lives in `preprocessing/` and `models/`.

## Files

| File | What it does |
|---|---|
| `uxsim_network.py` | Builds the UXSIM 4x4 grid topology. `NetworkConfig` dataclass + `build_grid_network()` pure builder. |
| `traffic_simulator.py` | Thin wrapper around `World.exec_simulation()` + `analyzer.vehicles_to_pandas()`. |
| `kafka_publisher.py` | `TrafficKafkaPublisher` class, `PublisherConfig`, `load_config()`, `main()` entry point. Drives the simulation step-by-step and emits one Kafka record per UXSIM row, with exponential backoff retries. |
| `kafka_source.py` | `read_kafka_stream(spark, bootstrap, topic, starting_offsets)`. Pure helper that returns a `DataFrame` from the Kafka source. |
| `spark_session.py` | `build_spark_session(...)`. Creates a `SparkSession` wired to the MongoDB Spark Connector and clamps the JVM log level to WARN by default. |
| `mongo_sink.py` | `make_mongo_writer(uri, db, coll)`. Returns a closure suitable for `DataStreamWriter.foreachBatch(...)`. Always uses `mode("append")`; the Spark `outputMode` is orthogonal. |

## Entry points

- `python -m traffic_pipeline.ingestion.kafka_publisher`: runs the producer on the host (requires a Kafka broker reachable at `KAFKA_BOOTSTRAP_SERVERS_HOST`, typically `localhost:19092`).
- `docker compose up uxsim-producer`: runs the same producer in a container, targeting `redpanda:9092` over the `dsnet` network.

## Interactions with other directories

- Reads its defaults from `config/default.yaml`.
- The event schema produced here is the one expected by `preprocessing/schemas.py`. **Changing the JSON payload requires a coordinated edit across this file, the Spark schema, and any model function that references new columns.**
- The Spark helpers (`spark_session.py`, `kafka_source.py`, `mongo_sink.py`) are consumed by `scripts/run_consumer.py`. They do not depend on each other.

## Invariants and gotchas

- The producer container is gated on `redpanda` healthcheck + `topic-init` completing in `docker-compose.yml`; do not relax this without expecting the first batch to land in a "topic does not exist" error.
- `kafka-python-ng` (not stock `kafka-python`) is pinned in `requirements.txt`. Stock `kafka-python 2.0.2` raises `ModuleNotFoundError` on Python 3.12 because of its vendored `six`.
- `mongo_sink.py` lives here, even though it writes data, because `ingestion/` already owns the Kafka producer side-effect class. There is no separate `common/` package by design.
- `outputMode("append")` on a groupBy in Spark requires a watermark. The unbounded `link_stats` sink intentionally uses `outputMode("update")`; do not flip it without also adding a watermark.
