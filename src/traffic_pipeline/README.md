# traffic_pipeline/

Source package for the traffic streaming pipeline. Code is grouped by functionality, not by type. Each subpackage has its own README that goes deeper.

## Subpackages

| Subpackage | Responsibility |
|---|---|
| `ingestion/` | UXSIM simulation, the Kafka producer, and the edge I/O helpers used by the Spark driver (Spark session builder, Kafka source reader, Mongo sink writer). |
| `preprocessing/` | Spark schema for UXSIM events and the pure parser that turns Kafka binary payloads into typed columns. |
| `models/` | Pure aggregation functions (`compute_link_stats`, `compute_windowed_stats`). No I/O. |
| `evaluation/` | PyMongo helpers and sample aggregation queries used to verify the pipeline and to feed the course report. |
| `serving/` | FastAPI service that exposes the aggregated MongoDB collections over HTTP (bonus deliverable). |

## Boundary rules

- Pure logic lives in `preprocessing/` and `models/`. They depend on Spark types but never open a connection.
- All side effects (Kafka, Spark, Mongo, HTTP) live in `ingestion/` and `serving/`. Long-lived state objects (`KafkaProducer`, `SparkSession`, `MongoClient`, `TrafficRepository`) are the only legitimate OOP classes in this codebase. Everything else is a free function.
- `evaluation/` is read-only over MongoDB; it does not write back.
- Subpackages do not import each other across boundaries arbitrarily. The dependency direction is:

  ```
  ingestion ──▶ preprocessing ──▶ models
       │                              │
       └──── (foreachBatch closure) ──┘
                  ▼
              MongoDB
                  ▼
         evaluation, serving
  ```

## Package metadata

The package is installed in editable mode (`pip install -e .`) from the `src/` layout declared in `pyproject.toml`. Inside Docker, the `PYTHONPATH` is set to `/app/src` instead.

## Submodule init style

Every subpackage's `__init__.py` is intentionally minimal (just a docstring). No eager re-exports. The reason is that several modules (`kafka_publisher`, `run_consumer`) are run with `python -m ...`, and eager re-exports cause a `RuntimeWarning` about double-import when the module gets promoted to `__main__`. Import the full path:

```python
from traffic_pipeline.ingestion.kafka_publisher import TrafficKafkaPublisher
```
