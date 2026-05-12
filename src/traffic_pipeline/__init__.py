"""Streaming pipeline for UXSIM-generated vehicle traffic.

The package is organised by concern (CLAUDE.md section 1.3):

* :mod:`traffic_pipeline.ingestion` -- UXSIM simulation and Kafka producer.
* :mod:`traffic_pipeline.preprocessing` -- Spark schemas and JSON parsing.
* :mod:`traffic_pipeline.models` -- Pure-function aggregations.
* :mod:`traffic_pipeline.evaluation` -- MongoDB inspection helpers.
* :mod:`traffic_pipeline.serving` -- FastAPI REST API (bonus).
"""

__version__ = "0.1.0"
