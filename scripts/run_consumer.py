"""Spark Structured Streaming driver: UXSIM events into MongoDB.

This script is the ``spark-submit`` entry point of the consumer side of
the pipeline. It wires three independent streaming queries on top of a
single Kafka ``readStream``:

* **raw**: the parsed events, appended to ``traffic.raw_data``.
* **link_stats**: ``groupBy(t, link)`` aggregates, written in ``update``
  mode to ``traffic.stats``. The aggregation has no watermark on
  simulation time, so ``outputMode("append")`` would be illegal here.
* **windowed**: 30s tumbling-window aggregates with watermark, appended
  to ``traffic.stats_windowed`` (bonus 1 + 2).

All transformations (parsing in
:mod:`traffic_pipeline.preprocessing.stream_parser`, aggregations in
:mod:`traffic_pipeline.models`) are pure functions imported from the
library. This module owns only side effects: session, source, sinks.

Layered configuration: ``config/default.yaml`` (lowest), then environment
variables, then CLI flags (highest), mirroring the producer in
:mod:`traffic_pipeline.ingestion.kafka_publisher`.

Example:
    spark-submit --master spark://spark-master:7077 \\
        --jars /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.2.jar,... \\
        scripts/run_consumer.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    # PyYAML is optional. Inside the spark-job container we run as a
    # non-root user without a writable home, so we cannot pip-install it
    # at startup; env vars carry every setting we need. Host-side runs
    # should `pip install pyyaml` to enable YAML defaults.
    yaml = None  # type: ignore[assignment]

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from traffic_pipeline.ingestion.kafka_source import read_kafka_stream
from traffic_pipeline.ingestion.mongo_sink import make_mongo_writer
from traffic_pipeline.ingestion.spark_session import build_spark_session
from traffic_pipeline.models.link_stats import compute_link_stats
from traffic_pipeline.models.windowed_stats import compute_windowed_stats
from traffic_pipeline.preprocessing.schemas import UXSIM_EVENT_SCHEMA
from traffic_pipeline.preprocessing.stream_parser import parse_kafka_stream

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsumerConfig:
    """Runtime parameters for the streaming consumer driver."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    starting_offsets: str
    mongo_uri: str
    mongo_database: str
    coll_raw: str
    coll_stats: str
    coll_windowed: str
    spark_app_name: str
    spark_master: str | None
    checkpoint_root: str
    window_seconds: int
    watermark: str
    debug: bool


def _resolve_default_config_path() -> Path:
    env_override = os.environ.get("TRAFFIC_PIPELINE_CONFIG")
    if env_override:
        return Path(env_override)
    # scripts/ -> repo root
    return Path(__file__).resolve().parents[1] / "config" / "default.yaml"


def load_config(
    config_path: Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
) -> ConsumerConfig:
    """Compose the consumer config from YAML, env, and CLI overrides."""
    path = config_path or _resolve_default_config_path()
    raw: dict[str, Any] = {}
    if yaml is None:
        logger.info("PyYAML not installed; using built-in defaults and env vars only.")
    elif path.exists():
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    else:
        logger.warning("Config file %s not found; relying on defaults and env.", path)

    kafka_section = raw.get("kafka", {}) or {}
    mongo_section = raw.get("mongo", {}) or {}
    mongo_collections = mongo_section.get("collections", {}) or {}
    spark_section = raw.get("spark", {}) or {}
    streaming_section = spark_section.get("streaming", {}) or {}

    cfg = ConsumerConfig(
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            kafka_section.get("bootstrap_servers", "redpanda:9092"),
        ),
        kafka_topic=os.environ.get(
            "KAFKA_TOPIC", kafka_section.get("topic", "uxsim")
        ),
        starting_offsets=os.environ.get(
            "KAFKA_STARTING_OFFSETS",
            spark_section.get("starting_offsets", "latest"),
        ),
        mongo_uri=os.environ.get(
            "MONGO_URI", mongo_section.get("uri", "mongodb://mongo:27017")
        ),
        mongo_database=os.environ.get(
            "MONGO_DATABASE", mongo_section.get("database", "traffic")
        ),
        coll_raw=mongo_collections.get("raw", "raw_data"),
        coll_stats=mongo_collections.get("stats", "stats"),
        coll_windowed=mongo_collections.get("windowed", "stats_windowed"),
        spark_app_name=spark_section.get("app_name", "UXSIM-Consumer"),
        spark_master=os.environ.get(
            "SPARK_MASTER", spark_section.get("master", "spark://spark-master:7077")
        ),
        checkpoint_root=os.environ.get(
            "CHECKPOINT_ROOT",
            spark_section.get("checkpoint_root", "/tmp/checkpoints"),
        ),
        window_seconds=int(streaming_section.get("window_seconds", 30)),
        watermark=str(streaming_section.get("watermark", "60 seconds")),
        debug=False,
    )

    if overrides:
        cfg = _apply_overrides(cfg, overrides)
    return cfg


def _apply_overrides(cfg: ConsumerConfig, ov: dict[str, Any]) -> ConsumerConfig:
    from dataclasses import replace

    updates: dict[str, Any] = {}
    mapping = {
        "kafka_bootstrap": "kafka_bootstrap_servers",
        "kafka_topic": "kafka_topic",
        "starting_offsets": "starting_offsets",
        "mongo_uri": "mongo_uri",
        "database": "mongo_database",
        "spark_master": "spark_master",
        "checkpoint_root": "checkpoint_root",
        "window_seconds": "window_seconds",
        "watermark": "watermark",
        "debug": "debug",
    }
    for cli_key, attr in mapping.items():
        if cli_key in ov and ov[cli_key] is not None:
            updates[attr] = ov[cli_key]
    return replace(cfg, **updates) if updates else cfg


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traffic-consumer",
        description="Spark Structured Streaming consumer: Kafka -> parse -> MongoDB.",
    )
    parser.add_argument("--kafka-bootstrap", dest="kafka_bootstrap")
    parser.add_argument("--kafka-topic", dest="kafka_topic")
    parser.add_argument(
        "--starting-offsets",
        choices=["latest", "earliest"],
        dest="starting_offsets",
    )
    parser.add_argument("--mongo-uri", dest="mongo_uri")
    parser.add_argument("--database", dest="database", help="Target Mongo database name.")
    parser.add_argument("--spark-master", dest="spark_master")
    parser.add_argument(
        "--checkpoint-root",
        dest="checkpoint_root",
        help="Filesystem path for Structured Streaming checkpoints.",
    )
    parser.add_argument("--window-seconds", type=int, dest="window_seconds")
    parser.add_argument("--watermark", dest="watermark")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Also stream link_stats to the console sink for debugging.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level. Default: INFO.",
    )
    return parser


def _start_mongo_sink(
    stream: DataFrame,
    *,
    mongo_uri: str,
    database: str,
    collection: str,
    output_mode: str,
    checkpoint: str,
    query_name: str,
) -> StreamingQuery:
    """Wire a streaming DataFrame to a Mongo collection via ``foreachBatch``."""
    logger.info(
        "Starting query %r -> %s.%s (mode=%s, checkpoint=%s).",
        query_name,
        database,
        collection,
        output_mode,
        checkpoint,
    )
    return (
        stream.writeStream.queryName(query_name)
        .outputMode(output_mode)
        .foreachBatch(make_mongo_writer(mongo_uri, database, collection))
        .option("checkpointLocation", checkpoint)
        .start()
    )


def run(cfg: ConsumerConfig) -> int:
    """Build the streaming graph and block until any query terminates.

    The three sinks share one parsed stream; Spark plans each as an
    independent micro-batch query with its own checkpoint directory.
    """
    spark = build_spark_session(
        app_name=cfg.spark_app_name,
        master=cfg.spark_master,
        mongo_uri=cfg.mongo_uri,
    )
    logger.info(
        "Spark session ready: master=%s, app=%s.",
        cfg.spark_master or "(submit-default)",
        cfg.spark_app_name,
    )

    raw_stream = read_kafka_stream(
        spark,
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        topic=cfg.kafka_topic,
        starting_offsets=cfg.starting_offsets,
    )
    parsed = parse_kafka_stream(raw_stream, UXSIM_EVENT_SCHEMA)

    link_stats = compute_link_stats(parsed)
    windowed = compute_windowed_stats(
        parsed,
        window_seconds=cfg.window_seconds,
        watermark=cfg.watermark,
    )

    checkpoint_root = cfg.checkpoint_root.rstrip("/")
    queries = [
        _start_mongo_sink(
            parsed,
            mongo_uri=cfg.mongo_uri,
            database=cfg.mongo_database,
            collection=cfg.coll_raw,
            output_mode="append",
            checkpoint=f"{checkpoint_root}/raw",
            query_name="raw",
        ),
        _start_mongo_sink(
            link_stats,
            mongo_uri=cfg.mongo_uri,
            database=cfg.mongo_database,
            collection=cfg.coll_stats,
            # groupBy("t","link") without a watermark -> "update" only.
            output_mode="update",
            checkpoint=f"{checkpoint_root}/stats",
            query_name="link_stats",
        ),
        _start_mongo_sink(
            windowed,
            mongo_uri=cfg.mongo_uri,
            database=cfg.mongo_database,
            collection=cfg.coll_windowed,
            # Watermark declared inside compute_windowed_stats -> append legal.
            output_mode="append",
            checkpoint=f"{checkpoint_root}/windowed",
            query_name="windowed_stats",
        ),
    ]

    if cfg.debug:
        logger.info("Debug console sink enabled for link_stats.")
        queries.append(
            link_stats.writeStream.queryName("link_stats_console")
            .outputMode("update")
            .format("console")
            .option("truncate", False)
            .start()
        )

    logger.info("All %d streaming queries started; awaiting termination.", len(queries))
    spark.streams.awaitAnyTermination()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # py4j chatters one INFO per Python-to-JVM callback; the bridge is
    # healthy when it's quiet, noisy when it's busy. Useful for debugging
    # the gateway itself, useless for everyone else.
    for noisy in ("py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    overrides = {
        k: v for k, v in vars(args).items()
        if v is not None and k != "log_level"
    }
    cfg = load_config(overrides=overrides)
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
