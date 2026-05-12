"""Kafka ``readStream`` helper for the streaming consumer.

Thin wrapper around the Spark Structured Streaming Kafka source so the
driver in :mod:`scripts.run_consumer` reads end-to-end like English. The
function performs no transformation; decoding the binary ``value``
payload into typed columns is the job of
:func:`traffic_pipeline.preprocessing.stream_parser.parse_kafka_stream`.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str = "redpanda:9092",
    topic: str = "uxsim",
    starting_offsets: str = "latest",
) -> DataFrame:
    """Open a Kafka streaming source as a Spark ``DataFrame``.

    Args:
        spark: Active :class:`SparkSession` produced by
            :func:`traffic_pipeline.ingestion.spark_session.build_spark_session`.
        bootstrap_servers: Internal listener address. Inside ``dsnet``
            this is ``redpanda:9092``; host-side runs use
            ``localhost:19092``.
        topic: Kafka topic name (typically ``uxsim``).
        starting_offsets: ``"latest"`` to skip backlog, ``"earliest"`` to
            replay everything currently retained.

    Returns:
        Unbounded ``DataFrame`` with the standard Kafka source columns
        (``key``, ``value``, ``topic``, ``partition``, ``offset``,
        ``timestamp``, ``timestampType``). Only ``value`` is consumed
        downstream.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
