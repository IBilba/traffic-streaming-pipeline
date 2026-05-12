"""SparkSession factory for the streaming consumer.

The ``SparkSession`` is a long-lived driver-state object built once at
the top of :mod:`scripts.run_consumer`. Keeping its construction in a
dedicated module makes it trivial to swap masters (``spark://...`` vs
``local[*]``) and to point Mongo writes at different URIs without
touching the orchestrator.

The required JARs (``spark-sql-kafka-0-10``, ``mongo-spark-connector``,
their transitive Mongo Java driver pieces, ``commons-pool2``) ship
inside the ``ezachar/db_project2026:v0.2`` image at ``/opt/spark/jars``
and are passed via ``--jars`` on ``spark-submit``. We do not declare
them via ``spark.jars.packages`` to avoid a network resolve at session
start.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str = "UXSIM-Consumer",
    master: str | None = "spark://spark-master:7077",
    mongo_uri: str = "mongodb://mongo:27017",
    log_level: str = "WARN",
) -> SparkSession:
    """Build the streaming consumer's :class:`SparkSession`.

    Args:
        app_name: Spark UI / log label.
        master: Cluster master URL. Pass ``None`` to let an outer
            ``spark-submit --master ...`` win, or ``"local[*]"`` for a
            host-side smoke test outside Docker.
        mongo_uri: Connection URI consumed by the MongoDB Spark
            Connector v10.x via ``spark.mongodb.write.connection.uri``.
            Per-collection ``database`` / ``collection`` are set on each
            ``write`` call in :mod:`traffic_pipeline.ingestion.mongo_sink`.
        log_level: Spark JVM log level. ``"WARN"`` silences the per-task
            INFO firehose (broadcast tracking, DAG scheduler chatter,
            offset commits) while still surfacing actual problems. Pass
            ``"INFO"`` if you need the full Spark trace.

    Returns:
        A configured (or pre-existing) ``SparkSession``.
    """
    builder = SparkSession.builder.appName(app_name).config(
        "spark.mongodb.write.connection.uri", mongo_uri
    )
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
