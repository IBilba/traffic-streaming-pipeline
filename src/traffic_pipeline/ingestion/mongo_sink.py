"""MongoDB sink factory for Spark Structured Streaming.

The MongoDB Spark Connector v10.x does not implement a native streaming
sink for ``writeStream.format("mongodb")``; the supported pattern is
``foreachBatch`` with a regular batch ``DataFrameWriter`` per micro-batch.
This module exposes a small factory that closes over the target database
and collection so the orchestrator in :mod:`scripts.run_consumer` reads
declaratively.

Side effects live entirely behind the returned closure so the rest of
the pipeline (parsing, aggregation) stays pure.
"""

from __future__ import annotations

import logging
from typing import Callable

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

BatchWriter = Callable[[DataFrame, int], None]


def make_mongo_writer(uri: str, database: str, collection: str) -> BatchWriter:
    """Return a ``foreachBatch`` closure that appends to one collection.

    Each invocation writes the micro-batch with ``mode("append")``. The
    Spark output mode (``append`` for raw and windowed, ``update`` for
    the unbounded ``link_stats`` aggregation) is orthogonal: Spark
    applies it before the rows reach this writer, so a single ``append``
    against Mongo is correct for all three sinks. UXSIM's ``t`` is
    monotonic per run, so duplicate ``(t, link)`` keys do not arise
    within a session.

    Args:
        uri: MongoDB connection URI (e.g., ``mongodb://mongo:27017``).
            The Mongo Spark Connector consults its ``database`` /
            ``collection`` options per call, so the same URI is reused
            across sinks.
        database: Target Mongo database (typically ``traffic``).
        collection: Target collection (``raw_data`` / ``stats`` /
            ``stats_windowed``).

    Returns:
        Callable matching the ``foreachBatch(func, batch_id)`` contract.
    """

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        logger.info(
            "Mongo sink: writing %d rows to %s.%s (batch %d)",
            count,
            database,
            collection,
            batch_id,
        )
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("spark.mongodb.connection.uri", uri)
            .option("database", database)
            .option("collection", collection)
            .save()
        )

    return _write
