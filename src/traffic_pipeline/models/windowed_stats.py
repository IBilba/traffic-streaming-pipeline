"""Tumbling-window aggregations over simulation time (bonus 1 + 2).

The assignment asks for windowed statistics; by default 30-second
windows of vehicle counts and min/avg/max speed per link. Time here is
*simulation* time (the UXSIM ``t`` column, seconds since the start of
the run), not wall-clock; the cast to a timestamp is purely a vehicle
to let Spark's ``window`` function do the bucketing.

Like :mod:`traffic_pipeline.models.link_stats`, this is a pure
``DataFrame -> DataFrame`` transformation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min, window
from pyspark.sql.types import TimestampType

from traffic_pipeline.models.link_stats import DEFAULT_STATUS_LINKS


def compute_windowed_stats(
    parsed: DataFrame,
    window_seconds: int = 30,
    watermark: str = "60 seconds",
    status_links: frozenset[str] | set[str] = DEFAULT_STATUS_LINKS,
) -> DataFrame:
    """Aggregate per-link statistics over fixed simulation-time windows.

    Casts the UXSIM ``t`` column (sim-seconds) to a timestamp, applies a
    watermark so that ``outputMode("append")`` is legal downstream, and
    computes count + avg/min/max of speed per (window, link).

    The output schema flattens the Spark ``window`` struct into two
    columns (``window_start``, ``window_end``) so it is friendly for
    MongoDB persistence and the FastAPI layer in Phase 8.

    Args:
        parsed: DataFrame matching
            :data:`traffic_pipeline.preprocessing.schemas.UXSIM_EVENT_SCHEMA`.
        window_seconds: Tumbling window size in simulation seconds.
            Defaults to 30 per the assignment.
        watermark: Allowed lateness expression (Spark duration string).
            Must be at least as large as the inter-event spacing the
            producer emits; 60s is comfortable for ``PRODUCE_INTERVAL_SEC=1``.
        status_links: Sentinel ``link`` values to exclude before
            aggregating.

    Returns:
        DataFrame with columns ``window_start``, ``window_end``,
        ``link``, ``vcount``, ``vspeed_avg``, ``vspeed_min``,
        ``vspeed_max``.

    Note:
        Watermarking is mandatory for ``outputMode("append")`` on a
        ``groupBy`` with windows; ``update`` mode would also work but
        append is the natural fit for an immutable time-bucketed sink.
    """
    filtered = parsed.where(~col("link").isin(list(status_links))) if status_links else parsed

    with_event_ts = filtered.withColumn(
        "event_ts",
        col("t").cast("long").cast(TimestampType()),
    ).withWatermark("event_ts", watermark)

    aggregated = (
        with_event_ts.groupBy(window(col("event_ts"), f"{window_seconds} seconds"), col("link"))
        .agg(
            count("*").alias("vcount"),
            avg("v").alias("vspeed_avg"),
            spark_min("v").alias("vspeed_min"),
            spark_max("v").alias("vspeed_max"),
        )
    )

    return aggregated.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("link"),
        col("vcount"),
        col("vspeed_avg"),
        col("vspeed_min"),
        col("vspeed_max"),
    )
