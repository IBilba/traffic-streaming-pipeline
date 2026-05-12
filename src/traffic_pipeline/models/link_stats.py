"""Per-(t, link) traffic aggregations.

Mirrors the assignment's primary requirement: for each simulation step
``t`` and each link, compute the number of vehicles currently on the
link (``vcount``) and their mean speed (``vspeed``).

The transformation is a pure function on a DataFrame so it can be
unit-tested against a batch input without a Kafka source or a
streaming context.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count

#: UXSIM emits these sentinel values in the ``link`` column for vehicles
#: that are not yet on an edge (``waiting_at_origin_node``) or have
#: finished their trip (``trip_end``). Their ``x``, ``s``, ``v`` are all
#: ``-1``. They are real events worth keeping in ``traffic.raw_data``
#: but they must not contaminate the per-link aggregation.
DEFAULT_STATUS_LINKS: frozenset[str] = frozenset({"waiting_at_origin_node", "trip_end"})


def compute_link_stats(
    parsed: DataFrame,
    status_links: frozenset[str] | set[str] = DEFAULT_STATUS_LINKS,
) -> DataFrame:
    """Aggregate per-(t, link) traffic statistics.

    Drops UXSIM status rows (see :data:`DEFAULT_STATUS_LINKS`), then
    groups by simulation time and link and computes:

    * ``vcount``: number of platoon records on the link at time ``t``.
    * ``vspeed``: mean of the UXSIM ``v`` column at time ``t``. The
      assignment text calls this *speed*; the simulator emits it as
      ``v`` and the simulator wins.

    The function is pure: no side effects, no ``SparkSession`` access,
    no schema assumptions beyond the presence of ``t``, ``link``, ``v``.

    Args:
        parsed: Streaming or batch DataFrame matching
            :data:`traffic_pipeline.preprocessing.schemas.UXSIM_EVENT_SCHEMA`.
        status_links: Set of ``link`` values to treat as status flags
            rather than real edges. Pass an empty set to skip filtering
            (useful when an upstream stage already filtered).

    Returns:
        DataFrame with columns ``t``, ``link``, ``vcount``, ``vspeed``.

    Note:
        This aggregation has no watermark, so its writer must use
        ``outputMode("update")`` rather than ``"append"``. The
        ``groupBy("t","link")`` is unbounded in event time, and Spark
        refuses ``append`` on an aggregation without a watermark.
    """
    filtered = parsed.where(~col("link").isin(list(status_links))) if status_links else parsed
    return filtered.groupBy("t", "link").agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed"),
    )
