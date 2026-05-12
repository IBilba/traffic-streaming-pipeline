"""Sample analytics queries over ``traffic.stats``.

These mirror the questions the assignment text asks at the end of
Project_Streaming_2026 and the bonus speed-range objective:

* mean speed per link over the whole simulation;
* the N most congested links (by peak vehicle count);
* the link with the largest difference between maximum and minimum
  observed mean speed (bonus 2 expressed as a use case).

Implemented as PyMongo aggregation pipelines so the Spark layer is not
needed at query time; the report can paste the JSON straight into a
table. Each function takes a database handle plus the collection name
so callers can point them at a non-default collection in tests.
"""

from __future__ import annotations

from typing import Any, Mapping

from pymongo.database import Database

DEFAULT_STATS_COLLECTION = "stats"


def avg_speed_per_link(
    db: Database, *, collection: str = DEFAULT_STATS_COLLECTION
) -> list[Mapping[str, Any]]:
    """Return mean ``vspeed`` per link across all simulation steps.

    Pipeline: ``group by link`` then ``avg(vspeed)``. Results are sorted
    by descending mean speed so the fastest links surface first.

    Returns:
        List of ``{"link": str, "avg_speed": float, "samples": int}``
        documents. ``samples`` is the number of ``(t, link)`` rows that
        contributed to the average (useful for spotting low-confidence
        averages on rarely-occupied links).
    """
    pipeline = [
        {
            "$group": {
                "_id": "$link",
                "avg_speed": {"$avg": "$vspeed"},
                "samples": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "link": "$_id",
                "avg_speed": 1,
                "samples": 1,
            }
        },
        {"$sort": {"avg_speed": -1}},
    ]
    return list(db[collection].aggregate(pipeline))


def top_n_congested(
    db: Database, n: int = 5, *, collection: str = DEFAULT_STATS_COLLECTION
) -> list[Mapping[str, Any]]:
    """Return the ``n`` links with the largest peak ``vcount``.

    A "congested" link is one that, at some simulation step, held the
    most vehicles. The pipeline takes ``max(vcount)`` per link and
    returns the top ``n``. ``argmax_t`` is included so the report can
    pinpoint when the peak occurred.

    Args:
        db: Connected PyMongo database handle.
        n: Number of links to return. Must be positive.
        collection: Stats collection name.

    Returns:
        List of ``{"link", "peak_vcount", "argmax_t"}`` documents,
        sorted by descending ``peak_vcount``.

    Raises:
        ValueError: If ``n`` is not a positive integer.
    """
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")
    pipeline = [
        {"$sort": {"vcount": -1}},
        {
            "$group": {
                "_id": "$link",
                "peak_vcount": {"$first": "$vcount"},
                "argmax_t": {"$first": "$t"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "link": "$_id",
                "peak_vcount": 1,
                "argmax_t": 1,
            }
        },
        {"$sort": {"peak_vcount": -1}},
        {"$limit": n},
    ]
    return list(db[collection].aggregate(pipeline))


def max_speed_delta(
    db: Database, n: int = 5, *, collection: str = DEFAULT_STATS_COLLECTION
) -> list[Mapping[str, Any]]:
    """Return links sorted by the spread between max and min ``vspeed``.

    Bonus 2 of the assignment asks for per-link max/min speed. As a
    standalone query that is not very illuminating; the interesting
    derived quantity is the *range* (``max - min``), which highlights
    links whose flow regime changes the most over the simulation.

    Args:
        db: Connected PyMongo database handle.
        n: Number of links to return. Must be positive.
        collection: Stats collection name.

    Returns:
        List of ``{"link", "max_speed", "min_speed", "delta"}``,
        descending by ``delta``.

    Raises:
        ValueError: If ``n`` is not a positive integer.
    """
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")
    pipeline = [
        {
            "$group": {
                "_id": "$link",
                "max_speed": {"$max": "$vspeed"},
                "min_speed": {"$min": "$vspeed"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "link": "$_id",
                "max_speed": 1,
                "min_speed": 1,
                "delta": {"$subtract": ["$max_speed", "$min_speed"]},
            }
        },
        {"$sort": {"delta": -1}},
        {"$limit": n},
    ]
    return list(db[collection].aggregate(pipeline))
