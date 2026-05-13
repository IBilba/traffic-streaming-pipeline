"""PyMongo repository backing the REST API.

The repository binds a :class:`pymongo.database.Database` handle plus
the collection names the streaming pipeline writes to, and exposes a
small set of read-only queries that mirror the analytical questions in
:mod:`traffic_pipeline.evaluation.sample_queries`. Keeping the two
layers separate (instead of importing one from the other) lets the REST
shape evolve independently of the report-oriented helpers; the SQL-like
intent is shared, the projections are not.

The class has no FastAPI imports on purpose so the queries can be unit
tested against a ``mongomock`` or hand-rolled fake database.
"""

from __future__ import annotations

from typing import Any, Mapping

from pymongo.database import Database


class TrafficRepository:
    """Read-only PyMongo gateway over the ``traffic.*`` collections.

    Attributes:
        db: Connected PyMongo database handle.
        stats_collection: Collection holding per-step link aggregates
            (default ``stats``). Populated by ``compute_link_stats``.
        raw_collection: Collection holding raw UXSIM events (default
            ``raw_data``). Currently unused by the API but kept here so
            future endpoints can reach it without re-plumbing config.
        windowed_collection: Collection holding 30-second windowed
            aggregates (default ``stats_windowed``).
    """

    def __init__(
        self,
        db: Database,
        *,
        stats_collection: str = "stats",
        raw_collection: str = "raw_data",
        windowed_collection: str = "stats_windowed",
    ) -> None:
        self.db = db
        self.stats_collection = stats_collection
        self.raw_collection = raw_collection
        self.windowed_collection = windowed_collection

    def ping(self) -> bool:
        """Return ``True`` iff the underlying MongoDB responds to ``ping``.

        Used by the ``/health`` endpoint to distinguish "API process
        alive" from "API process alive and Mongo reachable".
        """
        return bool(self.db.command("ping").get("ok"))

    def latest_t(self) -> float | None:
        """Return the largest ``t`` value present in the stats collection.

        Returns:
            The maximum simulation time seen so far, or ``None`` if the
            collection is empty (typical right after ``docker compose
            up`` before the first batch lands).
        """
        doc = self.db[self.stats_collection].find_one(
            sort=[("t", -1)],
            projection={"_id": 0, "t": 1},
        )
        if doc is None:
            return None
        return float(doc["t"])

    def link_state_at(
        self, t: float, tolerance: float = 1.0
    ) -> list[Mapping[str, Any]]:
        """Return the most recent ``(vcount, vspeed)`` per link near time ``t``.

        For each link, keeps the row with the largest ``t`` in the
        window ``[t - tolerance, t]``. Links with no samples in that
        window are omitted. Used by the dashboard network map to color
        edges by their current state without a second collection scan.

        Args:
            t: Simulation time to snapshot at.
            tolerance: How far back (in simulation seconds) to look for
                a sample when the exact step ``t`` has no row for a
                given link. Defaults to one second, which matches the
                producer's default emission interval.

        Returns:
            List of ``{"link", "vcount", "vspeed", "t"}`` documents.

        Raises:
            ValueError: If ``tolerance`` is negative.
        """
        if tolerance < 0:
            raise ValueError(f"tolerance must be non-negative, got {tolerance}")
        pipeline = [
            {"$match": {"t": {"$gte": t - tolerance, "$lte": t}}},
            {"$sort": {"t": -1}},
            {
                "$group": {
                    "_id": "$link",
                    "vcount": {"$first": "$vcount"},
                    "vspeed": {"$first": "$vspeed"},
                    "t": {"$first": "$t"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "link": "$_id",
                    "vcount": 1,
                    "vspeed": 1,
                    "t": 1,
                }
            },
        ]
        return list(self.db[self.stats_collection].aggregate(pipeline))

    def avg_speed_per_link(self) -> list[Mapping[str, Any]]:
        """Mean ``vspeed`` per link across the whole simulation.

        Returns:
            List of ``{"link", "avg_speed", "samples"}`` documents,
            sorted by descending mean speed. ``samples`` is the number
            of ``(t, link)`` rows that contributed to the average.
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
        return list(self.db[self.stats_collection].aggregate(pipeline))

    def top_n_congested(self, n: int = 5) -> list[Mapping[str, Any]]:
        """Top ``n`` links by peak ``vcount``.

        Args:
            n: How many links to return. Must be positive.

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
        return list(self.db[self.stats_collection].aggregate(pipeline))

    def link_detail(self, link_id: str) -> Mapping[str, Any] | None:
        """Return summary + time series for a single link.

        The summary aggregates ``vcount`` and ``vspeed`` across all
        simulation steps that touched the link; the time series is the
        raw ``(t, vcount, vspeed)`` rows ordered by ``t``. Both are
        capped at the stats collection so the response stays bounded by
        the simulation length.

        Args:
            link_id: Link identifier, e.g. ``"I1W1"``.

        Returns:
            Mapping with keys ``link``, ``summary`` (``samples``,
            ``avg_speed``, ``min_speed``, ``max_speed``, ``peak_vcount``,
            ``first_t``, ``last_t``), and ``series`` (list of
            ``{t, vcount, vspeed}``). Returns ``None`` if no rows match
            ``link_id``.
        """
        collection = self.db[self.stats_collection]
        summary_pipeline = [
            {"$match": {"link": link_id}},
            {
                "$group": {
                    "_id": "$link",
                    "samples": {"$sum": 1},
                    "avg_speed": {"$avg": "$vspeed"},
                    "min_speed": {"$min": "$vspeed"},
                    "max_speed": {"$max": "$vspeed"},
                    "peak_vcount": {"$max": "$vcount"},
                    "first_t": {"$min": "$t"},
                    "last_t": {"$max": "$t"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "samples": 1,
                    "avg_speed": 1,
                    "min_speed": 1,
                    "max_speed": 1,
                    "peak_vcount": 1,
                    "first_t": 1,
                    "last_t": 1,
                }
            },
        ]
        summary = next(iter(collection.aggregate(summary_pipeline)), None)
        if summary is None:
            return None

        series_cursor = (
            collection.find(
                {"link": link_id},
                projection={"_id": 0, "t": 1, "vcount": 1, "vspeed": 1},
            )
            .sort("t", 1)
        )
        series = list(series_cursor)
        return {"link": link_id, "summary": summary, "series": series}
