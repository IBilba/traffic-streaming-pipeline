"""Lightweight read-only helpers for the ``traffic.*`` collections.

These functions answer the kind of questions you need to ask while
verifying the pipeline by hand or while taking screenshots for the
report: "how many events landed?", "what does a typical document look
like?", "what is the latest simulation time we have seen?".

All helpers take an already-connected :class:`pymongo.database.Database`
so that callers control the connection lifecycle (and so the functions
stay trivially unit-testable against a mocked database).
"""

from __future__ import annotations

from typing import Any, Mapping

from pymongo.collection import Collection
from pymongo.database import Database

DEFAULT_RAW_COLLECTION = "raw_data"
DEFAULT_STATS_COLLECTION = "stats"
DEFAULT_WINDOWED_COLLECTION = "stats_windowed"


def count_documents(db: Database, collection: str) -> int:
    """Return the document count of ``collection`` in ``db``.

    Uses ``count_documents({})`` rather than ``estimated_document_count``
    so the result is exact even right after a fresh ``compose up`` while
    Mongo's metadata is still warming up.

    Args:
        db: Connected PyMongo database handle.
        collection: Collection name within ``db``.

    Returns:
        Exact number of documents currently stored.
    """
    return db[collection].count_documents({})


def count_raw(db: Database, *, collection: str = DEFAULT_RAW_COLLECTION) -> int:
    """Count documents in the raw events collection (default ``raw_data``)."""
    return count_documents(db, collection)


def count_stats(db: Database, *, collection: str = DEFAULT_STATS_COLLECTION) -> int:
    """Count documents in the per-step stats collection (default ``stats``)."""
    return count_documents(db, collection)


def count_windowed(
    db: Database, *, collection: str = DEFAULT_WINDOWED_COLLECTION
) -> int:
    """Count documents in the windowed stats collection (default ``stats_windowed``)."""
    return count_documents(db, collection)


def latest_n(
    db: Database,
    collection: str,
    n: int = 5,
    *,
    sort_field: str = "t",
) -> list[Mapping[str, Any]]:
    """Return the ``n`` documents with the largest value of ``sort_field``.

    Args:
        db: Connected PyMongo database handle.
        collection: Collection to query.
        n: Number of documents to return. Must be positive.
        sort_field: Field to order by (descending). ``t`` for ``raw_data``
            and ``stats``; ``window_end`` for ``stats_windowed``.

    Returns:
        List of raw documents (newest first). Empty if the collection
        is empty.

    Raises:
        ValueError: If ``n`` is not a positive integer.
    """
    if n <= 0:
        raise ValueError(f"n must be positive, got {n}")
    cursor = db[collection].find().sort(sort_field, -1).limit(n)
    return list(cursor)


def distinct_links(
    db: Database, *, collection: str = DEFAULT_STATS_COLLECTION
) -> list[str]:
    """Return the sorted set of distinct ``link`` values in ``collection``.

    Status sentinels (``waiting_at_origin_node``, ``trip_end``) are
    filtered out in the ``stats`` collection by ``compute_link_stats``,
    so this typically returns only real road edges when called against
    ``stats``. Against ``raw_data`` the sentinels will be included.
    """
    return sorted(str(v) for v in db[collection].distinct("link"))


def sample_document(
    db: Database, collection: str
) -> Mapping[str, Any] | None:
    """Return a single document from ``collection`` (or ``None`` if empty).

    Convenient for ``print(json.dumps(sample_document(db, 'stats'),
    default=str, indent=2))`` while writing the report.
    """
    return db[collection].find_one()


def collection_handles(
    db: Database,
    *,
    raw: str = DEFAULT_RAW_COLLECTION,
    stats: str = DEFAULT_STATS_COLLECTION,
    windowed: str = DEFAULT_WINDOWED_COLLECTION,
) -> dict[str, Collection]:
    """Return a mapping of well-known collection roles to live handles.

    Helps callers avoid repeating the three collection-name constants.
    Keys are ``"raw"``, ``"stats"``, ``"windowed"``.
    """
    return {"raw": db[raw], "stats": db[stats], "windowed": db[windowed]}
