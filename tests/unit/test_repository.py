"""Unit tests for :mod:`traffic_pipeline.serving.repository`.

The PyMongo ``Database`` handle is stubbed with ``unittest.mock`` rather
than spun up against ``mongomock``: the repository methods are thin
adapters whose value is in the *shape* of the pipelines they send, not
in re-implementing Mongo's aggregation semantics. The fake captures the
pipeline that each method emits so we can assert it pins the right
stages (``$group`` keys, ``$sort`` directions, ``$limit``, etc.) and
that the canned result is returned untouched.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from traffic_pipeline.serving.repository import TrafficRepository


def _make_db(
    *,
    aggregate_results: dict[str, list[dict[str, Any]]] | None = None,
    find_results: dict[str, list[dict[str, Any]]] | None = None,
    ping_ok: bool = True,
) -> MagicMock:
    """Build a fake PyMongo ``Database`` that returns canned data.

    Each collection access (``db[name]``) yields a per-collection
    ``MagicMock`` so the test can inspect ``aggregate`` / ``find``
    invocations after the fact. ``aggregate`` returns the canned list
    for the requested collection (or an empty list when missing).
    """
    aggregate_results = aggregate_results or {}
    find_results = find_results or {}

    collections: dict[str, MagicMock] = {}

    def get_collection(name: str) -> MagicMock:
        if name not in collections:
            coll = MagicMock(name=f"collection[{name}]")
            coll.aggregate.return_value = iter(aggregate_results.get(name, []))
            cursor = MagicMock(name=f"cursor[{name}]")
            cursor.sort.return_value = iter(find_results.get(name, []))
            coll.find.return_value = cursor
            collections[name] = coll
        return collections[name]

    db = MagicMock(name="database")
    db.__getitem__.side_effect = get_collection
    db.command.return_value = {"ok": 1 if ping_ok else 0}
    return db


def test_ping_returns_true_when_admin_ping_ok() -> None:
    db = _make_db(ping_ok=True)
    repo = TrafficRepository(db)

    assert repo.ping() is True
    db.command.assert_called_once_with("ping")


def test_ping_returns_false_when_admin_ping_not_ok() -> None:
    db = _make_db(ping_ok=False)
    repo = TrafficRepository(db)

    assert repo.ping() is False


def test_avg_speed_per_link_runs_expected_pipeline_and_returns_rows() -> None:
    canned = [
        {"link": "I1W1", "avg_speed": 30.9, "samples": 1520},
        {"link": "I2I3", "avg_speed": 12.4, "samples": 800},
    ]
    db = _make_db(aggregate_results={"stats": canned})
    repo = TrafficRepository(db)

    result = repo.avg_speed_per_link()

    assert result == canned

    # The pipeline must group by link, compute avg/count, project the
    # final shape, and sort descending by avg_speed.
    pipeline = db["stats"].aggregate.call_args.args[0]
    stages = [next(iter(s)) for s in pipeline]
    assert stages == ["$group", "$project", "$sort"]
    assert pipeline[0]["$group"]["_id"] == "$link"
    assert pipeline[0]["$group"]["avg_speed"] == {"$avg": "$vspeed"}
    assert pipeline[-1]["$sort"] == {"avg_speed": -1}


def test_top_n_congested_emits_limit_stage_and_passes_n_through() -> None:
    canned = [
        {"link": "I1W1", "peak_vcount": 63, "argmax_t": 1820.0},
        {"link": "I2I3", "peak_vcount": 60, "argmax_t": 1750.0},
        {"link": "I3I4", "peak_vcount": 55, "argmax_t": 1900.0},
    ]
    db = _make_db(aggregate_results={"stats": canned})
    repo = TrafficRepository(db)

    result = repo.top_n_congested(n=3)

    assert result == canned
    pipeline = db["stats"].aggregate.call_args.args[0]
    # Final stage is $limit with the requested n.
    assert pipeline[-1] == {"$limit": 3}


def test_top_n_congested_rejects_non_positive_n() -> None:
    repo = TrafficRepository(_make_db())
    with pytest.raises(ValueError):
        repo.top_n_congested(n=0)
    with pytest.raises(ValueError):
        repo.top_n_congested(n=-2)


def test_link_detail_returns_none_when_no_summary_row() -> None:
    db = _make_db(aggregate_results={"stats": []})
    repo = TrafficRepository(db)

    assert repo.link_detail("NOPE") is None
    # No series fetch should happen when the link is unknown.
    db["stats"].find.assert_not_called()


def test_link_detail_combines_summary_and_ordered_series() -> None:
    summary_doc = {
        "samples": 1535,
        "avg_speed": 30.9,
        "min_speed": 5.0,
        "max_speed": 50.0,
        "peak_vcount": 18,
        "first_t": 25.0,
        "last_t": 3595.0,
    }
    series_docs = [
        {"t": 25.0, "vcount": 1, "vspeed": 30.0},
        {"t": 30.0, "vcount": 2, "vspeed": 28.0},
    ]
    db = _make_db(
        aggregate_results={"stats": [summary_doc]},
        find_results={"stats": series_docs},
    )
    repo = TrafficRepository(db)

    result = repo.link_detail("I1W1")
    assert result == {
        "link": "I1W1",
        "summary": summary_doc,
        "series": series_docs,
    }

    # The series query filters on the requested link and sorts by t asc.
    coll = db["stats"]
    find_call = coll.find.call_args
    assert find_call.args[0] == {"link": "I1W1"}
    coll.find.return_value.sort.assert_called_once_with("t", 1)


def test_latest_t_returns_max_t_from_sorted_find_one() -> None:
    db = _make_db()
    db["stats"].find_one.return_value = {"t": 1820.0}
    repo = TrafficRepository(db)

    assert repo.latest_t() == 1820.0

    find_one_call = db["stats"].find_one.call_args
    assert find_one_call.kwargs["sort"] == [("t", -1)]
    assert find_one_call.kwargs["projection"] == {"_id": 0, "t": 1}


def test_latest_t_returns_none_when_collection_is_empty() -> None:
    db = _make_db()
    db["stats"].find_one.return_value = None
    repo = TrafficRepository(db)

    assert repo.latest_t() is None


def test_link_state_at_emits_match_sort_group_pipeline_and_returns_rows() -> None:
    canned = [
        {"link": "I1W1", "vcount": 12, "vspeed": 28.4, "t": 1820.0},
        {"link": "I2I3", "vcount": 3, "vspeed": 41.0, "t": 1820.0},
    ]
    db = _make_db(aggregate_results={"stats": canned})
    repo = TrafficRepository(db)

    result = repo.link_state_at(t=1820.0, tolerance=2.0)

    assert result == canned
    pipeline = db["stats"].aggregate.call_args.args[0]
    stages = [next(iter(s)) for s in pipeline]
    assert stages == ["$match", "$sort", "$group", "$project"]
    assert pipeline[0]["$match"] == {"t": {"$gte": 1818.0, "$lte": 1820.0}}
    assert pipeline[1]["$sort"] == {"t": -1}
    assert pipeline[2]["$group"]["_id"] == "$link"
    assert pipeline[2]["$group"]["vcount"] == {"$first": "$vcount"}
    assert pipeline[2]["$group"]["vspeed"] == {"$first": "$vspeed"}


def test_link_state_at_rejects_negative_tolerance() -> None:
    repo = TrafficRepository(_make_db())
    with pytest.raises(ValueError):
        repo.link_state_at(t=10.0, tolerance=-0.1)


def test_link_state_at_routes_to_custom_stats_collection() -> None:
    db = _make_db(aggregate_results={"per_link_stats": []})
    repo = TrafficRepository(db, stats_collection="per_link_stats")

    repo.link_state_at(t=5.0)
    db.__getitem__.assert_any_call("per_link_stats")


def test_custom_collection_names_route_queries_to_the_right_collection() -> None:
    db = _make_db(
        aggregate_results={"per_link_stats": [{"link": "X", "avg_speed": 1.0, "samples": 1}]},
    )
    repo = TrafficRepository(db, stats_collection="per_link_stats")

    repo.avg_speed_per_link()
    db.__getitem__.assert_any_call("per_link_stats")
