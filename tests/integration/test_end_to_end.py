"""End-to-end integration test for the running compose stack.

Marked ``@pytest.mark.integration`` so default ``pytest`` runs skip it.
Run with ``pytest -m integration`` once ``docker compose up -d`` has
warmed the stack.

The test attempts a short-timeout connection to MongoDB on the host
port (default ``27018``, overridable via ``MONGO_URI_HOST``). If the
broker is unreachable, the test is *skipped* rather than failed; this
keeps ``pytest -m integration`` usable on machines where the stack is
intentionally down.
"""

from __future__ import annotations

import os
import time

import pytest

pymongo = pytest.importorskip("pymongo")
from pymongo.errors import PyMongoError  # noqa: E402

pytestmark = pytest.mark.integration


MONGO_URI = os.environ.get("MONGO_URI_HOST", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DATABASE", "traffic")
WARMUP_SECONDS = float(os.environ.get("INTEGRATION_WARMUP_SECONDS", "30"))


def _connect_or_skip():
    """Return a live ``MongoClient`` or skip the test when unreachable."""
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
    except PyMongoError as exc:
        pytest.skip(f"MongoDB at {MONGO_URI} unreachable: {exc}")
    return client


def test_pipeline_writes_raw_and_stats_documents() -> None:
    """After a short warmup, both raw and stats collections are non-empty.

    Allows up to ``WARMUP_SECONDS`` for the producer-Kafka-Spark-Mongo
    chain to produce data; polls every two seconds rather than sleeping
    blindly so a fast laptop finishes quickly.
    """
    client = _connect_or_skip()
    db = client[MONGO_DB]

    deadline = time.monotonic() + WARMUP_SECONDS
    raw_count = stats_count = 0
    while time.monotonic() < deadline:
        raw_count = db["raw_data"].estimated_document_count()
        stats_count = db["stats"].estimated_document_count()
        if raw_count > 0 and stats_count > 0:
            break
        time.sleep(2)

    assert raw_count > 0, "traffic.raw_data is empty -- producer or Spark job not writing"
    assert stats_count > 0, "traffic.stats is empty -- link aggregation sink not committing"
