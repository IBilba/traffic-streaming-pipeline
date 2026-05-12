"""FastAPI application exposing traffic statistics from MongoDB.

Endpoints:

* ``GET /health`` -- liveness plus ``mongo_ok`` flag.
* ``GET /links/avg-speed`` -- mean ``vspeed`` per link.
* ``GET /links/top-congested?n=5`` -- top ``n`` links by peak ``vcount``.
* ``GET /links/{link_id}`` -- summary plus time series for one link.

The MongoDB client is opened during the lifespan startup and closed on
shutdown, so the process holds exactly one pool for its lifetime. The
:class:`~traffic_pipeline.serving.repository.TrafficRepository` instance
is also built once and injected into request handlers via FastAPI's
``Depends``.

Run locally with::

    uvicorn traffic_pipeline.serving.api:app --host 0.0.0.0 --port 8000

Configuration (env vars, see ``.env.example`` for the full set):

* ``MONGO_URI`` (default ``mongodb://mongo:27017``) -- in-network URI;
  use ``mongodb://localhost:27018`` for a host-side run against the
  Dockerized stack.
* ``MONGO_DATABASE`` (default ``traffic``).
* ``MONGO_STATS_COLLECTION`` (default ``stats``).
* ``MONGO_RAW_COLLECTION`` (default ``raw_data``).
* ``MONGO_WINDOWED_COLLECTION`` (default ``stats_windowed``).
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Mapping

from fastapi import Depends, FastAPI, HTTPException, Query
from pymongo import MongoClient

from traffic_pipeline.serving.repository import TrafficRepository

logger = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    """Return the trimmed value of env var ``name`` or ``default`` if unset/empty."""
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the MongoDB client on startup, close it on shutdown.

    The client is stored on ``app.state`` and a single
    :class:`TrafficRepository` is built on top of it so request handlers
    do not pay reconnection cost.
    """
    uri = _env("MONGO_URI", "mongodb://mongo:27017")
    database = _env("MONGO_DATABASE", "traffic")
    stats = _env("MONGO_STATS_COLLECTION", "stats")
    raw = _env("MONGO_RAW_COLLECTION", "raw_data")
    windowed = _env("MONGO_WINDOWED_COLLECTION", "stats_windowed")

    logger.info("Connecting to MongoDB at %s (db=%s)", uri, database)
    client: MongoClient[Any] = MongoClient(uri)
    try:
        app.state.mongo_client = client
        app.state.repository = TrafficRepository(
            client[database],
            stats_collection=stats,
            raw_collection=raw,
            windowed_collection=windowed,
        )
        yield
    finally:
        logger.info("Closing MongoDB client")
        client.close()


app = FastAPI(
    title="Traffic Stats API",
    description="Read-only access to the traffic.* collections produced by the streaming pipeline.",
    version="0.1.0",
    lifespan=lifespan,
)


def get_repository() -> TrafficRepository:
    """FastAPI dependency that returns the singleton repository.

    Raises ``RuntimeError`` if the lifespan has not run yet, which only
    happens in misconfigured test setups (the real app always boots via
    ``uvicorn``, which drives the lifespan).
    """
    repo = getattr(app.state, "repository", None)
    if repo is None:
        raise RuntimeError("Repository is not initialised; lifespan did not run")
    return repo


@app.get("/health")
def health(repo: TrafficRepository = Depends(get_repository)) -> Mapping[str, Any]:
    """Liveness probe plus MongoDB reachability.

    Returns ``{"status": "ok", "mongo_ok": bool}``. The endpoint never
    raises: a Mongo outage is reflected in ``mongo_ok=False`` so that
    upstream healthcheckers can still parse the JSON response.
    """
    try:
        mongo_ok = repo.ping()
    except Exception as exc:
        logger.warning("Mongo ping failed: %s", exc)
        mongo_ok = False
    return {"status": "ok", "mongo_ok": mongo_ok}


@app.get("/links/avg-speed")
def list_avg_speed(
    repo: TrafficRepository = Depends(get_repository),
) -> list[Mapping[str, Any]]:
    """Mean ``vspeed`` per link, sorted by descending speed."""
    return repo.avg_speed_per_link()


@app.get("/links/top-congested")
def list_top_congested(
    n: int = Query(5, ge=1, le=100, description="How many links to return (1..100)."),
    repo: TrafficRepository = Depends(get_repository),
) -> list[Mapping[str, Any]]:
    """Top ``n`` links by peak ``vcount``."""
    return repo.top_n_congested(n)


@app.get("/links/{link_id}")
def get_link_detail(
    link_id: str,
    repo: TrafficRepository = Depends(get_repository),
) -> Mapping[str, Any]:
    """Summary plus time series for a single link.

    Returns ``404`` if ``link_id`` has no rows in the stats collection
    (typically because the simulation has not produced data for that
    edge yet, or the identifier is wrong).
    """
    detail = repo.link_detail(link_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Unknown link: {link_id}")
    return detail
