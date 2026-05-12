"""Shared pytest fixtures for the test suite.

A single session-scoped :class:`~pyspark.sql.SparkSession` is reused
across all unit tests. Spinning up a JVM costs ~3-5 seconds; doing it
once per session keeps the suite fast.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Return a local-mode SparkSession suitable for unit tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("traffic-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
