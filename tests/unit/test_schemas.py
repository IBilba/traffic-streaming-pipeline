"""Unit tests for :mod:`traffic_pipeline.preprocessing`.

These tests run against a local Spark session — no Kafka broker, no
streaming. They verify that:

* The UXSIM schema declares the expected columns with the expected
  Spark types.
* ``parse_kafka_stream`` decodes a JSON-bytes payload into one row per
  message with correctly typed top-level columns.
* Status rows that UXSIM emits (``link == "trip_end"`` and friends) are
  preserved by the parser — semantic filtering is a downstream
  concern, not a parsing one.
"""

from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import DoubleType, IntegerType, StringType

from traffic_pipeline.preprocessing.schemas import UXSIM_EVENT_SCHEMA
from traffic_pipeline.preprocessing.stream_parser import parse_kafka_stream


def _json_value_df(spark: SparkSession, record: dict):
    """Build a one-row DataFrame mirroring the Kafka source shape.

    Goes through ``spark.range`` + ``expr`` rather than
    ``createDataFrame``: on Windows the latter spawns a Python worker
    that intermittently times out the JVM-side socket accept. Building
    the row in Catalyst keeps the test purely on the JVM.
    """
    payload = json.dumps(record).replace("'", "\\'")
    return spark.range(1).select(expr(f"CAST('{payload}' AS BINARY) AS value"))


def test_schema_has_expected_fields() -> None:
    expected = {
        "name": StringType(),
        "dn": IntegerType(),
        "orig": StringType(),
        "dest": StringType(),
        "t": DoubleType(),
        "link": StringType(),
        "x": DoubleType(),
        "s": DoubleType(),
        "v": DoubleType(),
    }
    fields = {f.name: f.dataType for f in UXSIM_EVENT_SCHEMA.fields}
    assert fields == expected


def test_parse_kafka_stream_decodes_typed_columns(spark: SparkSession) -> None:
    record = {
        "name": "veh_0",
        "dn": 5,
        "orig": "I1",
        "dest": "I4",
        "t": 12.5,
        "link": "I1I2",
        "x": 110.0,
        "s": 20.0,
        "v": 13.4,
    }
    parsed = parse_kafka_stream(_json_value_df(spark, record), UXSIM_EVENT_SCHEMA)

    assert [f.name for f in parsed.schema.fields] == list(record.keys())
    [row] = parsed.collect()
    assert row.asDict() == record


def test_parse_kafka_stream_preserves_status_rows(spark: SparkSession) -> None:
    """``trip_end`` rows must reach ``raw_data``; filtering belongs in ``models/``."""
    status = {
        "name": "veh_42",
        "dn": 3,
        "orig": "I1",
        "dest": "I4",
        "t": 99.0,
        "link": "trip_end",
        "x": -1.0,
        "s": -1.0,
        "v": -1.0,
    }
    [row] = parse_kafka_stream(_json_value_df(spark, status), UXSIM_EVENT_SCHEMA).collect()

    assert row["link"] == "trip_end"
    assert row["v"] == -1.0
