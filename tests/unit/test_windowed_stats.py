"""Unit tests for :mod:`traffic_pipeline.models.windowed_stats`.

These tests exercise the windowed aggregation on a batch DataFrame.
Watermarking is a streaming-only concern, but the rest of the
transformation (cast to timestamp, ``window``, group-by, flatten)
runs identically in batch mode, which is enough to verify the schema
and the per-window arithmetic.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from traffic_pipeline.models.windowed_stats import compute_windowed_stats


def _rows_df(spark: SparkSession, rows: list[tuple]):
    literal = ", ".join(
        f"('{name}', {dn}, '{orig}', '{dest}', CAST({t} AS DOUBLE), '{link}', "
        f"CAST({x} AS DOUBLE), CAST({s} AS DOUBLE), CAST({v} AS DOUBLE))"
        for (name, dn, orig, dest, t, link, x, s, v) in rows
    )
    return spark.sql(
        f"SELECT * FROM VALUES {literal} AS t(name, dn, orig, dest, t, link, x, s, v)"
    )


def test_compute_windowed_stats_output_schema(spark: SparkSession) -> None:
    df = _rows_df(spark, [("v1", 1, "I1", "I4", 0.0, "I1I2", 1.0, 1.0, 5.0)])
    fields = [f.name for f in compute_windowed_stats(df).schema.fields]
    assert fields == [
        "window_start",
        "window_end",
        "link",
        "vcount",
        "vspeed_avg",
        "vspeed_min",
        "vspeed_max",
    ]


def test_compute_windowed_stats_buckets_by_30_seconds(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 0.0, "I1I2", 1.0, 1.0, 10.0),
            ("v2", 1, "I1", "I4", 15.0, "I1I2", 1.0, 1.0, 20.0),
            ("v3", 1, "I1", "I4", 29.0, "I1I2", 1.0, 1.0, 30.0),
            ("v4", 1, "I1", "I4", 30.0, "I1I2", 1.0, 1.0, 40.0),
            ("v5", 1, "I1", "I4", 59.0, "I1I2", 1.0, 1.0, 60.0),
        ],
    )

    by_window = {
        (r["window_start"].second, r["window_end"].second): r
        for r in compute_windowed_stats(df).collect()
    }

    first = by_window[(0, 30)]
    assert first["vcount"] == 3
    assert first["vspeed_min"] == 10.0
    assert first["vspeed_max"] == 30.0
    assert first["vspeed_avg"] == 20.0

    second = by_window[(30, 0)]
    assert second["vcount"] == 2
    assert second["vspeed_min"] == 40.0
    assert second["vspeed_max"] == 60.0


def test_compute_windowed_stats_excludes_status_rows(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 1.0, "I1I2", 1.0, 1.0, 10.0),
            ("v2", 1, "I1", "I4", 1.0, "waiting_at_origin_node", -1.0, -1.0, -1.0),
            ("v3", 1, "I1", "I4", 1.0, "trip_end", -1.0, -1.0, -1.0),
        ],
    )

    links = {r["link"] for r in compute_windowed_stats(df).collect()}
    assert links == {"I1I2"}


def test_compute_windowed_stats_respects_custom_window(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 0.0, "I1I2", 1.0, 1.0, 10.0),
            ("v2", 1, "I1", "I4", 9.0, "I1I2", 1.0, 1.0, 20.0),
            ("v3", 1, "I1", "I4", 10.0, "I1I2", 1.0, 1.0, 30.0),
        ],
    )

    rows = compute_windowed_stats(df, window_seconds=10).collect()
    by_start = {r["window_start"].second: r["vcount"] for r in rows}

    assert by_start[0] == 2
    assert by_start[10] == 1
