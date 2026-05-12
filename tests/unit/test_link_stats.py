"""Unit tests for :mod:`traffic_pipeline.models.link_stats`.

Inputs are built via ``spark.sql("VALUES ...")``, a JVM-only path,
to dodge the Windows ``createDataFrame`` socket-timeout footgun
(``createDataFrame`` spawns a Python worker that the JVM cannot reach
on Windows loopback; pure-Catalyst paths avoid the handshake).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from traffic_pipeline.models.link_stats import DEFAULT_STATUS_LINKS, compute_link_stats


def _rows_df(spark: SparkSession, rows: list[tuple]):
    """Build a DataFrame of UXSIM-shaped rows without a Python worker."""
    literal = ", ".join(
        f"('{name}', {dn}, '{orig}', '{dest}', CAST({t} AS DOUBLE), '{link}', "
        f"CAST({x} AS DOUBLE), CAST({s} AS DOUBLE), CAST({v} AS DOUBLE))"
        for (name, dn, orig, dest, t, link, x, s, v) in rows
    )
    return spark.sql(
        f"SELECT * FROM VALUES {literal} AS t(name, dn, orig, dest, t, link, x, s, v)"
    )


def test_compute_link_stats_groups_by_t_and_link(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 10.0, "I1I2", 50.0, 10.0, 12.0),
            ("v2", 1, "I1", "I4", 10.0, "I1I2", 70.0, 10.0, 18.0),
            ("v3", 1, "I2", "I4", 10.0, "I2I3", 30.0, 10.0, 20.0),
            ("v1", 1, "I1", "I4", 11.0, "I1I2", 60.0, 10.0, 14.0),
        ],
    )

    out = {(r["t"], r["link"]): (r["vcount"], r["vspeed"]) for r in compute_link_stats(df).collect()}

    assert out[(10.0, "I1I2")] == (2, 15.0)
    assert out[(10.0, "I2I3")] == (1, 20.0)
    assert out[(11.0, "I1I2")] == (1, 14.0)


def test_compute_link_stats_output_schema(spark: SparkSession) -> None:
    df = _rows_df(spark, [("v1", 1, "I1", "I4", 0.0, "I1I2", 1.0, 1.0, 5.0)])
    assert [f.name for f in compute_link_stats(df).schema.fields] == [
        "t",
        "link",
        "vcount",
        "vspeed",
    ]


def test_compute_link_stats_excludes_status_rows(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 5.0, "I1I2", 10.0, 5.0, 13.0),
            ("v2", 1, "I1", "I4", 5.0, "waiting_at_origin_node", -1.0, -1.0, -1.0),
            ("v3", 1, "I1", "I4", 5.0, "trip_end", -1.0, -1.0, -1.0),
        ],
    )

    rows = compute_link_stats(df).collect()
    links = {r["link"] for r in rows}

    assert links == {"I1I2"}
    assert all(link not in links for link in DEFAULT_STATUS_LINKS)


def test_compute_link_stats_keeps_status_when_filter_disabled(spark: SparkSession) -> None:
    df = _rows_df(
        spark,
        [
            ("v1", 1, "I1", "I4", 5.0, "trip_end", -1.0, -1.0, -1.0),
        ],
    )

    rows = compute_link_stats(df, status_links=frozenset()).collect()

    assert len(rows) == 1
    assert rows[0]["link"] == "trip_end"
