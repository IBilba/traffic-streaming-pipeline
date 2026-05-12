"""Pure transformations that decode raw Kafka payloads into typed rows.

This module is intentionally I/O-free: it neither builds a
``SparkSession`` nor opens a ``readStream``. Side-effecting wiring lives
in ``scripts/run_consumer.py`` (Phase 5). Keeping the transformation
pure means it can be exercised against a batch ``createDataFrame`` in
unit tests without a Kafka broker.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType


def parse_kafka_stream(df_raw: DataFrame, schema: StructType) -> DataFrame:
    """Decode the Kafka ``value`` column into typed top-level columns.

    Applies the canonical Spark idiom for JSON-over-Kafka:

    1. Cast the binary ``value`` to a UTF-8 string.
    2. Apply ``from_json`` with the supplied schema, materializing one
       nested struct column.
    3. Flatten the struct so each JSON key becomes its own column.

    Args:
        df_raw: DataFrame produced by
            ``spark.readStream.format("kafka").load()``. Must expose a
            ``value`` column of ``BinaryType`` containing UTF-8 encoded
            JSON.
        schema: Expected JSON shape — typically
            :data:`traffic_pipeline.preprocessing.schemas.UXSIM_EVENT_SCHEMA`.

    Returns:
        DataFrame with one column per top-level field of ``schema``.
        Rows whose ``value`` does not parse as JSON, or whose JSON does
        not match the schema, surface as all-null rows — they are not
        dropped here. Downstream aggregations are responsible for any
        semantic filtering (e.g., excluding UXSIM status rows).
    """
    return (
        df_raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )
