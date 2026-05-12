"""Spark schemas for events flowing through the traffic pipeline.

Schemas are static, declarative metadata — kept apart from any I/O so
they can be imported by producers, consumers, and tests without dragging
in a ``SparkSession``.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

#: Schema of a single UXSIM event as serialized by
#: :class:`traffic_pipeline.ingestion.kafka_publisher.TrafficKafkaPublisher`.
#:
#: Mirrors the columns produced by ``World.analyzer.vehicles_to_pandas()``:
#:
#: ====== ============ ==========================================
#: Column Type         Meaning
#: ====== ============ ==========================================
#: name   StringType   Vehicle (platoon) identifier.
#: dn     IntegerType  Platoon size — vehicles grouped for perf.
#: orig   StringType   Origin node name.
#: dest   StringType   Destination node name.
#: t      DoubleType   Simulation time in seconds (not wall-clock).
#: link   StringType   Link the platoon is on, OR a status flag
#:                     (``"waiting_at_origin_node"``, ``"trip_end"``).
#: x      DoubleType   Position on the link. ``-1`` for status rows.
#: s      DoubleType   Spacing to leader. ``-1`` for status rows.
#: v      DoubleType   Speed in km/h. ``-1`` for status rows. The
#:                     assignment text calls this ``speed``; UXSIM
#:                     calls it ``v`` and UXSIM wins.
#: ====== ============ ==========================================
#:
#: Note:
#:     Status rows (``link in {"waiting_at_origin_node", "trip_end"}``)
#:     are preserved by the parser and stored verbatim in
#:     ``traffic.raw_data``. Semantic filtering happens downstream in
#:     :mod:`traffic_pipeline.models.link_stats` so that the raw sink
#:     reflects exactly what the simulator emitted.
UXSIM_EVENT_SCHEMA: StructType = (
    StructType()
    .add("name", StringType())
    .add("dn", IntegerType())
    .add("orig", StringType())
    .add("dest", StringType())
    .add("t", DoubleType())
    .add("link", StringType())
    .add("x", DoubleType())
    .add("s", DoubleType())
    .add("v", DoubleType())
)
