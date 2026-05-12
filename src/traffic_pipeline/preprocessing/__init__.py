"""Preprocessing subpackage: Spark schemas and stream parsing primitives.

Submodules:
    schemas: Static ``StructType`` definitions for UXSIM-produced events.
    stream_parser: Pure transformation that turns a raw Kafka ``value``
        column into a typed DataFrame matching the UXSIM schema.

As with :mod:`traffic_pipeline.ingestion`, this ``__init__`` deliberately
exposes nothing: keeping it empty avoids the ``RuntimeWarning: ... found
in sys.modules ...`` trap when any submodule is run via ``python -m``.
Import the names you need directly from the submodule.
"""
