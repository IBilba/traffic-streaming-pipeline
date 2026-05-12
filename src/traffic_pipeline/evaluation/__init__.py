"""Read-only helpers and ad-hoc analytics over the populated MongoDB.

Modules in this package run as plain PyMongo clients. They never start a
SparkSession and never write back to the collections; their job is to
inspect what the streaming pipeline has produced and to provide the
sample analytics queries that the assignment expects in the report.

Kept intentionally empty of re-exports so that ``python -m`` execution
of any submodule does not double-load it under two identities.
"""
