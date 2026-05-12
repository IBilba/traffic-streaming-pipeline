"""FastAPI service exposing the ``traffic.*`` collections over HTTP.

The package is split in two layers:

* :mod:`traffic_pipeline.serving.repository` is a pure PyMongo wrapper
  that answers the same analytical questions as
  :mod:`traffic_pipeline.evaluation.sample_queries`. It has no FastAPI
  imports so it can be unit-tested against a mocked database.
* :mod:`traffic_pipeline.serving.api` is the FastAPI application; it
  wires the repository behind a small set of HTTP endpoints and owns
  the MongoDB client lifecycle.

Kept empty of re-exports so ``uvicorn traffic_pipeline.serving.api:app``
does not double-load any submodule under two identities.
"""
