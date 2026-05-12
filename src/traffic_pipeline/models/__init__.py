"""Pure-function aggregations over parsed UXSIM events.

Each module in this package exposes a ``DataFrame -> DataFrame``
transformation with no I/O. Side-effecting wiring (readStream,
foreachBatch sinks, checkpoint paths) lives in
``scripts/run_consumer.py``.

Kept intentionally empty of re-exports so that ``python -m`` execution
of any submodule does not double-load it under two identities
(see Phase 1 footgun in ``handoff.md``).
"""
