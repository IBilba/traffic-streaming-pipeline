"""Ingestion subpackage: build UXSIM networks and publish events to Kafka.

Submodules:
    uxsim_network: Pure topology builder (``build_grid_network``).
    traffic_simulator: Executes the simulation (``simulate_traffic``).
    kafka_publisher: Streams events to Redpanda (``TrafficKafkaPublisher``,
        ``PublisherConfig``, ``main``).

We deliberately avoid re-exporting names from submodules at this level:
the package is the CLI entry point (``python -m
traffic_pipeline.ingestion.kafka_publisher``), and eager imports would
load the submodule before Python promotes it to ``__main__``, triggering
``RuntimeWarning: ... found in sys.modules ...``. Import from the
submodule directly:

    from traffic_pipeline.ingestion.kafka_publisher import TrafficKafkaPublisher
"""
