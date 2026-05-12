"""Kafka publisher that streams UXSIM vehicle events to Redpanda.

Configuration is layered: YAML defaults from
``config/default.yaml`` are overridden by environment variables, which
are themselves overridden by CLI flags.

Example:
    >>> python -m traffic_pipeline.ingestion.kafka_publisher
    >>> python -m traffic_pipeline.ingestion.kafka_publisher --dry-run
    >>> python -m traffic_pipeline.ingestion.kafka_publisher \\
    ...     --bootstrap-servers localhost:19092 --topic uxsim --interval 0.5
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError

from traffic_pipeline.ingestion.traffic_simulator import simulate_traffic
from traffic_pipeline.ingestion.uxsim_network import NetworkConfig, build_grid_network

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublisherConfig:
    """Runtime parameters for :class:`TrafficKafkaPublisher`.

    Attributes:
        bootstrap_servers: Broker address (``host:port``).
        topic: Kafka topic to publish to.
        produce_interval_sec: Wall-clock pause between simulation steps
            when replaying records, used to mimic real-time streaming.
        max_retries: Per-message retry budget on transient errors.
        initial_backoff_sec: Backoff before the first retry.
        backoff_multiplier: Multiplier applied between successive
            retries (exponential backoff).
    """

    bootstrap_servers: str
    topic: str
    produce_interval_sec: float
    max_retries: int
    initial_backoff_sec: float
    backoff_multiplier: float


def _to_json_safe(value: Any) -> Any:
    """Convert numpy scalars / arrays to native Python types.

    ``json.dumps`` raises on ``numpy.int64`` and friends, which is what
    ``vehicles_to_pandas()`` returns for every numeric column. This
    helper is the same coercion shipped in the original course
    skeleton.
    """
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


class TrafficKafkaPublisher:
    """Publish a stream of UXSIM vehicle events to a Kafka topic.

    The ``KafkaProducer`` it wraps owns network sockets and a background
    sender thread. Aggregations and pure data
    transformations stay functional.
    """

    def __init__(self, config: PublisherConfig, *, dry_run: bool = False) -> None:
        """Initialise the publisher.

        Args:
            config: Runtime configuration.
            dry_run: If True, skip the broker connection entirely and
                emit JSON payloads to stdout. Lets you exercise the
                producer path without Redpanda running.
        """
        self._config = config
        self._dry_run = dry_run
        self._producer: KafkaProducer | None = None
        if not dry_run:
            self._producer = KafkaProducer(
                bootstrap_servers=config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=config.max_retries,
            )
            logger.info("KafkaProducer connected to %s.", config.bootstrap_servers)

    def publish_records(self, records: Iterable[dict]) -> int:
        """Publish each record without pacing between them.

        Args:
            records: Iterable of JSON-safe dicts (caller is responsible
                for the coercion -- use :func:`_to_json_safe` on numpy
                values).

        Returns:
            Number of records accepted by the broker (or printed in
            dry-run mode).
        """
        published = 0
        for record in records:
            if self._send_with_retry(record):
                published += 1
        if self._producer is not None:
            self._producer.flush()
        return published

    def publish_dataframe_by_time(
        self,
        df: pd.DataFrame,
        time_col: str = "t",
    ) -> int:
        """Replay a vehicle log dataframe, one simulation step at a time.

        All records sharing the same ``time_col`` value are flushed
        together, then the publisher sleeps for
        ``produce_interval_sec`` -- this is how a finished simulation
        gets transformed into a paced, real-time-looking event stream.

        Args:
            df: DataFrame from ``vehicles_to_pandas()``.
            time_col: Column carrying the simulation step (defaults to
                ``t``, the UXSIM convention).

        Returns:
            Total number of records published.
        """
        total = 0
        for step in sorted(df[time_col].unique()):
            snapshot = df[df[time_col] == step]
            for _, row in snapshot.iterrows():
                payload = {k: _to_json_safe(v) for k, v in row.to_dict().items()}
                if self._send_with_retry(payload):
                    total += 1
            if self._producer is not None:
                self._producer.flush()
            logger.debug("Flushed step t=%s (%d rows).", step, len(snapshot))
            time.sleep(self._config.produce_interval_sec)
        return total

    def close(self) -> None:
        """Flush in-flight messages and close the underlying producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            logger.info("KafkaProducer closed.")

    def __enter__(self) -> TrafficKafkaPublisher:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _send_with_retry(self, payload: dict) -> bool:
        """Send a single payload, retrying transient :class:`KafkaError`.

        Returns:
            True on success, False if the retry budget was exhausted.
        """
        if self._dry_run:
            print(json.dumps(payload))
            return True

        assert self._producer is not None  # narrows the type for mypy
        backoff = self._config.initial_backoff_sec
        for attempt in range(1, self._config.max_retries + 1):
            try:
                self._producer.send(self._config.topic, payload)
                return True
            except KafkaError as exc:
                if attempt == self._config.max_retries:
                    logger.error(
                        "Giving up on record after %d attempts: %s",
                        attempt,
                        exc,
                    )
                    return False
                logger.warning(
                    "Send failed (attempt %d/%d): %s. Backing off %.1fs.",
                    attempt,
                    self._config.max_retries,
                    exc,
                    backoff,
                )
                time.sleep(backoff)
                backoff *= self._config.backoff_multiplier
        return False


# ----------------------------------------------------------------------------
# Configuration loading & CLI entry point
# ----------------------------------------------------------------------------

def _resolve_default_config_path() -> Path:
    """Return the path to ``config/default.yaml``.

    The path is overridable via the ``TRAFFIC_PIPELINE_CONFIG`` env var,
    which is useful in containers where the source tree lives at
    ``/app`` instead of the repo root.
    """
    env_override = os.environ.get("TRAFFIC_PIPELINE_CONFIG")
    if env_override:
        return Path(env_override)
    # Walk up from this module: ingestion -> traffic_pipeline -> src -> repo root.
    return Path(__file__).resolve().parents[3] / "config" / "default.yaml"


def load_config(
    config_path: Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
) -> tuple[NetworkConfig, PublisherConfig]:
    """Compose the network and publisher configs from all sources.

    Precedence (lowest -> highest): YAML file -> environment variables
    -> explicit ``overrides`` (typically argparse output).

    Args:
        config_path: YAML file to read. Defaults to the value returned
            by :func:`_resolve_default_config_path`.
        overrides: Dict whose entries trump everything else. Only keys
            understood by the override helpers are honoured; the rest
            are ignored.

    Returns:
        ``(NetworkConfig, PublisherConfig)`` tuple.
    """
    path = config_path or _resolve_default_config_path()
    raw: dict[str, Any] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    else:
        logger.warning("Config file %s not found -- relying on defaults / env.", path)

    sim_section = raw.get("simulation", {}) or {}
    kafka_section = raw.get("kafka", {}) or {}
    retries_section = kafka_section.get("retries", {}) or {}

    env_seed = os.environ.get("SIMULATION_SEED")
    seed: int | None
    if env_seed:
        seed = int(env_seed)
    else:
        yaml_seed = sim_section.get("seed")
        seed = int(yaml_seed) if yaml_seed is not None else None

    network = NetworkConfig(
        tmax=int(os.environ.get("SIMULATION_TMAX", sim_section.get("tmax", 3600))),
        deltan=int(sim_section.get("deltan", 5)),
        seed=seed,
        signal_time=int(sim_section.get("signal_time", 20)),
        demand_avg=float(sim_section.get("demand", 2.0)),
        demand_step_seconds=int(sim_section.get("demand_step_seconds", 30)),
    )

    publisher = PublisherConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            kafka_section.get("bootstrap_servers", "redpanda:9092"),
        ),
        topic=os.environ.get(
            "KAFKA_TOPIC",
            kafka_section.get("topic", "uxsim"),
        ),
        produce_interval_sec=float(
            os.environ.get(
                "PRODUCE_INTERVAL_SEC",
                kafka_section.get("produce_interval_sec", 1.0),
            )
        ),
        max_retries=int(retries_section.get("max_attempts", 5)),
        initial_backoff_sec=float(retries_section.get("initial_backoff_sec", 1.0)),
        backoff_multiplier=float(retries_section.get("backoff_multiplier", 2.0)),
    )

    if overrides:
        network = _apply_network_overrides(network, overrides)
        publisher = _apply_publisher_overrides(publisher, overrides)
    return network, publisher


def _apply_network_overrides(cfg: NetworkConfig, ov: dict[str, Any]) -> NetworkConfig:
    """Return a new :class:`NetworkConfig` with CLI overrides applied."""
    updates: dict[str, Any] = {}
    if ov.get("tmax") is not None:
        updates["tmax"] = ov["tmax"]
    # `--seed 0` is a legitimate user choice, so we cannot treat 0 as "unset".
    if "seed" in ov and ov["seed"] is not None:
        updates["seed"] = ov["seed"]
    return replace(cfg, **updates) if updates else cfg


def _apply_publisher_overrides(cfg: PublisherConfig, ov: dict[str, Any]) -> PublisherConfig:
    """Return a new :class:`PublisherConfig` with CLI overrides applied."""
    updates: dict[str, Any] = {}
    if ov.get("bootstrap_servers"):
        updates["bootstrap_servers"] = ov["bootstrap_servers"]
    if ov.get("topic"):
        updates["topic"] = ov["topic"]
    if ov.get("interval") is not None:
        updates["produce_interval_sec"] = ov["interval"]
    return replace(cfg, **updates) if updates else cfg


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traffic-producer",
        description="Run a UXSIM simulation and publish vehicle events to Kafka/Redpanda.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Override KAFKA_BOOTSTRAP_SERVERS (e.g. localhost:19092).",
    )
    parser.add_argument("--topic", help="Override KAFKA_TOPIC.")
    parser.add_argument(
        "--interval",
        type=float,
        help="Override PRODUCE_INTERVAL_SEC (seconds between simulation steps).",
    )
    parser.add_argument(
        "--tmax",
        type=int,
        help="Override SIMULATION_TMAX (total simulated seconds).",
    )
    parser.add_argument("--seed", type=int, help="Override SIMULATION_SEED.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON payloads to stdout instead of publishing to a broker.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m traffic_pipeline.ingestion.kafka_publisher``."""
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Keep only non-None CLI values so we don't shadow YAML/env defaults
    # with argparse's None placeholders.
    overrides = {
        k: v for k, v in vars(args).items()
        if v is not None and k not in {"dry_run", "log_level"}
    }
    network_cfg, publisher_cfg = load_config(overrides=overrides)

    logger.info("Building UXSIM network (tmax=%d, seed=%s).", network_cfg.tmax, network_cfg.seed)
    world = build_grid_network(network_cfg)

    df = simulate_traffic(world)

    with TrafficKafkaPublisher(publisher_cfg, dry_run=args.dry_run) as publisher:
        sent = publisher.publish_dataframe_by_time(df)
        logger.info("Published %d records to topic %r.", sent, publisher_cfg.topic)
    return 0


if __name__ == "__main__":
    sys.exit(main())
