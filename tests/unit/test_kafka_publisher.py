"""Unit tests for :mod:`traffic_pipeline.ingestion.kafka_publisher`.

The broker is mocked end-to-end. We verify that:

* ``_to_json_safe`` coerces numpy scalars / arrays to plain Python types,
  which is what ``json.dumps`` (used by the kafka-python serializer)
  needs to succeed.
* Dry-run mode prints one JSON line per record and never instantiates a
  ``KafkaProducer``.
* The real path constructs a ``KafkaProducer`` with the configured
  bootstrap address, a UTF-8 JSON ``value_serializer``, and the
  configured retry count; each record routes through ``send(topic, ...)``;
  ``flush`` runs at the end of ``publish_records`` and inside ``close``.
* Transient ``KafkaError`` failures retry up to the configured budget;
  permanent failures stop counting toward the published total.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
from kafka.errors import KafkaError

from traffic_pipeline.ingestion.kafka_publisher import (
    PublisherConfig,
    TrafficKafkaPublisher,
    _to_json_safe,
)


@pytest.fixture
def fast_config() -> PublisherConfig:
    """A publisher config whose backoffs do not slow the suite down."""
    return PublisherConfig(
        bootstrap_servers="redpanda:9092",
        topic="uxsim",
        produce_interval_sec=0.0,
        max_retries=3,
        initial_backoff_sec=0.0,
        backoff_multiplier=1.0,
    )


def test_to_json_safe_coerces_numpy_scalars_and_arrays() -> None:
    assert _to_json_safe(np.int64(7)) == 7
    assert isinstance(_to_json_safe(np.int64(7)), int)

    assert _to_json_safe(np.float32(1.5)) == pytest.approx(1.5)
    assert isinstance(_to_json_safe(np.float64(1.5)), float)

    assert _to_json_safe(np.array([1, 2, 3])) == [1, 2, 3]

    # Native types pass through untouched.
    assert _to_json_safe("foo") == "foo"
    assert _to_json_safe(42) == 42


def test_dry_run_prints_json_and_skips_broker(
    fast_config: PublisherConfig, mocker, capsys
) -> None:
    producer_cls = mocker.patch("traffic_pipeline.ingestion.kafka_publisher.KafkaProducer")

    publisher = TrafficKafkaPublisher(fast_config, dry_run=True)
    sent = publisher.publish_records([{"t": 0.0, "link": "I1I2", "v": 12.3}])

    assert sent == 1
    producer_cls.assert_not_called()
    out_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(out_lines) == 1
    assert json.loads(out_lines[0]) == {"t": 0.0, "link": "I1I2", "v": 12.3}


def test_publish_records_constructs_producer_and_sends(
    fast_config: PublisherConfig, mocker
) -> None:
    producer_cls = mocker.patch("traffic_pipeline.ingestion.kafka_publisher.KafkaProducer")
    fake_producer = producer_cls.return_value

    publisher = TrafficKafkaPublisher(fast_config)
    records = [
        {"t": 0.0, "link": "I1I2", "v": 10.0},
        {"t": 1.0, "link": "I2I3", "v": 11.5},
    ]
    sent = publisher.publish_records(records)

    # Connection used the configured address and retry budget.
    producer_cls.assert_called_once()
    kwargs = producer_cls.call_args.kwargs
    assert kwargs["bootstrap_servers"] == "redpanda:9092"
    assert kwargs["retries"] == fast_config.max_retries

    # The serializer must produce UTF-8 JSON bytes; check it round-trips a
    # representative payload rather than asserting on the lambda's identity.
    serializer = kwargs["value_serializer"]
    encoded = serializer({"link": "I1I2", "v": 10.0})
    assert isinstance(encoded, bytes)
    assert json.loads(encoded.decode("utf-8")) == {"link": "I1I2", "v": 10.0}

    # Every record was forwarded with the configured topic.
    assert fake_producer.send.call_count == 2
    for call, expected in zip(fake_producer.send.call_args_list, records, strict=True):
        topic, payload = call.args
        assert topic == "uxsim"
        assert payload == expected
    assert sent == 2
    fake_producer.flush.assert_called()  # publish_records flushes once at the end.


def test_send_with_retry_retries_then_succeeds(
    fast_config: PublisherConfig, mocker
) -> None:
    producer_cls = mocker.patch("traffic_pipeline.ingestion.kafka_publisher.KafkaProducer")
    fake_producer = producer_cls.return_value
    # Fail twice, then succeed. max_retries=3 means attempts 1, 2, 3 are
    # available -- so the third attempt should win.
    fake_producer.send.side_effect = [KafkaError("boom"), KafkaError("boom"), None]

    publisher = TrafficKafkaPublisher(fast_config)
    sent = publisher.publish_records([{"t": 0.0, "link": "I1I2", "v": 10.0}])

    assert sent == 1
    assert fake_producer.send.call_count == 3


def test_send_with_retry_gives_up_when_budget_exhausted(
    fast_config: PublisherConfig, mocker
) -> None:
    producer_cls = mocker.patch("traffic_pipeline.ingestion.kafka_publisher.KafkaProducer")
    fake_producer = producer_cls.return_value
    fake_producer.send.side_effect = KafkaError("permanently down")

    publisher = TrafficKafkaPublisher(fast_config)
    sent = publisher.publish_records([{"t": 0.0, "link": "I1I2", "v": 10.0}])

    # All retries consumed, no record counted as published.
    assert sent == 0
    assert fake_producer.send.call_count == fast_config.max_retries


def test_close_flushes_and_closes_underlying_producer(
    fast_config: PublisherConfig, mocker
) -> None:
    producer_cls = mocker.patch("traffic_pipeline.ingestion.kafka_publisher.KafkaProducer")
    fake_producer = producer_cls.return_value

    with TrafficKafkaPublisher(fast_config) as publisher:
        publisher.publish_records([{"t": 0.0, "link": "I1I2", "v": 10.0}])

    fake_producer.close.assert_called_once()
    # flush is called both at end-of-batch and in close; assert at least once.
    assert fake_producer.flush.call_count >= 1
