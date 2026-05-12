"""End-to-end smoke test for the running compose stack.

Runs from the *host* (laptop) side, not inside a container. Confirms
that the streaming pipeline is alive and producing data by checking:

1. Every expected Docker container is up.
2. The Kafka topic exists on the broker.
3. ``traffic.raw_data`` has at least one document.
4. ``traffic.stats`` has at least one document.

Exit code is ``0`` on success and ``1`` on any failure, so the script
slots cleanly into CI later or into a shell ``&&`` chain. The output is
deliberately copy-pasteable into the project report.

Examples:
    python scripts/verify_pipeline.py
    python scripts/verify_pipeline.py --mongo-uri mongodb://localhost:27018
    python scripts/verify_pipeline.py --min-raw 1000 --min-stats 50
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from traffic_pipeline.evaluation.mongo_inspector import count_raw, count_stats

EXPECTED_CONTAINERS = (
    "redpanda",
    "mongo",
    "spark-master",
    "spark-worker",
    "spark-job",
    "uxsim-producer",
)


@dataclass
class CheckResult:
    """Outcome of a single check, used to drive the printed summary."""

    name: str
    ok: bool
    detail: str


def _run(cmd: list[str], *, timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def check_containers(expected: tuple[str, ...] = EXPECTED_CONTAINERS) -> CheckResult:
    """Check that every expected container is listed as running by Docker."""
    if not shutil.which("docker"):
        return CheckResult("containers", False, "docker CLI not on PATH")
    proc = _run(["docker", "ps", "--format", "{{.Names}}"])
    if proc.returncode != 0:
        return CheckResult("containers", False, f"docker ps failed: {proc.stderr.strip()}")
    running = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    missing = [name for name in expected if name not in running]
    if missing:
        return CheckResult(
            "containers",
            False,
            f"missing: {', '.join(missing)} (running: {', '.join(sorted(running)) or 'none'})",
        )
    return CheckResult("containers", True, f"{len(expected)}/{len(expected)} up")


def check_topic(topic: str, *, broker_container: str = "redpanda") -> CheckResult:
    """Check that ``topic`` exists on the in-network Redpanda broker."""
    if not shutil.which("docker"):
        return CheckResult("topic", False, "docker CLI not on PATH")
    proc = _run(["docker", "exec", broker_container, "rpk", "topic", "list"])
    if proc.returncode != 0:
        return CheckResult("topic", False, f"rpk topic list failed: {proc.stderr.strip()}")
    topics = {
        line.split()[0]
        for line in proc.stdout.splitlines()[1:]
        if line.strip() and not line.startswith("NAME")
    }
    if topic not in topics:
        return CheckResult(
            "topic", False, f"{topic!r} not in {sorted(topics) or 'no topics'}"
        )
    return CheckResult("topic", True, f"{topic!r} exists")


def check_collection_nonempty(
    mongo_uri: str,
    database: str,
    *,
    raw_collection: str,
    stats_collection: str,
    min_raw: int,
    min_stats: int,
) -> tuple[CheckResult, CheckResult]:
    """Open a single Mongo connection and check both collections."""
    try:
        client: MongoClient = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        db = client[database]
        raw_n = count_raw(db, collection=raw_collection)
        stats_n = count_stats(db, collection=stats_collection)
    except PyMongoError as exc:
        msg = f"mongo error: {exc}"
        return (
            CheckResult("raw_data", False, msg),
            CheckResult("stats", False, msg),
        )
    finally:
        try:
            client.close()  # type: ignore[has-type]
        except Exception:
            pass

    raw_ok = raw_n >= min_raw
    stats_ok = stats_n >= min_stats
    return (
        CheckResult(
            "raw_data",
            raw_ok,
            f"{raw_n} docs (min {min_raw})",
        ),
        CheckResult(
            "stats",
            stats_ok,
            f"{stats_n} docs (min {min_stats})",
        ),
    )


def _print_results(results: list[CheckResult]) -> int:
    ok_count = sum(1 for r in results if r.ok)
    for r in results:
        marker = "[OK]  " if r.ok else "[FAIL]"
        print(f"{marker} {r.name}: {r.detail}")
    print()
    summary_status = "OK" if ok_count == len(results) else "FAIL"
    print(f"[{summary_status}] {ok_count}/{len(results)} checks passed")
    return 0 if ok_count == len(results) else 1


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify_pipeline",
        description="Smoke-check the running traffic-streaming-pipeline stack.",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MONGO_URI_HOST", "mongodb://localhost:27018"),
        help="Host-side Mongo URI. Default: %(default)s.",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("MONGO_DATABASE", "traffic"),
        help="Target database name. Default: %(default)s.",
    )
    parser.add_argument(
        "--raw-collection",
        default=os.environ.get("MONGO_RAW_COLLECTION", "raw_data"),
    )
    parser.add_argument(
        "--stats-collection",
        default=os.environ.get("MONGO_STATS_COLLECTION", "stats"),
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "uxsim"),
        help="Kafka topic to check. Default: %(default)s.",
    )
    parser.add_argument(
        "--min-raw",
        type=int,
        default=1,
        help="Minimum acceptable raw_data document count. Default: 1.",
    )
    parser.add_argument(
        "--min-stats",
        type=int,
        default=1,
        help="Minimum acceptable stats document count. Default: 1.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code."""
    args = _build_arg_parser().parse_args(argv)

    results: list[CheckResult] = []
    results.append(check_containers())
    results.append(check_topic(args.topic))
    raw_res, stats_res = check_collection_nonempty(
        args.mongo_uri,
        args.database,
        raw_collection=args.raw_collection,
        stats_collection=args.stats_collection,
        min_raw=args.min_raw,
        min_stats=args.min_stats,
    )
    results.append(raw_res)
    results.append(stats_res)

    return _print_results(results)


if __name__ == "__main__":
    sys.exit(main())
