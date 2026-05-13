"""Structural integrity tests for the dashboard network geometry.

These tests do not assert that node names or link names match the
UXSIM topology byte-for-byte: that invariant is documented in the
module docstring and enforced by code review when the topology
changes. What they do check is that the dictionaries are internally
consistent (every link endpoint resolves to a known node, every node
is touched by at least one link) and that ``link_segments`` produces
one finite-coordinate entry per link.
"""

from __future__ import annotations

import math

from traffic_pipeline.dashboard.network_geometry import (
    LINKS,
    NODES,
    link_segments,
)


def test_every_link_endpoint_exists_in_nodes() -> None:
    for link_name, (start, end) in LINKS.items():
        assert start in NODES, f"link {link_name}: unknown start node {start}"
        assert end in NODES, f"link {link_name}: unknown end node {end}"


def test_every_node_is_referenced_by_at_least_one_link() -> None:
    touched: set[str] = set()
    for start, end in LINKS.values():
        touched.add(start)
        touched.add(end)
    orphaned = set(NODES) - touched
    assert orphaned == set(), f"orphaned nodes: {orphaned}"


def test_link_segments_returns_one_finite_entry_per_link() -> None:
    segments = link_segments()
    assert len(segments) == len(LINKS)
    seen_links = {seg["link"] for seg in segments}
    assert seen_links == set(LINKS)
    for seg in segments:
        for key in ("x0", "y0", "x1", "y1"):
            value = seg[key]
            assert isinstance(value, float)
            assert math.isfinite(value), f"{seg['link']}.{key} is not finite"


def test_link_segments_endpoints_match_nodes_table() -> None:
    for seg in link_segments():
        start_xy = NODES[seg["start"]]
        end_xy = NODES[seg["end"]]
        assert (seg["x0"], seg["y0"]) == (float(start_xy[0]), float(start_xy[1]))
        assert (seg["x1"], seg["y1"]) == (float(end_xy[0]), float(end_xy[1]))


def test_link_names_are_unique() -> None:
    assert len(LINKS) == len(set(LINKS))


def test_node_coordinates_are_unique() -> None:
    coords = list(NODES.values())
    assert len(coords) == len(set(coords)), "two nodes share the same (x, y)"
