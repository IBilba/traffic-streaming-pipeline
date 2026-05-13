"""Static node and link geometry for the dashboard network map.

The numbers in this module mirror the UXSIM topology built by
``_add_nodes`` and ``_add_links`` in
:mod:`traffic_pipeline.ingestion.uxsim_network`. They are duplicated
here (instead of imported) so the dashboard container does not depend
on UXSIM, which is a heavy dependency only the producer image needs.

If the producer topology ever changes (new nodes, renamed links,
different grid spacing), update this module in lockstep. The unit tests
in ``tests/unit/test_dashboard_geometry.py`` only check structural
integrity (every link references known nodes, every node is reachable),
not that the names match UXSIM byte-for-byte; the source-of-truth check
is the cross-component invariant called out in this docstring.

Network shape (E-W arterial, signal-controlled N-S streets)::

        N1  N2  N3  N4
        |   |   |   |
    W1--I1--I2--I3--I4-<E1
        |   |   |   |
        S1  S2  S3  S4

Link naming follows the UXSIM ``_add_links`` convention of
``end_node + start_node`` for the E-W arterial and the S-N streets, and
``start_node + end_node`` for the N-S streets. The link name is what
appears in the ``link`` column of the Mongo collections, so it is the
join key for the dashboard map.
"""

from __future__ import annotations

from typing import Mapping

# Node name -> (x, y) on the abstract grid. Units are arbitrary; only
# relative positions matter for the Plotly figure.
NODES: Mapping[str, tuple[float, float]] = {
    "I1": (1, 0),
    "I2": (2, 0),
    "I3": (3, 0),
    "I4": (4, 0),
    "W1": (0, 0),
    "E1": (5, 0),
    "N1": (1, 1),
    "N2": (2, 1),
    "N3": (3, 1),
    "N4": (4, 1),
    "S1": (1, -1),
    "S2": (2, -1),
    "S3": (3, -1),
    "S4": (4, -1),
}

# Link name -> (start_node, end_node). The pair encodes flow direction so
# the map can draw an arrowhead; the geometric segment itself is
# direction-agnostic.
LINKS: Mapping[str, tuple[str, str]] = {
    # E-W arterial, all westbound (matches the demand bias in the
    # producer: most cars head toward W1).
    "I1W1": ("I1", "W1"),
    "I2I1": ("I2", "I1"),
    "I3I2": ("I3", "I2"),
    "I4I3": ("I4", "I3"),
    "E1I4": ("E1", "I4"),
    # Columns 1 and 3: southbound only.
    "N1I1": ("N1", "I1"),
    "I1S1": ("I1", "S1"),
    "N3I3": ("N3", "I3"),
    "I3S3": ("I3", "S3"),
    # Columns 2 and 4: northbound only.
    "S2I2": ("S2", "I2"),
    "I2N2": ("I2", "N2"),
    "S4I4": ("S4", "I4"),
    "I4N4": ("I4", "N4"),
}


def link_segments() -> list[dict[str, float | str]]:
    """Return one drawable segment per link.

    Returns:
        List of ``{"link", "start", "end", "x0", "y0", "x1", "y1"}``
        dicts in declaration order. Coordinates are the Cartesian
        endpoints from :data:`NODES`; ``link`` is the Mongo join key.
    """
    segments: list[dict[str, float | str]] = []
    for link_name, (start, end) in LINKS.items():
        x0, y0 = NODES[start]
        x1, y1 = NODES[end]
        segments.append(
            {
                "link": link_name,
                "start": start,
                "end": end,
                "x0": float(x0),
                "y0": float(y0),
                "x1": float(x1),
                "y1": float(y1),
            }
        )
    return segments
