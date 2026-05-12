"""UXSIM network topology builder.

Defines a 4x4-like grid road network with signalled intersections,
boundary source/sink nodes, and probabilistic origin-destination demand.
The module is intentionally side-effect-free: it constructs and returns
a configured :class:`uxsim.World` but does *not* call
``exec_simulation``.

Topology::

        N1  N2  N3  N4
        |   |   |   |
    W1--I1--I2--I3--I4-<E1
        |   |   |   |
        S1  S2  S3  S4

I1..I4 are signalled four-way intersections; the rest are boundary
nodes (sources for the E-W arterial and the N-S streets).
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from uxsim import World


@dataclass(frozen=True)
class NetworkConfig:
    """Parameters that drive grid construction and OD demand.

    Attributes:
        tmax: Total simulated time in seconds. Demand is generated up to
            this horizon.
        deltan: UXSIM platoon size -- the simulator groups ``deltan``
            vehicles into a single platoon for performance.
        seed: Optional random seed for reproducible demand. ``None``
            yields a fresh random stream on each invocation.
        signal_time: Base traffic-light cycle in seconds.
        split_factor_a: Green-time multiplier for signal phase A.
        split_factor_b: Green-time multiplier for signal phase B.
        link_length: Length of every link in metres.
        ew_free_flow_speed: Free-flow speed (km/h) on the E-W arterial.
        ns_free_flow_speed: Free-flow speed (km/h) on the N-S streets.
        jam_density: Vehicles per metre at jam (UXSIM density-flow
            parameter).
        ew_lanes: Number of lanes on the E-W arterial links.
        demand_avg: Mean OD demand (vehicles / second) used as the upper
            bound of the uniform sampler.
        demand_step_seconds: How often demand is re-sampled, in
            simulated seconds.
    """

    tmax: int = 3600
    deltan: int = 5
    seed: int | None = None
    signal_time: int = 20
    split_factor_a: int = 1
    split_factor_b: int = 2
    link_length: int = 500
    ew_free_flow_speed: int = 50
    ns_free_flow_speed: int = 30
    jam_density: float = 0.2
    ew_lanes: int = 3
    demand_avg: float = 2.0
    demand_step_seconds: int = 30


def build_grid_network(config: NetworkConfig | None = None) -> World:
    """Construct the grid network and register OD demand on it.

    Args:
        config: Network parameters. Defaults to :class:`NetworkConfig`.

    Returns:
        A configured :class:`uxsim.World` ready to be passed to
        :func:`traffic_pipeline.ingestion.traffic_simulator.simulate_traffic`.
    """
    cfg = config or NetworkConfig()

    world = World(
        name="",
        deltan=cfg.deltan,
        tmax=cfg.tmax,
        print_mode=1,
        save_mode=0,
        show_mode=0,
        random_seed=cfg.seed,
        duo_update_time=600,
    )
    # UXSIM does not seed Python's global random; do it ourselves so demand is
    # reproducible when a seed is supplied.
    random.seed(cfg.seed)

    nodes = _add_nodes(world, cfg)
    _add_links(world, cfg, nodes)
    _register_demand(world, cfg, nodes)
    return world


def _add_nodes(world: World, cfg: NetworkConfig) -> dict[str, object]:
    """Add all named nodes and return them keyed by name."""
    green_a = cfg.signal_time * cfg.split_factor_a
    green_b = cfg.signal_time * cfg.split_factor_b
    signal = [green_a, green_b]

    nodes: dict[str, object] = {
        "I1": world.addNode("I1", 1, 0, signal=signal),
        "I2": world.addNode("I2", 2, 0, signal=signal),
        "I3": world.addNode("I3", 3, 0, signal=signal),
        "I4": world.addNode("I4", 4, 0, signal=signal),
        "W1": world.addNode("W1", 0, 0),
        "E1": world.addNode("E1", 5, 0),
        "N1": world.addNode("N1", 1, 1),
        "N2": world.addNode("N2", 2, 1),
        "N3": world.addNode("N3", 3, 1),
        "N4": world.addNode("N4", 4, 1),
        "S1": world.addNode("S1", 1, -1),
        "S2": world.addNode("S2", 2, -1),
        "S3": world.addNode("S3", 3, -1),
        "S4": world.addNode("S4", 4, -1),
    }
    return nodes


def _add_links(world: World, cfg: NetworkConfig, nodes: dict[str, object]) -> None:
    """Add bidirectional E-W arterials and N-S streets with signal groups."""
    n = nodes

    # E <-> W arterial: signal group 0. Names follow the existing convention
    # n2.name + n1.name to match the original course skeleton.
    ew_pairs = [
        (n["W1"], n["I1"]),
        (n["I1"], n["I2"]),
        (n["I2"], n["I3"]),
        (n["I3"], n["I4"]),
        (n["I4"], n["E1"]),
    ]
    for n1, n2 in ew_pairs:
        world.addLink(
            n2.name + n1.name,
            n2,
            n1,
            length=cfg.link_length,
            free_flow_speed=cfg.ew_free_flow_speed,
            jam_density=cfg.jam_density,
            number_of_lanes=cfg.ew_lanes,
            signal_group=0,
        )

    # N -> S streets: signal group 1.
    ns_pairs = [
        (n["N1"], n["I1"]),
        (n["I1"], n["S1"]),
        (n["N3"], n["I3"]),
        (n["I3"], n["S3"]),
    ]
    for n1, n2 in ns_pairs:
        world.addLink(
            n1.name + n2.name,
            n1,
            n2,
            length=cfg.link_length,
            free_flow_speed=cfg.ns_free_flow_speed,
            jam_density=cfg.jam_density,
            signal_group=1,
        )

    # S -> N streets: also signal group 1 in the original code.
    sn_pairs = [
        (n["N2"], n["I2"]),
        (n["I2"], n["S2"]),
        (n["N4"], n["I4"]),
        (n["I4"], n["S4"]),
    ]
    for n1, n2 in sn_pairs:
        world.addLink(
            n2.name + n1.name,
            n2,
            n1,
            length=cfg.link_length,
            free_flow_speed=cfg.ns_free_flow_speed,
            jam_density=cfg.jam_density,
            signal_group=1,
        )


def _register_demand(
    world: World,
    cfg: NetworkConfig,
    nodes: dict[str, object],
) -> None:
    """Register probabilistic OD demand for every demand step."""
    n = nodes
    quarter_pairs = [
        (n["N1"], n["S1"]),
        (n["S2"], n["N2"]),
        (n["N3"], n["S3"]),
        (n["S4"], n["N4"]),
    ]
    three_quarters_pairs = [
        (n["E1"], n["W1"]),
        (n["N1"], n["W1"]),
        (n["S2"], n["W1"]),
        (n["N3"], n["W1"]),
        (n["S4"], n["W1"]),
    ]
    for t in range(0, cfg.tmax, cfg.demand_step_seconds):
        # Demand magnitude is freshly sampled for each window. The 0.25 / 0.75
        # split mirrors the course skeleton: most cars head west towards W1.
        dem = random.uniform(0, cfg.demand_avg)
        for n1, n2 in quarter_pairs:
            world.adddemand(n1, n2, t, t + cfg.demand_step_seconds, dem * 0.25)
        for n1, n2 in three_quarters_pairs:
            world.adddemand(n1, n2, t, t + cfg.demand_step_seconds, dem * 0.75)
