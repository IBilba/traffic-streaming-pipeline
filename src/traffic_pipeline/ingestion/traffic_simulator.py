"""Execute a UXSIM world and surface its vehicle log as a DataFrame.

This is the one side-effect-heavy module in the ingestion subpackage: it
runs the simulation to completion..
"""

from __future__ import annotations

import logging

import pandas as pd
from uxsim import World

logger = logging.getLogger(__name__)


def simulate_traffic(world: World) -> pd.DataFrame:
    """Run the simulation to completion and return the vehicle log.

    ``World.exec_simulation`` is blocking; it iterates every simulation
    step until ``tmax`` is reached. For the course's 3600 s scenario the
    wall-clock cost is a few seconds.

    Args:
        world: A configured world from
            :func:`traffic_pipeline.ingestion.uxsim_network.build_grid_network`.

    Returns:
        DataFrame returned by ``world.analyzer.vehicles_to_pandas()``.
        Columns: ``name``, ``dn``, ``orig``, ``dest``, ``t``, ``link``,
        ``x``, ``s``, ``v`` (UXSIM exposes vehicle speed as ``v``, not
        ``speed`` -- see CLAUDE.md section 7 glossary).
    """
    logger.info("Running UXSIM simulation (tmax=%s s).", world.TMAX)
    world.exec_simulation()
    df = world.analyzer.vehicles_to_pandas()
    logger.info(
        "Simulation finished: %d vehicle-step records, %d unique links.",
        len(df),
        df["link"].nunique() if "link" in df.columns else 0,
    )
    return df
