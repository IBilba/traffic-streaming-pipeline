"""Streamlit dashboard for the traffic streaming pipeline.

The app holds exactly one ``MongoClient`` per process (cached via
``st.cache_resource``) and renders three tabs:

* **Overview** -- KPI row, top-N congested links, mean speed table.
* **Network map** -- Plotly figure of the UXSIM grid coloured by
  ``vspeed`` and sized by ``vcount`` at a selected simulation time.
* **Link detail** -- summary card and time series for a single link,
  switchable between the per-step ``stats`` collection and the
  30-second windowed collection.

Auto-refresh runs every two seconds via ``streamlit-autorefresh`` and
can be paused from the sidebar.

Run locally with::

    MONGO_URI=mongodb://localhost:27018 \\
        streamlit run src/traffic_pipeline/dashboard/app.py

Configuration (env vars, same naming as the FastAPI service):

* ``MONGO_URI`` (default ``mongodb://mongo:27017``).
* ``MONGO_DATABASE`` (default ``traffic``).
* ``MONGO_RAW_COLLECTION`` (default ``raw_data``).
* ``MONGO_STATS_COLLECTION`` (default ``stats``).
* ``MONGO_WINDOWED_COLLECTION`` (default ``stats_windowed``).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

from traffic_pipeline.dashboard.network_geometry import (
    LINKS,
    NODES,
    link_segments,
)
from traffic_pipeline.serving.repository import TrafficRepository

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_MS = 2_000


def _env(name: str, default: str) -> str:
    """Return the trimmed value of env var ``name`` or ``default`` if unset."""
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


@st.cache_resource
def get_repository() -> TrafficRepository:
    """Return a singleton :class:`TrafficRepository` for the process.

    ``st.cache_resource`` keeps the underlying ``MongoClient`` alive
    across Streamlit reruns so the connection pool is paid for once,
    matching the lifespan pattern in the FastAPI service.
    """
    uri = _env("MONGO_URI", "mongodb://mongo:27017")
    database = _env("MONGO_DATABASE", "traffic")
    stats = _env("MONGO_STATS_COLLECTION", "stats")
    raw = _env("MONGO_RAW_COLLECTION", "raw_data")
    windowed = _env("MONGO_WINDOWED_COLLECTION", "stats_windowed")

    logger.info("Connecting to MongoDB at %s (db=%s)", uri, database)
    client: MongoClient[Any] = MongoClient(uri)
    return TrafficRepository(
        client[database],
        stats_collection=stats,
        raw_collection=raw,
        windowed_collection=windowed,
    )


def _ping_mongo(repo: TrafficRepository) -> bool:
    """Return True iff MongoDB responds to ping, swallowing transport errors."""
    try:
        return repo.ping()
    except Exception as exc:
        logger.warning("Mongo ping failed: %s", exc)
        return False


def _raw_event_count(repo: TrafficRepository) -> int:
    """Estimated count of raw UXSIM events landed so far."""
    try:
        return int(repo.db[repo.raw_collection].estimated_document_count())
    except Exception as exc:
        logger.warning("estimated_document_count failed: %s", exc)
        return 0


def _windowed_series(
    repo: TrafficRepository, link_id: str
) -> list[Mapping[str, Any]]:
    """Time series for one link from the 30-second windowed collection."""
    cursor = (
        repo.db[repo.windowed_collection]
        .find(
            {"link": link_id},
            projection={
                "_id": 0,
                "window_start": 1,
                "window_end": 1,
                "vcount": 1,
                "vspeed_avg": 1,
                "vspeed_min": 1,
                "vspeed_max": 1,
            },
        )
        .sort("window_start", 1)
    )
    return list(cursor)


def _speed_to_color(vspeed: float, free_flow: float = 50.0) -> str:
    """Map a vspeed value to a green-to-red color (free flow = green)."""
    ratio = max(0.0, min(1.0, vspeed / free_flow))
    red = int(round(220 * (1 - ratio)))
    green = int(round(180 * ratio))
    return f"rgb({red},{green},60)"


def _width_for_vcount(vcount: float, vmax: float) -> float:
    """Map vcount to a Plotly line width in [2, 14]."""
    if vmax <= 0:
        return 2.0
    return 2.0 + 12.0 * min(1.0, vcount / vmax)


def render_sidebar(repo: TrafficRepository) -> bool:
    """Render sidebar controls. Returns True iff auto-refresh is enabled."""
    st.sidebar.header("Connection")
    st.sidebar.code(_env("MONGO_URI", "mongodb://mongo:27017"))
    st.sidebar.write(f"Database: `{_env('MONGO_DATABASE', 'traffic')}`")
    mongo_ok = _ping_mongo(repo)
    status = "ok" if mongo_ok else "unreachable"
    st.sidebar.write(f"Status: **{status}**")

    st.sidebar.header("Refresh")
    auto = st.sidebar.checkbox("Auto-refresh every 2s", value=True)
    if st.sidebar.button("Refresh now"):
        st.rerun()
    return auto


def render_overview(repo: TrafficRepository) -> None:
    """KPI row, top-N congested links, mean-speed table."""
    st.subheader("Pipeline overview")

    raw_count = _raw_event_count(repo)
    t_max = repo.latest_t()
    avg_speed_rows = repo.avg_speed_per_link()

    kpi_a, kpi_b, kpi_c = st.columns(3)
    kpi_a.metric("Raw events", f"{raw_count:,}")
    kpi_b.metric(
        "Current sim time (s)",
        f"{t_max:.1f}" if t_max is not None else "n/a",
    )
    kpi_c.metric("Active links (stats)", len(avg_speed_rows))

    st.subheader("Top congested links")
    top_n = st.slider("How many links", min_value=1, max_value=13, value=5)
    top = repo.top_n_congested(top_n)
    if not top:
        st.info("No stats rows yet. Wait for the first Spark batch.")
    else:
        df_top = pd.DataFrame(top)
        st.bar_chart(df_top.set_index("link")["peak_vcount"])
        st.dataframe(df_top, hide_index=True, use_container_width=True)

    st.subheader("Mean speed per link")
    if avg_speed_rows:
        st.dataframe(
            pd.DataFrame(avg_speed_rows),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No stats rows yet.")


def _build_network_figure(
    state_by_link: Mapping[str, Mapping[str, Any]],
) -> go.Figure:
    """Compose the Plotly network map from per-link state and static geometry."""
    fig = go.Figure()

    # Background nodes
    node_x = [xy[0] for xy in NODES.values()]
    node_y = [xy[1] for xy in NODES.values()]
    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            text=list(NODES),
            textposition="top center",
            marker=dict(size=14, color="#444"),
            hoverinfo="text",
            showlegend=False,
        )
    )

    vmax = max(
        (state.get("vcount", 0) for state in state_by_link.values()),
        default=0,
    )

    for seg in link_segments():
        link_name = seg["link"]
        state = state_by_link.get(link_name)
        if state is None:
            color = "#bbbbbb"
            width = 2.0
            label = f"{link_name}<br>no data"
        else:
            vspeed = float(state.get("vspeed", 0.0))
            vcount = float(state.get("vcount", 0.0))
            color = _speed_to_color(vspeed)
            width = _width_for_vcount(vcount, float(vmax))
            label = (
                f"{link_name}<br>vcount: {vcount:.0f}"
                f"<br>vspeed: {vspeed:.1f} km/h"
            )

        fig.add_trace(
            go.Scatter(
                x=[seg["x0"], seg["x1"]],
                y=[seg["y0"], seg["y1"]],
                mode="lines",
                line=dict(color=color, width=width),
                hoverinfo="text",
                hovertext=label,
                showlegend=False,
            )
        )

    fig.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(visible=False, range=[-0.7, 5.7]),
        yaxis=dict(visible=False, range=[-1.7, 1.7], scaleanchor="x"),
        plot_bgcolor="#fafafa",
    )
    return fig


def render_network_map(repo: TrafficRepository) -> None:
    """Plotly map of the UXSIM grid coloured by live link state."""
    st.subheader("Network map")
    t_max = repo.latest_t()
    if t_max is None:
        st.info("No stats rows yet. Wait for the first Spark batch.")
        return

    t = st.slider(
        "Simulation time (s)",
        min_value=0.0,
        max_value=float(t_max),
        value=float(t_max),
        step=1.0,
        help="Scrub through the simulation; defaults to the latest step.",
    )

    state_rows = repo.link_state_at(t, tolerance=2.0)
    state_by_link = {row["link"]: row for row in state_rows}
    fig = _build_network_figure(state_by_link)
    st.plotly_chart(fig, use_container_width=True)

    if state_rows:
        st.caption(
            "Color: vspeed (red = slow, green = free flow). "
            "Line width: vcount (thicker = more vehicles)."
        )
    else:
        st.caption("No samples in the selected window; all links shown grey.")


def render_link_detail(repo: TrafficRepository) -> None:
    """Per-link summary card and time series."""
    st.subheader("Link detail")
    link_id = st.selectbox("Link", sorted(LINKS))
    source = st.radio(
        "Source",
        options=("per-step (stats)", "windowed (30s)"),
        horizontal=True,
    )

    if source.startswith("per-step"):
        detail = repo.link_detail(link_id)
        if detail is None:
            st.info(f"No rows for link {link_id} yet.")
            return
        _render_summary_card(detail["summary"])
        df = pd.DataFrame(detail["series"])
        if df.empty:
            st.info("Summary present but no time series rows.")
            return
        st.line_chart(df.set_index("t")[["vcount"]])
        st.line_chart(df.set_index("t")[["vspeed"]])
    else:
        rows = _windowed_series(repo, link_id)
        if not rows:
            st.info(f"No windowed rows for link {link_id} yet.")
            return
        df = pd.DataFrame(rows)
        df["window_start"] = pd.to_datetime(df["window_start"])
        st.line_chart(df.set_index("window_start")[["vcount"]])
        st.line_chart(
            df.set_index("window_start")[
                ["vspeed_avg", "vspeed_min", "vspeed_max"]
            ]
        )


def _render_summary_card(summary: Mapping[str, Any]) -> None:
    """Render the per-link summary dict as four metric tiles."""
    cols = st.columns(4)
    cols[0].metric("Samples", f"{int(summary['samples']):,}")
    cols[1].metric("Avg speed (km/h)", f"{summary['avg_speed']:.1f}")
    cols[2].metric("Peak vcount", f"{int(summary['peak_vcount'])}")
    cols[3].metric(
        "t range (s)",
        f"{summary['first_t']:.0f}..{summary['last_t']:.0f}",
    )


def main() -> None:
    """Entry point invoked by ``streamlit run``."""
    st.set_page_config(
        page_title="Traffic Pipeline Dashboard",
        layout="wide",
    )
    st.title("Traffic streaming pipeline")
    st.caption(
        "Live view of the UXSIM -> Redpanda -> Spark -> MongoDB pipeline."
    )

    repo = get_repository()
    auto_refresh = render_sidebar(repo)
    if auto_refresh:
        st_autorefresh(interval=REFRESH_INTERVAL_MS, key="poll")

    overview_tab, map_tab, detail_tab = st.tabs(
        ["Overview", "Network map", "Link detail"]
    )
    with overview_tab:
        render_overview(repo)
    with map_tab:
        render_network_map(repo)
    with detail_tab:
        render_link_detail(repo)


main()
