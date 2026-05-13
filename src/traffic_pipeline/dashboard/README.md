# `dashboard/`

Streamlit application that visualises the live state of the streaming
pipeline. Reads MongoDB directly through
`traffic_pipeline.serving.repository.TrafficRepository`; the FastAPI
service is **not** a prerequisite.

## Files

| File | Purpose |
|---|---|
| `app.py` | Streamlit entry point. Three tabs: Overview (KPIs + top-N), Network map (Plotly figure colored by `vspeed` / sized by `vcount`), Link detail (selectbox + time series). |
| `network_geometry.py` | Static `NODES` and `LINKS` dictionaries mirroring the UXSIM topology. Pure data, no I/O. |
| `__init__.py` | Package marker. |

## Invariants and gotchas

- `network_geometry.LINKS` keys must match the link names produced by
  `traffic_pipeline.ingestion.uxsim_network._add_links` byte-for-byte
  (they are the `link` column in the Mongo collections; mismatch means
  the map shows links with no live data). The module docstring names
  the source-of-truth function so a future rename is hard to miss.
- The Streamlit app holds exactly one `MongoClient` for the process
  lifetime via `@st.cache_resource`, mirroring the lifespan pattern in
  `serving/api.py`.
- Auto-refresh runs at 2 second intervals via `streamlit-autorefresh`.
  Users can pause it from the sidebar; an idle Mongo (producer stopped)
  is reflected as frozen KPIs, never as an exception.

## Neighbour interaction

- **`serving/repository.py`**: source of every Mongo query the
  dashboard runs. New methods (`latest_t`, `link_state_at`) were added
  there rather than in this directory so the FastAPI service can use
  them too.
- **`ingestion/uxsim_network.py`**: source of truth for the network
  topology mirrored by `network_geometry.py`.
- **`docker-compose.yml`**: the `dashboard` service is gated behind the
  `--profile dashboard` flag, same gating pattern as `traffic-api`.

## Running locally (outside Docker)

```bash
pip install -r requirements.txt
MONGO_URI=mongodb://localhost:27018 \
    streamlit run src/traffic_pipeline/dashboard/app.py
```

Port 27018 is the host-side Mongo port shifted by `docker-compose.yml`
to avoid clashing with a host MongoDB on the default 27017.
