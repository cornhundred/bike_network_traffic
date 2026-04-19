from __future__ import annotations

from pathlib import Path

import anywidget
import traitlets

_BUNDLE = Path(__file__).resolve().parent / "bundled" / "widget.js"


class BikeFlowMapWidget(anywidget.AnyWidget):
    """Deck.gl map of bike stations with flows, synced with a Celldega clustergram via traitlets."""

    _esm = _BUNDLE

    stations = traitlets.List(trait=traitlets.Dict(), default_value=[]).tag(sync=True)
    edge_index = traitlets.Dict(default_value={}).tag(sync=True)
    click_info = traitlets.Dict(default_value={}).tag(sync=True)
    selected_rows = traitlets.List(default_value=[]).tag(sync=True)
    selected_cols = traitlets.List(default_value=[]).tag(sync=True)
    width = traitlets.Int(560).tag(sync=True)
    height = traitlets.Int(800).tag(sync=True)
    debug = traitlets.Bool(False).tag(sync=True)
    palette_rgb = traitlets.List(default_value=[]).tag(sync=True)
    matrix_axis_slice = traitlets.Dict(default_value={}).tag(sync=True)
    matrix_slice_request_out = traitlets.Dict(default_value={}).tag(sync=True)
    cg_row_names = traitlets.List(default_value=[]).tag(sync=True)
    cg_col_names = traitlets.List(default_value=[]).tag(sync=True)
    spatial_mix = traitlets.Float(0.0).tag(sync=True)

    # Per-cluster alpha-shape neighborhoods. See ``nbhd.compute_cluster_alpha_shapes``
    # for the wire format. Empty dict disables the neighborhood layer entirely.
    cluster_polygons = traitlets.Dict(default_value={}).tag(sync=True)
    # Index into ``cluster_polygons["levels_miles"]``. Driven by the resolution slider.
    alpha_index = traitlets.Int(4).tag(sync=True)
    # UI toggles surfaced as buttons in the widget control panel.
    show_neighborhoods = traitlets.Bool(True).tag(sync=True)
    show_stations = traitlets.Bool(True).tag(sync=True)
