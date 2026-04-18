"""Higher-level helpers that wrap Celldega + the deck.gl flow widget.

These keep the notebooks short by hiding the two-pass clustering trick
(cluster once to read flat labels off the dendrogram, then re-cluster with
those labels in metadata so the dendrogram colorbar matches the map) and
the UMAP-on-the-map embedding.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bike_network_traffic.util import (
    DEFAULT_STATION_PALETTE_HEX,
    build_coord_df,
    hex_palette_to_rgb,
)
from bike_network_traffic.widget import BikeFlowMapWidget

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "flat_clusters_from_matrix",
    "make_station_clustergram",
    "make_flow_widget",
    "link_flow_to_clustergram",
]


def flat_clusters_from_matrix(mat: Any, n_clusters: int = 20) -> dict[str, int]:
    """Cut the dendrogram on a clustered ``celldega.clust.Matrix`` to get flat labels.

    Tries the column linkage first (matches the matrix's own column order in
    the Clustergram), falls back to the row linkage. Returns ``{}`` if no
    linkage was produced (e.g. matrix wasn't clustered yet).
    """
    import numpy as np
    from scipy.cluster.hierarchy import fcluster

    if mat is None or getattr(mat, "data", None) is None:
        return {}
    linkage_bundle = mat.viz.get("linkage") or {}
    for axis in ("col", "row"):
        Z = np.asarray(linkage_bundle.get(axis) or [], dtype=float)
        if Z.size == 0:
            continue
        try:
            labels = fcluster(Z, t=n_clusters, criterion="maxclust")
        except Exception:
            continue
        names = (
            mat.data.columns.astype(str).tolist()
            if axis == "col"
            else mat.data.index.astype(str).tolist()
        )
        if len(labels) != len(names):
            continue
        return {str(a): int(b) for a, b in zip(names, labels, strict=True)}
    return {}


def _station_palette_hex() -> list[str]:
    """Okabe-Ito + Celldega's default categorical palette, joined."""
    try:
        from celldega.clust.constants import _COLOR_PALETTE  # type: ignore[attr-defined]
    except Exception:
        _COLOR_PALETTE = ()
    okabe_ito = (
        "#E69F00",
        "#56B4E9",
        "#009E73",
        "#F0E442",
        "#0072B2",
        "#D55E00",
        "#CC79A7",
    )
    return list(okabe_ito) + list(_COLOR_PALETTE)


def make_station_clustergram(
    transition_prob: "pd.DataFrame",
    n_clusters: int = 30,
    *,
    palette: list[str] | None = None,
) -> tuple[Any, Any, dict[str, int]]:
    """Two-pass clustering -> (matrix, Clustergram, cluster_map).

    Parameters
    ----------
    transition_prob
        Destination-probability DataFrame (rows=destinations, cols=origins).
    n_clusters
        Number of flat clusters to cut the column dendrogram into.
    palette
        Hex colors used to color the ``station_cluster`` metadata band; defaults
        to Okabe-Ito + the Celldega categorical palette.

    Returns
    -------
    (mat, cgm, cluster_map)
        ``mat`` is a clustered ``celldega.clust.Matrix`` with ``station_cluster``
        in row+col metadata, ``cgm`` is the corresponding ``Clustergram``, and
        ``cluster_map`` maps station name -> cluster id (``int``).
    """
    import celldega as dega
    import pandas as pd

    palette = palette if palette is not None else _station_palette_hex()

    probe = dega.clust.Matrix(transition_prob)
    probe.cluster(force=True)
    cluster_map = flat_clusters_from_matrix(probe, n_clusters=n_clusters)

    def _label(name: str) -> str:
        return str(int(cluster_map.get(str(name), 0)))

    meta_row = pd.DataFrame(
        {"station_cluster": [_label(i) for i in transition_prob.index]},
        index=transition_prob.index,
    )
    meta_col = pd.DataFrame(
        {"station_cluster": [_label(i) for i in transition_prob.columns]},
        index=transition_prob.columns,
    )

    mat = dega.clust.Matrix(
        transition_prob,
        meta_row=meta_row,
        meta_col=meta_col,
        row_attr=["station_cluster"],
        col_attr=["station_cluster"],
    )
    mat.cluster(force=True)

    max_c = int(max(cluster_map.values(), default=0))
    cat_colors: dict[str, str] = {"0": "#b0b4be"}
    for cid in range(1, max_c + 1):
        cat_colors[str(cid)] = palette[(cid - 1) % len(palette)]
    mat.set_global_cat_colors(cat_colors)
    mat.make_viz()

    cgm = dega.viz.Clustergram(matrix=mat)
    return mat, cgm, cluster_map


def make_flow_widget(
    stations: "pd.DataFrame",
    transition_prob: "pd.DataFrame",
    cluster_map: dict[str, int] | None = None,
    *,
    width: int = 560,
    height: int = 700,
    palette: list[str] | None = None,
    debug: bool = False,
    umap_neighbors: int = 8,
    umap_min_dist: float = 0.3,
) -> BikeFlowMapWidget:
    """Build a :class:`BikeFlowMapWidget` populated from a stations + matrix pair.

    Computes a UMAP embedding from the transition matrix and rescales it onto
    the city's lat/lng bounding box so the map can morph between geographic
    and "embedding" layouts via the ``spatial_mix`` slider.
    """
    import numpy as np
    import pandas as pd

    palette = palette if palette is not None else _station_palette_hex()
    cluster_map = cluster_map or {}

    coord_df = build_coord_df(stations)
    coord_df["cluster_id"] = (
        coord_df["station_name"].astype(str).map(cluster_map).fillna(0).astype(int)
    )

    try:
        import scanpy as sc
        from anndata import AnnData

        adata = AnnData(X=transition_prob.values.astype(np.float32))
        adata.obs_names = pd.Index(transition_prob.index.astype(str))
        adata.var_names = pd.Index(transition_prob.columns.astype(str))
        sc.pp.neighbors(adata, n_neighbors=umap_neighbors, use_rep="X")
        sc.tl.umap(adata, min_dist=umap_min_dist)
        umap_raw = adata.obsm["X_umap"]
        u_min, u_max = umap_raw.min(axis=0), umap_raw.max(axis=0)
        u_norm = (umap_raw - u_min) / (u_max - u_min + 1e-10)
        lat_lo, lat_hi = coord_df["lat"].min(), coord_df["lat"].max()
        lng_lo, lng_hi = coord_df["lng"].min(), coord_df["lng"].max()
        pad_lat, pad_lng = (lat_hi - lat_lo) * 0.05, (lng_hi - lng_lo) * 0.05
        umap_lng = (lng_lo + pad_lng) + u_norm[:, 0] * (lng_hi - lng_lo - 2 * pad_lng)
        umap_lat = (lat_lo + pad_lat) + u_norm[:, 1] * (lat_hi - lat_lo - 2 * pad_lat)
        umap_lookup = {
            str(name): (float(umap_lng[i]), float(umap_lat[i]))
            for i, name in enumerate(adata.obs_names)
        }
    except Exception:
        # scanpy/anndata are optional; fall back to the geographic position so
        # the widget still works without them installed.
        umap_lookup = {}

    flow = BikeFlowMapWidget(width=width, height=height, debug=debug)
    flow.palette_rgb = hex_palette_to_rgb(palette or list(DEFAULT_STATION_PALETTE_HEX))
    flow.stations = [
        {
            "name": str(r.station_name),
            "lat": float(r.lat),
            "lng": float(r.lng),
            "station_id": str(r.station_id),
            "cluster_id": int(r.cluster_id),
            "umap_lng": umap_lookup.get(str(r.station_name), (float(r.lng), float(r.lat)))[0],
            "umap_lat": umap_lookup.get(str(r.station_name), (float(r.lng), float(r.lat)))[1],
        }
        for r in coord_df.itertuples()
    ]
    flow.edge_index = {}
    return flow


def link_flow_to_clustergram(flow: BikeFlowMapWidget, cgm: Any) -> None:
    """Wire ``jsdlink``s between the flow map and a Celldega ``Clustergram``.

    Frontend-only links so the pair survives in static HTML (no kernel needed).
    """
    from ipywidgets import jsdlink

    jsdlink((cgm, "click_info"), (flow, "click_info"))
    jsdlink((cgm, "selected_rows"), (flow, "selected_rows"))
    jsdlink((cgm, "selected_cols"), (flow, "selected_cols"))
    jsdlink((cgm, "matrix_slice_result"), (flow, "matrix_axis_slice"))
    jsdlink((flow, "matrix_slice_request_out"), (cgm, "matrix_slice_request"))
    jsdlink((cgm, "row_names"), (flow, "cg_row_names"))
    jsdlink((cgm, "col_names"), (flow, "cg_col_names"))
