"""Per-cluster alpha-shape neighborhoods for the bike flow map widget.

Computes alpha shapes for each station cluster at multiple distance
resolutions (in **miles**) using ``celldega.nbhd.alpha_shape``, and packages
the resulting polygons into a compact, JSON-friendly format that the
``BikeFlowMapWidget`` can render as a layer beneath the stations.

The format pairs each polygon vertex with its UMAP counterpart so the
frontend can morph neighborhoods alongside the stations when the
Spatial<->UMAP slider moves, without recomputing alpha shapes in UMAP
space.

Wire format (one dict, sent as a single anywidget traitlet)::

    {
      "levels_miles": [r0, r1, ..., r9],   # 10 floats, log-spaced
      "polygons": [
        {
          "cluster_id": int,                # matches stations[*].cluster_id
          "by_level": [                     # one entry per level
            [                               # polygons for this level
              {
                "geo":  [outer_ring, hole1, ...],   # each ring: [[lng,lat], ...]
                "umap": [outer_ring, hole1, ...],   # parallel coords in UMAP space
              },
              ...
            ],
            ...
          ]
        },
        ...
      ]
    }

A "level" is the *inverse alpha radius* in miles; smaller = tighter / more
detailed concave hull, larger = closer to the convex hull. Set with
``BikeFlowMapWidget.alpha_index`` (0..len(levels_miles)-1).
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "DEFAULT_LEVELS_MILES",
    "compute_cluster_alpha_shapes",
]

# Log-spaced from 0.05 mi (~80m, block-scale) to 5 mi (cross-borough).
# 10 levels — coarse-grained enough that the slider feels responsive.
DEFAULT_LEVELS_MILES: tuple[float, ...] = (
    0.05, 0.084, 0.140, 0.236, 0.395, 0.663, 1.114, 1.870, 3.140, 5.0,
)

_MILES_TO_METERS = 1609.344


def _local_meters_projector(
    center_lng: float, center_lat: float
) -> tuple[callable, float, float]:
    """Return ``(forward, mx_per_deg_lng, my_per_deg_lat)`` for a local
    equirectangular projection centered at ``(center_lng, center_lat)``.

    Accurate to ~0.5% over a city-scale extent (~30km), which is plenty for
    visualization-grade alpha shapes. ``forward(lng, lat) -> (x_m, y_m)``.
    """
    R = 6_371_000.0  # earth mean radius, meters
    cos_lat = math.cos(math.radians(center_lat))
    mx_per_deg_lng = math.radians(1.0) * R * cos_lat
    my_per_deg_lat = math.radians(1.0) * R

    def forward(lng: float, lat: float) -> tuple[float, float]:
        return ((lng - center_lng) * mx_per_deg_lng, (lat - center_lat) * my_per_deg_lat)

    return forward, mx_per_deg_lng, my_per_deg_lat


def _ring_to_compact(ring, lookup, precision: int) -> tuple[list, list]:
    """Map projected-meter ring vertices back to (geo, umap) coord arrays."""
    geo, umap = [], []
    for x, y in ring:
        key = (round(float(x), 2), round(float(y), 2))
        info = lookup.get(key)
        if info is None:
            # Vertex didn't come from an input station (shouldn't happen with
            # libpysal alpha shapes, but be defensive). Fall back to the
            # inverse projection of the meter coord and reuse it for UMAP.
            geo.append([round(float(x), precision), round(float(y), precision)])
            umap.append(geo[-1])
        else:
            lng, lat, ulng, ulat = info
            geo.append([round(lng, precision), round(lat, precision)])
            umap.append([round(ulng, precision), round(ulat, precision)])
    return geo, umap


def compute_cluster_alpha_shapes(
    stations: "pd.DataFrame",
    cluster_map: dict[str, int],
    *,
    levels_miles: tuple[float, ...] | None = None,
    umap_lookup: dict[str, tuple[float, float]] | None = None,
    min_stations: int = 4,
    coord_precision: int = 5,
) -> dict:
    """Pre-compute alpha-shape neighborhoods for each station cluster.

    Parameters
    ----------
    stations
        DataFrame with at least ``station_name``, ``lat``, ``lng`` columns
        (the canonical schema returned by ``build_stations``).
    cluster_map
        ``{station_name: cluster_id}``. Stations with cluster_id 0 (the
        "unclustered" sentinel) are skipped.
    levels_miles
        Inverse-alpha radii in miles. Defaults to 10 log-spaced levels from
        0.05 mi to 5 mi. The frontend slider exposes one level at a time.
    umap_lookup
        ``{station_name: (umap_lng, umap_lat)}`` for morphing polygons into
        the UMAP layout. If missing for a station, that station's geographic
        coord is reused (so morph collapses to identity for it).
    min_stations
        Skip clusters with fewer points than this (alpha_shape needs >=4).
    coord_precision
        Decimal places to round lat/lng to. 5 = ~1m precision, plenty for
        visualization and gives ~3x JSON shrink vs full float64.

    Returns
    -------
    dict
        Compact wire format described in the module docstring. Empty
        ``"polygons"`` list if ``celldega.nbhd.alpha_shape`` is unavailable
        or no cluster has enough points.
    """
    import numpy as np

    try:
        from celldega.nbhd import alpha_shape as _alpha_shape
    except Exception:
        return {"levels_miles": list(levels_miles or DEFAULT_LEVELS_MILES), "polygons": []}

    levels = tuple(float(r) for r in (levels_miles or DEFAULT_LEVELS_MILES))
    umap_lookup = umap_lookup or {}

    # Stations frame -> per-cluster lng/lat arrays + name index.
    df = stations[["station_name", "lat", "lng"]].dropna()
    df = df.assign(
        cluster_id=df["station_name"].astype(str).map(cluster_map).fillna(0).astype(int)
    )
    df = df[df["cluster_id"] > 0]
    if df.empty:
        return {"levels_miles": list(levels), "polygons": []}

    center_lng = float(df["lng"].mean())
    center_lat = float(df["lat"].mean())
    project, _, _ = _local_meters_projector(center_lng, center_lat)

    # Precompute meter coords once and a lookup from rounded meter coord
    # back to (lng, lat, umap_lng, umap_lat). libpysal's alpha shapes return
    # vertices that are exactly the input points, so this lookup is what
    # lets us morph polygon vertices into UMAP space without recomputing
    # alpha shapes there.
    df = df.assign(
        x_m=[project(lng, lat)[0] for lng, lat in zip(df["lng"], df["lat"])],
        y_m=[project(lng, lat)[1] for lng, lat in zip(df["lng"], df["lat"])],
    )

    coord_lookup: dict[tuple[float, float], tuple[float, float, float, float]] = {}
    for r in df.itertuples():
        key = (round(float(r.x_m), 2), round(float(r.y_m), 2))
        ulng, ulat = umap_lookup.get(str(r.station_name), (float(r.lng), float(r.lat)))
        coord_lookup[key] = (float(r.lng), float(r.lat), float(ulng), float(ulat))

    out_polys: list[dict] = []
    for cid, sub in df.groupby("cluster_id"):
        if len(sub) < min_stations:
            continue
        pts = sub[["x_m", "y_m"]].to_numpy(dtype=np.float64)
        by_level: list[list[dict]] = []
        for r_miles in levels:
            inv_alpha_m = r_miles * _MILES_TO_METERS
            try:
                multi = _alpha_shape(pts, inv_alpha_m)
            except Exception:
                by_level.append([])
                continue
            polys: list[dict] = []
            # MultiPolygon -> iterate Polygon parts -> exterior + interiors
            geoms = list(getattr(multi, "geoms", [])) or ([multi] if not multi.is_empty else [])
            for poly in geoms:
                if poly.is_empty:
                    continue
                rings = [list(poly.exterior.coords)]
                rings += [list(h.coords) for h in poly.interiors]
                geo_rings, umap_rings = [], []
                for ring in rings:
                    g, u = _ring_to_compact(ring, coord_lookup, coord_precision)
                    if len(g) >= 3:
                        geo_rings.append(g)
                        umap_rings.append(u)
                if geo_rings:
                    polys.append({"geo": geo_rings, "umap": umap_rings})
            by_level.append(polys)
        if any(by_level):
            out_polys.append({"cluster_id": int(cid), "by_level": by_level})

    return {"levels_miles": list(levels), "polygons": out_polys}
