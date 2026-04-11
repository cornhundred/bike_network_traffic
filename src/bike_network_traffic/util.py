from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# Matches `STATION_PALETTE_HEX` in `js/bike_flow_map_widget.mjs` (Okabe-Ito + Celldega-style tail).
DEFAULT_STATION_PALETTE_HEX: tuple[str, ...] = (
    "#E69F00",
    "#56B4E9",
    "#009E73",
    "#F0E442",
    "#0072B2",
    "#D55E00",
    "#CC79A7",
    "#393b79",
    "#aec7e8",
    "#ff7f0e",
    "#ffbb78",
    "#98df8a",
    "#bcbd22",
    "#404040",
    "#ff9896",
    "#c5b0d5",
    "#8c5648",
    "#1f77b4",
    "#5254a3",
    "#FFDB58",
    "#c49c94",
    "#e377c2",
    "#7f7f7f",
    "#2ca02c",
    "#9467bd",
    "#dbdb8d",
    "#17becf",
    "#637939",
    "#6b6ecf",
    "#9c9ede",
    "#d62728",
    "#8ca252",
    "#8c6d31",
    "#bd9e39",
    "#e7cb94",
    "#843c39",
    "#ad494a",
    "#d6616b",
    "#7b4173",
    "#a55194",
    "#ce6dbd",
    "#de9ed6",
)


def hex_palette_to_rgb(colors: list[str] | tuple[str, ...]) -> list[list[int]]:
    """`#RRGGBB` strings to ``[[r, g, b], ...]`` for the widget's ``palette_rgb`` trait."""
    out: list[list[int]] = []
    for h in colors:
        s = str(h).strip()
        if len(s) == 7 and s.startswith("#"):
            out.append([int(s[i : i + 2], 16) for i in (1, 3, 5)])
    return out


def build_coord_df(stations: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate ``station_name`` rows (mean lat/lng, join ids) like the example notebooks."""
    import pandas as pd

    return stations.groupby("station_name", as_index=False).agg(
        lat=("lat", "mean"),
        lng=("lng", "mean"),
        station_id=(
            "station_id",
            lambda s: ", ".join(sorted({str(x) for x in s if pd.notna(x)})),
        ),
    )
