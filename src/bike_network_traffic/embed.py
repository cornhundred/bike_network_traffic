from __future__ import annotations

from pathlib import Path
from typing import Any

from ipywidgets.embed import embed_minimal_html


def save_minimal_html(
    *views: Any,
    path: str | Path,
    **kwargs: Any,
) -> None:
    """Write HTML with embedded widget state (ipywidgets ``embed_minimal_html``).

    Map raster tiles expect a normal HTTP origin; open the file via
    ``python -m http.server`` (or similar), not ``file://``.
    """
    embed_minimal_html(Path(path), views=views, **kwargs)
