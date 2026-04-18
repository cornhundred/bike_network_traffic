"""Targeted warning filters for the noisy third-party deps celldega pulls in.

We only silence specific, well-understood warnings (matched by message + module)
rather than blanket-ignoring categories — that way unrelated warnings still
surface.

Call :func:`silence_warnings` *before* importing celldega for the import-time
warnings (dask, xarray_schema) to be suppressed:

>>> from bike_network_traffic import silence_warnings
>>> silence_warnings()
>>> import celldega as dega
"""

from __future__ import annotations

import warnings

__all__ = ["silence_warnings"]


def silence_warnings(*, also_celldega: bool = True) -> None:
    """Install warning filters for the known-noisy deps. Safe to call repeatedly.

    Parameters
    ----------
    also_celldega
        Also silence celldega's "Large matrix may cause memory issues" warning,
        which spams during clustering of the full city transition matrices.
    """
    # dask: legacy DataFrame -> query-planning deprecation (fires on first import).
    warnings.filterwarnings(
        "ignore",
        message=".*legacy Dask DataFrame implementation is deprecated.*",
        category=FutureWarning,
        module=r"dask\.dataframe.*",
    )

    # xarray_schema: pulls in `pkg_resources`, which itself emits a
    # deprecation warning on import.
    warnings.filterwarnings(
        "ignore",
        message=".*pkg_resources is deprecated as an API.*",
        category=UserWarning,
        module=r"xarray_schema.*",
    )

    # anndata: deprecation notice for read_text relocation.
    warnings.filterwarnings(
        "ignore",
        message=".*Importing read_text from `anndata` is deprecated.*",
        category=FutureWarning,
        module=r"anndata.*",
    )

    if also_celldega:
        # celldega: large-matrix advisory, fires every clustering pass.
        warnings.filterwarnings(
            "ignore",
            message=r".*Large matrix .* may cause memory issues.*",
            category=UserWarning,
            module=r"celldega\.clust.*",
        )
