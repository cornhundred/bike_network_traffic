"""Fetch bike-share trip data from public S3 buckets and build Celldega-ready tables.

The four supported cities each publish monthly trip CSVs as zip archives in a public
S3 bucket. This module wraps that with a tiny API:

>>> from bike_network_traffic import get_bike_data
>>> ds = get_bike_data("nyc", year=2026, month=3)        # one month
>>> ds = get_bike_data("boston", year=2025, month=[6, 7, 8])  # three months
>>> stations, transition_prob = ds.stations, ds.transition_prob

``stations`` is a canonical station table (``station_id``, ``station_name``, ``lat``,
``lng``) and ``transition_prob`` is a destination-probability matrix where columns
are origin stations and rows are destination stations (each column sums to 1.0).
Both can be fed directly to ``celldega.clust.Matrix`` and the
``BikeFlowMapWidget``.
"""

from __future__ import annotations

import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "CITIES",
    "BikeData",
    "get_bike_data",
    "list_available_months",
    "build_stations",
    "build_transition_prob",
]

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# City registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CitySpec:
    key: str
    label: str
    bucket_url: str  # listing endpoint, must end with '/'
    monthly_patterns: tuple[str, ...]  # tried in order; first that exists wins
    aliases: tuple[str, ...] = ()


CITIES: dict[str, _CitySpec] = {
    "nyc": _CitySpec(
        key="nyc",
        label="Citi Bike (NYC)",
        bucket_url="https://s3.amazonaws.com/tripdata/",
        monthly_patterns=(
            "{year}{month:02d}-citibike-tripdata.csv.zip",
            "{year}{month:02d}-citibike-tripdata.zip",
        ),
        aliases=("citibike", "new_york", "newyork"),
    ),
    "boston": _CitySpec(
        key="boston",
        label="Bluebikes (Boston)",
        bucket_url="https://s3.amazonaws.com/hubway-data/",
        monthly_patterns=(
            "{year}{month:02d}-bluebikes-tripdata.csv.zip",
            "{year}{month:02d}-bluebikes-tripdata.zip",
            "{year}{month:02d}-hubway-tripdata.zip",
        ),
        aliases=("bluebikes", "hubway"),
    ),
    "dc": _CitySpec(
        key="dc",
        label="Capital Bikeshare (DC)",
        bucket_url="https://s3.amazonaws.com/capitalbikeshare-data/",
        monthly_patterns=(
            "{year}{month:02d}-capitalbikeshare-tripdata.zip",
        ),
        aliases=("washington", "capitalbikeshare", "capital_bikeshare"),
    ),
    "chicago": _CitySpec(
        key="chicago",
        label="Divvy (Chicago)",
        bucket_url="https://divvy-tripdata.s3.amazonaws.com/",
        monthly_patterns=(
            "{year}{month:02d}-divvy-tripdata.zip",
        ),
        aliases=("divvy",),
    ),
}


def _resolve_city(city: str) -> _CitySpec:
    key = city.strip().lower().replace("-", "_")
    if key in CITIES:
        return CITIES[key]
    for spec in CITIES.values():
        if key in spec.aliases:
            return spec
    raise ValueError(
        f"Unknown city {city!r}. Choose one of: {sorted(CITIES)} "
        f"(aliases: {sorted(a for s in CITIES.values() for a in s.aliases)})."
    )


# ---------------------------------------------------------------------------
# S3 listing + download
# ---------------------------------------------------------------------------

_KEY_RE = re.compile(r"<Key>([^<]+)</Key>")
_USER_AGENT = "bike_network_traffic/0.1 (+https://github.com/broadinstitute/celldega)"


def _http_get(url: str, timeout: float = 60.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _list_bucket_keys(bucket_url: str) -> list[str]:
    """List all keys in an S3 bucket via its public ``?list-type=2`` paginated endpoint."""
    keys: list[str] = []
    token: str | None = None
    while True:
        url = bucket_url + "?list-type=2"
        if token is not None:
            url += "&continuation-token=" + urllib.parse.quote(token)
        body = _http_get(url).decode("utf-8", errors="replace")
        keys.extend(_KEY_RE.findall(body))
        truncated = "<IsTruncated>true</IsTruncated>" in body
        if not truncated:
            break
        m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", body)
        if not m:
            break
        token = m.group(1)
    return keys


# Match e.g. '202603' anywhere in a key (year 2010-2099, month 01-12).
_YYYYMM_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)")


def list_available_months(city: str) -> list[tuple[int, int]]:
    """Return a sorted list of ``(year, month)`` tuples present in ``city``'s bucket."""
    spec = _resolve_city(city)
    keys = _list_bucket_keys(spec.bucket_url)
    found: set[tuple[int, int]] = set()
    for k in keys:
        # Skip Jersey City "JC-" satellite files in NYC bucket (separate system).
        base = k.split("/")[-1]
        if base.startswith("JC-") or base.startswith("__MACOSX"):
            continue
        m = _YYYYMM_RE.search(base)
        if m:
            found.add((int(m.group(1)), int(m.group(2))))
    return sorted(found)


def _default_cache_dir() -> Path:
    env = os.environ.get("BIKE_NETWORK_TRAFFIC_CACHE")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".cache" / "bike_network_traffic"


def _download_with_progress(url: str, dest: Path, *, progress: bool) -> None:
    """Stream ``url`` to ``dest.tmp`` then atomically rename. Skips if dest exists."""
    if dest.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        chunk = 1 << 20  # 1 MiB
        read = 0
        with open(tmp, "wb") as f:
            while True:
                buf = resp.read(chunk)
                if not buf:
                    break
                f.write(buf)
                read += len(buf)
                if progress and total:
                    pct = 100.0 * read / total
                    print(
                        f"  {dest.name}: {read / 1e6:6.1f} / {total / 1e6:6.1f} MB ({pct:5.1f}%)",
                        end="\r",
                        flush=True,
                    )
        if progress and total:
            print()
    tmp.replace(dest)


def _try_download_month(
    spec: _CitySpec, year: int, month: int, cache_dir: Path, *, progress: bool
) -> Path:
    """Try each filename pattern for (year, month); return path to the cached zip."""
    last_err: Exception | None = None
    for pattern in spec.monthly_patterns:
        key = pattern.format(year=year, month=month)
        url = spec.bucket_url + key
        dest = cache_dir / spec.key / key
        if dest.exists():
            return dest
        try:
            if progress:
                print(f"Downloading {url}")
            _download_with_progress(url, dest, progress=progress)
            return dest
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 404:
                continue
            raise
    raise FileNotFoundError(
        f"No archive found for {spec.label} {year}-{month:02d} "
        f"(tried {[p.format(year=year, month=month) for p in spec.monthly_patterns]}). "
        f"Use list_available_months({spec.key!r}) to see what's published."
    ) from last_err


def _extract_csvs(zip_path: Path) -> list[Path]:
    """Extract any CSVs in ``zip_path`` next to it (cached) and return their paths."""
    out_dir = zip_path.with_suffix("")  # drop trailing .zip
    if out_dir.suffix == ".csv":  # ".csv.zip" -> strip the .csv too so each archive owns a folder
        out_dir = out_dir.with_suffix("")
    out_dir = out_dir.parent / (out_dir.name + "_extracted")
    if not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                base = os.path.basename(name)
                # Skip MACOSX resource forks and directories.
                if not base or name.startswith("__MACOSX/") or base.startswith("._"):
                    continue
                if not name.lower().endswith(".csv"):
                    continue
                with zf.open(name) as src, open(out_dir / base, "wb") as dst:
                    dst.write(src.read())
    return sorted(out_dir.glob("*.csv"))


# ---------------------------------------------------------------------------
# Trip CSV -> normalized DataFrame
# ---------------------------------------------------------------------------

# Columns we always want, post-normalization.
_TRIP_COLUMNS = (
    "start_station_id",
    "start_station_name",
    "start_lat",
    "start_lng",
    "end_station_id",
    "end_station_name",
    "end_lat",
    "end_lng",
)

# Map legacy column names (pre-2020 NYC/Boston, pre-2018 DC) onto the modern schema.
_LEGACY_RENAME = {
    "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_lat",
    "start station longitude": "start_lng",
    "end station id": "end_station_id",
    "end station name": "end_station_name",
    "end station latitude": "end_lat",
    "end station longitude": "end_lng",
    # Some older Divvy files use these:
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
}


def _read_trip_csv(path: Path) -> "pd.DataFrame":
    import pandas as pd

    df = pd.read_csv(path, low_memory=False)
    df = df.rename(columns={c: _LEGACY_RENAME.get(c, c) for c in df.columns})
    missing = [c for c in _TRIP_COLUMNS if c not in df.columns]
    if missing:
        # Older Divvy/NYC files can be missing lat/lng entirely; in that case the
        # caller will need to provide a stations table separately. For the
        # modern API we keep this strict so callers see a useful error.
        raise ValueError(
            f"{path.name}: missing required columns {missing}. "
            f"Got: {list(df.columns)[:20]}"
        )
    return df[list(_TRIP_COLUMNS)]


# ---------------------------------------------------------------------------
# Public builders (also exported for users with their own DataFrames)
# ---------------------------------------------------------------------------


def build_stations(trips: "pd.DataFrame") -> "pd.DataFrame":
    """Canonical ``station_id, station_name, lat, lng`` table from start+end columns."""
    import pandas as pd

    starts = trips[["start_station_id", "start_station_name", "start_lat", "start_lng"]].rename(
        columns={
            "start_station_id": "station_id",
            "start_station_name": "station_name",
            "start_lat": "lat",
            "start_lng": "lng",
        }
    )
    ends = trips[["end_station_id", "end_station_name", "end_lat", "end_lng"]].rename(
        columns={
            "end_station_id": "station_id",
            "end_station_name": "station_name",
            "end_lat": "lat",
            "end_lng": "lng",
        }
    )
    return (
        pd.concat([starts, ends], ignore_index=True)
        .dropna(subset=["station_id", "station_name", "lat", "lng"])
        .groupby(["station_id", "station_name"], as_index=False)[["lat", "lng"]]
        .mean()
        .sort_values("station_name")
        .reset_index(drop=True)
    )


def build_transition_prob(trips: "pd.DataFrame") -> "pd.DataFrame":
    """Destination-probability matrix; columns are origin stations summing to 1.0.

    Rows are destination stations, columns are origin stations, and each column
    is normalized so it sums to 1. This matches the orientation that the
    notebooks feed into ``celldega.clust.Matrix``.
    """
    import pandas as pd

    valid = trips.dropna(
        subset=[
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
        ]
    )
    counts = pd.crosstab(
        valid["end_station_name"].astype(str),
        valid["start_station_name"].astype(str),
    )
    col_sums = counts.sum(axis=0).replace(0, pd.NA)
    return counts.div(col_sums, axis=1).fillna(0.0)


# ---------------------------------------------------------------------------
# Top-level API
# ---------------------------------------------------------------------------


@dataclass
class BikeData:
    """Container returned by :func:`get_bike_data`."""

    city: str
    year: int
    months: tuple[int, ...]
    stations: "pd.DataFrame"
    transition_prob: "pd.DataFrame"
    trips: "pd.DataFrame | None" = None  # populated when ``return_trips=True``

    def __iter__(self):  # so callers can do `stations, transition_prob = ds`
        yield self.stations
        yield self.transition_prob


def _coerce_months(month: int | Iterable[int] | None) -> tuple[int, ...]:
    if month is None:
        return tuple(range(1, 13))
    if isinstance(month, int):
        return (month,)
    if isinstance(month, Sequence) or hasattr(month, "__iter__"):
        out = tuple(int(m) for m in month)
        if not out:
            raise ValueError("month iterable was empty")
        return out
    raise TypeError(f"month must be int, iterable of int, or None; got {type(month).__name__}")


def get_bike_data(
    city: str,
    year: int,
    month: int | Iterable[int] | None = None,
    *,
    cache_dir: str | Path | None = None,
    progress: bool = False,
    return_trips: bool = False,
) -> BikeData:
    """Download trip archives and return a Celldega-ready :class:`BikeData`.

    Parameters
    ----------
    city
        One of ``'nyc'``, ``'boston'``, ``'dc'``, ``'chicago'`` (or common aliases
        like ``'citibike'``, ``'bluebikes'``, ``'capitalbikeshare'``, ``'divvy'``).
    year
        Four-digit year, e.g. ``2026``.
    month
        Single month (``int``), iterable of months (e.g. ``[6, 7, 8]``), or
        ``None`` to download every month of ``year`` that exists in the bucket.
    cache_dir
        Where to store downloaded zips and extracted CSVs. Defaults to
        ``$BIKE_NETWORK_TRAFFIC_CACHE`` or ``~/.cache/bike_network_traffic``.
    progress
        If ``True``, print one line per zip download plus a per-chunk progress
        indicator and a "Reading N CSV file(s)..." line. Off by default so
        notebooks stay quiet; turn it on for long first-time downloads.
    return_trips
        Also attach the raw concatenated trip DataFrame on the returned
        :class:`BikeData`. Off by default since it can be large.

    Returns
    -------
    :class:`BikeData`
        Fields: ``stations`` (DataFrame), ``transition_prob`` (DataFrame),
        plus ``city``, ``year``, ``months``, and optionally ``trips``.
        Iterable as ``stations, transition_prob = get_bike_data(...)``.
    """
    import pandas as pd

    spec = _resolve_city(city)
    months = _coerce_months(month)
    cache = Path(cache_dir).expanduser() if cache_dir else _default_cache_dir()
    cache.mkdir(parents=True, exist_ok=True)

    csv_paths: list[Path] = []
    missing: list[int] = []
    for m in months:
        try:
            zip_path = _try_download_month(spec, year, m, cache, progress=progress)
        except FileNotFoundError as e:
            if month is None:
                # Auto-mode: silently skip months that don't exist yet.
                log.info("skipping %s %d-%02d: %s", spec.key, year, m, e)
                missing.append(m)
                continue
            raise
        csv_paths.extend(_extract_csvs(zip_path))

    if not csv_paths:
        raise FileNotFoundError(
            f"No CSV files extracted for {spec.label} {year} months={months}; "
            f"missing={missing}. Bucket: {spec.bucket_url}"
        )

    if progress:
        print(f"Reading {len(csv_paths)} CSV file(s)...")
    trips = pd.concat((_read_trip_csv(p) for p in csv_paths), ignore_index=True)

    stations = build_stations(trips)
    transition_prob = build_transition_prob(trips)

    return BikeData(
        city=spec.key,
        year=year,
        months=tuple(m for m in months if m not in missing),
        stations=stations,
        transition_prob=transition_prob,
        trips=trips if return_trips else None,
    )
