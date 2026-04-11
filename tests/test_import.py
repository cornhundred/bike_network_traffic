from pathlib import Path

from bike_network_traffic import BikeFlowMapWidget
from bike_network_traffic.widget import _BUNDLE


def test_bundle_on_disk() -> None:
    assert _BUNDLE.is_file()
    assert _BUNDLE.stat().st_size > 10_000


def test_widget_instantiates() -> None:
    w = BikeFlowMapWidget()
    assert isinstance(w._esm, (str, Path))
