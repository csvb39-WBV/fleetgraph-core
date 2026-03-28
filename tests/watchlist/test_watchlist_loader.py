from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.watchlist_loader import load_seed_enriched, load_verified_subset


def test_verified_subset_loads_successfully() -> None:
    records = load_verified_subset()

    assert len(records) == 27
    assert records[0]["company_name"] == "Turner Construction"
    assert records[0]["priority_tier"] == "1"
    assert records[0]["website_domain"] == "turnerconstruction.com"


def test_seed_enriched_loads_successfully() -> None:
    records = load_seed_enriched()

    assert len(records) == 300
    assert records[0]["company_name"] == "Turner Construction"
    assert records[1]["company_name"] == "Bechtel"


def test_watchlist_loader_is_deterministic() -> None:
    first = load_verified_subset()
    second = load_verified_subset()

    assert first == second


def test_watchlist_loader_normalizes_blank_optional_fields() -> None:
    records = load_verified_subset()

    assert records[0]["chief_risk_officer_name"] is None
    assert isinstance(records[0]["sources"], list)
    assert len(records[0]["sources"]) >= 1
