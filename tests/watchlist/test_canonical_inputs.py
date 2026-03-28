from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.canonical_inputs import get_watchlist_input_paths, locate_watchlist_csv


def test_canonical_input_locator_finds_downloaded_files() -> None:
    paths = get_watchlist_input_paths()

    assert paths["verified_subset"].endswith("factledger_fleetgraph_icp_300_verified_subset.csv")
    assert paths["seed_enriched"].endswith("factledger_fleetgraph_icp_300_seed_enriched.csv")


def test_canonical_input_locator_is_deterministic() -> None:
    first = locate_watchlist_csv("factledger_fleetgraph_icp_300_verified_subset.csv")
    second = locate_watchlist_csv("factledger_fleetgraph_icp_300_verified_subset.csv")

    assert first == second
