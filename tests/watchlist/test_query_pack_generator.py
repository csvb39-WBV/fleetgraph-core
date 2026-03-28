from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.query_pack_generator import generate_company_query_pack
from fleetgraph.watchlist.watchlist_loader import load_verified_subset


def test_query_pack_ordering_is_deterministic() -> None:
    company = load_verified_subset()[0]

    first = generate_company_query_pack(company)
    second = generate_company_query_pack(company)

    assert first == second
    assert [query_definition["query_id"] for query_definition in first] == [
        "watchlist_lawsuit_filed",
        "watchlist_audit_investigation",
        "watchlist_dispute_delay_default",
        "watchlist_subpoena_claim",
        "watchlist_category_dispute",
        "watchlist_domain_investigation",
    ]


def test_query_pack_company_first_shape() -> None:
    company = load_verified_subset()[0]
    query_pack = generate_company_query_pack(company)

    assert all(query_definition["intent_type"] == "event_based" for query_definition in query_pack)
    assert all("Turner Construction" in query_definition["query"] for query_definition in query_pack)
    assert all(query_definition["max_results"] > 0 for query_definition in query_pack)
