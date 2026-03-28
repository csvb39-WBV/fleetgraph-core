from __future__ import annotations

from pathlib import Path


_VERIFIED_SUBSET_FILENAME = "factledger_fleetgraph_icp_300_verified_subset.csv"
_SEED_ENRICHED_FILENAME = "factledger_fleetgraph_icp_300_seed_enriched.csv"
_ALLOWED_FILENAMES = {
    _VERIFIED_SUBSET_FILENAME,
    _SEED_ENRICHED_FILENAME,
}


def _default_search_roots() -> tuple[Path, ...]:
    project_root = Path(__file__).resolve().parents[3]
    return (
        project_root / "data" / "watchlist",
        Path.home() / "Downloads",
    )


def locate_watchlist_csv(filename: str, search_roots: tuple[str | Path, ...] | None = None) -> str:
    if filename not in _ALLOWED_FILENAMES:
        raise ValueError("invalid_watchlist_filename")
    roots = tuple(Path(root).resolve() for root in (search_roots or _default_search_roots()))
    for root in roots:
        candidate = root / filename
        if candidate.exists():
            return str(candidate)
    raise FileNotFoundError(filename)


def get_watchlist_input_paths(search_roots: tuple[str | Path, ...] | None = None) -> dict[str, str]:
    return {
        "verified_subset": locate_watchlist_csv(_VERIFIED_SUBSET_FILENAME, search_roots=search_roots),
        "seed_enriched": locate_watchlist_csv(_SEED_ENRICHED_FILENAME, search_roots=search_roots),
    }
