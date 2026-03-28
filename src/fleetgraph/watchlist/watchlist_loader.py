from __future__ import annotations

import csv
import re
from pathlib import Path
from urllib import parse

from fleetgraph.watchlist.canonical_inputs import get_watchlist_input_paths


_REQUIRED_FIELDNAMES = (
    "company_name",
    "category",
    "segment",
    "priority_tier",
    "website",
    "hq_city",
    "hq_state",
    "hq_zip",
    "phone",
    "ceo_name",
    "cfo_name",
    "chief_risk_officer_name",
    "sources",
    "verification_status",
    "notes",
)


def _normalize_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized_value = value.strip()
    return normalized_value or None


def _normalize_sources(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    normalized_sources: list[str] = []
    for raw_source in value.split(";"):
        normalized_source = raw_source.strip()
        if normalized_source != "" and normalized_source not in normalized_sources:
            normalized_sources.append(normalized_source)
    return normalized_sources


def _website_domain(website: str | None) -> str | None:
    if website is None:
        return None
    parsed_url = parse.urlparse(website)
    domain = parsed_url.netloc.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def _stable_company_id(company_name: str, hq_state: str | None) -> str:
    slug_base = re.sub(r"[^a-z0-9]+", "-", company_name.lower()).strip("-")
    slug_state = re.sub(r"[^a-z0-9]+", "-", (hq_state or "na").lower()).strip("-") or "na"
    return f"{slug_base}--{slug_state}"


def _validate_headers(fieldnames: list[str] | None) -> None:
    if fieldnames is None or tuple(fieldnames) != _REQUIRED_FIELDNAMES:
        raise ValueError("invalid_watchlist_headers")


def _normalize_row(row: dict[str, str], row_index: int) -> dict[str, object]:
    company_name = _normalize_optional_text(row.get("company_name"))
    if company_name is None:
        raise ValueError(f"invalid_watchlist_row:{row_index}")
    hq_state = _normalize_optional_text(row.get("hq_state"))
    website = _normalize_optional_text(row.get("website"))
    normalized_row = {
        "company_id": _stable_company_id(company_name, hq_state),
        "company_name": company_name,
        "category": _normalize_optional_text(row.get("category")),
        "segment": _normalize_optional_text(row.get("segment")),
        "priority_tier": _normalize_optional_text(row.get("priority_tier")),
        "website": website,
        "website_domain": _website_domain(website),
        "hq_city": _normalize_optional_text(row.get("hq_city")),
        "hq_state": hq_state,
        "hq_zip": _normalize_optional_text(row.get("hq_zip")),
        "main_phone": _normalize_optional_text(row.get("phone")),
        "ceo_name": _normalize_optional_text(row.get("ceo_name")),
        "cfo_name": _normalize_optional_text(row.get("cfo_name")),
        "chief_risk_officer_name": _normalize_optional_text(row.get("chief_risk_officer_name")),
        "sources": _normalize_sources(row.get("sources")),
        "verification_status": _normalize_optional_text(row.get("verification_status")),
        "notes": _normalize_optional_text(row.get("notes")),
        "original_row": {field_name: row.get(field_name, "") for field_name in _REQUIRED_FIELDNAMES},
    }
    return normalized_row


def load_watchlist_csv(csv_path: str | Path) -> list[dict[str, object]]:
    resolved_path = Path(csv_path).resolve()
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        _validate_headers(reader.fieldnames)
        normalized_rows = [_normalize_row(row, row_index) for row_index, row in enumerate(reader, start=1)]
    return normalized_rows


def load_verified_subset(search_roots: tuple[str | Path, ...] | None = None) -> list[dict[str, object]]:
    input_paths = get_watchlist_input_paths(search_roots=search_roots)
    return load_watchlist_csv(input_paths["verified_subset"])


def load_seed_enriched(search_roots: tuple[str | Path, ...] | None = None) -> list[dict[str, object]]:
    input_paths = get_watchlist_input_paths(search_roots=search_roots)
    return load_watchlist_csv(input_paths["seed_enriched"])
