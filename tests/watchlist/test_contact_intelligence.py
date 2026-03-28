from __future__ import annotations

import pathlib
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


from fleetgraph.watchlist.contact_classifier import classify_email, classify_phone
from fleetgraph.watchlist.contact_extractor import (
    extract_address_lines,
    extract_classified_emails,
    extract_contact_pages,
    extract_direct_phones,
    extract_leadership_pages,
)


def test_email_classification_and_phone_classification_are_correct() -> None:
    assert classify_email("jane.doe@example.com") == {
        "type": "direct_email",
        "confidence": "high",
        "is_direct": True,
        "domain_match": True,
    }
    assert classify_email("info@example.com") == {
        "type": "general_email",
        "confidence": "high",
        "is_direct": False,
        "domain_match": True,
    }
    assert classify_phone("(212) 555-0100") == {
        "type": "phone",
        "confidence": "medium",
        "is_direct": True,
        "domain_match": False,
    }


def test_contact_extraction_detects_emails_phones_pages_and_addresses() -> None:
    search_results = [
        {
            "title": "Reach Turner Construction at (212) 555-0100",
            "snippet": "Contact jane.doe@turnerconstruction.com or info@turnerconstruction.com at 120 West 45th Street New York, NY 10036.",
            "url": "https://example.com/contact/turner",
            "source_provider": "rss_news",
        },
        {
            "title": "Leadership page",
            "snippet": "Executive team listed here.",
            "url": "https://example.com/leadership/turner-team",
            "source_provider": "duckduckgo_html",
        },
    ]

    emails = extract_classified_emails(search_results, website_domain="turnerconstruction.com")
    phones = extract_direct_phones(search_results)
    contact_pages = extract_contact_pages(search_results)
    leadership_pages = extract_leadership_pages(search_results)
    addresses = extract_address_lines(search_results)

    assert emails == {
        "published_emails": [
            {
                "email": "jane.doe@turnerconstruction.com",
                "source_url": "https://example.com/contact/turner",
                "confidence": "high",
                "type": "direct_email",
                "is_direct": True,
            }
        ],
        "general_emails": [
            {
                "email": "info@turnerconstruction.com",
                "source_url": "https://example.com/contact/turner",
                "confidence": "high",
                "type": "general_email",
                "is_direct": False,
            }
        ],
    }
    assert phones == [
        {
            "phone": "(212) 555-0100",
            "source_url": "https://example.com/contact/turner",
            "confidence": "medium",
            "type": "phone",
            "is_direct": True,
        }
    ]
    assert contact_pages == [
        "https://example.com/contact/turner",
        "https://example.com/leadership/turner-team",
    ]
    assert leadership_pages == [
        "https://example.com/leadership/turner-team",
    ]
    assert addresses == [
        "120 West 45th Street",
        "New York, NY 10036",
    ]


def test_contact_extraction_handles_invalid_inputs_without_fabrication() -> None:
    assert extract_classified_emails(None, website_domain=None) == {
        "published_emails": [],
        "general_emails": [],
    }
    assert extract_direct_phones(None) == []
    assert extract_contact_pages(None) == []
    assert extract_leadership_pages(None) == []
    assert extract_address_lines(None) == []
