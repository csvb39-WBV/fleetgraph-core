from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from fleetgraph_core.discovery.evidence_acquisition import (
    acquire_evidence,
    classify_url,
    extract_evidence_from_html,
)


def test_extracts_email_domains() -> None:
    html = "Reach us at security@alphafleet.com for support."
    signals = extract_evidence_from_html(html, "https://example.com/contact")
    assert any(s["type"] == "shared_email_domain" and s["value"] == "alphafleet.com" for s in signals)


def test_extracts_domains() -> None:
    html = "Our partner site is https://shared-services.net and docs.shared-services.net"
    signals = extract_evidence_from_html(html, "https://example.com/about", current_domain="example.com")
    domains = [s["value"] for s in signals if s["type"] == "mentioned_domain"]
    assert "shared-services.net" in domains


def test_extracts_phone() -> None:
    html = "Call +1 (555) 222-3344 for escalations."
    signals = extract_evidence_from_html(html, "https://example.com/contact")
    assert any(s["type"] == "shared_phone" and s["value"] == "15552223344" for s in signals)


def test_classifies_url_types() -> None:
    assert classify_url("https://a.com/contact") == "contact"
    assert classify_url("https://a.com/about") == "about"
    assert classify_url("https://a.com/privacy") == "legal"
    assert classify_url("https://a.com/terms") == "legal"
    assert classify_url("https://a.com/partners") == "partner"
    assert classify_url("https://a.com/") == "home"


def test_deduplicates_signals() -> None:
    html = "mail a@dup.com and b@dup.com"
    signals = extract_evidence_from_html(html, "https://dup.com/contact")
    email_domains = [s for s in signals if s["type"] == "shared_email_domain"]
    assert len(email_domains) == 1


def test_acquire_evidence_attaches_without_breaking_original() -> None:
    records = [
        {
            "signal_id": "SIG-001",
            "organization_name": "Alpha Fleet Services",
            "domain": "alphafleet.com",
            "source_url": "https://alphafleet.com/contact",
            "html": "Contact legal@alphafleet.com call +1 555-111-2222",
        }
    ]

    output = acquire_evidence(records)

    assert "evidence_signals" not in records[0]
    assert "evidence_signals" in output[0]
    assert any(s["type"] == "shared_email_domain" for s in output[0]["evidence_signals"])
