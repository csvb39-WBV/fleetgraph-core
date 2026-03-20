import json

from fastapi.testclient import TestClient

from src.fleetgraph_core.api.relationship_signal_service import create_app, get_output_path


def _make_record(
    output_record_id: str = "formattedsignal:relationshipsignal:example.com",
    signal_id: str = "relationshipsignal:example.com",
    signal_type: str = "shared_domain_relationship_detected",
    domain: str = "example.com",
    domain_classification: str = "corporate",
    organization_count: int = 2,
    link_count: int = 1,
    organization_node_ids=None,
    organization_node_pairs=None,
    supporting_unified_organization_ids=None,
    supporting_source_ids=None,
):
    if organization_node_ids is None:
        organization_node_ids = ["node-1", "node-2"]
    if organization_node_pairs is None:
        organization_node_pairs = ["node-1|node-2"]
    if supporting_unified_organization_ids is None:
        supporting_unified_organization_ids = ["u1", "u2"]
    if supporting_source_ids is None:
        supporting_source_ids = ["s1", "s2"]

    return {
        "output_record_id": output_record_id,
        "output_schema_version": "1.0",
        "signal_id": signal_id,
        "signal_type": signal_type,
        "domain": domain,
        "domain_classification": domain_classification,
        "organization_count": organization_count,
        "link_count": link_count,
        "organization_node_ids": organization_node_ids,
        "organization_node_pairs": organization_node_pairs,
        "supporting_unified_organization_ids": supporting_unified_organization_ids,
        "supporting_source_ids": supporting_source_ids,
        "candidate_state": "relationship_signal_formatted",
    }


def _make_payload(records=None):
    if records is None:
        records = [_make_record()]

    return {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": len(records),
        "records": records,
    }


def test_get_output_path_uses_default_and_env_override(monkeypatch) -> None:
    monkeypatch.delenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", raising=False)
    assert get_output_path() == "relationship_signals_output.json"

    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", "custom.json")
    assert get_output_path() == "custom.json"


def test_create_app_returns_fastapi_app() -> None:
    app = create_app()
    assert app is not None


def test_get_health_returns_exact_payload() -> None:
    client = TestClient(create_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_relationship_signals_output_returns_full_payload(monkeypatch, tmp_path) -> None:
    payload = _make_payload(
        [
            _make_record(signal_id="relationshipsignal:alpha.com", domain="alpha.com"),
            _make_record(signal_id="relationshipsignal:beta.com", domain="beta.com"),
        ]
    )
    output_path = tmp_path / "relationship_signals_output.json"
    output_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", str(output_path))
    client = TestClient(create_app())

    response = client.get("/relationship-signals/output")

    assert response.status_code == 200
    assert response.json() == payload


def test_get_relationship_signals_records_preserves_order(monkeypatch, tmp_path) -> None:
    payload = _make_payload(
        [
            _make_record(signal_id="relationshipsignal:alpha.com", domain="alpha.com"),
            _make_record(signal_id="relationshipsignal:beta.com", domain="beta.com"),
        ]
    )
    output_path = tmp_path / "relationship_signals_output.json"
    output_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", str(output_path))
    client = TestClient(create_app())

    response = client.get("/relationship-signals/records")

    assert response.status_code == 200
    assert response.json() == payload["records"]


def test_get_relationship_signals_summary_returns_exact_summary(monkeypatch, tmp_path) -> None:
    payload = _make_payload([_make_record()])
    output_path = tmp_path / "relationship_signals_output.json"
    output_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", str(output_path))
    client = TestClient(create_app())

    response = client.get("/relationship-signals/summary")

    assert response.status_code == 200
    assert response.json() == {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": 1,
    }


def test_missing_file_returns_stable_http_500(monkeypatch, tmp_path) -> None:
    missing_path = tmp_path / "missing.json"
    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", str(missing_path))
    client = TestClient(create_app())

    response = client.get("/relationship-signals/output")

    assert response.status_code == 500
    assert response.json() == {"detail": "relationship signal output unavailable"}


def test_invalid_payload_returns_stable_http_500(monkeypatch, tmp_path) -> None:
    invalid_payload = {
        "output_type": "relationship_signal_output",
        "output_schema_version": "1.0",
        "record_count": 1,
        "records": [],
    }
    output_path = tmp_path / "relationship_signals_output.json"
    output_path.write_text(json.dumps(invalid_payload), encoding="utf-8")
    monkeypatch.setenv("FLEETGRAPH_RELATIONSHIP_SIGNAL_OUTPUT_PATH", str(output_path))
    client = TestClient(create_app())

    response = client.get("/relationship-signals/output")

    assert response.status_code == 500
    assert response.json() == {"detail": "relationship signal output unavailable"}


def test_no_unauthorized_routes() -> None:
    client = TestClient(create_app())

    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404
    assert client.get("/unauthorized").status_code == 404
    assert client.post("/relationship-signals/output").status_code == 405