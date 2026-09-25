from fastapi.testclient import TestClient


def test_health(api_main):
    client = TestClient(api_main.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_ready(api_main):
    client = TestClient(api_main.app)
    r = client.get("/ready")
    assert r.status_code == 200


def test_scenario_endpoint(api_main):
    client = TestClient(api_main.app)
    r = client.post("/api/v1/demo/scenarios/rapid_withdrawal")
    assert r.status_code == 200
    body = r.json()
    assert body["events_injected"] >= 2
    assert "expected" in body


def test_pipeline_status(api_main):
    client = TestClient(api_main.app)
    r = client.get("/api/v1/pipeline/status")
    assert r.status_code == 200
    body = r.json()
    names = [s["name"] for s in body["stages"]]
    assert names == ["DATABASE", "CDC", "STREAM", "ENRICH", "ANALYZE", "RISK", "ALERT"]
    assert isinstance(body.get("components"), list)
    assert len(body["components"]) >= 8
    assert isinstance(body.get("edges"), list)
    ids = {c["id"] for c in body["components"]}
    assert {"db", "kafka", "ml", "risk", "alerts", "api"}.issubset(ids)
    for c in body["components"]:
        assert c["status"] in ("healthy", "down")


def test_metrics_summary_observability(api_main):
    client = TestClient(api_main.app)
    r = client.get("/api/v1/metrics/summary")
    assert r.status_code == 200
    body = r.json()
    assert "high_risk_players" in body
    assert "rule_hit_frequency" in body
    assert "risk_histogram" in body
    assert "alert_volume" in body
    assert "platform" in body
    assert "ai_latency_ms" in body
    assert isinstance(body["pipeline_stages"], list)
    assert len(body["alert_volume"]) == 12
