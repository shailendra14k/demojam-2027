"""Unit tests for AML rules, features, risk aggregation, and API pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "aml-rules"))
sys.path.insert(0, str(ROOT / "services" / "feature-engine"))
sys.path.insert(0, str(ROOT / "services" / "risk-engine"))
sys.path.insert(0, str(ROOT / "services" / "explainability"))
sys.path.insert(0, str(ROOT / "ai" / "adapter"))
sys.path.insert(0, str(REPO_ROOT / "sql" / "scenarios"))
sys.path.insert(0, str(ROOT / "backend"))

from feature_engine import FeatureEngine
from rules_engine import evaluate_rules
from risk_aggregator import aggregate, score_to_level
from explain import explain
from sentinel.aml_model import AMLModel
from behavioral import get_adapter
from casino_common.models import MLPrediction, RiskLevel
from run_scenario import build_events, EXPECTED
from datetime import datetime, timezone


def test_structuring_hits_aml001():
    eng = FeatureEngine()
    pid = "11111111-1111-1111-1111-111111111111"
    features = {}
    for e in build_events("structuring"):
        fv = eng.ingest(pid, {**e, "player_id": pid})
        features = fv.flatten()
    last = build_events("structuring")[-1]
    result = evaluate_rules(pid, last, features, "e1", "c1", last.get("transaction_id"))
    ids = [h.rule_id for h in result.hits]
    assert "AML-001" in ids
    assert result.aggregate_score > 0.5


def test_rapid_withdrawal_hits_aml003():
    eng = FeatureEngine()
    pid = "22222222-2222-2222-2222-222222222222"
    features = {}
    for e in build_events("rapid_withdrawal"):
        features = eng.ingest(pid, {**e, "player_id": pid}).flatten()
    last = build_events("rapid_withdrawal")[-1]
    result = evaluate_rules(pid, last, features, "e2", "c2", last.get("transaction_id"))
    assert any(h.rule_id == "AML-003" for h in result.hits)


def test_feature_windows_present():
    eng = FeatureEngine()
    fv = eng.ingest("p1", {"amount": 100, "transaction_type": "DEPOSIT", "timestamp": datetime.now(timezone.utc).isoformat()})
    flat = fv.flatten()
    assert "w5m_tx_count" in flat
    assert "w24h_deposit_sum" in flat
    assert "w30d_avg_tx_amount" in flat


def test_ml_model_predicts():
    """Scoring loads offline artifacts from model/artifacts (no in-process train)."""
    from pathlib import Path

    artifacts = Path(__file__).resolve().parents[4] / "model" / "artifacts"
    model = AMLModel(model_dir=artifacts)
    model.load()
    out = model.predict({"w24h_deposit_sum": 3.0, "w1h_withdrawal_sum": 2.0, "static_shared_device_count": 4})
    assert 0.0 <= out["score"] <= 1.0
    assert out["label"] in ("SUSPICIOUS", "BENIGN")


def test_risk_levels():
    assert score_to_level(0.9) == RiskLevel.CRITICAL
    assert score_to_level(0.7) == RiskLevel.HIGH
    assert score_to_level(0.5) == RiskLevel.MEDIUM
    assert score_to_level(0.1) == RiskLevel.LOW


def test_explain_not_vague():
    eng = FeatureEngine()
    pid = "p-x"
    features = {}
    for e in build_events("behavioral_deviation"):
        features = eng.ingest(pid, e).flatten()
    last = build_events("behavioral_deviation")[-1]
    rules = evaluate_rules(pid, last, features, "e", "c", last.get("transaction_id"))
    ml = MLPrediction(
        event_id="e", player_id=pid, correlation_id="c",
        timestamp=datetime.now(timezone.utc),
        model_name="t", model_version="1", score=0.7, label="SUSPICIOUS",
        shap_top=[{"feature": "w24h_deposit_sum", "value": 1.0}],
    )
    beh = get_adapter("mock").score(pid, last, features, "e", "c")
    assessment = aggregate(rules, ml, beh)
    exp = explain(assessment)
    assert "AI says" not in exp["narrative"]
    assert exp["disclaimer"]
    assert exp["factor_breakdown"]


def test_all_scenarios_defined():
    for name in EXPECTED:
        events = build_events(name)
        assert len(events) >= 1


def test_rapid_wire_hits_aml013():
    eng = FeatureEngine()
    hits = set()
    for e in build_events("rapid_wire"):
        fv = eng.ingest(e["player_id"], e)
        r = evaluate_rules(e["player_id"], e, fv.flatten(), "e", e["player_id"], e.get("transaction_id"))
        hits |= {h.rule_id for h in r.hits}
    assert "AML-013" in hits


def test_tito_hits_aml014():
    eng = FeatureEngine()
    hits = set()
    for e in build_events("tito"):
        fv = eng.ingest(e["player_id"], e)
        r = evaluate_rules(e["player_id"], e, fv.flatten(), "e", e["player_id"], e.get("transaction_id"))
        hits |= {h.rule_id for h in r.hits}
    assert "AML-014" in hits


def test_watchlist_hits_aml015():
    eng = FeatureEngine()
    hits = set()
    for e in build_events("watchlist"):
        fv = eng.ingest(e["player_id"], e)
        r = evaluate_rules(e["player_id"], e, fv.flatten(), "e", e["player_id"], e.get("transaction_id"))
        hits |= {h.rule_id for h in r.hits}
    assert "AML-015" in hits


def test_high_velocity_hits_aml011():
    eng = FeatureEngine()
    hits = set()
    for e in build_events("high_velocity"):
        fv = eng.ingest(e["player_id"], e)
        r = evaluate_rules(e["player_id"], e, fv.flatten(), "e", e["player_id"], e.get("transaction_id"))
        hits |= {h.rule_id for h in r.hits}
    assert "AML-011" in hits


def test_kyc_weight_in_aggregate():
    from risk_aggregator import _weights

    w = _weights()
    assert abs(sum(w.values()) - 1.0) < 1e-6
    assert "kyc" in w


def test_api_pipeline_structuring(api_main):
    api_main.ALERTS.clear()
    api_main.EVENTS.clear()
    out = api_main.process_event(build_events("structuring")[-1])
    for e in build_events("structuring"):
        out = api_main.process_event(e)
    assert out["risk"]["final_score"] >= 0
    assert out["risk"]["risk_level"] in ("LOW", "MEDIUM", "HIGH", "CRITICAL")
