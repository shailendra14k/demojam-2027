"""Jev decision-layer unit tests (mock mode — no API key)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from casino_common.jev_decision import attach_decision, decide_alert  # noqa: E402


def test_mock_structuring_escalates(monkeypatch):
    monkeypatch.setenv("JEV_MODE", "mock")
    alert = {
        "alert_id": "a1",
        "player_id": "C11012",
        "score": 0.82,
        "risk_level": "HIGH",
        "rule_hits": [{"rule_id": "AML-001", "rule_name": "Structuring"}],
        "summary": "Multiple deposits under CTR",
        "explanations": ["AML-001 Structuring"],
    }
    d = decide_alert(alert, force=True)
    assert d is not None
    assert d["disposition"] == "escalate"
    assert d["red_flags"]["structuring"] >= 0.55
    assert d["suggested_status"] == "ESCALATED"


def test_mock_low_closes(monkeypatch):
    monkeypatch.setenv("JEV_MODE", "mock")
    alert = {
        "alert_id": "a2",
        "player_id": "C1",
        "score": 0.2,
        "risk_level": "LOW",
        "rule_hits": [],
        "summary": "weak",
    }
    d = decide_alert(alert, force=True)
    assert d is not None
    assert d["disposition"] == "close"


def test_off_skips(monkeypatch):
    monkeypatch.setenv("JEV_MODE", "off")
    alert = {"alert_id": "a3", "score": 0.9, "risk_level": "HIGH", "rule_hits": []}
    assert decide_alert(alert, force=True) is None
    attach_decision(alert, force=True)
    assert "jev_decision" not in alert


def test_attach_fields(monkeypatch):
    monkeypatch.setenv("JEV_MODE", "mock")
    alert = {
        "alert_id": "a4",
        "player_id": "C2",
        "score": 0.6,
        "risk_level": "MEDIUM",
        "rule_hits": [{"rule_id": "AML-003"}],
    }
    attach_decision(alert, force=True)
    assert alert["jev_disposition"] in ("escalate", "need_info", "close")
    assert alert["jev_suggested_status"]


def test_reconcile_overrides_weak_close(monkeypatch):
    from casino_common.jev_decision import _mock_decide, _reconcile_disposition, build_state

    monkeypatch.setenv("JEV_MODE", "live")
    state = build_state(
        {
            "alert_id": "a5",
            "score": 0.9,
            "risk_level": "HIGH",
            "rule_hits": [{"rule_id": "AML-001"}],
            "summary": "structuring",
        }
    )
    heuristic = _mock_decide(state)
    assert heuristic["disposition"] == "escalate"
    live = {
        "disposition": "close",
        "disposition_confidence": 0.03,
        "suggested_status": "CLOSED",
        "mode": "openjev",
        "model": "verdict-1.4",
    }
    out = _reconcile_disposition(
        live,
        heuristic,
        raw_disp={
            "choice": "close",
            "confidence": 0.03,
            "probabilities": {"escalate": 0.3, "need_info": 0.25, "close": 0.45},
        },
    )
    assert out["disposition"] == "escalate"
    assert out["mode"] == "hybrid"
    assert out["policy_applied"] is True
    assert "Escalate" in out["rationale"] or "escalate" in out["rationale"].lower()


def test_policy_covers_three_dispositions(monkeypatch):
    monkeypatch.setenv("JEV_MODE", "mock")
    escalate = decide_alert(
        {
            "alert_id": "e",
            "score": 0.95,
            "risk_level": "CRITICAL",
            "rule_hits": [{"rule_id": "AML-015"}],
        },
        force=True,
    )
    need = decide_alert(
        {
            "alert_id": "n",
            "score": 0.55,
            "risk_level": "MEDIUM",
            "rule_hits": [{"rule_id": "AML-007"}],
        },
        force=True,
    )
    close = decide_alert(
        {"alert_id": "c", "score": 0.15, "risk_level": "LOW", "rule_hits": []},
        force=True,
    )
    assert escalate and escalate["disposition"] == "escalate"
    assert need and need["disposition"] == "need_info"
    assert close and close["disposition"] == "close"
