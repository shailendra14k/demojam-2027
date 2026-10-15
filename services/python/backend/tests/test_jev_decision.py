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
