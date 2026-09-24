"""Integration-style tests for demo scenarios expected outcomes."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "sql" / "scenarios"))

from run_scenario import EXPECTED, build_events


def _run(api_main, name: str):
    api_main.ALERTS.clear()
    results = [api_main.process_event(e) for e in build_events(name)]
    return results


def test_structuring_expected_risk(api_main):
    results = _run(api_main, "structuring")
    levels = {r["risk"]["risk_level"] for r in results}
    assert levels & set(EXPECTED["structuring"]["expected_risk"])


def test_minimal_gaming_produces_alert_or_medium(api_main):
    results = _run(api_main, "minimal_gaming")
    assert any(r["risk"]["final_score"] > 0.3 for r in results)


def test_shared_device_rule(api_main):
    results = _run(api_main, "shared_device")
    hits = []
    for r in results:
        hits.extend(h["rule_id"] for h in r["rules"]["hits"])
    assert "AML-005" in hits or any(r["risk"]["final_score"] > 0 for r in results)
