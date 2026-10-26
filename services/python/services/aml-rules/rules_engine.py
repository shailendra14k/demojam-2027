"""AML Rules AML-001 .. AML-015 — ours + cage/BSA rules from demojam.

All thresholds are env-configurable. Evidence remains structured JSON.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from casino_common.models import RiskLevel, RuleHit, RuleResult


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


THRESHOLDS = {
    "AML001_CTR_THRESHOLD": _f("AML001_CTR_THRESHOLD", 10000.0),
    "AML001_NEAR_PCT": _f("AML001_NEAR_PCT", 0.98),
    "AML001_MIN_COUNT_24H": _f("AML001_MIN_COUNT_24H", 3),
    "AML002_DEPOSIT_MIN": _f("AML002_DEPOSIT_MIN", 10000.0),
    "AML002_WAGER_RATIO_MAX": _f("AML002_WAGER_RATIO_MAX", 0.05),
    "AML003_WINDOW_MINUTES": _f("AML003_WINDOW_MINUTES", 60),  # matches w1h_* features
    "AML003_AMOUNT_MIN": _f("AML003_AMOUNT_MIN", 5000.0),
    "AML004_CHIP_CASH_RATIO": _f("AML004_CHIP_CASH_RATIO", 0.8),
    "AML004_MIN_SIDE": _f("AML004_MIN_SIDE", 5000.0),
    "AML005_SHARED_DEVICE_MIN": _f("AML005_SHARED_DEVICE_MIN", 3),
    "AML006_GEO_VELOCITY_KM": _f("AML006_GEO_VELOCITY_KM", 500),
    "AML007_DEVIATION_MULT": _f("AML007_DEVIATION_MULT", 10.0),
    "AML008_SPIKE_AMOUNT": _f("AML008_SPIKE_AMOUNT", 20000.0),
    # Ported from demojam (Red Hat cage / BSA story)
    "AML009_CTR_CASH": _f("AML009_CTR_CASH", 10000.0),
    "AML010_STRUCTURING_LOW": _f("AML010_STRUCTURING_LOW", 8000.0),
    "AML010_STRUCTURING_HIGH": _f("AML010_STRUCTURING_HIGH", 9999.0),
    "AML010_JUST_BELOW_MIN": _f("AML010_JUST_BELOW_MIN", 2),
    "AML011_VELOCITY_TXN_1H": _f("AML011_VELOCITY_TXN_1H", 5),
    "AML011_CAGE_VISITS_24H": _f("AML011_CAGE_VISITS_24H", 6),
    "AML012_PAYMENT_METHODS": _f("AML012_PAYMENT_METHODS", 3),
    "AML013_WIRE_IN_SINGLE": _f("AML013_WIRE_IN_SINGLE", 15000.0),
    "AML013_WIRE_IN_24H": _f("AML013_WIRE_IN_24H", 10000.0),
    "AML013_CASHOUT_OF_WIRE": _f("AML013_CASHOUT_OF_WIRE", 0.6),
    "AML013_WAGER_RATIO_MAX": _f("AML013_WAGER_RATIO_MAX", 0.25),
    "AML014_TITO_AMOUNT": _f("AML014_TITO_AMOUNT", 5000.0),
}


def evaluate_rules(
    player_id: str,
    event: dict[str, Any],
    features: dict[str, float],
    event_id: str,
    correlation_id: str,
    transaction_id: str | None = None,
) -> RuleResult:
    hits: list[RuleHit] = []
    amount = float(event.get("amount") if event.get("amount") is not None else 0)
    tx_type = (
        event.get("transaction_type")
        or event.get("event_type")
        or (event.get("metadata") or {}).get("txn_type")
        or "TRANSACTION"
    )
    tx_type = str(tx_type).upper()
    payment = str(
        event.get("payment_method")
        or event.get("channel")
        or (event.get("metadata") or {}).get("payment_method")
        or "UNKNOWN"
    ).upper()
    location = str(event.get("location") or "Casino floor")
    currency = str(event.get("currency") or "USD")
    now = datetime.now(timezone.utc)

    is_cash = tx_type in ("CASH_IN", "CASH_OUT") or payment == "CASH" or (
        tx_type in ("DEPOSIT", "BUY_IN", "WITHDRAWAL") and payment in ("CASH", "CAGE")
    )

    dep_24h = features.get("w24h_deposit_count", 0)
    dep_sum_24h = features.get("w24h_deposit_sum", 0)
    wager_sum_24h = features.get("w24h_wager_sum", 0)
    wd_sum_1h = features.get("w1h_withdrawal_sum", 0)
    dep_sum_1h = features.get("w1h_deposit_sum", 0)
    shared_devices = features.get("static_shared_device_count", 0)
    baseline_avg = features.get("w30d_avg_tx_amount", 0) or features.get("static_avg_tx_amount", 100)
    chip_sum_24h = features.get("w24h_chip_sum", 0)
    cage_sum_24h = features.get("w24h_cage_sum", 0)
    cash_in_24h = features.get("w24h_cash_in_sum", 0) or (
        dep_sum_24h if is_cash or payment in ("CASH", "CAGE") else 0
    )
    cash_out_24h = features.get("w24h_cash_out_sum", 0)
    just_below = features.get("w24h_just_below_10k_count", 0)
    cage_visits = features.get("w24h_cage_visits", 0)
    txn_1h = features.get("w1h_tx_count", 0)
    pay_methods = features.get("w24h_unique_payment_methods", 0)
    wire_in_24h = features.get("w24h_wire_in_sum", 0)
    buyin_base = max(cash_in_24h, wire_in_24h, dep_sum_24h, 1.0)
    wager_ratio = (wager_sum_24h / buyin_base) if buyin_base > 0 else features.get("w24h_wager_to_buyin_ratio", 1.0)
    watchlist = features.get("static_watchlist_hit", 0)
    is_pep = features.get("static_is_pep", 0)

    # AML-001 Structuring: multiple near-CTR deposits
    near = THRESHOLDS["AML001_CTR_THRESHOLD"] * THRESHOLDS["AML001_NEAR_PCT"]
    if (
        tx_type in ("DEPOSIT", "BUY_IN", "CASH_IN")
        and amount >= near
        and amount < THRESHOLDS["AML001_CTR_THRESHOLD"]
        and dep_24h >= THRESHOLDS["AML001_MIN_COUNT_24H"]
    ):
        hits.append(
            RuleHit(
                rule_id="AML-001",
                rule_name="Structuring / Smurfing",
                severity=RiskLevel.HIGH,
                score=0.85,
                description="Multiple deposits just under CTR threshold within 24h",
                evidence={
                    "amount": amount,
                    "deposit_count_24h": dep_24h,
                    "threshold": THRESHOLDS["AML001_CTR_THRESHOLD"],
                },
            )
        )

    # AML-002 Minimal gaming
    if (
        tx_type in ("WITHDRAWAL", "CASH_OUT")
        and dep_sum_24h >= THRESHOLDS["AML002_DEPOSIT_MIN"]
        and dep_sum_24h > 0
        and (wager_sum_24h / dep_sum_24h) <= THRESHOLDS["AML002_WAGER_RATIO_MAX"]
    ):
        hits.append(
            RuleHit(
                rule_id="AML-002",
                rule_name="Minimal Gaming",
                severity=RiskLevel.HIGH,
                score=0.8,
                description="Large deposits with negligible wagering before withdrawal",
                evidence={
                    "deposit_sum_24h": dep_sum_24h,
                    "wager_sum_24h": wager_sum_24h,
                    "ratio": wager_sum_24h / dep_sum_24h if dep_sum_24h else 0,
                },
            )
        )

    # AML-003 Rapid withdrawal (uses w1h_* — window minutes documented for evidence)
    if (
        tx_type in ("WITHDRAWAL", "CASH_OUT")
        and amount >= THRESHOLDS["AML003_AMOUNT_MIN"]
        and dep_sum_1h >= THRESHOLDS["AML003_AMOUNT_MIN"]
        and wd_sum_1h >= amount * 0.9
    ):
        hits.append(
            RuleHit(
                rule_id="AML-003",
                rule_name="Rapid Withdrawal",
                severity=RiskLevel.CRITICAL,
                score=0.9,
                description="Funds withdrawn shortly after deposit",
                evidence={
                    "amount": amount,
                    "deposit_sum_1h": dep_sum_1h,
                    "withdrawal_sum_1h": wd_sum_1h,
                    "window_minutes": THRESHOLDS["AML003_WINDOW_MINUTES"],
                },
            )
        )

    # AML-004 Chip / cage layering
    if chip_sum_24h > 0 and cage_sum_24h > 0:
        ratio = min(chip_sum_24h, cage_sum_24h) / max(chip_sum_24h, cage_sum_24h)
        if ratio >= THRESHOLDS["AML004_CHIP_CASH_RATIO"] and max(chip_sum_24h, cage_sum_24h) >= THRESHOLDS["AML004_MIN_SIDE"]:
            hits.append(
                RuleHit(
                    rule_id="AML-004",
                    rule_name="Chip-Cage Layering",
                    severity=RiskLevel.MEDIUM,
                    score=0.65,
                    description="Coordinated chip and cage activity suggesting layering",
                    evidence={"chip_sum_24h": chip_sum_24h, "cage_sum_24h": cage_sum_24h, "ratio": ratio},
                )
            )

    # AML-005 Shared device
    if shared_devices >= THRESHOLDS["AML005_SHARED_DEVICE_MIN"]:
        hits.append(
            RuleHit(
                rule_id="AML-005",
                rule_name="Shared Device",
                severity=RiskLevel.HIGH,
                score=0.75,
                description="Device fingerprint linked to multiple player accounts",
                evidence={"shared_device_count": shared_devices},
            )
        )

    # AML-006 Impossible travel
    geo_km = features.get("w5m_geo_distance_km", 0)
    if geo_km >= THRESHOLDS["AML006_GEO_VELOCITY_KM"]:
        hits.append(
            RuleHit(
                rule_id="AML-006",
                rule_name="Impossible Travel",
                severity=RiskLevel.HIGH,
                score=0.8,
                description="Location change exceeds feasible travel velocity",
                evidence={"geo_distance_km": geo_km},
            )
        )

    # AML-007 Behavioral deviation
    if baseline_avg > 0 and amount >= baseline_avg * THRESHOLDS["AML007_DEVIATION_MULT"]:
        hits.append(
            RuleHit(
                rule_id="AML-007",
                rule_name="Behavioral Deviation",
                severity=RiskLevel.HIGH,
                score=0.78,
                description="Transaction amount far exceeds player historical baseline",
                evidence={"amount": amount, "baseline_avg": baseline_avg, "multiplier": amount / baseline_avg},
            )
        )

    # AML-008 High-value spike
    if amount >= THRESHOLDS["AML008_SPIKE_AMOUNT"] and tx_type in (
        "DEPOSIT",
        "BUY_IN",
        "CASH_OUT",
        "WITHDRAWAL",
        "CASH_IN",
        "WIRE_IN",
    ):
        hits.append(
            RuleHit(
                rule_id="AML-008",
                rule_name="High-Value Spike",
                severity=RiskLevel.MEDIUM,
                score=0.7,
                description="Single transaction exceeds high-value spike threshold",
                evidence={"amount": amount, "threshold": THRESHOLDS["AML008_SPIKE_AMOUNT"]},
            )
        )

    # AML-009 CTR — single cash >= $10k (BSA-style, from demojam)
    if is_cash and amount >= THRESHOLDS["AML009_CTR_CASH"]:
        hits.append(
            RuleHit(
                rule_id="AML-009",
                rule_name="CTR Threshold",
                severity=RiskLevel.CRITICAL,
                score=0.92,
                description="Single cash transaction meets or exceeds $10,000 CTR threshold",
                evidence={"amount": amount, "threshold": THRESHOLDS["AML009_CTR_CASH"]},
            )
        )

    # AML-009b aggregate via same id family — use AML-009 when 24h cash hits CTR
    if cash_in_24h >= THRESHOLDS["AML009_CTR_CASH"] or cash_out_24h >= THRESHOLDS["AML009_CTR_CASH"]:
        if not any(h.rule_id == "AML-009" for h in hits):
            hits.append(
                RuleHit(
                    rule_id="AML-009",
                    rule_name="CTR Threshold Aggregate",
                    severity=RiskLevel.HIGH,
                    score=0.88,
                    description="24h cash-in or cash-out meets CTR aggregate threshold",
                    evidence={"cash_in_24h": cash_in_24h, "cash_out_24h": cash_out_24h},
                )
            )

    # AML-010 Cage structuring band ($8k–$9,999 cash across cages)
    band = THRESHOLDS["AML010_STRUCTURING_LOW"] <= amount <= THRESHOLDS["AML010_STRUCTURING_HIGH"] and is_cash
    if band or just_below >= THRESHOLDS["AML010_JUST_BELOW_MIN"]:
        if cage_visits >= 2 or just_below >= THRESHOLDS["AML010_JUST_BELOW_MIN"]:
            if not any(h.rule_id == "AML-001" for h in hits):
                hits.append(
                    RuleHit(
                        rule_id="AML-010",
                        rule_name="Cage Structuring",
                        severity=RiskLevel.HIGH,
                        score=0.86,
                        description="Sub-$10k cash moves across multiple cage visits",
                        evidence={
                            "just_below_10k_count_24h": just_below,
                            "cage_visits_24h": cage_visits,
                            "amount": amount,
                        },
                    )
                )

    # AML-011 High velocity
    if txn_1h >= THRESHOLDS["AML011_VELOCITY_TXN_1H"] or cage_visits >= THRESHOLDS["AML011_CAGE_VISITS_24H"]:
        hits.append(
            RuleHit(
                rule_id="AML-011",
                rule_name="High Velocity",
                severity=RiskLevel.MEDIUM,
                score=0.72,
                description="Burst transaction or cage-visit activity",
                evidence={"txn_count_1h": txn_1h, "cage_visits_24h": cage_visits},
            )
        )

    # AML-012 Multi-instrument
    if pay_methods >= THRESHOLDS["AML012_PAYMENT_METHODS"]:
        hits.append(
            RuleHit(
                rule_id="AML-012",
                rule_name="Multi-Instrument",
                severity=RiskLevel.MEDIUM,
                score=0.68,
                description="Multiple payment instruments used within 24h",
                evidence={"unique_payment_methods_24h": pay_methods},
            )
        )

    # AML-013 Rapid wire
    if tx_type == "WIRE_IN" and amount >= THRESHOLDS["AML013_WIRE_IN_SINGLE"]:
        hits.append(
            RuleHit(
                rule_id="AML-013",
                rule_name="Rapid Wire",
                severity=RiskLevel.HIGH,
                score=0.78,
                description="Large single wire-in",
                evidence={"amount": amount},
            )
        )
    if (
        wire_in_24h >= THRESHOLDS["AML013_WIRE_IN_24H"]
        and cash_out_24h >= wire_in_24h * THRESHOLDS["AML013_CASHOUT_OF_WIRE"]
        and (wager_ratio or 0) < THRESHOLDS["AML013_WAGER_RATIO_MAX"]
    ):
        hits.append(
            RuleHit(
                rule_id="AML-013",
                rule_name="Rapid Wire Cash-Out",
                severity=RiskLevel.CRITICAL,
                score=0.88,
                description="Wire-in followed by cash-out with little gaming",
                evidence={
                    "wire_in_24h": wire_in_24h,
                    "cash_out_24h": cash_out_24h,
                    "wager_ratio": wager_ratio,
                },
            )
        )

    # AML-014 TITO
    if tx_type in ("TITO_ISSUE", "TITO_REDEEM") and amount >= THRESHOLDS["AML014_TITO_AMOUNT"]:
        hits.append(
            RuleHit(
                rule_id="AML-014",
                rule_name="Large TITO",
                severity=RiskLevel.MEDIUM,
                score=0.55,
                description="Large ticket-in / ticket-out activity",
                evidence={"amount": amount, "transaction_type": tx_type},
            )
        )

    # AML-015 Watchlist / PEP
    if watchlist:
        hits.append(
            RuleHit(
                rule_id="AML-015",
                rule_name="Watchlist Match",
                severity=RiskLevel.CRITICAL,
                score=0.98,
                description="Player matches watch-list / enhanced KYC",
                evidence={"watchlist_hit": True},
            )
        )
    elif is_pep:
        hits.append(
            RuleHit(
                rule_id="AML-015",
                rule_name="PEP",
                severity=RiskLevel.HIGH,
                score=0.78,
                description="Politically exposed person",
                evidence={"pep_flag": True},
            )
        )

    # Dedupe by rule_id keeping highest score
    best: dict[str, RuleHit] = {}
    for h in hits:
        prev = best.get(h.rule_id)
        if prev is None or h.score > prev.score:
            best[h.rule_id] = h
    hits = list(best.values())

    agg = min(1.0, sum(h.score for h in hits) / max(len(hits), 1) * (1 + 0.1 * (len(hits) - 1))) if hits else 0.0
    return RuleResult(
        event_id=event_id,
        player_id=player_id,
        transaction_id=transaction_id,
        correlation_id=correlation_id,
        timestamp=now,
        hits=hits,
        aggregate_score=agg,
        amount=round(amount, 2),
        transaction_type=tx_type or "TRANSACTION",
        currency=currency,
        location=location,
        payment_method=payment or "UNKNOWN",
    )


RULE_CATALOG = [
    {"id": "AML-001", "name": "Structuring / Smurfing", "severity": "HIGH"},
    {"id": "AML-002", "name": "Minimal Gaming", "severity": "HIGH"},
    {"id": "AML-003", "name": "Rapid Withdrawal", "severity": "CRITICAL"},
    {"id": "AML-004", "name": "Chip-Cage Layering", "severity": "MEDIUM"},
    {"id": "AML-005", "name": "Shared Device", "severity": "HIGH"},
    {"id": "AML-006", "name": "Impossible Travel", "severity": "HIGH"},
    {"id": "AML-007", "name": "Behavioral Deviation", "severity": "HIGH"},
    {"id": "AML-008", "name": "High-Value Spike", "severity": "MEDIUM"},
    {"id": "AML-009", "name": "CTR Threshold", "severity": "CRITICAL"},
    {"id": "AML-010", "name": "Cage Structuring", "severity": "HIGH"},
    {"id": "AML-011", "name": "High Velocity", "severity": "MEDIUM"},
    {"id": "AML-012", "name": "Multi-Instrument", "severity": "MEDIUM"},
    {"id": "AML-013", "name": "Rapid Wire", "severity": "CRITICAL"},
    {"id": "AML-014", "name": "Large TITO", "severity": "MEDIUM"},
    {"id": "AML-015", "name": "Watchlist / PEP", "severity": "CRITICAL"},
]
