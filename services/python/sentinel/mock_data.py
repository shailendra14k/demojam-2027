"""Mock fallback data so the UI is demo-ready even before CDC is live."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

NOW = datetime.now(timezone.utc)


def kpis() -> dict:
    return {
        "transactionsToday": 12481,
        "transactionsDelta": 8.4,
        "activePlayers": 4286,
        "activePlayersDelta": 2.1,
        "alertsToday": 154,
        "alertsDelta": 17,
        "criticalAlerts": 6,
        "criticalDelta": 4,
        "openCases": 9,
        "openCasesDelta": -2,
        "sarsPending": 6,
        "sarsDelta": 1,
        "source": "mock",
    }


def events() -> list[dict]:
    rows = [
        ("C10281", 9800, "Cash Out", "Cage 03", "STRUCTURING"),
        ("C10811", 25000, "Wire In", "Bank XYZ", "RAPID_WIRE"),
        ("C11012", 400, "Blackjack", "Table 18", "NORMAL"),
        ("C11120", 220, "TITO Issue", "Slot Bank 04", "TITO"),
        ("C10281", 9500, "Cash Out", "Cage 07", "STRUCTURING"),
        ("C11550", 180, "TITO Redeem", "Cage 01", "TITO"),
        ("C10990", 15000, "Cash In", "Cage 02", "WATCHLIST"),
        ("C11440", 8900, "Cash Out", "Cage 05", "STRUCTURING"),
        ("C11012", 50, "Blackjack", "Table 18", "NORMAL"),
        ("C10402", 1200, "Cash In", "Cage 01", "NORMAL"),
    ]
    out = []
    for i, (pid, amt, action, loc, tag) in enumerate(rows):
        ts = NOW - timedelta(seconds=90 * i)
        out.append(
            {
                "eventId": f"MOCK-{i:03d}",
                "time": ts.strftime("%H:%M:%S"),
                "createdAt": ts.isoformat(),
                "playerId": pid,
                "amount": amt,
                "action": action,
                "location": loc,
                "status": tag,
                "riskScore": {"STRUCTURING": 82, "RAPID_WIRE": 76, "WATCHLIST": 91}.get(tag, 12),
            }
        )
    return out


def alerts() -> list[dict]:
    return [
        {
            "alertId": "ALT-1001",
            "playerId": "C10281",
            "playerName": "Michael Carter",
            "alertType": "STRUCTURING",
            "priority": "HIGH",
            "riskScore": 82,
            "explanation": "Multiple $9.5k–$9.8k cash-outs across Cage 03 and Cage 07.",
            "status": "OPEN",
            "createdAt": (NOW - timedelta(minutes=40)).isoformat(),
            "matchedRules": ["STRUCTURING", "HIGH_VELOCITY"],
        },
        {
            "alertId": "ALT-1002",
            "playerId": "C10990",
            "playerName": "Elena Volkov",
            "alertType": "WATCHLIST",
            "priority": "CRITICAL",
            "riskScore": 91,
            "explanation": "PEP + watchlist hit with multi-rail cash cycle.",
            "status": "OPEN",
            "createdAt": (NOW - timedelta(minutes=12)).isoformat(),
            "matchedRules": ["WATCHLIST_MATCH", "PEP", "MULTI_INSTRUMENT"],
        },
        {
            "alertId": "ALT-1003",
            "playerId": "C10811",
            "playerName": "James Okonkwo",
            "alertType": "RAPID_WIRE",
            "priority": "HIGH",
            "riskScore": 76,
            "explanation": "Wire-in $25,000 with limited subsequent gaming.",
            "status": "OPEN",
            "createdAt": (NOW - timedelta(minutes=35)).isoformat(),
            "matchedRules": ["RAPID_WIRE", "MINIMAL_PLAY"],
        },
        {
            "alertId": "ALT-1004",
            "playerId": "C11440",
            "playerName": "Lucas Silva",
            "alertType": "STRUCTURING",
            "priority": "HIGH",
            "riskScore": 71,
            "explanation": "Repeat $8.9k cash-outs just below CTR threshold.",
            "status": "OPEN",
            "createdAt": (NOW - timedelta(minutes=8)).isoformat(),
            "matchedRules": ["STRUCTURING"],
        },
    ]


def customers() -> list[dict]:
    return [
        {"playerId": "C10281", "name": "Michael Carter", "country": "Mexico", "riskScore": 82, "monthVolume": 184200, "tier": "HIGH"},
        {"playerId": "C10990", "name": "Elena Volkov", "country": "Russia", "riskScore": 91, "monthVolume": 267500, "tier": "CRITICAL"},
        {"playerId": "C10811", "name": "James Okonkwo", "country": "Nigeria", "riskScore": 76, "monthVolume": 142000, "tier": "HIGH"},
        {"playerId": "C11440", "name": "Lucas Silva", "country": "Brazil", "riskScore": 71, "monthVolume": 98000, "tier": "HIGH"},
        {"playerId": "C10402", "name": "Priya Mehta", "country": "India", "riskScore": 44, "monthVolume": 31200, "tier": "MEDIUM"},
        {"playerId": "C11330", "name": "Amira Hassan", "country": "UAE", "riskScore": 52, "monthVolume": 44100, "tier": "MEDIUM"},
        {"playerId": "C11012", "name": "Daniel Nguyen", "country": "USA", "riskScore": 18, "monthVolume": 6400, "tier": "LOW"},
        {"playerId": "C11120", "name": "Sofia Rossi", "country": "Italy", "riskScore": 16, "monthVolume": 5100, "tier": "LOW"},
    ]


def cases() -> list[dict]:
    return [
        {"caseId": "CASE-1001", "playerId": "C10281", "playerName": "Michael Carter", "title": "Structuring across Cage 03/07", "priority": "HIGH", "status": "UNDER_REVIEW", "assignee": "Sarah Wilson", "createdAt": (NOW - timedelta(hours=4)).isoformat()},
        {"caseId": "CASE-1002", "playerId": "C10990", "playerName": "Elena Volkov", "title": "PEP watchlist + multi-rail cash", "priority": "CRITICAL", "status": "ASSIGNED", "assignee": "Sarah Wilson", "createdAt": (NOW - timedelta(hours=2)).isoformat()},
        {"caseId": "CASE-1003", "playerId": "C10811", "playerName": "James Okonkwo", "title": "Wire-in / minimal play", "priority": "HIGH", "status": "NEW", "assignee": None, "createdAt": (NOW - timedelta(hours=1)).isoformat()},
        {"caseId": "CASE-1004", "playerId": "C11440", "playerName": "Lucas Silva", "title": "Repeat $8.9k cash-outs", "priority": "MEDIUM", "status": "NEW", "assignee": None, "createdAt": (NOW - timedelta(minutes=50)).isoformat()},
    ]


def sars() -> list[dict]:
    return [
        {"sarId": "SAR-9001", "caseId": "CASE-1002", "playerId": "C10990", "playerName": "Elena Volkov", "status": "PENDING", "amount": 267500, "narrative": "PEP customer cycling cash through cage and wire rails.", "createdAt": (NOW - timedelta(hours=2)).isoformat()},
        {"sarId": "SAR-9002", "caseId": "CASE-1001", "playerId": "C10281", "playerName": "Michael Carter", "status": "PENDING", "amount": 184200, "narrative": "Possible structuring via multiple sub-$10k cash-outs.", "createdAt": (NOW - timedelta(hours=3)).isoformat()},
    ]


def rules_catalog() -> list[dict]:
    return [
        {"id": "CTR_THRESHOLD", "name": "CTR single transaction", "description": "Cash in/out ≥ $10,000 in a single move.", "severity": "HIGH", "enabled": True},
        {"id": "CTR_THRESHOLD_AGGREGATE", "name": "CTR aggregate 24h", "description": "Aggregate cash in or out ≥ $10,000 in 24 hours.", "severity": "HIGH", "enabled": True},
        {"id": "STRUCTURING", "name": "Structuring detection", "description": "Multiple $8k–$9.9k cash transactions across cage windows.", "severity": "HIGH", "enabled": True},
        {"id": "MINIMAL_PLAY", "name": "Large deposit → minimal gaming", "description": "High buy-in, <10% wagered, then cash-out.", "severity": "HIGH", "enabled": True},
        {"id": "HIGH_VELOCITY", "name": "High velocity", "description": "Burst of cage visits or transactions in a short window.", "severity": "MEDIUM", "enabled": True},
        {"id": "MULTI_INSTRUMENT", "name": "Multiple payment instruments", "description": "Three or more rails used in 24 hours.", "severity": "MEDIUM", "enabled": True},
        {"id": "RAPID_WIRE", "name": "Rapid wire", "description": "Large wire-in followed by cash-out with little play.", "severity": "HIGH", "enabled": True},
        {"id": "WATCHLIST_MATCH", "name": "Watch-list / PEP", "description": "OFAC / enhanced KYC / PEP match.", "severity": "CRITICAL", "enabled": True},
    ]


def alerts_by_type() -> list[dict]:
    return [
        {"type": "Structuring", "count": 42},
        {"type": "Rapid Wire", "count": 28},
        {"type": "Minimal Play", "count": 21},
        {"type": "Velocity", "count": 18},
        {"type": "Watchlist", "count": 9},
        {"type": "TITO", "count": 14},
        {"type": "Multi-rail", "count": 11},
    ]


def pipeline() -> list[dict]:
    return [
        {"id": "transaction", "label": "Casino Transaction", "status": "ONLINE"},
        {"id": "debezium", "label": "CDC / Debezium", "status": "ONLINE"},
        {"id": "kafka", "label": "AMQ Streams", "status": "ONLINE"},
        {"id": "rules", "label": "Rule Engine", "status": "ONLINE"},
        {"id": "scoring", "label": "Risk Scoring", "status": "ONLINE"},
        {"id": "alert", "label": "Alert", "status": "ONLINE"},
        {"id": "investigation", "label": "Investigation", "status": "ONLINE"},
    ]


def network() -> dict:
    return {
        "nodes": [
            {"id": "C10281", "name": "Michael Carter", "score": 82},
            {"id": "C11440", "name": "Lucas Silva", "score": 71},
            {"id": "C10811", "name": "James Okonkwo", "score": 76},
            {"id": "C10990", "name": "Elena Volkov", "score": 91},
        ],
        "edges": [
            {"from": "C10281", "to": "C11440", "type": "CORRELATED_CAGE", "weight": 0.86},
            {"from": "C10811", "to": "C10990", "type": "CHIP_TRANSFER", "weight": 0.64},
        ],
    }
