"""Industry category packs for Realtime TxnIQ.

Casino is the live demo pack (wired to AML-001..015). Other industries ship
pre-populated pattern rules for admin selection; detection engines can plug in later.
"""

from __future__ import annotations

from typing import Any

# Plain-English meanings for the live casino rule engine.
CASINO_RULE_PLAIN: dict[str, str] = {
    "AML-001": "Looks for several cash deposits just under the reporting limit — a structuring pattern over time, not one transaction.",
    "AML-002": "Large money in, almost no play, then money out. Pattern: moving funds rather than gambling.",
    "AML-003": "Money comes in and leaves again very quickly. Pattern: rapid in-and-out.",
    "AML-004": "Chips and cage cash are mixed in ways that can hide the source of funds over a session.",
    "AML-005": "The same phone or device fingerprint appears on multiple player accounts — linked-device pattern.",
    "AML-006": "Activity jumps between places too fast for normal travel — impossible-travel pattern.",
    "AML-007": "Spending diverges from this player's usual behavior over their history.",
    "AML-008": "A single amount is far above this player's normal size — spike pattern.",
    "AML-009": "Daily cash activity reaches the regulatory reporting threshold (CTR).",
    "AML-010": "Several cage cash moves sit in the just-under-limit band — cage structuring pattern.",
    "AML-011": "Many transactions in a short window — high-velocity pattern.",
    "AML-012": "Several payment methods used in one day — multi-instrument layering pattern.",
    "AML-013": "Large wire in, then cash out with little play — rapid wire pattern.",
    "AML-014": "Unusually large ticket-in / ticket-out (TITO) activity.",
    "AML-015": "Player matches watchlist or PEP — elevated identity risk pattern.",
}

INDUSTRY_PACKS: dict[str, dict[str, Any]] = {
    "casino": {
        "id": "casino",
        "label": "Casino & gaming",
        "tagline": "Live floor, cage, and wagering transaction patterns",
        "entity_label": "Player",
        "stream_label": "Casino transactions",
        "pattern_focus": "Structuring, shared devices, rapid wire, velocity, CTR",
        "rules": [
            {"id": "AML-001", "name": "Structuring / Smurfing", "severity": "HIGH", "pattern": "repeat_near_threshold"},
            {"id": "AML-002", "name": "Minimal Gaming", "severity": "HIGH", "pattern": "in_out_low_play"},
            {"id": "AML-003", "name": "Rapid Withdrawal", "severity": "CRITICAL", "pattern": "fast_in_out"},
            {"id": "AML-004", "name": "Chip-Cage Layering", "severity": "MEDIUM", "pattern": "instrument_mix"},
            {"id": "AML-005", "name": "Shared Device", "severity": "HIGH", "pattern": "linked_device"},
            {"id": "AML-006", "name": "Impossible Travel", "severity": "HIGH", "pattern": "geo_jump"},
            {"id": "AML-007", "name": "Behavioral Deviation", "severity": "HIGH", "pattern": "baseline_drift"},
            {"id": "AML-008", "name": "High-Value Spike", "severity": "MEDIUM", "pattern": "amount_spike"},
            {"id": "AML-009", "name": "CTR Threshold", "severity": "CRITICAL", "pattern": "regulatory_threshold"},
            {"id": "AML-010", "name": "Cage Structuring", "severity": "HIGH", "pattern": "repeat_near_threshold"},
            {"id": "AML-011", "name": "High Velocity", "severity": "MEDIUM", "pattern": "burst_velocity"},
            {"id": "AML-012", "name": "Multi-Instrument", "severity": "MEDIUM", "pattern": "instrument_mix"},
            {"id": "AML-013", "name": "Rapid Wire", "severity": "CRITICAL", "pattern": "wire_then_cash"},
            {"id": "AML-014", "name": "Large TITO", "severity": "MEDIUM", "pattern": "amount_spike"},
            {"id": "AML-015", "name": "Watchlist / PEP", "severity": "CRITICAL", "pattern": "identity_flag"},
        ],
        "rule_plain": CASINO_RULE_PLAIN,
    },
    "banking": {
        "id": "banking",
        "label": "Banking",
        "tagline": "Account, transfer, and cash-activity pattern intelligence",
        "entity_label": "Customer",
        "stream_label": "Banking transactions",
        "pattern_focus": "Structuring, mule accounts, rapid transfers, velocity",
        "rules": [
            {
                "id": "BNK-001",
                "name": "Deposit structuring",
                "severity": "HIGH",
                "pattern": "repeat_near_threshold",
                "description": "Many deposits just under reporting limits across hours or days.",
            },
            {
                "id": "BNK-002",
                "name": "Rapid fund pass-through",
                "severity": "CRITICAL",
                "pattern": "fast_in_out",
                "description": "Money arrives and leaves the account very quickly with little balance time.",
            },
            {
                "id": "BNK-003",
                "name": "Mule account velocity",
                "severity": "HIGH",
                "pattern": "burst_velocity",
                "description": "Sudden burst of inbound transfers from many unrelated senders.",
            },
            {
                "id": "BNK-004",
                "name": "Shared device / session",
                "severity": "HIGH",
                "pattern": "linked_device",
                "description": "The same device or browser session is used across multiple customer logins.",
            },
            {
                "id": "BNK-005",
                "name": "Cross-border spike",
                "severity": "MEDIUM",
                "pattern": "geo_jump",
                "description": "Unusual jump in cross-border transfers versus the customer’s normal pattern.",
            },
        ],
        "rule_plain": {},
    },
    "payments": {
        "id": "payments",
        "label": "Payments",
        "tagline": "Card, wallet, and merchant payment pattern intelligence",
        "entity_label": "Merchant / payer",
        "stream_label": "Payment events",
        "pattern_focus": "Velocity, device sharing, refund abuse, geographic jumps",
        "rules": [
            {
                "id": "PAY-001",
                "name": "Payment velocity burst",
                "severity": "HIGH",
                "pattern": "burst_velocity",
                "description": "Too many payment attempts in a short window from one payer or device.",
            },
            {
                "id": "PAY-002",
                "name": "Refund / chargeback loop",
                "severity": "HIGH",
                "pattern": "in_out_low_play",
                "description": "Repeated pay-then-refund cycles that look like money movement, not commerce.",
            },
            {
                "id": "PAY-003",
                "name": "Shared payment device",
                "severity": "MEDIUM",
                "pattern": "linked_device",
                "description": "One device fingerprint appears across many unrelated payment accounts.",
            },
            {
                "id": "PAY-004",
                "name": "Merchant structuring",
                "severity": "HIGH",
                "pattern": "repeat_near_threshold",
                "description": "Many settlements just under a review threshold.",
            },
        ],
        "rule_plain": {},
    },
    "insurance": {
        "id": "insurance",
        "label": "Insurance",
        "tagline": "Claims and premium payment pattern intelligence",
        "entity_label": "Policyholder",
        "stream_label": "Claims & premium events",
        "pattern_focus": "Claim bursts, identity links, unusual payout paths",
        "rules": [
            {
                "id": "INS-001",
                "name": "Claim burst pattern",
                "severity": "HIGH",
                "pattern": "burst_velocity",
                "description": "Several claims filed in a short period across related policies.",
            },
            {
                "id": "INS-002",
                "name": "Linked claimant devices",
                "severity": "HIGH",
                "pattern": "linked_device",
                "description": "The same device is used to file claims for different policyholders.",
            },
            {
                "id": "INS-003",
                "name": "Premium in / payout out",
                "severity": "MEDIUM",
                "pattern": "fast_in_out",
                "description": "Premiums paid then large payouts follow unusually fast.",
            },
            {
                "id": "INS-004",
                "name": "Watchlist party on claim",
                "severity": "CRITICAL",
                "pattern": "identity_flag",
                "description": "A party on the claim matches a watchlist or elevated risk identity.",
            },
        ],
        "rule_plain": {},
    },
    "telecom": {
        "id": "telecom",
        "label": "Telecom",
        "tagline": "Subscriber, top-up, and usage pattern intelligence",
        "entity_label": "Subscriber",
        "stream_label": "Telecom events",
        "pattern_focus": "SIM farms, top-up velocity, shared devices",
        "rules": [
            {
                "id": "TEL-001",
                "name": "Top-up velocity",
                "severity": "HIGH",
                "pattern": "burst_velocity",
                "description": "Many prepaid top-ups in a short time on one or linked SIMs.",
            },
            {
                "id": "TEL-002",
                "name": "SIM / device farm",
                "severity": "CRITICAL",
                "pattern": "linked_device",
                "description": "One handset or fingerprint appears across many subscriber identities.",
            },
            {
                "id": "TEL-003",
                "name": "Impossible travel usage",
                "severity": "HIGH",
                "pattern": "geo_jump",
                "description": "Usage locations change faster than physical travel allows.",
            },
            {
                "id": "TEL-004",
                "name": "Near-threshold top-ups",
                "severity": "MEDIUM",
                "pattern": "repeat_near_threshold",
                "description": "Repeated top-ups sit just under an internal review limit.",
            },
        ],
        "rule_plain": {},
    },
}


def list_industries() -> list[dict[str, Any]]:
    return [
        {
            "id": p["id"],
            "label": p["label"],
            "tagline": p["tagline"],
            "entity_label": p["entity_label"],
            "stream_label": p["stream_label"],
            "pattern_focus": p["pattern_focus"],
            "rule_count": len(p["rules"]),
        }
        for p in INDUSTRY_PACKS.values()
    ]


def get_pack(category: str | None) -> dict[str, Any]:
    key = (category or "casino").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {"gaming": "casino", "bank": "banking", "payment": "payments", "telco": "telecom"}
    key = aliases.get(key, key)
    return INDUSTRY_PACKS.get(key) or INDUSTRY_PACKS["casino"]


def enrich_rules(category: str | None) -> list[dict[str, Any]]:
    pack = get_pack(category)
    plain_map: dict[str, str] = dict(pack.get("rule_plain") or {})
    items: list[dict[str, Any]] = []
    for r in pack.get("rules") or []:
        rid = str(r.get("id") or "")
        desc = (
            plain_map.get(rid)
            or r.get("description")
            or f"Pattern check “{r.get('name') or rid}” watches for unusual behavior over time."
        )
        items.append(
            {
                "id": rid,
                "name": r.get("name") or rid,
                "severity": r.get("severity") or "MEDIUM",
                "pattern": r.get("pattern") or "pattern",
                "description": desc,
                "plain_english": desc,
                "category": pack["id"],
                "category_label": pack["label"],
            }
        )
    return items
