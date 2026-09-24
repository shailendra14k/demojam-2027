#!/usr/bin/env python3
"""AML demo scenario injectors with expected outcomes.

Scenarios:
  structuring, minimal_gaming, rapid_withdrawal, shared_device, behavioral_deviation
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

EXPECTED = {
    "normal": {
        "expected_rules": [],
        "expected_risk": ["LOW"],
        "description": "Benign single deposit within a normal player pattern (no alert expected)",
    },
    "structuring": {
        "expected_rules": ["AML-001"],
        "expected_risk": ["MEDIUM", "HIGH", "CRITICAL"],
        "description": "Multiple deposits just under CTR threshold within 24h",
    },
    "minimal_gaming": {
        "expected_rules": ["AML-002"],
        "expected_risk": ["MEDIUM", "HIGH"],
        "description": "Large deposit followed by minimal wagering then withdrawal",
    },
    "rapid_withdrawal": {
        "expected_rules": ["AML-003"],
        "expected_risk": ["HIGH", "CRITICAL"],
        "description": "Deposit then rapid full withdrawal within minutes",
    },
    "shared_device": {
        "expected_rules": ["AML-005"],
        "expected_risk": ["MEDIUM", "HIGH"],
        "description": "Multiple accounts using the same device fingerprint",
    },
    "behavioral_deviation": {
        "expected_rules": ["AML-007", "AML-008"],
        "expected_risk": ["MEDIUM", "HIGH", "CRITICAL"],
        "description": "Sudden spike in transaction size vs player baseline",
    },
    "rapid_wire": {
        "expected_rules": ["AML-013"],
        "expected_risk": ["HIGH", "CRITICAL"],
        "description": "Large wire-in followed by cash-out with little play",
    },
    "tito": {
        "expected_rules": ["AML-014"],
        "expected_risk": ["MEDIUM", "HIGH"],
        "description": "Large TITO issue/redeem",
    },
    "high_velocity": {
        "expected_rules": ["AML-011"],
        "expected_risk": ["MEDIUM", "HIGH"],
        "description": "Burst of cage cash transactions in one hour",
    },
    "watchlist": {
        "expected_rules": ["AML-015", "AML-009"],
        "expected_risk": ["HIGH", "CRITICAL"],
        "description": "Watchlisted player with large cash activity",
    },
}


# Stable demo players from sql/02-seed.sql (VARCHAR ids — not UUIDs).
SEED_PLAYERS = [
    "C10281", "C10402", "C10811", "C10990", "C11012", "C11120",
    "C11201", "C11330", "C11440", "C11550", "C11660", "C11770",
]


def vid(s: str, prefix: str = "TXN", n: int = 12) -> str:
    """Deterministic VARCHAR id fitting A's PK widths."""
    digest = hashlib.md5(s.encode()).hexdigest()[:n].upper()
    out = f"{prefix}-{digest}"
    return out[:64]


def player_id(seed: int, index: int) -> str:
    return SEED_PLAYERS[index % len(SEED_PLAYERS)]


def duuid(s: str) -> str:
    """Back-compat alias → VARCHAR txn id (not a UUID)."""
    return vid(s, "TXN", 12)


def build_events(scenario: str, seed: int = 42) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    events: list[dict[str, Any]] = []

    if scenario == "normal":
        pid = player_id(seed, 99)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"normal-dep-{seed}"),
                "transaction_type": "DEPOSIT",
                "amount": 250.0,
                "currency": "USD",
                "channel": "MOBILE",
                "timestamp": now.isoformat(),
                "scenario_tag": "normal",
            }
        )
    elif scenario == "structuring":
        pid = player_id(seed, 100)
        for i in range(6):
            events.append(
                {
                    "event_type": "TRANSACTION",
                    "player_id": pid,
                    "transaction_id": duuid(f"struct-{seed}-{i}"),
                    "transaction_type": "DEPOSIT",
                    "amount": 9900.0,
                    "currency": "USD",
                    "channel": "CAGE",
                    "payment_method": "CASH",
                    "location": f"Cage-{i % 3}",
                    "timestamp": (now - timedelta(hours=20 - i * 3)).isoformat(),
                    "scenario_tag": "structuring",
                }
            )
    elif scenario == "minimal_gaming":
        pid = player_id(seed, 101)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"mingame-dep-{seed}"),
                "transaction_type": "DEPOSIT",
                "amount": 25000.0,
                "currency": "USD",
                "channel": "ONLINE",
                "timestamp": (now - timedelta(hours=2)).isoformat(),
                "scenario_tag": "minimal_gaming",
            }
        )
        events.append(
            {
                "event_type": "WAGER",
                "player_id": pid,
                "transaction_id": duuid(f"mingame-wag-{seed}"),
                "amount": 50.0,
                "currency": "USD",
                "game_type": "SLOTS",
                "timestamp": (now - timedelta(hours=1, minutes=30)).isoformat(),
                "scenario_tag": "minimal_gaming",
            }
        )
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"mingame-wd-{seed}"),
                "transaction_type": "WITHDRAWAL",
                "amount": 24900.0,
                "currency": "USD",
                "channel": "ONLINE",
                "timestamp": (now - timedelta(minutes=20)).isoformat(),
                "scenario_tag": "minimal_gaming",
            }
        )
    elif scenario == "rapid_withdrawal":
        pid = player_id(seed, 102)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"rapid-dep-{seed}"),
                "transaction_type": "DEPOSIT",
                "amount": 15000.0,
                "currency": "USD",
                "channel": "MOBILE",
                "timestamp": (now - timedelta(minutes=8)).isoformat(),
                "scenario_tag": "rapid_withdrawal",
            }
        )
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"rapid-wd-{seed}"),
                "transaction_type": "WITHDRAWAL",
                "amount": 14950.0,
                "currency": "USD",
                "channel": "MOBILE",
                "timestamp": (now - timedelta(minutes=2)).isoformat(),
                "scenario_tag": "rapid_withdrawal",
            }
        )
    elif scenario == "shared_device":
        for i in range(10, 15):
            pid = player_id(seed, i)
            events.append(
                {
                    "event_type": "TRANSACTION",
                    "player_id": pid,
                    "transaction_id": duuid(f"shared-{seed}-{i}"),
                    "transaction_type": "DEPOSIT",
                    "amount": 3000.0,
                    "currency": "USD",
                    "channel": "MOBILE",
                    "device_fingerprint": f"shared-device-{seed}",
                    "timestamp": (now - timedelta(minutes=30 - i)).isoformat(),
                    "scenario_tag": "shared_device",
                }
            )
    elif scenario == "behavioral_deviation":
        pid = player_id(seed, 103)
        # baseline small txs then huge spike
        for i in range(5):
            events.append(
                {
                    "event_type": "TRANSACTION",
                    "player_id": pid,
                    "transaction_id": duuid(f"behav-base-{seed}-{i}"),
                    "transaction_type": "DEPOSIT",
                    "amount": 100.0,
                    "currency": "USD",
                    "channel": "ONLINE",
                    "timestamp": (now - timedelta(days=10 - i)).isoformat(),
                    "scenario_tag": "behavioral_deviation",
                }
            )
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"behav-spike-{seed}"),
                "transaction_type": "DEPOSIT",
                "amount": 48000.0,
                "currency": "USD",
                "channel": "CAGE",
                "timestamp": now.isoformat(),
                "scenario_tag": "behavioral_deviation",
            }
        )
    elif scenario == "rapid_wire":
        pid = player_id(seed, 110)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"wire-in-{seed}"),
                "transaction_type": "WIRE_IN",
                "amount": 25000.0,
                "currency": "USD",
                "channel": "WIRE",
                "payment_method": "WIRE",
                "timestamp": (now - timedelta(minutes=40)).isoformat(),
                "scenario_tag": "rapid_wire",
            }
        )
        events.append(
            {
                "event_type": "WAGER",
                "player_id": pid,
                "transaction_id": duuid(f"wire-wag-{seed}"),
                "amount": 200.0,
                "currency": "USD",
                "timestamp": (now - timedelta(minutes=25)).isoformat(),
                "scenario_tag": "rapid_wire",
            }
        )
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"wire-cashout-{seed}"),
                "transaction_type": "CASH_OUT",
                "amount": 18000.0,
                "currency": "USD",
                "channel": "CAGE",
                "payment_method": "CASH",
                "location": "Cage-A",
                "timestamp": (now - timedelta(minutes=5)).isoformat(),
                "scenario_tag": "rapid_wire",
            }
        )
    elif scenario == "tito":
        pid = player_id(seed, 111)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"tito-issue-{seed}"),
                "transaction_type": "TITO_ISSUE",
                "amount": 7500.0,
                "currency": "USD",
                "channel": "FLOOR",
                "timestamp": (now - timedelta(minutes=15)).isoformat(),
                "scenario_tag": "tito",
            }
        )
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"tito-redeem-{seed}"),
                "transaction_type": "TITO_REDEEM",
                "amount": 7200.0,
                "currency": "USD",
                "channel": "FLOOR",
                "timestamp": now.isoformat(),
                "scenario_tag": "tito",
            }
        )
    elif scenario == "high_velocity":
        pid = player_id(seed, 112)
        for i in range(6):
            events.append(
                {
                    "event_type": "TRANSACTION",
                    "player_id": pid,
                    "transaction_id": duuid(f"vel-{seed}-{i}"),
                    "transaction_type": "CASH_IN",
                    "amount": 1200.0 + i * 50,
                    "currency": "USD",
                    "channel": "CAGE",
                    "payment_method": "CASH",
                    "location": f"Cage-{i % 3}",
                    "timestamp": (now - timedelta(minutes=50 - i * 7)).isoformat(),
                    "scenario_tag": "high_velocity",
                }
            )
    elif scenario == "watchlist":
        pid = player_id(seed, 113)
        events.append(
            {
                "event_type": "TRANSACTION",
                "player_id": pid,
                "transaction_id": duuid(f"wl-cash-{seed}"),
                "transaction_type": "CASH_IN",
                "amount": 15000.0,
                "currency": "USD",
                "channel": "CAGE",
                "payment_method": "CASH",
                "location": "Cage-VIP",
                "watchlist_hit": True,
                "timestamp": now.isoformat(),
                "scenario_tag": "watchlist",
            }
        )
    else:
        raise ValueError(f"Unknown scenario: {scenario}")
    return events


def inject_postgres(events: list[dict[str, Any]]) -> None:
    """Write scenario events into A's VARCHAR schema (for local CDC testing)."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", os.getenv("POSTGRES_HOST", "casino-db")),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "casino"),
        user=os.getenv("POSTGRES_USER", "casino"),
        password=os.getenv("POSTGRES_PASSWORD", "casino"),
    )
    try:
        with conn.cursor() as cur:
            for e in events:
                et = (e.get("event_type") or "TRANSACTION").upper()
                if et not in ("TRANSACTION", "WAGER", "CHIP", "CAGE"):
                    # Still persist generic money moves as transactions
                    if "amount" not in e:
                        continue
                pid = str(e["player_id"])[:32]
                tid = str(e.get("transaction_id") or vid(f"auto-{pid}-{e.get('timestamp')}"))[:64]
                tx_type = (e.get("transaction_type") or e.get("event_type") or "CASH_IN").upper()
                if tx_type in ("DEPOSIT", "TRANSACTION"):
                    tx_type = "CASH_IN"
                elif tx_type == "WITHDRAWAL":
                    tx_type = "CASH_OUT"
                amount = float(e.get("amount") or 0)
                loc = e.get("location")
                pm = e.get("payment_method") or e.get("channel") or "CASH"
                ts = e["timestamp"]
                if et == "WAGER" or tx_type == "WAGER":
                    cur.execute(
                        """
                        INSERT INTO wagers (
                            wager_id, player_id, session_id, game_type, amount, win_amount, location, created_at
                        ) VALUES (%s, %s, %s, %s, %s, 0, %s, %s::timestamptz)
                        ON CONFLICT (wager_id) DO NOTHING
                        """,
                        (tid, pid, f"SES-{pid}"[:64], e.get("game_type") or "SLOTS", amount, loc, ts),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            transaction_id, player_id, txn_type, amount, currency,
                            location, payment_method, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::timestamptz)
                        ON CONFLICT (transaction_id) DO UPDATE SET amount = EXCLUDED.amount
                        """,
                        (tid, pid, tx_type, amount, e.get("currency", "USD"), loc, pm, ts),
                    )
                    if tx_type in ("CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT") or str(pm).upper() == "CASH":
                        cur.execute(
                            """
                            INSERT INTO cage_transactions (
                                cage_txn_id, player_id, direction, amount, cage_window, teller_id, created_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s::timestamptz)
                            ON CONFLICT (cage_txn_id) DO NOTHING
                            """,
                            (
                                ("CAG-" + tid[4:])[:64] if tid.startswith("TXN-") else f"CAG-{tid}"[:64],
                                pid,
                                tx_type if tx_type in ("CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT") else "CASH_IN",
                                amount,
                                loc or "Cage",
                                "T-DEMO",
                                ts,
                            ),
                        )
                if e.get("scenario_tag") == "shared_device" and e.get("device_fingerprint"):
                    cur.execute(
                        """
                        INSERT INTO devices (device_id, player_id, device_type, fingerprint, last_seen_at)
                        VALUES (%s, %s, 'MOBILE', %s, now())
                        ON CONFLICT (device_id) DO UPDATE SET fingerprint = EXCLUDED.fingerprint, last_seen_at = now()
                        """,
                        (f"DEV-{pid}"[:32], pid, e["device_fingerprint"]),
                    )
        conn.commit()
    finally:
        conn.close()


def inject_api(events: list[dict[str, Any]], api_url: str) -> dict[str, Any]:
    import urllib.request

    req = urllib.request.Request(
        f"{api_url.rstrip('/')}/api/v1/demo/inject",
        data=json.dumps({"events": events}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=list(EXPECTED.keys()))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mode", choices=["print", "postgres", "api"], default="print")
    parser.add_argument("--api-url", default=os.getenv("API_URL", "http://localhost:8080"))
    args = parser.parse_args()

    events = build_events(args.scenario, args.seed)
    meta = EXPECTED[args.scenario]
    result = {"scenario": args.scenario, "expected": meta, "events": events}

    if args.mode == "print":
        print(json.dumps(result, indent=2))
    elif args.mode == "postgres":
        inject_postgres(events)
        print(json.dumps({"injected": len(events), "via": "postgres", **meta}, indent=2))
    else:
        resp = inject_api(events, args.api_url)
        print(json.dumps({"injected": len(events), "via": "api", "response": resp, **meta}, indent=2))


if __name__ == "__main__":
    main()
