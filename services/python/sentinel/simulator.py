"""Background traffic generator — writes to PostgreSQL so Debezium emits CDC."""

from __future__ import annotations

import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path

from . import db
from .config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sentinel.simulator")

_CFG_DIR = Path(os.getenv("SIMULATOR_CONFIG_DIR", "/etc/casino-config"))

PLAYERS = [
    "C10281", "C10402", "C10811", "C10990", "C11012", "C11120",
    "C11201", "C11330", "C11440", "C11550", "C11660", "C11770",
]

NORMAL = [
    ("C11012", "BUY_IN", 80, 400, "Table 18", "CHIPS"),
    ("C11012", "BLACKJACK", 25, 200, "Table 18", "CHIPS"),
    ("C11120", "TITO_ISSUE", 20, 180, "Slot Bank 04", "TITO"),
    ("C11120", "SLOTS", 5, 40, "Slot Bank 04", "TITO"),
    ("C11550", "TITO_REDEEM", 40, 220, "Cage 01", "TITO"),
    ("C11660", "CASH_IN", 200, 800, "Cage 01", "CASH"),
    ("C10402", "CASH_IN", 300, 1500, "Cage 02", "CASH"),
    ("C11201", "BUY_IN", 100, 600, "Table 09", "CHIPS"),
    ("C11330", "ROULETTE", 50, 400, "Table 04", "CHIPS"),
]


def _read_file(name: str) -> str | None:
    path = _CFG_DIR / name
    try:
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return None


def simulator_interval_sec() -> float:
    raw = _read_file("SIMULATOR_INTERVAL_SEC") or os.getenv("SIMULATOR_INTERVAL_SEC")
    try:
        return max(1.0, float(raw if raw is not None else settings.simulator_interval_sec))
    except (TypeError, ValueError):
        return max(1.0, float(settings.simulator_interval_sec))


def simulator_enabled() -> bool:
    raw = (_read_file("SIMULATOR_ENABLED") or os.getenv("SIMULATOR_ENABLED") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return bool(settings.simulator_enabled)


def insert_transaction(player_id: str, txn_type: str, amount: float, location: str, method: str) -> str:
    txn_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
    db.execute(
        """
        INSERT INTO transactions
          (transaction_id, player_id, txn_type, amount, currency, location, payment_method, created_at)
        VALUES (%s,%s,%s,%s,'USD',%s,%s, now())
        """,
        (txn_id, player_id, txn_type, amount, location, method),
    )
    if txn_type in {"CASH_IN", "CASH_OUT", "WIRE_IN", "WIRE_OUT"}:
        db.execute(
            """
            INSERT INTO cage_transactions
              (cage_txn_id, player_id, direction, amount, cage_window, teller_id, created_at)
            VALUES (%s,%s,%s,%s,%s,%s, now())
            """,
            (f"CAG-{uuid.uuid4().hex[:12].upper()}", player_id, txn_type, amount, location, f"T-{random.randint(100,499)}"),
        )
    if txn_type in {"BLACKJACK", "SLOTS", "ROULETTE", "POKER"}:
        db.execute(
            """
            INSERT INTO wagers
              (wager_id, player_id, session_id, game_type, amount, win_amount, location, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s, now())
            """,
            (
                f"WGR-{uuid.uuid4().hex[:12].upper()}",
                player_id,
                f"SES-{player_id}",
                txn_type,
                amount,
                round(amount * random.choice([0, 0, 0.4, 1.6]), 2),
                location,
            ),
        )
    return txn_id


def run_scenario(name: str) -> dict:
    name = name.lower().replace("_", "-")
    created: list[str] = []
    if name in {"structuring", "structure"}:
        for loc, amt in (("Cage 03", 9800), ("Cage 07", 9500), ("Cage 05", 8900)):
            created.append(insert_transaction("C10281", "CASH_OUT", amt, loc, "CASH"))
            time.sleep(0.4)
        return {"scenario": "structuring", "playerId": "C10281", "transactions": created}

    if name in {"rapid-wire", "rapidwire", "wire"}:
        created.append(insert_transaction("C10811", "WIRE_IN", 25000, "Bank XYZ", "WIRE"))
        time.sleep(0.5)
        created.append(insert_transaction("C10811", "CASH_OUT", 18000, "Cage 02", "CASH"))
        return {"scenario": "rapid-wire", "playerId": "C10811", "transactions": created}

    if name in {"minimal-play", "minimal"}:
        created.append(insert_transaction("C11440", "CASH_IN", 20000, "Cage 04", "CASH"))
        time.sleep(0.3)
        created.append(insert_transaction("C11440", "BLACKJACK", 150, "Table 11", "CHIPS"))
        time.sleep(0.3)
        created.append(insert_transaction("C11440", "CASH_OUT", 19200, "Cage 04", "CASH"))
        return {"scenario": "minimal-play", "playerId": "C11440", "transactions": created}

    if name in {"velocity", "high-velocity"}:
        for i in range(6):
            created.append(insert_transaction("C10402", "CASH_IN", 1200 + i * 50, f"Cage 0{(i % 3) + 1}", "CASH"))
            time.sleep(0.2)
        return {"scenario": "high-velocity", "playerId": "C10402", "transactions": created}

    if name in {"watchlist", "pep"}:
        created.append(insert_transaction("C10990", "CASH_IN", 15000, "Cage 02", "CASH"))
        time.sleep(0.3)
        created.append(insert_transaction("C10990", "WIRE_OUT", 12000, "Wire Desk", "WIRE"))
        return {"scenario": "watchlist", "playerId": "C10990", "transactions": created}

    if name in {"tito"}:
        created.append(insert_transaction("C11120", "TITO_ISSUE", 6400, "Slot Bank 04", "TITO"))
        created.append(insert_transaction("C11550", "TITO_REDEEM", 6100, "Cage 01", "TITO"))
        return {"scenario": "tito", "playerId": "C11120", "transactions": created}

    if name in {"mixed", "demo", "start"}:
        results = [run_scenario(s) for s in ("structuring", "rapid-wire", "minimal-play")]
        return {"scenario": "mixed", "parts": results}

    raise ValueError(f"Unknown scenario: {name}")


def loop() -> None:
    last_logged: float | None = None
    while True:
        try:
            interval = simulator_interval_sec()
            enabled = simulator_enabled()
            if last_logged != interval:
                log.info("Simulator loop interval=%ss enabled=%s", interval, enabled)
                last_logged = interval
            if enabled:
                player, txn_type, lo, hi, loc, method = random.choice(NORMAL)
                amt = round(random.uniform(lo, hi), 2)
                txn_id = insert_transaction(player, txn_type, amt, loc, method)
                log.info("sim %s %s %s %s", txn_id, player, txn_type, amt)
            # Sleep in 1s chunks so ConfigMap / UI changes apply without long waits
            remaining = interval
            while remaining > 0:
                step = min(1.0, remaining)
                time.sleep(step)
                remaining -= step
                if simulator_interval_sec() != interval:
                    break
        except Exception as exc:  # noqa: BLE001
            log.warning("simulator tick failed: %s", exc)
            time.sleep(5)


def main() -> None:
    db.wait_for_db()
    log.info("Casino Sentinel simulator ready")
    loop()


if __name__ == "__main__":
    main()
