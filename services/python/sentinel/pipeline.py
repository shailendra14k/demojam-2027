"""CDC → normalize → enrich → features → rules/ML/behavioral/KYC → aggregate → alerts.

Each stage publishes to Kafka so the Streams Console shows the architecture hop-by-hop.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from . import aggregator, behavioral, db, kafka_util, ml, rules
from .config import settings
from .features import FeatureEngine
from .models import EnrichedEvent, NormalizedEvent, PlayerProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sentinel.pipeline")

CDC_TOPICS = [
    "casino.cdc.players",
    "casino.cdc.player_accounts",
    "casino.cdc.kyc",
    "casino.cdc.kyc_profiles",
    "casino.cdc.transactions",
    "casino.cdc.wagers",
    "casino.cdc.chips",
    "casino.cdc.chip_transactions",
    "casino.cdc.cage",
    "casino.cdc.cage_transactions",
    "casino.cdc.gaming_sessions",
    "casino.cdc.payment_methods",
    "casino.cdc.devices",
    # Debezium default names when RegexRouter is missing (prefix casino.cdc + schema)
    "casino.cdc.public.players",
    "casino.cdc.public.player_accounts",
    "casino.cdc.public.kyc_profiles",
    "casino.cdc.public.transactions",
    "casino.cdc.public.wagers",
    "casino.cdc.public.chip_transactions",
    "casino.cdc.public.cage_transactions",
    "casino.cdc.public.gaming_sessions",
    "casino.cdc.public.payment_methods",
    "casino.cdc.public.devices",
]

TABLE_TYPE = {
    "players": None,
    "player_accounts": None,
    "kyc_profiles": None,
    "payment_methods": None,
    "devices": None,
    "transactions": "TRANSACTION",
    "wagers": "WAGER",
    "gaming_sessions": "SESSION",
    "chip_transactions": "CHIP",
    "cage_transactions": "CAGE",
}

TYPE_MAP = {
    "CASH_IN": "CASH_IN",
    "CASH_OUT": "CASH_OUT",
    "WIRE_IN": "WIRE_IN",
    "WIRE_OUT": "WIRE_OUT",
    "BUY_IN": "CASH_IN",
    "CHIP_PURCHASE": "CHIP_BUY",
    "CHIP_REDEMPTION": "CHIP_REDEEM",
    "CHIP_BUY": "CHIP_BUY",
    "CHIP_REDEEM": "CHIP_REDEEM",
    "TITO_ISSUE": "TITO_ISSUE",
    "TITO_REDEEM": "TITO_REDEEM",
    "BLACKJACK": "BLACKJACK",
    "SLOTS": "SLOTS",
    "ROULETTE": "ROULETTE",
    "POKER": "POKER",
    "WAGER": "GAMING_WAGER",
}

ACTION_LABEL = {
    "CASH_IN": "Cash In",
    "CASH_OUT": "Cash Out",
    "WIRE_IN": "Wire In",
    "WIRE_OUT": "Wire Out",
    "TITO_ISSUE": "TITO Issue",
    "TITO_REDEEM": "TITO Redeem",
    "BLACKJACK": "Blackjack",
    "SLOTS": "Slots",
    "ROULETTE": "Roulette",
    "POKER": "Poker",
    "GAMING_WAGER": "Wager",
    "CHIP_BUY": "Chip Buy",
    "CHIP_REDEEM": "Chip Redeem",
}


class Pipeline:
    def __init__(self) -> None:
        self.profiles: dict[str, PlayerProfile] = {}
        self.engine = FeatureEngine()
        self.prod = None

    def load_profiles(self) -> None:
        rows = db.fetch_all(
            """
            SELECT p.player_id, p.first_name, p.last_name, p.country, p.pep_flag, p.risk_tier,
                   COALESCE(k.kyc_status, 'VERIFIED') AS kyc_status,
                   COALESCE(k.risk_rating, 'LOW') AS kyc_risk_rating,
                   COALESCE(k.watchlist_hit, FALSE) AS watchlist_hit,
                   COALESCE(k.prior_alerts, 0) AS prior_alerts
            FROM players p
            LEFT JOIN kyc_profiles k ON k.player_id = p.player_id
            """
        )
        for r in rows:
            self.profiles[r["player_id"]] = PlayerProfile(
                player_id=r["player_id"],
                first_name=r["first_name"],
                last_name=r["last_name"],
                country=r["country"],
                pep_flag=bool(r["pep_flag"]),
                risk_tier=r["risk_tier"],
                kyc_status=r["kyc_status"],
                kyc_risk_rating=r["kyc_risk_rating"],
                watchlist_hit=bool(r["watchlist_hit"]),
                prior_alerts=int(r["prior_alerts"] or 0),
            )
        pay = db.fetch_all("SELECT player_id, method_type FROM payment_methods")
        for r in pay:
            prof = self.profiles.get(r["player_id"])
            if prof and r["method_type"] not in prof.payment_methods:
                prof.payment_methods.append(r["method_type"])
        log.info("Loaded %s player profiles", len(self.profiles))

    def handle(self, topic: str, value: Any) -> None:
        payload = _unwrap(value)
        if not payload:
            return
        table = _table_of(topic, payload)
        if table in {"cage_transactions", "chip_transactions", "gaming_sessions", "payment_methods", "devices", "player_accounts"}:
            return
        if table in {"players", "kyc_profiles"}:
            self._upsert_profile(table, payload)
            kafka_util.publish(self.prod, "casino.cdc.kyc" if table == "kyc_profiles" else "casino.cdc.players", payload.get("player_id", ""), payload)
            return
        if table in {"payment_methods", "devices", "player_accounts", "gaming_sessions"}:
            return

        event = self._normalize(table, payload)
        if event is None:
            return
        if db.fetch_one("SELECT event_id FROM aml.events WHERE event_id = %s", (event.event_id,)):
            return

        cdc_topic = {
            "transactions": "casino.cdc.transactions",
            "wagers": "casino.cdc.wagers",
            "chip_transactions": "casino.cdc.chips",
            "cage_transactions": "casino.cdc.cage",
        }.get(table)
        if cdc_topic:
            kafka_util.publish(self.prod, cdc_topic, event.player_id, payload)

        if self.prod:
            kafka_util.publish(self.prod, "casino.events.normalized", event.player_id, event.model_dump(mode="json"))

        profile = self.profiles.get(event.player_id)
        enriched = EnrichedEvent(
            **event.model_dump(),
            player_name=profile.full_name if profile else event.player_id,
            country=profile.country if profile else "",
            pep_flag=profile.pep_flag if profile else False,
            kyc_status=profile.kyc_status if profile else "VERIFIED",
            kyc_risk_rating=profile.kyc_risk_rating if profile else "LOW",
            watchlist_hit=profile.watchlist_hit if profile else False,
            prior_alerts=profile.prior_alerts if profile else 0,
        )
        kafka_util.publish(self.prod, "casino.events.enriched", enriched.player_id, enriched.model_dump(mode="json"))

        feat = self.engine.update(enriched)
        kafka_util.publish(self.prod, "aml.features", feat.player_id, feat.model_dump())

        rule_score, matched, rule_factors = rules.evaluate_rules(enriched, feat, profile)
        ml_score, ml_factors = ml.score_xgboost(enriched, feat, profile)
        beh_score, beh_factors = behavioral.score_behavioral(enriched, feat, profile)
        kyc = aggregator.kyc_score(
            watchlist=enriched.watchlist_hit,
            pep=enriched.pep_flag,
            rating=enriched.kyc_risk_rating,
            prior_alerts=enriched.prior_alerts,
        )
        breakdown = aggregator.aggregate(
            rule_score, ml_score, beh_score, kyc, matched, rule_factors + ml_factors + beh_factors
        )

        kafka_util.publish(self.prod, "aml.scores.rules", enriched.player_id, {"score": rule_score, "matched": matched})
        kafka_util.publish(self.prod, "aml.scores.ml", enriched.player_id, {"score": ml_score})
        kafka_util.publish(self.prod, "aml.scores.behavioral", enriched.player_id, {"score": beh_score})
        kafka_util.publish(self.prod, "aml.scores.kyc", enriched.player_id, {"score": kyc})

        tag = rules.status_tag(matched, enriched.canonical_type)
        self._persist_event(enriched, tag, breakdown.final)
        self._persist_score(enriched, breakdown)

        if breakdown.final >= 40 and matched:
            self._persist_alert(enriched, breakdown, tag)
            kafka_util.publish(
                self.prod,
                "aml.alerts",
                enriched.player_id,
                {
                    "player_id": enriched.player_id,
                    "player_name": enriched.player_name,
                    "risk_score": breakdown.final,
                    "tier": breakdown.tier,
                    "matched_rules": matched,
                    "explanation": breakdown.explanation,
                },
            )
            if breakdown.tier in {"HIGH", "CRITICAL"}:
                self._ensure_case(enriched, breakdown)

        kafka_util.publish(
            self.prod,
            "aml.player.risk_profiles",
            enriched.player_id,
            {"player_id": enriched.player_id, "risk_score": breakdown.final, "tier": breakdown.tier},
        )
        self._touch_pipeline()

    def _normalize(self, table: str, payload: dict) -> NormalizedEvent | None:
        player_id = payload.get("player_id")
        if not player_id:
            return None
        amount = float(payload.get("amount") or payload.get("total_wagered") or 0)
        raw_type = (
            payload.get("txn_type")
            or payload.get("direction")
            or payload.get("game_type")
            or table
        )
        canonical = TYPE_MAP.get(str(raw_type).upper(), str(raw_type).upper())
        location = payload.get("location") or payload.get("cage_window") or ""
        method = payload.get("payment_method") or payload.get("method_type") or ""
        ts = _parse_ts(payload.get("created_at") or payload.get("started_at"))
        return NormalizedEvent(
            event_id=str(
                payload.get("transaction_id")
                or payload.get("wager_id")
                or payload.get("chip_txn_id")
                or payload.get("cage_txn_id")
                or uuid.uuid4()
            ),
            player_id=str(player_id),
            source_table=table,
            canonical_type=canonical,
            amount=amount,
            location=str(location or ""),
            payment_method=str(method or ""),
            created_at=ts,
            raw=payload,
        )

    def _upsert_profile(self, table: str, payload: dict) -> None:
        pid = payload.get("player_id")
        if not pid:
            return
        current = self.profiles.get(pid) or PlayerProfile(player_id=pid)
        if table == "players":
            current.first_name = payload.get("first_name") or current.first_name
            current.last_name = payload.get("last_name") or current.last_name
            current.country = payload.get("country") or current.country
            current.pep_flag = bool(payload.get("pep_flag", current.pep_flag))
            current.risk_tier = payload.get("risk_tier") or current.risk_tier
        else:
            current.kyc_status = payload.get("kyc_status") or current.kyc_status
            current.kyc_risk_rating = payload.get("risk_rating") or current.kyc_risk_rating
            current.watchlist_hit = bool(payload.get("watchlist_hit", current.watchlist_hit))
            current.prior_alerts = int(payload.get("prior_alerts") or current.prior_alerts)
        self.profiles[pid] = current

    def _persist_event(self, ev: EnrichedEvent, tag: str, score: int) -> None:
        db.execute(
            """
            INSERT INTO aml.events
              (event_id, player_id, event_type, amount, location, status_tag, risk_score, payload, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                ev.event_id,
                ev.player_id,
                ACTION_LABEL.get(ev.canonical_type, ev.canonical_type.replace("_", " ").title()),
                ev.amount,
                ev.location,
                tag,
                score,
                db.Jsonb(ev.model_dump(mode="json")),
                ev.created_at,
            ),
        )

    def _persist_score(self, ev: EnrichedEvent, bd) -> None:
        db.execute(
            """
            INSERT INTO aml.player_scores
              (player_id, player_name, country, risk_score, risk_tier, month_volume, explanation, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (player_id) DO UPDATE SET
              risk_score = EXCLUDED.risk_score,
              risk_tier = EXCLUDED.risk_tier,
              month_volume = aml.player_scores.month_volume + EXCLUDED.month_volume,
              explanation = EXCLUDED.explanation,
              updated_at = now()
            """,
            (ev.player_id, ev.player_name, ev.country, bd.final, bd.tier, ev.amount, bd.explanation),
        )

    def _persist_alert(self, ev: EnrichedEvent, bd, tag: str) -> None:
        alert_id = f"ALT-{uuid.uuid4().hex[:10].upper()}"
        db.execute(
            """
            INSERT INTO aml.alerts
              (alert_id, player_id, player_name, alert_type, priority, risk_score,
               rule_score, ml_score, behavioral_score, kyc_score, matched_rules,
               explanation, factors, triggering_ids, status, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN', now())
            """,
            (
                alert_id,
                ev.player_id,
                ev.player_name,
                tag,
                bd.tier,
                bd.final,
                bd.rules,
                bd.xgboost,
                bd.behavioral,
                bd.kyc,
                bd.matched_rules,
                bd.explanation,
                db.Jsonb(bd.factors),
                [ev.event_id],
            ),
        )

    def _ensure_case(self, ev: EnrichedEvent, bd) -> None:
        existing = db.fetch_one(
            """
            SELECT case_id FROM aml.cases
            WHERE player_id = %s AND status NOT IN ('CLOSED', 'FILED')
            ORDER BY created_at DESC LIMIT 1
            """,
            (ev.player_id,),
        )
        if existing:
            return
        case_id = f"CASE-{uuid.uuid4().hex[:8].upper()}"
        db.execute(
            """
            INSERT INTO aml.cases (case_id, player_id, player_name, title, priority, status, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,'NEW', now(), now())
            """,
            (case_id, ev.player_id, ev.player_name, f"{bd.matched_rules[0]} — {ev.player_name}", bd.tier),
        )

    def _touch_pipeline(self) -> None:
        db.execute(
            """
            INSERT INTO aml.pipeline_status (component, status, detail, updated_at)
            VALUES ('debezium','ONLINE','CDC events flowing', now()),
                   ('kafka','ONLINE','AMQ Streams', now()),
                   ('rules','ONLINE','Rules engine evaluating', now()),
                   ('scoring','ONLINE','Risk aggregation', now()),
                   ('alert','ONLINE','Alerts persisted', now())
            ON CONFLICT (component) DO UPDATE SET status = EXCLUDED.status, detail = EXCLUDED.detail, updated_at = now()
            """
        )


def _unwrap(value: Any) -> dict | None:
    if not isinstance(value, dict):
        return None
    if "after" in value and isinstance(value.get("after"), dict):
        row = dict(value["after"])
        row["_op"] = value.get("op")
        src = value.get("source") or {}
        row["_table"] = src.get("table")
        return row
    if "payload" in value and isinstance(value["payload"], dict):
        return _unwrap(value["payload"])
    return value


def _table_of(topic: str, payload: dict) -> str:
    if payload.get("_table"):
        return str(payload["_table"])
    name = topic.split(".")[-1]
    aliases = {
        "kyc": "kyc_profiles",
        "chips": "chip_transactions",
        "cage": "cage_transactions",
        "players": "players",
        "transactions": "transactions",
        "wagers": "wagers",
    }
    return aliases.get(name, name)


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # Debezium microseconds or millis
        n = float(value)
        if n > 1e15:
            n = n / 1e6
        elif n > 1e12:
            n = n / 1e3
        return datetime.fromtimestamp(n / 1000 if n > 1e11 else n, tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _poll_postgres(pipe: Pipeline) -> None:
    """Fallback ingest when DIRECT_POLL=true (local / no CDC). OpenShift uses Kafka CDC."""
    seen: set[str] = set()
    log.info("Direct Postgres poller enabled")
    while True:
        try:
            rows = db.fetch_all(
                """
                SELECT transaction_id AS id, player_id, txn_type, amount, location, payment_method, created_at, 'transactions' AS _table
                FROM transactions
                ORDER BY created_at DESC
                LIMIT 40
                """
            )
            rows += db.fetch_all(
                """
                SELECT wager_id AS id, player_id, game_type AS txn_type, amount, location,
                       NULL AS payment_method, created_at, 'wagers' AS _table
                FROM wagers
                ORDER BY created_at DESC
                LIMIT 20
                """
            )
            for row in reversed(rows):
                rid = str(row["id"])
                if rid in seen:
                    continue
                seen.add(rid)
                payload = dict(row)
                payload["_table"] = row["_table"]
                pipe.handle("casino.cdc." + row["_table"], payload)
            if len(seen) > 5000:
                seen = set(list(seen)[-2000:])
        except Exception as exc:  # noqa: BLE001
            log.warning("poller: %s", exc)
        time.sleep(1.5)


def main() -> None:
    log.info("Casino Sentinel pipeline starting")
    db.wait_for_db()
    pipe = Pipeline()
    pipe.load_profiles()

    kafka_ok = False
    if settings.kafka_optional:
        try:
            kafka_util.wait_for_kafka(retries=15, delay=2.0)
            kafka_util.ensure_topics()
            pipe.prod = kafka_util.producer()
            kafka_ok = True
        except Exception as exc:  # noqa: BLE001
            log.warning("Kafka not ready (%s) — continuing with Postgres poll only", exc)
            pipe.prod = _NoopProducer()
    else:
        kafka_util.wait_for_kafka()
        kafka_util.ensure_topics()
        pipe.prod = kafka_util.producer()
        kafka_ok = True

    if kafka_ok:
        db.execute(
            """
            INSERT INTO aml.pipeline_status (component, status, detail, updated_at)
            VALUES ('kafka','ONLINE','AMQ Streams connected', now())
            ON CONFLICT (component) DO UPDATE SET status='ONLINE', updated_at=now()
            """
        )

    import threading

    if settings.direct_poll:
        threading.Thread(target=_poll_postgres, args=(pipe,), daemon=True).start()

    if kafka_ok:
        log.info("Consuming CDC topics: %s", CDC_TOPICS)
        kafka_util.consume_forever(CDC_TOPICS, "sentinel-pipeline", pipe.handle)
    else:
        log.info("Kafka unavailable; poller is the only ingest path")
        while True:
            time.sleep(30)


class _NoopProducer:
    def send(self, *args, **kwargs):
        return None

    def flush(self):
        return None


if __name__ == "__main__":
    main()
