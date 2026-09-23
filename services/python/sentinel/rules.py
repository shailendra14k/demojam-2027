"""Deterministic AML rules from the Casino Sentinel architecture."""

from __future__ import annotations

from .models import EnrichedEvent, PlayerFeatures, PlayerProfile

CTR_THRESHOLD = 10_000.0
STRUCTURING_LOW = 8_000.0
STRUCTURING_HIGH = 9_999.0


def evaluate_rules(
    event: EnrichedEvent,
    features: PlayerFeatures,
    profile: PlayerProfile | None,
) -> tuple[int, list[str], list[dict]]:
    matched: list[str] = []
    factors: list[dict] = []
    score = 0

    amount = float(event.amount or 0)
    cash_types = {"CASH_IN", "CASH_OUT"}
    is_cash = event.canonical_type in cash_types or (event.payment_method or "").upper() == "CASH"

    if is_cash and amount >= CTR_THRESHOLD:
        matched.append("CTR_THRESHOLD")
        score = max(score, 92)
        factors.append({"rule": "CTR_THRESHOLD", "detail": f"Single cash {amount:,.0f} exceeds $10,000"})

    if features.cash_in_24h >= CTR_THRESHOLD or features.cash_out_24h >= CTR_THRESHOLD:
        matched.append("CTR_THRESHOLD_AGGREGATE")
        score = max(score, 88)
        factors.append(
            {
                "rule": "CTR_THRESHOLD_AGGREGATE",
                "detail": (
                    f"24h cash-in ${features.cash_in_24h:,.0f} / "
                    f"cash-out ${features.cash_out_24h:,.0f}"
                ),
            }
        )

    just_below = STRUCTURING_LOW <= amount < STRUCTURING_HIGH and is_cash
    if just_below or features.just_below_10k_count_24h >= 2:
        if features.cage_visits_24h >= 2 or features.just_below_10k_count_24h >= 2:
            matched.append("STRUCTURING")
            score = max(score, 86)
            factors.append(
                {
                    "rule": "STRUCTURING",
                    "detail": (
                        f"{features.just_below_10k_count_24h} sub-$10k cash moves "
                        f"across {features.cage_visits_24h} cage visits"
                    ),
                }
            )

    ratio = features.wager_to_buyin_ratio
    if features.cash_in_24h >= 5_000 and ratio is not None and ratio < 0.10:
        if features.cash_out_24h >= features.cash_in_24h * 0.80:
            matched.append("MINIMAL_PLAY")
            score = max(score, 84)
            factors.append(
                {
                    "rule": "MINIMAL_PLAY",
                    "detail": (
                        f"Buy-in ${features.cash_in_24h:,.0f} with "
                        f"{ratio * 100:.1f}% wagered then heavy cash-out"
                    ),
                }
            )

    if features.txn_count_1h >= 5 or features.cage_visits_24h >= 6:
        matched.append("HIGH_VELOCITY")
        score = max(score, 72)
        factors.append(
            {
                "rule": "HIGH_VELOCITY",
                "detail": f"{features.txn_count_1h} txns/hour, {features.cage_visits_24h} cage visits/24h",
            }
        )

    if features.unique_payment_methods_24h >= 3:
        matched.append("MULTI_INSTRUMENT")
        score = max(score, 68)
        factors.append(
            {
                "rule": "MULTI_INSTRUMENT",
                "detail": f"{features.unique_payment_methods_24h} payment instruments in 24h",
            }
        )

    if event.canonical_type == "WIRE_IN" and amount >= 15_000:
        matched.append("RAPID_WIRE")
        score = max(score, 78)
        factors.append({"rule": "RAPID_WIRE", "detail": f"Large wire-in ${amount:,.0f}"})

    if features.wire_in_24h >= 10_000 and features.cash_out_24h >= features.wire_in_24h * 0.6:
        if (ratio or 0) < 0.25:
            matched.append("RAPID_WIRE")
            score = max(score, 88)
            factors.append(
                {
                    "rule": "RAPID_WIRE",
                    "detail": "Wire-in followed by cash-out with little gaming",
                }
            )

    if event.canonical_type in {"TITO_ISSUE", "TITO_REDEEM"} and amount >= 5_000:
        matched.append("TITO")
        score = max(score, 55)
        factors.append({"rule": "TITO", "detail": f"Large TITO {event.canonical_type} ${amount:,.0f}"})

    if profile and profile.watchlist_hit:
        matched.append("WATCHLIST_MATCH")
        score = max(score, 98)
        factors.append({"rule": "WATCHLIST_MATCH", "detail": "Player matches watch-list / enhanced KYC"})

    if profile and profile.pep_flag:
        matched.append("PEP")
        score = max(score, max(score, 70) + 8)
        score = min(score, 100)
        factors.append({"rule": "PEP", "detail": "Politically exposed person"})

    if not matched:
        score = min(18, int(min(amount, 4000) / 4000 * 18))

    return min(100, score), matched, factors


def status_tag(matched: list[str], canonical_type: str) -> str:
    if "STRUCTURING" in matched:
        return "STRUCTURING"
    if "RAPID_WIRE" in matched:
        return "RAPID_WIRE"
    if "WATCHLIST_MATCH" in matched or "PEP" in matched:
        return "WATCHLIST"
    if "MINIMAL_PLAY" in matched:
        return "MINIMAL_PLAY"
    if "HIGH_VELOCITY" in matched:
        return "VELOCITY"
    if "TITO" in matched or canonical_type.startswith("TITO"):
        return "TITO"
    if "CTR_THRESHOLD" in matched:
        return "CTR"
    return "NORMAL"
