from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class PlayerProfile(BaseModel):
    player_id: str
    first_name: str = ""
    last_name: str = ""
    country: str = "USA"
    pep_flag: bool = False
    risk_tier: str = "LOW"
    kyc_status: str = "VERIFIED"
    kyc_risk_rating: str = "LOW"
    watchlist_hit: bool = False
    prior_alerts: int = 0
    payment_methods: list[str] = Field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip() or self.player_id


class PlayerFeatures(BaseModel):
    player_id: str
    cash_in_1h: float = 0
    cash_out_1h: float = 0
    txn_count_1h: int = 0
    max_single_1h: float = 0
    cash_in_24h: float = 0
    cash_out_24h: float = 0
    txn_count_24h: int = 0
    cage_visits_24h: int = 0
    total_wagered_24h: float = 0
    total_won_24h: float = 0
    unique_locations_24h: int = 0
    unique_payment_methods_24h: int = 0
    just_below_10k_count_24h: int = 0
    wire_in_24h: float = 0
    wire_out_24h: float = 0
    month_volume: float = 0

    @property
    def wager_to_buyin_ratio(self) -> Optional[float]:
        if self.cash_in_24h <= 0:
            return None
        return self.total_wagered_24h / self.cash_in_24h


class NormalizedEvent(BaseModel):
    event_id: str
    player_id: str
    source_table: str
    canonical_type: str
    amount: float = 0
    currency: str = "USD"
    location: str = ""
    payment_method: str = ""
    created_at: datetime
    raw: dict[str, Any] = Field(default_factory=dict)


class EnrichedEvent(NormalizedEvent):
    player_name: str = ""
    country: str = ""
    pep_flag: bool = False
    kyc_status: str = "VERIFIED"
    kyc_risk_rating: str = "LOW"
    watchlist_hit: bool = False
    prior_alerts: int = 0


class ScoreBreakdown(BaseModel):
    rules: int
    xgboost: int
    behavioral: int
    kyc: int
    final: int
    tier: str
    matched_rules: list[str] = Field(default_factory=list)
    explanation: str = ""
    factors: list[dict[str, Any]] = Field(default_factory=list)


class AlertRecord(BaseModel):
    alert_id: str
    player_id: str
    player_name: str
    alert_type: str
    priority: str
    risk_score: int
    rule_score: int
    ml_score: int
    behavioral_score: int
    kyc_score: int
    matched_rules: list[str]
    explanation: str
    factors: list[dict[str, Any]]
    triggering_ids: list[str]
    status: str = "OPEN"
    created_at: datetime
