"""Shared Pydantic event and domain models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertStatus(str, Enum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    ESCALATED = "ESCALATED"
    CLOSED = "CLOSED"
    FALSE_POSITIVE = "FALSE_POSITIVE"


class TransactionType(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    BUY_IN = "BUY_IN"
    CASH_OUT = "CASH_OUT"
    TRANSFER = "TRANSFER"


class NormalizedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    event_source: str
    player_id: Optional[str] = None
    transaction_id: Optional[str] = None
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime
    amount: Optional[float] = None
    currency: str = "USD"
    channel: Optional[str] = None
    device_id: Optional[str] = None
    payment_method_id: Optional[str] = None
    location: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class EnrichedEvent(NormalizedEvent):
    kyc_tier: Optional[str] = None
    player_risk_score: Optional[float] = None
    account_age_days: Optional[int] = None
    jurisdiction: Optional[str] = None
    is_pep: bool = False
    features: dict[str, Any] = Field(default_factory=dict)


class FeatureVector(BaseModel):
    player_id: str
    transaction_id: Optional[str] = None
    event_id: str
    correlation_id: str
    timestamp: datetime
    window_5m: dict[str, float] = Field(default_factory=dict)
    window_1h: dict[str, float] = Field(default_factory=dict)
    window_24h: dict[str, float] = Field(default_factory=dict)
    window_7d: dict[str, float] = Field(default_factory=dict)
    window_30d: dict[str, float] = Field(default_factory=dict)
    static: dict[str, float] = Field(default_factory=dict)

    def flatten(self) -> dict[str, float]:
        out: dict[str, float] = {}
        for prefix, window in (
            ("w5m", self.window_5m),
            ("w1h", self.window_1h),
            ("w24h", self.window_24h),
            ("w7d", self.window_7d),
            ("w30d", self.window_30d),
            ("static", self.static),
        ):
            for k, v in window.items():
                out[f"{prefix}_{k}"] = float(v)
        return out


class RuleHit(BaseModel):
    rule_id: str
    rule_name: str
    severity: RiskLevel
    score: float
    description: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class RuleResult(BaseModel):
    event_id: str
    player_id: str
    transaction_id: Optional[str] = None
    correlation_id: str
    timestamp: datetime
    hits: list[RuleHit] = Field(default_factory=list)
    aggregate_score: float = 0.0


class MLPrediction(BaseModel):
    event_id: str
    player_id: str
    transaction_id: Optional[str] = None
    correlation_id: str
    timestamp: datetime
    model_name: str
    model_version: str
    score: float
    label: str
    features_used: list[str] = Field(default_factory=list)
    shap_top: list[dict[str, Any]] = Field(default_factory=list)


class BehavioralScore(BaseModel):
    event_id: str
    player_id: str
    correlation_id: str
    timestamp: datetime
    adapter: str
    score: float
    anomaly_flags: list[str] = Field(default_factory=list)
    explanation: dict[str, Any] = Field(default_factory=dict)


class RiskAssessment(BaseModel):
    event_id: str
    player_id: str
    transaction_id: Optional[str] = None
    correlation_id: str
    timestamp: datetime
    rules_score: float = 0.0
    ml_score: float = 0.0
    behavioral_score: float = 0.0
    final_score: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    rule_hits: list[RuleHit] = Field(default_factory=list)
    explanations: list[str] = Field(default_factory=list)
    weights: dict[str, float] = Field(default_factory=dict)


class Alert(BaseModel):
    alert_id: str = Field(default_factory=lambda: str(uuid4()))
    player_id: str
    transaction_id: Optional[str] = None
    event_id: Optional[str] = None
    correlation_id: str
    created_at: datetime
    updated_at: datetime
    risk_level: RiskLevel
    status: AlertStatus = AlertStatus.OPEN
    title: str
    summary: str
    score: float
    rule_hits: list[RuleHit] = Field(default_factory=list)
    explanations: list[str] = Field(default_factory=list)
    assigned_to: Optional[str] = None
    case_id: Optional[str] = None
