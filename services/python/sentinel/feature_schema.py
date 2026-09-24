"""Shared AML feature column schema (B / feature-engine flatten() format).

Used by offline training (`model/train_models.py`) and online scoring
(`sentinel.aml_model`). Keep these names aligned with feature-engine windows.
"""

from __future__ import annotations

FEATURE_COLUMNS: list[str] = [
    "w5m_tx_count",
    "w5m_tx_sum",
    "w5m_deposit_sum",
    "w5m_withdrawal_sum",
    "w1h_tx_count",
    "w1h_tx_sum",
    "w1h_deposit_sum",
    "w1h_withdrawal_sum",
    "w24h_tx_count",
    "w24h_tx_sum",
    "w24h_deposit_count",
    "w24h_deposit_sum",
    "w24h_withdrawal_sum",
    "w24h_wager_sum",
    "w24h_cash_in_sum",
    "w24h_cash_out_sum",
    "w24h_wire_in_sum",
    "w24h_just_below_10k_count",
    "w24h_cage_visits",
    "w24h_unique_payment_methods",
    "w7d_tx_sum",
    "w7d_avg_tx_amount",
    "w30d_tx_sum",
    "w30d_avg_tx_amount",
    "static_shared_device_count",
    "static_account_age_days",
    "static_is_pep",
    "static_watchlist_hit",
]

__all__ = ["FEATURE_COLUMNS"]
