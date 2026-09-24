"""Train AML models offline (B feature schema + A training tooling).

  cd model
  python3 -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt
  python train_models.py
  python train_models.py --csv /path/to/aml_train.csv

Writes joblib into ``model/artifacts/``. Scoring on OpenShift uses
``sentinel.aml_model`` (load/predict only) — this file is NOT imported at runtime.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
sys.path.insert(0, str(REPO / "services" / "python"))
from sentinel.feature_schema import FEATURE_COLUMNS  # noqa: E402

ARTIFACTS = ROOT / "artifacts"
RNG = np.random.default_rng(42)


def _pattern_row(pattern: str) -> tuple[np.ndarray, int]:
    """One synthetic row aligned to FEATURE_COLUMNS (B / feature-engine format)."""
    idx = {c: i for i, c in enumerate(FEATURE_COLUMNS)}
    x = RNG.normal(size=len(FEATURE_COLUMNS)) * 0.3
    x[idx["w24h_deposit_count"]] = RNG.uniform(1, 4)
    x[idx["w24h_deposit_sum"]] = RNG.uniform(200, 3000)
    x[idx["w24h_wager_sum"]] = x[idx["w24h_deposit_sum"]] * RNG.uniform(0.4, 1.5)
    x[idx["w24h_withdrawal_sum"]] = RNG.uniform(0, x[idx["w24h_deposit_sum"]] * 0.5)
    x[idx["w1h_tx_count"]] = RNG.uniform(1, 3)
    x[idx["w24h_tx_count"]] = RNG.uniform(2, 8)
    x[idx["static_account_age_days"]] = RNG.uniform(30, 900)
    x[idx["static_shared_device_count"]] = 1.0
    x[idx["static_is_pep"]] = 0.0
    x[idx["static_watchlist_hit"]] = 0.0
    label = 0

    if pattern == "structuring":
        x[idx["w24h_deposit_count"]] = RNG.uniform(3, 7)
        x[idx["w24h_deposit_sum"]] = RNG.uniform(25000, 45000)
        x[idx["w24h_just_below_10k_count"]] = RNG.uniform(2, 6)
        x[idx["w24h_cage_visits"]] = RNG.uniform(3, 6)
        x[idx["w24h_cash_in_sum"]] = x[idx["w24h_deposit_sum"]]
        x[idx["w24h_wager_sum"]] = x[idx["w24h_deposit_sum"]] * RNG.uniform(0.0, 0.15)
        label = 1
    elif pattern == "minimal_play":
        x[idx["w24h_deposit_sum"]] = RNG.uniform(10000, 30000)
        x[idx["w24h_cash_in_sum"]] = x[idx["w24h_deposit_sum"]]
        x[idx["w24h_wager_sum"]] = x[idx["w24h_deposit_sum"]] * RNG.uniform(0.01, 0.08)
        x[idx["w24h_withdrawal_sum"]] = x[idx["w24h_deposit_sum"]] * RNG.uniform(0.85, 0.99)
        x[idx["w24h_cash_out_sum"]] = x[idx["w24h_withdrawal_sum"]]
        label = 1
    elif pattern == "rapid_wire":
        x[idx["w24h_wire_in_sum"]] = RNG.uniform(15000, 40000)
        x[idx["w1h_deposit_sum"]] = x[idx["w24h_wire_in_sum"]]
        x[idx["w24h_cash_out_sum"]] = x[idx["w24h_wire_in_sum"]] * RNG.uniform(0.7, 0.95)
        x[idx["w24h_withdrawal_sum"]] = x[idx["w24h_cash_out_sum"]]
        x[idx["w24h_wager_sum"]] = x[idx["w24h_wire_in_sum"]] * RNG.uniform(0.0, 0.12)
        x[idx["w24h_unique_payment_methods"]] = RNG.uniform(2, 4)
        label = 1
    elif pattern == "velocity":
        x[idx["w1h_tx_count"]] = RNG.uniform(6, 14)
        x[idx["w24h_tx_count"]] = RNG.uniform(12, 30)
        x[idx["w24h_cage_visits"]] = RNG.uniform(6, 12)
        label = 1
    elif pattern == "watchlist":
        x[idx["static_watchlist_hit"]] = 1.0
        x[idx["static_is_pep"]] = 1.0 if RNG.random() < 0.5 else 0.0
        x[idx["w24h_deposit_sum"]] = RNG.uniform(5000, 25000)
        x[idx["w24h_cash_in_sum"]] = x[idx["w24h_deposit_sum"]]
        label = 1
    elif pattern == "shared_device":
        x[idx["static_shared_device_count"]] = RNG.uniform(3, 8)
        x[idx["w24h_deposit_sum"]] = RNG.uniform(1000, 8000)
        label = 1
    elif pattern == "ctr":
        x[idx["w24h_cash_in_sum"]] = RNG.uniform(10000, 40000)
        x[idx["w5m_deposit_sum"]] = x[idx["w24h_cash_in_sum"]]
        x[idx["w24h_deposit_sum"]] = x[idx["w24h_cash_in_sum"]]
        label = 1
    elif pattern == "multi_rail":
        x[idx["w24h_unique_payment_methods"]] = RNG.uniform(3, 5)
        x[idx["w24h_wire_in_sum"]] = RNG.uniform(5000, 20000)
        x[idx["w24h_cash_out_sum"]] = RNG.uniform(4000, 15000)
        x[idx["w24h_wager_sum"]] = (x[idx["w24h_wire_in_sum"]] + x[idx["w24h_cash_out_sum"]]) * RNG.uniform(
            0.0, 0.15
        )
        label = 1
    elif pattern == "high_roller_ok":
        x[idx["w24h_deposit_sum"]] = RNG.uniform(8000, 30000)
        x[idx["w24h_cash_in_sum"]] = x[idx["w24h_deposit_sum"]]
        x[idx["w24h_wager_sum"]] = x[idx["w24h_deposit_sum"]] * RNG.uniform(0.7, 2.0)
        x[idx["w24h_tx_count"]] = RNG.uniform(8, 20)
        label = 0
    else:  # normal
        label = 0

    if RNG.random() < 0.05:
        label = 1 - label
    return x.astype(np.float32), int(label)


def make_dataset(n: int = 24000) -> tuple[np.ndarray, np.ndarray]:
    mix = (
        ["normal"] * 40
        + ["high_roller_ok"] * 10
        + ["structuring"] * 12
        + ["minimal_play"] * 8
        + ["rapid_wire"] * 8
        + ["velocity"] * 6
        + ["watchlist"] * 4
        + ["shared_device"] * 4
        + ["ctr"] * 4
        + ["multi_rail"] * 4
    )
    X = np.zeros((n, len(FEATURE_COLUMNS)), dtype=np.float32)
    y = np.zeros(n, dtype=np.int32)
    for i in range(n):
        row, label = _pattern_row(mix[i % len(mix)])
        X[i] = row
        y[i] = label
    return X, y


def load_csv(path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Load labeled rows. Required: ``label`` (0/1). Columns match FEATURE_COLUMNS."""
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise SystemExit(f"No header in {path}")
        fields = {h.strip() for h in reader.fieldnames}
        if "label" not in fields:
            raise SystemExit("CSV must include a `label` column (0 = legitimate, 1 = AML).")
        missing = [n for n in FEATURE_COLUMNS if n not in fields]
        if missing:
            raise SystemExit(
                "CSV is missing feature columns required by live scoring:\n  " + ", ".join(missing)
            )
        X, y = [], []
        for row in reader:
            vec = []
            for name in FEATURE_COLUMNS:
                raw = row.get(name, "")
                vec.append(0.0 if raw in ("", None) else float(raw))
            X.append(vec)
            y.append(int(float(row["label"])))
    if len(X) < 50:
        raise SystemExit(f"Need at least 50 labeled rows, got {len(X)}")
    return np.asarray(X, dtype=np.float32), np.asarray(y, dtype=np.int32)


def _fit_and_write(X: np.ndarray, y: np.ndarray, source: str) -> None:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    stratify = y if len(np.unique(y)) > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=stratify
    )

    clf = XGBClassifier(
        n_estimators=140,
        max_depth=5,
        learning_rate=0.08,
        min_child_weight=20,
        reg_lambda=1.0,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        device="cpu",
        random_state=42,
        n_jobs=2,
    )
    clf.fit(X_train, y_train)
    proba = clf.predict_proba(X_test)[:, 1]
    auc = float(roc_auc_score(y_test, proba)) if len(np.unique(y_test)) > 1 else 0.0
    ap = float(average_precision_score(y_test, proba)) if len(np.unique(y_test)) > 1 else 0.0
    print(f"XGBoost  ROC-AUC={auc:.3f}  PR-AUC={ap:.3f}  positives={y.mean():.2%}")

    gain = clf.feature_importances_
    top = sorted(zip(FEATURE_COLUMNS, gain.tolist()), key=lambda t: t[1], reverse=True)[:8]

    X_norm = X_train[y_train == 0]
    if len(X_norm) < 10:
        X_norm = X_train
    iforest = IsolationForest(
        n_estimators=200,
        contamination=0.08,
        max_samples=min(2048, max(2, len(X_norm))),
        random_state=42,
        n_jobs=2,
    )
    iforest.fit(X_norm)

    # Payload includes feature_names so scoring can validate schema.
    payload = {"model": clf, "feature_names": FEATURE_COLUMNS, "kind": "xgboost"}
    joblib.dump(payload, ARTIFACTS / "aml_xgb.joblib")
    # Alias for older docs / copies
    joblib.dump(payload, ARTIFACTS / "aml_xgboost.joblib")
    joblib.dump(iforest, ARTIFACTS / "aml_iforest.joblib")

    meta = {
        "version": "2.0.0",
        "feature_names": FEATURE_COLUMNS,
        "model": "XGBClassifier",
        "tree_method": "hist",
        "device": "cpu",
        "source": source,
        "xgboost_roc_auc": round(auc, 4),
        "xgboost_pr_auc": round(ap, 4),
        "n_train": int(len(X_train)),
        "n_rows": int(len(X)),
        "positive_rate": round(float(y.mean()), 4),
        "top_features": top,
        "scoring_module": "sentinel.aml_model",
    }
    (ARTIFACTS / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"Wrote {ARTIFACTS / 'aml_xgb.joblib'}")
    print(f"Wrote {ARTIFACTS / 'aml_iforest.joblib'}")
    print(f"Wrote {ARTIFACTS / 'meta.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train Casino Sentinel XGBoost + Isolation Forest (offline)"
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Labeled CSV: FEATURE_COLUMNS + label (0=legit, 1=AML). Default: synthetic.",
    )
    parser.add_argument("--n", type=int, default=24000, help="Synthetic row count (default 24000)")
    args = parser.parse_args()
    if args.csv:
        path = args.csv.expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f"CSV not found: {path}")
        print(f"Loading labeled data from {path}")
        X, y = load_csv(path)
        source = str(path)
    else:
        print(f"No --csv given; training on synthetic patterns (n={args.n})")
        X, y = make_dataset(args.n)
        source = "synthetic"
    _fit_and_write(X, y, source)


if __name__ == "__main__":
    main()
