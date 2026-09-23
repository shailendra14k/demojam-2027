"""Train CPU AML models: XGBoost + Isolation Forest.

Synthetic demo data (default):
  python -m sentinel.train_models

Your labeled CSV (columns = FEATURE_NAMES + label):
  python -m sentinel.train_models --csv /path/to/aml_train.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import random

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from .vectorize import FEATURE_NAMES

ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
RNG = random.Random(42)


def _row(pattern: str) -> tuple[list[float], int]:
    amount = RNG.uniform(20, 600)
    cash_in = RNG.uniform(200, 2500)
    cash_out = RNG.uniform(0, cash_in * 0.6)
    wagered = cash_in * RNG.uniform(0.4, 1.8)
    txn_1h = RNG.randint(1, 3)
    txn_24h = RNG.randint(2, 8)
    cage = RNG.randint(1, 2)
    just_below = 0
    wire_in = 0.0
    wire_out = 0.0
    locs = RNG.randint(1, 2)
    methods = 1
    pep = 0
    watch = 0
    prior = 0
    kyc = 0
    is_cash = 1 if RNG.random() < 0.5 else 0
    is_wire = 0
    label = 0

    if pattern == "structuring":
        amount = RNG.uniform(8200, 9900)
        just_below = RNG.randint(2, 5)
        cage = RNG.randint(3, 6)
        cash_out = amount * just_below
        cash_in = RNG.uniform(0, 2000)
        wagered = cash_in * RNG.uniform(0.0, 0.2)
        txn_24h = just_below + RNG.randint(1, 4)
        locs = RNG.randint(2, 4)
        is_cash = 1
        label = 1
    elif pattern == "minimal_play":
        cash_in = RNG.uniform(8000, 25000)
        amount = cash_in
        wagered = cash_in * RNG.uniform(0.01, 0.08)
        cash_out = cash_in * RNG.uniform(0.85, 0.99)
        is_cash = 1
        txn_24h = RNG.randint(2, 4)
        label = 1
    elif pattern == "rapid_wire":
        wire_in = RNG.uniform(15000, 40000)
        amount = wire_in
        cash_out = wire_in * RNG.uniform(0.7, 0.95)
        wagered = wire_in * RNG.uniform(0.0, 0.12)
        is_wire = 1
        is_cash = 0
        methods = RNG.randint(2, 4)
        label = 1
    elif pattern == "velocity":
        txn_1h = RNG.randint(6, 12)
        txn_24h = RNG.randint(12, 28)
        cage = RNG.randint(6, 12)
        amount = RNG.uniform(400, 3000)
        cash_in = amount * txn_24h * 0.4
        label = 1
    elif pattern == "watchlist":
        watch = 1
        pep = 1 if RNG.random() < 0.6 else 0
        kyc = 3
        prior = RNG.randint(2, 8)
        amount = RNG.uniform(3000, 20000)
        cash_in = amount
        label = 1
    elif pattern == "ctr":
        amount = RNG.uniform(10000, 40000)
        is_cash = 1
        cash_in = amount if RNG.random() < 0.5 else 0
        cash_out = amount if cash_in == 0 else 0
        label = 1
    elif pattern == "high_roller_ok":
        amount = RNG.uniform(5000, 18000)
        cash_in = RNG.uniform(8000, 30000)
        wagered = cash_in * RNG.uniform(0.7, 2.2)
        cash_out = max(0, cash_in - wagered * RNG.uniform(0.2, 0.6))
        txn_24h = RNG.randint(8, 20)
        is_cash = 1
        label = 0
    elif pattern == "multi_rail":
        methods = RNG.randint(3, 5)
        wire_in = RNG.uniform(5000, 20000)
        cash_out = RNG.uniform(4000, 15000)
        amount = cash_out
        wagered = (wire_in + cash_out) * RNG.uniform(0.0, 0.15)
        is_cash = 1
        label = 1

    if RNG.random() < 0.06:
        label = 1 - label

    month = cash_in + cash_out + wire_in + RNG.uniform(0, 5000)
    ratio = (wagered / cash_in) if cash_in > 0 else 0.0
    vec = [
        float(amount),
        float(np.log10(max(amount, 1.0))),
        float(is_cash),
        float(is_wire),
        float(cash_in),
        float(cash_out),
        float(txn_1h),
        float(txn_24h),
        float(amount),
        float(cage),
        float(wagered),
        float(ratio),
        float(locs),
        float(methods),
        float(just_below),
        float(wire_in),
        float(wire_out),
        float(month),
        float(pep),
        float(watch),
        float(prior),
        float(kyc),
    ]
    assert len(vec) == len(FEATURE_NAMES)
    return vec, label


def load_csv(path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Load labeled rows. Required: `label` (0/1). Feature columns match FEATURE_NAMES.

    Missing `amount_log` is derived from `amount`. Extra columns are ignored.
    """
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise SystemExit(f"No header in {path}")
        fields = {h.strip() for h in reader.fieldnames}
        if "label" not in fields:
            raise SystemExit("CSV must include a `label` column (0 = legitimate, 1 = AML/SAR).")
        missing = [n for n in FEATURE_NAMES if n not in fields and n != "amount_log"]
        if missing:
            raise SystemExit(
                "CSV is missing feature columns required by live scoring:\n  "
                + ", ".join(missing)
            )
        X, y = [], []
        for row in reader:
            vec = []
            for name in FEATURE_NAMES:
                if name == "amount_log" and (name not in row or row[name] in ("", None)):
                    amt = float(row.get("amount") or 0)
                    vec.append(float(np.log10(max(amt, 1.0))))
                    continue
                raw = row.get(name, "")
                vec.append(0.0 if raw in ("", None) else float(raw))
            X.append(vec)
            y.append(int(float(row["label"])))
    if len(X) < 50:
        raise SystemExit(f"Need at least 50 labeled rows, got {len(X)}")
    return np.asarray(X, dtype=np.float32), np.asarray(y, dtype=np.int32)


def make_dataset(n: int = 24000) -> tuple[np.ndarray, np.ndarray]:
    mix = (
        ["normal"] * 45
        + ["high_roller_ok"] * 10
        + ["structuring"] * 12
        + ["minimal_play"] * 8
        + ["rapid_wire"] * 8
        + ["velocity"] * 6
        + ["watchlist"] * 4
        + ["ctr"] * 4
        + ["multi_rail"] * 3
    )
    X, y = [], []
    for _ in range(n):
        vec, label = _row(RNG.choice(mix))
        X.append(vec)
        y.append(label)
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
    auc = roc_auc_score(y_test, proba)
    ap = average_precision_score(y_test, proba)
    print(f"XGBoost  ROC-AUC={auc:.3f}  PR-AUC={ap:.3f}  positives={y.mean():.2%}")

    gain = clf.feature_importances_
    top = sorted(zip(FEATURE_NAMES, gain.tolist()), key=lambda x: x[1], reverse=True)[:8]

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

    payload = {"model": clf, "feature_names": FEATURE_NAMES}
    joblib.dump(payload, ARTIFACTS / "aml_xgboost.joblib")
    joblib.dump(payload, ARTIFACTS / "aml_gbt.joblib")
    joblib.dump(iforest, ARTIFACTS / "aml_iforest.joblib")
    meta = {
        "feature_names": FEATURE_NAMES,
        "model": "XGBClassifier",
        "tree_method": "hist",
        "device": "cpu",
        "source": source,
        "xgboost_roc_auc": round(float(auc), 4),
        "xgboost_pr_auc": round(float(ap), 4),
        "n_train": int(len(X_train)),
        "n_rows": int(len(X)),
        "positive_rate": round(float(y.mean()), 4),
        "top_features": top,
    }
    (ARTIFACTS / "meta.json").write_text(json.dumps(meta, indent=2))
    print(f"Wrote {ARTIFACTS / 'aml_xgboost.joblib'}")
    print(f"Wrote {ARTIFACTS / 'aml_iforest.joblib'}")
    print(f"Wrote {ARTIFACTS / 'meta.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Casino Sentinel XGBoost + Isolation Forest")
    parser.add_argument(
        "--csv",
        type=Path,
        help="Labeled CSV: feature columns + label (0=legit, 1=AML). Default: synthetic demo data.",
    )
    args = parser.parse_args()
    if args.csv:
        path = args.csv.expanduser().resolve()
        if not path.is_file():
            raise SystemExit(f"CSV not found: {path}")
        print(f"Loading labeled data from {path}")
        X, y = load_csv(path)
        source = str(path)
    else:
        print("No --csv given; training on synthetic demo patterns")
        X, y = make_dataset()
        source = "synthetic"
    _fit_and_write(X, y, source)


if __name__ == "__main__":
    main()
