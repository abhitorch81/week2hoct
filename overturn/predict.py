"""Overturn-probability model.

Provider order (OVERTURN_PREDICTOR=auto):
  1. TabPFN client (Prior Labs hosted TabPFN) if PRIORLABS_API_KEY is set. ONLY the
     de-identified tabular features below are sent -- never letter text or patient data.
  2. Local `tabpfn` package if importable (fully on-device).
  3. scikit-learn gradient boosting (always available).

All three sit behind one interface so you can swap them without touching the app. The
"what-if levers" are model-agnostic counterfactuals: re-score the claim with one input
changed and report the lift.
"""
from __future__ import annotations
import os, time
import numpy as np, pandas as pd
from . import config
from .synth import PAYERS, CARCS, PROCS

SPECS = sorted({p[2] for p in PROCS})
FEATS = ["payer", "carc", "specialty", "billed", "days_since_denial", "has_clinical_notes",
         "prior_auth_on_file", "in_network", "appeal_level"]


def _enc(df: pd.DataFrame) -> np.ndarray:
    X = pd.DataFrame({
        "payer": df["payer"].map(lambda v: PAYERS.index(v) if v in PAYERS else len(PAYERS)),
        "carc": df["carc"].astype(str).map(lambda v: CARCS.index(v) if v in CARCS else len(CARCS)),
        "specialty": df["specialty"].map(lambda v: SPECS.index(v) if v in SPECS else len(SPECS)),
        "billed": df["billed"].astype(float),
        "days_since_denial": df["days_since_denial"].astype(float),
        "has_clinical_notes": df["has_clinical_notes"].astype(int),
        "prior_auth_on_file": df["prior_auth_on_file"].astype(int),
        "in_network": df["in_network"].astype(int),
        "appeal_level": df["appeal_level"].astype(int),
    })
    return X[FEATS].to_numpy(dtype=float)


class Predictor:
    def __init__(self):
        self.df = pd.read_csv(config.DATA / "claims_history.csv")
        self.provider = "sklearn"
        self.note = ""
        self.auc = None
        self._fit()

    def _fit(self):
        want = config.PREDICTOR
        X, y = _enc(self.df), self.df["overturned"].to_numpy()
        rng = np.random.RandomState(0)
        idx = rng.permutation(len(X)); cut = int(len(X) * .8)
        tr, te = idx[:cut], idx[cut:]
        self._fit_provider(want, X[tr], y[tr])
        try:
            from sklearn.metrics import roc_auc_score
            self.auc = float(roc_auc_score(y[te], self._proba(X[te])))
        except Exception:
            self.auc = None
        # refit on everything for serving
        self._fit_provider(self.provider, X, y)

    def _fit_provider(self, want, X, y):
        if want in ("auto", "tabpfn-client") and config.PRIORLABS_API_KEY:
            try:
                from tabpfn_client import TabPFNClassifier, set_access_token
                set_access_token(config.PRIORLABS_API_KEY)
                sub = np.random.RandomState(1).permutation(len(X))[:1500]
                self.model = TabPFNClassifier(); self.model.fit(X[sub], y[sub])
                self.provider = "tabpfn-client"; return
            except Exception as e:
                self.note = f"tabpfn-client unavailable ({type(e).__name__}); fell back"
        if want in ("auto", "tabpfn-local"):
            try:
                from tabpfn import TabPFNClassifier
                sub = np.random.RandomState(1).permutation(len(X))[:1500]
                self.model = TabPFNClassifier(); self.model.fit(X[sub], y[sub])
                self.provider = "tabpfn-local"; return
            except Exception as e:
                if want == "tabpfn-local":
                    self.note = f"tabpfn-local unavailable ({type(e).__name__}); fell back"
        from sklearn.ensemble import HistGradientBoostingClassifier
        self.model = HistGradientBoostingClassifier(max_depth=4, learning_rate=.07, max_iter=220,
                                                    categorical_features=[0, 1, 2], random_state=0)
        self.model.fit(X, y); self.provider = "sklearn"

    def _proba(self, X):
        return self.model.predict_proba(X)[:, 1]

    def _row(self, f: dict) -> pd.DataFrame:
        return pd.DataFrame([{k: f.get(k) for k in FEATS}])

    def quick(self, f: dict) -> float:
        return float(self._proba(_enc(self._row(f)))[0])

    def predict(self, f: dict) -> dict:
        t0 = time.perf_counter()
        base = float(self._proba(_enc(self._row(f)))[0])
        levers = []
        tests = []
        if not f.get("has_clinical_notes"):
            tests.append(("Attach signed clinical notes for the date of service", {"has_clinical_notes": 1}))
        if (f.get("days_since_denial") or 0) > 14:
            tests.append(("File within 14 days of the denial", {"days_since_denial": 14}))
        if f.get("carc") == "197" and not f.get("prior_auth_on_file"):
            tests.append(("Include authorization number / retro-auth request", {"prior_auth_on_file": 1}))
        for label, chg in tests:
            g = dict(f); g.update(chg)
            lift = float(self._proba(_enc(self._row(g)))[0]) - base
            if lift > 0.005:
                levers.append({"label": label, "lift": round(lift, 4)})
        levers.sort(key=lambda x: -x["lift"])
        sim_n = int(((self.df["carc"].astype(str) == str(f.get("carc"))) & (self.df["payer"] == f.get("payer"))).sum())
        return {"p": round(base, 4), "provider": self.provider, "auc": self.auc, "levers": levers,
                "history_rows": len(self.df), "similar_history": sim_n, "note": self.note,
                "ms": round((time.perf_counter() - t0) * 1000, 1)}


_inst: Predictor | None = None


def get() -> Predictor:
    global _inst
    if _inst is None:
        _inst = Predictor()
    return _inst
