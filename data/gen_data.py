"""Generate the synthetic claims history used to train the overturn-probability model.

Outcomes follow a hidden logistic process so there is real signal to learn.
Every row is fake. Run:  python data/gen_data.py
"""
import csv, math, random, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from overturn.synth import PAYERS, CARCS, BASE_P, PROCS

PAYER_ADJ = {"Aetna": 0.0, "UnitedHealthcare": -0.15, "Cigna": -0.05, "BCBS": 0.1,
             "Humana": -0.1, "Medicare": 0.2, "Medicaid": 0.05}

def logit(p): return math.log(p / (1 - p))

def main(n=3000, seed=7):
    rng = random.Random(seed)
    rows = []
    for _ in range(n):
        carc = rng.choice(CARCS); payer = rng.choice(PAYERS)
        cpt, _, spec, *_rest, typ = rng.choice(PROCS)
        billed = round(typ * rng.uniform(0.8, 1.2), 2)
        days = int(min(180, max(1, rng.expovariate(1 / 40))))
        notes = int(rng.random() < .55); pa = int(rng.random() < .35)
        innet = int(rng.random() < .85); level = 1 if rng.random() < .75 else 2
        z = logit(BASE_P[carc]) + PAYER_ADJ[payer]
        z += 0.9 * notes * (carc in ("50", "97", "151", "96") ) + 0.3 * notes
        z += 0.8 * pa * (carc == "197") + 0.2 * pa
        z -= 0.012 * max(0, days - 30)
        z -= 0.00006 * billed
        z += 0.35 * innet - 0.2 * (level == 2) + rng.gauss(0, 0.5)
        y = int(rng.random() < 1 / (1 + math.exp(-z)))
        rows.append([payer, carc, cpt, spec, billed, days, notes, pa, innet, level, y])
    out = pathlib.Path(__file__).parent / "claims_history.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["payer", "carc", "cpt", "specialty", "billed", "days_since_denial",
                    "has_clinical_notes", "prior_auth_on_file", "in_network", "appeal_level", "overturned"])
        w.writerows(rows)
    print(f"wrote {out} ({n} rows, overturn rate {sum(r[-1] for r in rows)/n:.2%})")

if __name__ == "__main__":
    main()
