"""Compare keyword-only, vector-only and hybrid retrieval on a few realistic queries.

    python scripts/hybrid_compare.py                      # in-memory store
    set DATABASE_URL=postgresql://...  &  python scripts/hybrid_compare.py   # your Tiger Cloud service

Use the output (and a screenshot) in the write-up: where did hybrid beat either side alone?
"""
import os, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from overturn import vectorstore

QUERIES = [
    ("modifier 59 separate site bundled edit", "97", "Aetna"),
    ("prior authorization requested next morning urgent", "197", "Cigna"),
    ("filing limit proof clearinghouse acceptance report", "29", "UnitedHealthcare"),
    ("peer reviewer same specialty criteria medical necessity", "50", "BCBS"),
    ("missing rendering provider identifier corrected claim", "16", "Humana"),
]

st = vectorstore.get()
print(f"store: {st.kind} - {st.detail} - {st.count()} documents\n")
for q, carc, payer in QUERIES:
    print(f"QUERY: {q}   (CO-{carc}, {payer})")
    for mode in ("keyword", "vector", "hybrid"):
        hits = st.search(q, carc, payer, k=3, mode=mode)
        print(f"  {mode:8s}", [f"{h['id']}(CO-{h['carc']})" for h in hits])
    print()
