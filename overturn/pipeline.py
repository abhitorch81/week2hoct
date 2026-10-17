"""The appeal agent as an ordered list of steps over one JSON-serialisable state dict.

Because each step is a pure function of that state, the same steps run under:
  * durable.py            -- zero-dependency checkpoint/retry/resume runner (default)
  * temporal_workflow.py  -- Temporal workflow + activities (when TEMPORAL_ADDRESS is set)
"""
from __future__ import annotations
import re
from . import config, extract, kb, llm, predict, vectorstore, letter
from .synth import PROCS

SPEC_BY_CPT = {p[0]: p[2] for p in PROCS}
STEP_LABELS = {
    "ingest": "Read the notice", "extract": "Extract claim facts", "classify": "Decode the denial",
    "predict": "Score overturn odds", "retrieve": "Find similar wins", "triage": "Pick the best move",
    "draft": "Draft the appeal", "verify": "Fact-check the draft",
}


class FlakyTool(Exception):
    pass


def step_ingest(s, tr):
    t = s["text"].strip()
    phi = len(set(re.findall(r"\b[A-Z]\d{8,9}\b", t))) + len(re.findall(r"(?:patient|member id)\s*:", t, re.I))
    s["text"] = t
    return {"chars": len(t), "lines": t.count("\n") + 1, "phi_identifiers": phi,
            "note": "PHI is processed on this machine only."}


def step_extract(s, tr):
    f = extract.regex_extract(s["text"])
    f, out = extract.llm_fill(s["text"], f)
    f = extract.finalize(f)
    f = extract.jsonable(f)
    s["facts"] = f
    s["tokens"] = s.get("tokens", 0) + ((out["tokens_in"] + out["tokens_out"]) if out else 0)
    f["specialty"] = SPEC_BY_CPT.get(f.get("cpt"), "Primary Care")
    for k in ("has_clinical_notes", "prior_auth_on_file", "in_network"):
        f[k] = bool(s["inputs"].get(k, k == "in_network"))
    f["appeal_level"] = 1
    missing = [k for k in extract.REQUIRED if not f.get(k)]
    if missing:
        raise ValueError("Could not read required fields: " + ", ".join(missing))
    return {"facts": f, "llm_filled": f.get("_llm_filled", []), "missing": missing}


def step_classify(s, tr):
    f = s["facts"]
    e = kb.get(f["carc"])
    if not e:
        e = {"title": f"Unrecognised code CO-{f['carc']}", "meaning": f.get("reason_text") or "The payer did not give a standard reason.",
             "action": "appeal", "recode_success": 0.0, "argument": "We request review of this determination, which we believe was made in error, on the basis of the enclosed record.",
             "evidence": ["Clinical notes", "Original claim and remittance"], "tips": "Call the payer to confirm the basis for denial."}
    s["entry"] = e
    return {"code": f["carc"], "title": e["title"], "meaning": e["meaning"], "playbook": e["action"], "tips": e["tips"],
            "evidence": e["evidence"], "deadline": f.get("deadline"), "days_left": f.get("days_left"),
            "rarc": f.get("rarc")}


def step_predict(s, tr):
    pr = predict.get().predict(s["facts"])
    s["pred"] = pr
    return pr


def step_retrieve(s, tr):
    if s.get("chaos") == "flaky" and s.get("_attempt", 1) == 1:
        raise FlakyTool("vector store timed out (injected failure)")
    f = s["facts"]
    q = f"{s['entry']['title']} {f.get('cpt_desc') or ''} {f.get('icd10_desc') or ''} {s['entry']['meaning']}"
    store = vectorstore.get()
    wins = store.search(q, f["carc"], f.get("payer"), k=3)
    s["wins"] = wins
    return {"store": store.kind, "wins": wins}


def compute_options(f, e, prob, days_left):
    """Expected-value comparison of the three moves a denial-management team can make."""
    denied = f.get("denied") or 0.0
    expired = days_left is not None and days_left < 0
    p_appeal = prob * (0.25 if expired else 1.0)
    if e["action"] == "proof" and not f.get("has_clinical_notes"):
        p_appeal *= 0.5   # no dated proof of timely filing on hand
    p_recode = (e.get("recode_success", 0.0) if e["action"] == "recode" else 0.0) * (0.5 if expired else 1.0)
    opts = [
        {"id": "appeal", "label": "Appeal", "p": round(p_appeal, 3), "cost": config.COST_APPEAL,
         "ev": round(p_appeal * denied - config.COST_APPEAL, 2)},
        {"id": "recode", "label": "Correct & resubmit", "p": round(p_recode, 3), "cost": config.COST_RECODE,
         "ev": round(p_recode * denied - config.COST_RECODE, 2), "applicable": e["action"] == "recode"},
        {"id": "writeoff", "label": "Write off", "p": 0.0, "cost": 0.0, "ev": 0.0},
    ]
    best = max([o for o in opts if o.get("applicable", True)], key=lambda o: o["ev"])
    if best["ev"] <= 0:
        best = opts[2]
    return opts, best, expired


def step_triage(s, tr):
    f, e, pr = s["facts"], s["entry"], s["pred"]
    denied = f.get("denied") or 0.0
    opts, best, expired = compute_options(f, e, pr["p"], f.get("days_left"))
    why = {"appeal": f"Expected recovery ${best['ev'] + config.COST_APPEAL:,.0f} on ${denied:,.0f} denied beats the ${config.COST_APPEAL:.0f} cost to appeal.",
           "recode": f"This is a data/coding problem ({e['title'].lower()}): a corrected claim is cheaper and likelier to pay than a full appeal.",
           "writeoff": "Expected recovery does not cover the cost of working it" + (" and the appeal window has closed." if expired else ".")}[best["id"]]
    s["triage"] = {"options": opts, "recommended": best["id"], "why": why, "expired": expired,
                   "assumption": "Recovery is modelled at the denied amount; actual payment is the contracted allowed amount."}
    return s["triage"]


def step_draft(s, tr):
    f, e = s["facts"], s["entry"]
    d = letter.draft(f, e, s["triage"]["recommended"], s.get("wins", []))
    s["draft"] = d
    s["tokens"] = s.get("tokens", 0) + d["tokens_in"] + d["tokens_out"]
    return {"engine": d["engine"], "tokens": d["tokens_in"] + d["tokens_out"], "chars": len(d["text"]),
            "fallback_reason": d["fallback_reason"], "attempts": d.get("attempts", 0)}


def step_verify(s, tr):
    f, d = s["facts"], s["draft"]
    v = letter.verify(d["text"], f)
    replaced = False
    log = d.get("attempt_log") or []
    last_failed = log[-1]["failed"] if log else []     # problems found in the model's final attempt
    if last_failed:
        v["passed"] = False
    if not v["passed"] and d.get("template"):
        # guardrail: the model's draft contradicted the facts -> fall back to the deterministic letter
        failed = [{"name": c["name"], "detail": c["detail"]} for c in v["checks"] if c["critical"] and not c["ok"]]
        failed += [c for c in last_failed if c["name"] not in {x["name"] for x in failed}]
        d["rejected_llm_draft_issues"] = [c["name"] for c in failed]
        d["rejected_text"], d["rejected_engine"], d["rejected_failed"] = d.get("body") or d["text"], d["engine"], failed
        print(f"[overturn] model draft rejected ({d['engine']}): " + "; ".join(f"{c['name']} -> {c['detail']}" for c in failed), flush=True)
        d["text"], d["engine"] = d["template"], "template (model draft rejected by fact-check)"
        d["argument"] = d.get("template_argument", "")
        v = letter.verify(d["text"], f)
        replaced = True
    s["verify"] = v
    out = {"passed": v["passed"], "score": v["score"], "replaced_draft": replaced, "checks": v["checks"], "todo": v["todo"]}
    if replaced:
        out["rejected"] = {"engine": d["rejected_engine"], "text": d["rejected_text"], "failed": d["rejected_failed"]}
    return out


STEPS = [("ingest", step_ingest), ("extract", step_extract), ("classify", step_classify), ("predict", step_predict),
         ("retrieve", step_retrieve), ("triage", step_triage), ("draft", step_draft), ("verify", step_verify)]


def build_result(s, trace) -> dict:
    eg = llm.egress_summary()
    tokens = s.get("tokens", 0)
    d = s["draft"]
    return {
        "facts": s["facts"], "entry": {k: s["entry"][k] for k in ("title", "meaning", "action", "tips", "evidence")},
        "prediction": s["pred"], "wins": s.get("wins", []), "triage": s["triage"],
        "letter": {"argument": letter.scrub(d.get("argument", ""), s["facts"]), "text": d["text"], "engine": d["engine"], "fallback_reason": d.get("fallback_reason"),
                   "attempts": d.get("attempts", 0), "attempt_log": d.get("attempt_log"),
                   "rejected": d.get("rejected_llm_draft_issues"), "rejected_text": d.get("rejected_text"),
                   "rejected_failed": d.get("rejected_failed")},
        "verify": s["verify"],
        "engine": {"llm": llm.status(), "tokens": tokens, "calls": eg["calls"], "external_calls": eg["external_calls"],
                   "equiv_api_cost": round(tokens / 1e6 * config.API_REF_PRICE_PER_MTOK, 4)},
        "trace": trace.to_dict(),
    }
