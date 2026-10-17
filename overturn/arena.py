"""Model arena: same denial notices, same prompt, several open-weight models, scored by the same fact-checker.

Metric that matters here is the FIRST-ATTEMPT pass rate: how often a model's argument paragraphs
survive `letter.verify()` with no corrective retry. Data is synthetic. Backboard contestants are hosted
(external calls); the local Ollama baseline never leaves the machine.
"""
from __future__ import annotations
import time, collections
from . import backboard, config, tinker_model, extract, kb, letter, llm, synth, vectorstore
from .pipeline import SPEC_BY_CPT


def prepare_case(i: int) -> dict:
    row = synth.make_open_denial(i)
    text = synth.render_denial_letter(row)
    f = extract.jsonable(extract.finalize(extract.regex_extract(text)))
    f["specialty"] = SPEC_BY_CPT.get(f.get("cpt"), "Primary Care")
    for k in ("has_clinical_notes", "prior_auth_on_file", "in_network"):
        f[k] = bool(row.get(k, k == "in_network"))
    entry = kb.get(f["carc"])
    wins = vectorstore.get().search(f"{entry['title']} {f.get('cpt_desc') or ''} {entry['meaning']}", f["carc"], f.get("payer"), k=2)
    parts = letter._parts(f, entry)
    return {"id": f.get("claim_id"), "f": f, "entry": entry, "parts": parts, "prompt": letter.body_prompt(f, entry, wins, parts)}


def _local_fn():
    st = llm.status()
    if st["mode"] == "offline":
        return None
    return {"label": f"{st['model']} (local, Ollama)", "local": True,
            "call": lambda s, u: llm.chat(s, u, temperature=0.25, max_tokens=500)}


def contestants(models: list[dict], include_local: bool = True) -> list[dict]:
    out = []
    if include_local and (c := _local_fn()):
        out.append(c)
    if tinker_model.configured():
        out.append({"label": f"{config.TINKER_BASE_MODEL} base (Tinker)", "local": False,
                    "call": lambda s, u: tinker_model.chat(s, u)})
        if config.TINKER_MODEL_PATH:
            out.append({"label": f"{config.TINKER_BASE_MODEL} fine-tuned on winning appeals (Tinker)", "local": False,
                        "call": lambda s, u: tinker_model.chat(s, u, config.TINKER_MODEL_PATH)})
    for m in models:
        out.append({"label": f"{m['name']} (Backboard)", "local": False,
                    "call": (lambda s, u, m=m: backboard.chat(s, u, m.get("provider", ""), m["name"]))})
    return out


def run_one(call, case) -> dict:
    t0 = time.time()
    try:
        out = call(letter.BODY_SYSTEM, case["prompt"])
    except Exception as e:
        return {"case": case["id"], "error": f"{type(e).__name__}: {e}"[:240], "ok": False, "secs": round(time.time() - t0, 1)}
    body = letter.clean_body(out["text"])
    v = letter.verify(letter.assemble(case["parts"], body), case["f"])
    failed = [c["name"] for c in v["checks"] if c["critical"] and not c["ok"]]
    if len(body) < 80:
        failed.append("Argument section too short")
    return {"case": case["id"], "ok": not failed, "failed": failed, "secs": round(time.time() - t0, 1),
            "tokens": out["tokens_in"] + out["tokens_out"], "words": len(body.split()), "sample": body[:400]}


def summarize(label: str, local: bool, rows: list[dict]) -> dict:
    n = len(rows)
    good = [r for r in rows if r.get("ok")]
    errs = [r for r in rows if r.get("error")]
    fails = collections.Counter(x for r in rows for x in r.get("failed", []))
    timed = [r["secs"] for r in rows if not r.get("error")]
    return {"label": label, "local": local, "cases": n, "pass": len(good), "pass_rate": round(len(good) / n, 2) if n else 0,
            "errors": len(errs), "error_sample": errs[0]["error"] if errs else None,
            "avg_secs": round(sum(timed) / len(timed), 1) if timed else None,
            "avg_tokens": round(sum(r.get("tokens", 0) for r in rows) / n) if n else 0,
            "top_failures": fails.most_common(3)}


def stream(models: list[dict], n_cases: int = 6, include_local: bool = True):
    """Yield NDJSON-able events: start, progress, result (per contestant), done."""
    n_cases = max(1, min(int(n_cases), 12))
    cases = [prepare_case(i) for i in range(n_cases)]
    cons = contestants(models, include_local)
    yield {"type": "start", "contestants": [c["label"] for c in cons], "cases": n_cases,
           "external": any(not c["local"] for c in cons)}
    results = []
    for c in cons:
        rows = []
        for k, case in enumerate(cases):
            r = run_one(c["call"], case)
            rows.append(r)
            yield {"type": "progress", "label": c["label"], "done": k + 1, "of": n_cases, "ok": r["ok"]}
            if r.get("error") and k == 0:      # a broken contestant: don't burn N more failing calls
                rows += [dict(r, case=x["id"]) for x in cases[1:]]
                break
        s = summarize(c["label"], c["local"], rows)
        results.append(s)
        yield {"type": "result", **s}
    results.sort(key=lambda s: (-s["pass_rate"], s["avg_secs"] or 1e9))
    yield {"type": "done", "ranking": results, "egress": llm.egress_summary()}
