"""Appeal-letter drafting with guardrails.

Primary path: an open-weight model drafts the letter from verified facts + the denial-code
playbook + excerpts of past winning appeals. Fallback path: a deterministic template engine.
Either way the draft goes through `verify()`, which checks it against the extracted facts and
rejects hallucinated codes/amounts. A failed LLM draft is replaced by the template draft.
"""
from __future__ import annotations
import re
from . import llm, kb, config

BODY_SYSTEM = ("You are an experienced medical-billing appeals specialist. Write ONLY the argument section of an appeal "
               "letter: two short paragraphs, at most 170 words in total, firm, professional and specific. Use ONLY the facts "
               "supplied. Never invent clinical details, dates, dollar amounts, codes, authorization numbers, policy names or "
               "legal citations. Where a needed fact is missing, write a [bracketed placeholder]. Do NOT write a letterhead, "
               "RE: line, greeting, closing, signature or enclosure list: those are added automatically.")


def _money(x):
    return f"${x:,.2f}" if x is not None else "[amount]"


def facts_block(f: dict) -> str:
    return "\n".join(f"{k}: {v}" for k, v in [
        ("Payer", f.get("payer")), ("Claim number", f.get("claim_id")), ("Patient", f.get("patient")),
        ("Member ID", f.get("member_id")), ("Date of service", f.get("dos")), ("Provider", f.get("provider")),
        ("NPI", f.get("npi")), ("CPT", f"{f.get('cpt')} {f.get('cpt_desc') or ''}".strip()),
        ("Diagnosis", f"{f.get('icd10')} {f.get('icd10_desc') or ''}".strip()),
        ("Billed", _money(f.get("billed"))), ("Denied", _money(f.get("denied"))),
        ("Denial reason", f"CO-{f.get('carc')} {f.get('reason_text') or ''}".strip()),
        ("Denial date", f.get("denial_date")), ("Appeal deadline", f.get("deadline")),
        ("Clinical notes available", "yes" if f.get("has_clinical_notes") else "no"),
        ("Prior authorization on file", "yes" if f.get("prior_auth_on_file") else "no")] if v)


def _parts(f: dict, entry: dict) -> dict:
    """The letter is built from deterministic parts; only the argument paragraph can come from the model."""
    auth = "obtained and is referenced below" if f.get("prior_auth_on_file") else "[authorization status]"
    arg = kb.fill(entry["argument"], cpt=f"CPT {f.get('cpt')}", icd10=f.get("icd10") or "[diagnosis]",
                  dos=f.get("dos") or "[date of service]", auth_status=auth)
    enclosures = "\n".join(f"  {i+1}. {e}" for i, e in enumerate(entry["evidence"][:5]))
    ask = ("We request that the denial be overturned and the claim reprocessed for payment at the contracted rate. "
           "If the denial is upheld, please provide in writing the specific clinical criteria, plan provision or policy relied "
           "upon and the credentials of the reviewer, and confirm our right to the next level of appeal.")
    if entry["action"] == "recode":
        ask = ("We request that the claim be reprocessed using the corrected information enclosed and paid at the "
               "contracted rate. Please confirm receipt and the expected processing date.")
    if entry["title"].startswith("Not medically"):
        ask = "We also request a peer-to-peer discussion with a same-specialty reviewer. " + ask
    header = f"""{f.get('provider') or '[Provider name]'}
NPI {f.get('npi') or '[NPI]'}

{config.today().strftime('%B %d, %Y')}

{f.get('payer') or '[Payer]'} - Provider Appeals Department

RE: Appeal of claim denial
Claim number: {f.get('claim_id') or '[claim number]'}   Patient: {f.get('patient') or '[patient]'}   Member ID: {f.get('member_id') or '[member ID]'}
Date of service: {f.get('dos') or '[DOS]'}   Service: CPT {f.get('cpt') or '[CPT]'}{(' - ' + f['cpt_desc']) if f.get('cpt_desc') else ''}
Amount denied: {_money(f.get('denied'))}   Denial reason: CO-{f.get('carc') or '[code]'} ({entry['title']})

To the Appeals Reviewer:"""
    intro = (f"We are writing to appeal the denial of the claim referenced above, dated {f.get('denial_date') or '[denial date]'}. "
             f"{entry['meaning']} We respectfully disagree with this determination for the reasons below.")
    tail = f"""Enclosures:
{enclosures}

Thank you for your prompt attention. Please direct questions to our billing office.

Sincerely,

[Name and title]
{f.get('provider') or '[Provider name]'}
"""
    return {"header": header, "intro": intro, "arg": arg, "ask": ask, "tail": tail}


def assemble(parts: dict, body: str) -> str:
    return "\n\n".join([parts["header"], parts["intro"], body.strip(), parts["ask"], parts["tail"]])


def template_letter(f: dict, entry: dict, action: str = "") -> str:
    p = _parts(f, entry)
    return assemble(p, p["arg"])


_DROP_LEAD = re.compile(r"^\s*(RE:|Dear\b|To the\b|Claim number|Patient:|Member ID|Date of service|Amount denied)", re.I)
_CUT_AT = re.compile(r"^\s*(Enclosures?:?|Sincerely|Respectfully|Thank you|Best regards|Yours)", re.I)
# section headings of OUR prompt: small models sometimes echo them (and the excerpts under them) into the letter
PROMPT_ECHO = re.compile(r"^\s*(FACTS\b|WHY THE PAYER DENIED|CORE ARGUMENT TO ADAPT|EXCERPTS OF PAST WINNING APPEALS|"
                         r"YOUR PREVIOUS ATTEMPT|PREVIOUS ATTEMPT\b)", re.M)


def clean_body(text: str) -> str:
    """Keep only the argument paragraphs if the model added letterhead/closing anyway."""
    text = re.sub(r"[*#`]+", "", text)
    out = []
    for ln in text.splitlines():
        if _CUT_AT.match(ln) or PROMPT_ECHO.match(ln):
            break
        if not out and (not ln.strip() or _DROP_LEAD.match(ln)):
            continue
        out.append(ln)
    return "\n".join(out).strip()


def scrub(text: str, f: dict) -> str:
    """Replace identifiers so an argument paragraph can be saved to the shared knowledge base."""
    for key, token in (("patient", "[PATIENT]"), ("member_id", "[MEMBER ID]"), ("claim_id", "[CLAIM]"),
                       ("npi", "[NPI]"), ("provider", "[PROVIDER]")):
        if f.get(key):
            text = re.sub(re.escape(str(f[key])), token, text, flags=re.I)
    return text


def _fallback(tpl, reason, arg=""):
    return {"text": tpl, "engine": "template", "tokens_in": 0, "tokens_out": 0, "fallback_reason": reason, "attempts": 0,
            "template_argument": arg, "argument": arg}


def body_prompt(f: dict, entry: dict, wins: list[dict], parts: dict | None = None) -> str:
    """The user prompt for the argument paragraphs (shared by draft() and the model arena)."""
    parts = parts or _parts(f, entry)
    ex = "\n".join(f"- {w['text']}" for w in wins[:2])
    return (f"FACTS\n{facts_block(f)}\n\nWHY THE PAYER DENIED (plain English): {entry['meaning']}\n\n"
            f"CORE ARGUMENT TO ADAPT (keep its substance):\n{parts['arg']}\n\n"
            f"EXCERPTS OF PAST WINNING APPEALS (style/argument reference only; do NOT copy facts from them):\n{ex}\n")


def draft(f: dict, entry: dict, action: str, wins: list[dict]) -> dict:
    """Return {"text", "engine", "tokens_in", "tokens_out", "fallback_reason", "attempts", ...}.

    The model writes only the argument paragraphs. The letterhead, RE: block (claim number, amount,
    codes), request and enclosures are built from the extracted facts, so a small model cannot leave
    them out. The assembled letter is fact-checked; on failure the model gets ONE corrective retry
    that names exactly what was wrong, and after that the deterministic template is used.
    """
    parts = _parts(f, entry)
    tpl = assemble(parts, parts["arg"])
    if not llm.available():
        return _fallback(tpl, "no open model reachable", parts["arg"])
    base = body_prompt(f, entry, wins, parts)
    tin = tout = 0
    attempts, feedback, body, model, text = [], None, "", "", tpl
    for n in (1, 2):
        usr = base if not feedback else (base + "\nYOUR PREVIOUS ATTEMPT HAD THESE PROBLEMS. FIX THEM:\n" + feedback +
                                         "\n\nPREVIOUS ATTEMPT:\n" + body + "\n")
        try:
            out = llm.chat(BODY_SYSTEM, usr, temperature=0.25 if n == 1 else 0.1, max_tokens=500)
        except Exception as e:
            if n == 1:
                return _fallback(tpl, f"model error: {type(e).__name__}", parts["arg"])
            break
        tin, tout, model = tin + out["tokens_in"], tout + out["tokens_out"], out["model"]
        body = clean_body(out["text"])
        text = assemble(parts, body)
        v = verify(text, f)
        failed = [{"name": c["name"], "detail": c["detail"]} for c in v["checks"] if c["critical"] and not c["ok"]]
        if len(body) < 80:
            failed.append({"name": "Argument section too short", "detail": f"{len(body)} characters"})
        attempts.append({"attempt": n, "body": body, "failed": failed})
        if not failed:
            break
        feedback = "; ".join(f"{c['name']} ({c['detail']})" for c in failed)
    return {"text": text, "engine": f"llm:{model}", "tokens_in": tin, "tokens_out": tout, "fallback_reason": None,
            "template": tpl, "attempts": len(attempts), "attempt_log": attempts, "body": body,
            "template_argument": parts["arg"], "argument": body}


PLACEHOLDER_OK = re.compile(r"\[[^\]]{2,60}\]")


def verify(text: str, f: dict) -> dict:
    """Ground the letter against the extracted facts."""
    checks = []

    def add(name, ok, detail="", critical=True):
        checks.append({"name": name, "ok": bool(ok), "detail": detail, "critical": critical})

    add("No leaked prompt text", not PROMPT_ECHO.search(text), "model repeated its instructions or excerpts" if PROMPT_ECHO.search(text) else "none")
    add("Cites the claim number", f.get("claim_id") and f["claim_id"] in text, f.get("claim_id") or "")
    add("Names the payer", f.get("payer") and f["payer"].lower() in text.lower(), f.get("payer") or "", critical=False)
    add("References the procedure code", f.get("cpt") and f["cpt"] in text, f"CPT {f.get('cpt')}")
    add("References the denial code", f.get("carc") and re.search(rf"\b{f['carc']}\b", text), f"CO-{f.get('carc')}")
    amts = []
    for a in re.findall(r"\$\s?([\d,]+(?:\.\d+)?)", text):
        try:
            amts.append(float(a.replace(",", "")))
        except ValueError:
            pass
    if f.get("denied") is not None:
        add("States the denied amount correctly", any(abs(a - f["denied"]) < 1.0 for a in amts), _money(f["denied"]))
    # hallucination guard: every 5-digit code / dollar figure must trace to the facts
    allowed_nums = {str(f.get(k)) for k in ("cpt", "npi", "claim_id", "member_id") if f.get(k)}
    date_like = set(re.findall(r"\d{5}", " ".join(re.findall(r"\d{4}-\d{2}-\d{2}", text))))
    stray = [c for c in set(re.findall(r"(?<![\w.$,-])(\d{5})(?![\w-])", text)) if c not in allowed_nums
             and c not in re.sub(r"\D", "", str(f.get("claim_id") or "")) and c not in date_like]
    add("No unsupported procedure codes", not stray, ("Unsupported: " + ", ".join(stray)) if stray else "all codes trace to the notice")
    known = [v for v in (f.get("billed"), f.get("denied"), (f.get("billed") or 0) - (f.get("denied") or 0), f.get("billed")) if v is not None]
    strays = sorted({a for a in amts if not any(abs(a - k) < 1.0 for k in known)})
    add("No unsupported dollar amounts", not strays, ("Unsupported: " + ", ".join(f"${a:,.2f}" for a in strays)) if strays else "all amounts trace to the notice")
    add("No guaranteed-outcome language", not re.search(r"\b(guarantee|will be overturned|must pay|sue|lawsuit)\b", text, re.I), "", critical=False)
    if f.get("days_left") is not None:
        add("Inside the appeal window", f["days_left"] >= 0, f"{f['days_left']} days left" if f["days_left"] >= 0 else f"expired {-f['days_left']} days ago", critical=False)
    todo = sorted(set(PLACEHOLDER_OK.findall(text)))
    crit_fail = [c for c in checks if c["critical"] and not c["ok"]]
    return {"checks": checks, "todo": todo, "passed": not crit_fail,
            "score": round(sum(c["ok"] for c in checks) / len(checks), 2)}
