"""Turn a denial notice (text or PDF) into structured fields.

Deterministic regex extraction first (fast, auditable, works with no model); then, if an
open-weight model is reachable, it fills in anything the regexes missed -- as JSON that is
validated against the source text before it is accepted.
"""
from __future__ import annotations
import io, json, re
from datetime import date, datetime, timedelta
from . import llm, kb

PAYER_NAMES = ["Aetna", "UnitedHealthcare", "United Healthcare", "Cigna", "BCBS", "Blue Cross", "Humana", "Medicare", "Medicaid", "Anthem", "Kaiser"]


def read_bytes(name: str, data: bytes) -> str:
    if name.lower().endswith(".pdf"):
        from pypdf import PdfReader
        r = PdfReader(io.BytesIO(data))
        return "\n".join((p.extract_text() or "") for p in r.pages)
    return data.decode("utf-8", errors="replace")


def _money(s):
    return float(s.replace(",", "")) if s else None


def _d(s):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def regex_extract(t: str) -> dict:
    f: dict = {}

    def grab(pat, flags=re.I):
        m = re.search(pat, t, flags)
        return m.group(1).strip() if m else None

    f["claim_id"] = grab(r"(?:claim\s*(?:number|no\.?|#|id)|ICN)\s*[:#]?\s*([A-Z0-9-]{5,})")
    f["patient"] = grab(r"patient(?:\s*name)?\s*:\s*([^\n]+)")
    f["member_id"] = grab(r"member\s*id\s*:\s*([A-Z0-9-]+)")
    prov = grab(r"provider\s*:\s*([^\n]+)")
    if prov:
        m = re.match(r"(.*?)\s*\(NPI\s*(\d{10})\)", prov)
        f["provider"], f["npi"] = (m.group(1), m.group(2)) if m else (prov, grab(r"NPI\s*[:#]?\s*(\d{10})"))
    f["dos"] = (lambda x: _d(x) if x else None)(grab(r"date of service\s*:\s*([^\n]+)"))
    f["denial_date"] = (lambda x: _d(x) if x else None)(grab(r"date of (?:notice|denial)\s*:\s*([^\n]+)"))
    head = t[:300]
    f["payer"] = next((p for p in PAYER_NAMES if re.search(p, head, re.I)), None)
    if f["payer"] in ("United Healthcare",):
        f["payer"] = "UnitedHealthcare"
    if f["payer"] == "Blue Cross":
        f["payer"] = "BCBS"
    m = re.search(r"CPT\s*(\d{5})\s*[-–:]\s*([^\n]+)", t, re.I)
    if m:
        f["cpt"], f["cpt_desc"] = m.group(1), m.group(2).strip()
    m = re.search(r"diagnosis\s*:\s*([A-TV-Z]\d{2}(?:\.\w{1,4})?)\s*(?:[-–]\s*([^\n]+))?", t, re.I)
    if m:
        f["icd10"], f["icd10_desc"] = m.group(1).upper(), (m.group(2) or "").strip()
    f["billed"] = _money(grab(r"billed(?: amount)?\s*:\s*\$?([\d,]+\.?\d*)"))
    f["denied"] = _money(grab(r"denied(?: amount)?\s*:\s*\$?([\d,]+\.?\d*)"))
    codes = re.findall(r"\b(CO|PR|OA|PI|CR)[-\s]?(\d{1,3})\b", t)
    f["carc_group"] = codes[0][0].upper() if codes else None
    f["carc"] = codes[0][1] if codes else (grab(r"reason code\s*[:#]?\s*(\d{1,3})") or None)
    f["rarc"] = re.findall(r"\b([NM]\d{1,3})\b", t)[:3]
    f["reason_text"] = grab(r"(?:CO|PR|OA)[-\s]?\d+[^\n]*\n([^\n]+)")
    m = re.search(r"within\s+(\d{2,3})\s+days", t, re.I)
    f["appeal_window_days"] = int(m.group(1)) if m else None
    return f


REQUIRED = ["claim_id", "payer", "cpt", "denied", "carc", "denial_date"]


def llm_fill(text: str, f: dict) -> tuple[dict, dict | None]:
    missing = [k for k in REQUIRED if not f.get(k)]
    if not missing or not llm.available():
        return f, None
    sys = "You extract fields from US healthcare claim denial notices. Reply with one JSON object only. Use null if a field is absent. Never guess."
    usr = ("Fields: claim_id, payer, cpt (5 digits), denied (number), carc (digits only, from CO-xx/PR-xx), "
           "denial_date (YYYY-MM-DD).\n\nNOTICE:\n" + text[:6000])
    try:
        out = llm.chat(sys, usr, json_mode=True, temperature=0, max_tokens=300)
        j = json.loads(re.search(r"\{.*\}", out["text"], re.S).group(0))
    except Exception:
        return f, None
    for k in missing:
        v = j.get(k)
        if v in (None, "", "null"):
            continue
        if k == "denial_date":
            v = _d(str(v))
        elif k == "denied":
            try: v = float(v)
            except Exception: continue
        elif k == "carc":
            v = re.sub(r"\D", "", str(v))
        # grounding check: the value must literally appear in the source text
        if k in ("claim_id", "cpt", "carc") and str(v) not in text:
            continue
        if v:
            f[k] = v
            f.setdefault("_llm_filled", []).append(k)
    return f, out


def finalize(f: dict) -> dict:
    """Derive deadline and sanity fields."""
    from .config import today
    TODAY = today()
    win = f.get("appeal_window_days") or kb.window_days(f.get("payer") or "")
    f["appeal_window_days"] = win
    dd = f.get("denial_date")
    if dd:
        f["deadline"] = dd + timedelta(days=win)
        f["days_left"] = (f["deadline"] - TODAY).days
        f["days_since_denial"] = (TODAY - dd).days
    if f.get("billed") and f.get("denied") and f["denied"] > f["billed"]:
        f["warnings"] = f.get("warnings", []) + ["Denied amount exceeds billed amount: verify the notice."]
    return f


def jsonable(f: dict) -> dict:
    return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in f.items()}
