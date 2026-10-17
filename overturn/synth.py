"""Synthetic (fake) denial data. No real patients, payers' internal data, or PHI.

Used for: the open-denial queue, sample denial letters, and the claims history
that the overturn-probability model is trained on.
"""
from __future__ import annotations
import random
from datetime import date, timedelta

PAYERS = ["Aetna", "UnitedHealthcare", "Cigna", "BCBS", "Humana", "Medicare", "Medicaid"]
CARCS = ["50", "16", "4", "11", "97", "29", "197", "18", "96", "151", "252", "109"]
BASE_P = {"50": .45, "16": .78, "4": .80, "11": .72, "97": .40, "29": .18, "197": .35,
          "18": .12, "96": .15, "151": .30, "252": .70, "109": .30}
# (cpt, description, specialty, icd10, icd10 description, typical billed)
PROCS = [
    ("72148", "MRI lumbar spine without contrast", "Radiology", "M54.16", "Radiculopathy, lumbar region", 1450),
    ("99214", "Office visit, established patient, moderate complexity", "Primary Care", "E11.65", "Type 2 diabetes with hyperglycemia", 245),
    ("29881", "Knee arthroscopy with meniscectomy", "Orthopedics", "M23.221", "Derangement of posterior horn of medial meniscus", 6200),
    ("97110", "Therapeutic exercise, each 15 minutes", "Physical Therapy", "M25.561", "Pain in right knee", 118),
    ("93306", "Echocardiogram, complete with Doppler", "Cardiology", "I50.9", "Heart failure, unspecified", 980),
    ("45378", "Diagnostic colonoscopy", "Gastroenterology", "K92.1", "Melena", 2750),
    ("64483", "Transforaminal epidural injection, lumbar", "Pain Management", "M51.26", "Other intervertebral disc displacement, lumbar", 3100),
    ("90837", "Psychotherapy, 60 minutes", "Behavioral Health", "F33.1", "Major depressive disorder, recurrent, moderate", 210),
    ("27447", "Total knee arthroplasty", "Orthopedics", "M17.11", "Primary osteoarthritis, right knee", 31800),
    ("70553", "MRI brain with and without contrast", "Radiology", "G43.909", "Migraine, unspecified", 2900),
]
CLINICS = ["Riverbend Family Medicine", "Lakeside Orthopedics", "Northgate Imaging Center",
           "Summit Pain & Spine", "Harbor Cardiology Associates", "Maple Street Behavioral Health"]
FIRST = ["Alex", "Jordan", "Sam", "Priya", "Maria", "Chen", "Omar", "Taylor", "Fatima", "Diego", "Noor", "Lena"]
LAST = ["Rivera", "Patel", "Nguyen", "Okafor", "Schmidt", "Haddad", "Kim", "Brooks", "Moreau", "Silva", "Ito", "Cohen"]

REASON_TEXT = {
    "50": "The service does not meet medical necessity criteria for the diagnosis reported.",
    "16": "Claim/service lacks information or has submission/billing error(s). Remark N290: missing/incomplete/invalid rendering provider identifier.",
    "4": "The procedure code is inconsistent with the modifier used or a required modifier is missing.",
    "11": "The diagnosis is inconsistent with the procedure.",
    "97": "The benefit for this service is included in the payment/allowance for another service/procedure that has already been adjudicated.",
    "29": "The time limit for filing has expired.",
    "197": "Precertification/authorization/notification is absent.",
    "18": "Exact duplicate claim/service.",
    "96": "Non-covered charge(s).",
    "151": "Payment adjusted because the payer deems the information submitted does not support this many/frequency of services.",
    "252": "An attachment/other documentation is required to adjudicate this claim/service.",
    "109": "Claim/service not covered by this payer/contractor. You must send the claim/service to the correct payer/contractor.",
}
from . import config
TODAY = config.today()


def make_open_denial(i: int, rng: random.Random | None = None) -> dict:
    rng = rng or random.Random(1000 + i)
    cpt, desc, spec, icd, icd_desc, typ = rng.choice(PROCS)
    carc = rng.choice(CARCS)
    payer = rng.choice(PAYERS)
    billed = round(typ * rng.uniform(0.9, 1.15), 2)
    denied = round(billed * rng.uniform(0.55, 1.0), 2)
    age = rng.choice([4, 7, 12, 18, 25, 33, 41, 55, 70, 90, 120])
    denial_date = TODAY - timedelta(days=age)
    dos = denial_date - timedelta(days=rng.randint(18, 48))
    return {
        "id": f"CLM-{26000 + i}", "payer": payer, "carc": carc, "cpt": cpt, "cpt_desc": desc,
        "specialty": spec, "icd10": icd, "icd10_desc": icd_desc, "billed": billed, "denied": denied,
        "denial_date": denial_date.isoformat(), "dos": dos.isoformat(),
        "patient": f"{rng.choice(FIRST)} {rng.choice(LAST)}", "member_id": f"{rng.choice('WXYZ')}{rng.randint(10**8, 10**9-1)}",
        "provider": rng.choice(CLINICS), "npi": str(rng.randint(10**9, 10**10-1)),
        "has_clinical_notes": rng.random() < .6, "prior_auth_on_file": rng.random() < .35,
        "in_network": rng.random() < .85, "appeal_level": 1,
    }


def render_denial_letter(d: dict) -> str:
    """Render a realistic (fake) EOB / denial notice from a record."""
    rarc = {"16": "N290", "252": "M127", "50": "N115", "97": "N20", "4": "N519", "11": "N657"}.get(d["carc"], "N130")
    return f"""{d['payer'].upper()} HEALTH PLAN
Notice of Claim Denial / Explanation of Benefits

Date of notice: {d['denial_date']}
Claim number: {d['id']}
Patient: {d['patient']}
Member ID: {d['member_id']}
Provider: {d['provider']} (NPI {d['npi']})
Date of service: {d['dos']}

Service detail
CPT {d['cpt']} - {d['cpt_desc']}
Diagnosis: {d['icd10']} - {d['icd10_desc']}
Billed amount: ${d['billed']:,.2f}
Denied amount: ${d['denied']:,.2f}

Adjustment reason: CO-{d['carc']}  Remark: {rarc}
{REASON_TEXT[d['carc']]}

Your appeal rights
If you disagree with this decision you may request a review by submitting a written appeal
with supporting documents within 180 days of the date of this notice. Please reference the
claim number above. Appeals can be mailed to the Provider Appeals Department or submitted
through the payer portal.
"""
