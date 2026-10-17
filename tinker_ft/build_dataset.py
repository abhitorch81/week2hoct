"""Build the SFT dataset for Tinker: (prompt, completion) pairs for the *argument section only*.

prompt     = exactly what the app sends the model (letter.BODY_SYSTEM + letter.body_prompt)
completion = playbook argument for that denial code + the lessons of a past winning appeal for the same code,
             kept only if the assembled letter passes letter.verify() and the argument is <= 170 words.

All data is synthetic. With a real clinic you would replace the completions with the argument paragraphs of
appeals that actually won (see the "Mark as won" button, which saves exactly that).

    python -m tinker_ft.build_dataset [N=300]  ->  data/tinker_sft.jsonl
"""
from __future__ import annotations
import json, random, sys
from overturn import arena, config, letter, vectorstore

OUT = config.DATA / "tinker_sft.jsonl"


def build(n: int = 300, seed: int = 7):
    rng = random.Random(seed)
    store = vectorstore.get()
    kept, dropped = [], 0
    for i in range(n):
        case = arena.prepare_case(1000 + i)          # same prepare/prompt code path as the arena
        f, entry, parts = case["f"], case["entry"], case["parts"]
        same = [d for d in vectorstore._load_corpus() if d["carc"] == str(f["carc"])]
        extra = rng.choice(same)["text"] if same else ""
        sentences = [s.strip() for s in extra.replace("\n", " ").split(". ") if s.strip()]
        lesson = (" ".join(s.rstrip(".") + "." for s in sentences[:2])) if sentences else ""
        completion = parts["arg"].strip() + ("\n\n" + lesson if lesson else "")
        text = letter.assemble(parts, completion)
        v = letter.verify(text, f)
        if not v["passed"] or len(completion.split()) > 170 or len(completion) < 80:
            dropped += 1
            continue
        kept.append({"system": letter.BODY_SYSTEM, "prompt": case["prompt"], "completion": completion, "carc": f["carc"]})
    return kept, dropped


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    rows, dropped = build(n)
    OUT.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    print(f"wrote {len(rows)} examples to {OUT} ({dropped} dropped by the fact-check/length filter)")
