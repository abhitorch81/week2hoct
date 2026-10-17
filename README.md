# Overturn: a local-first denial appeal agent for small clinics

Paste or upload a payer denial notice (EOB, PDF or text). Overturn:

1. **Reads it** and extracts the claim facts (regex first, open model fills gaps, every model-filled value is grounded against the source text).
2. **Decodes the denial** (CARC/RARC playbook with plain-English meaning and an evidence checklist) and computes the appeal deadline.
3. **Predicts overturn odds** from claim history (TabPFN, or scikit-learn fallback) and shows *what would move the odds* (e.g. "attach clinical notes: +9 pts").
4. **Retrieves similar past wins** (hybrid search: keyword/BM25 + vector, fused with RRF, boosted by denial code and payer; learns from wins you mark).
5. **Picks the best move** by expected value: appeal, correct & resubmit, or write off.
6. **Drafts the appeal** with an open-weight model, then **fact-checks** it against the notice. The letterhead, claim block, request and enclosures are built from the facts; the model writes only the argument. A failing draft gets one corrective retry, then falls back to a deterministic letter, and the rejected draft is shown.
7. Runs as a **durable workflow** (retries, checkpoints, crash/resume) with a **trace waterfall** (and Sentry mirroring).

Everything runs on your machine. The header shows the model, where it ran, and a live count of external model calls (it is 0 unless you point it at a remote endpoint).

> All data in this repo is **synthetic**. Letters are **drafts for billing-staff review**; this is not legal or clinical advice. Appeal windows and criteria are payer- and plan-specific; confirm against the notice.

## Run it (2 minutes)

```bash
pip install -r requirements.txt
ollama pull qwen2.5:7b-instruct        # any open-weight chat model works
python run.py                          # http://127.0.0.1:8000
python -m unittest discover -s tests   # 7 tests, includes a fake-Ollama hallucination test
```

No model installed? It still works: the badge shows "template engine" and letters come from the deterministic playbook. With llama.cpp / vLLM / LM Studio set `OPENAI_BASE_URL=http://127.0.0.1:8080/v1`.

## Why open models matter here

* **PHI never leaves the building.** A denial notice contains a patient name, member ID and diagnosis. With a local open-weight model there is no business-associate agreement to negotiate with an API vendor, and nothing to leak.
* **Unit economics.** An appeal is worth tens to thousands of dollars; per-token pricing on every notice, retry and re-draft eats the margin on a $90 claim. Local inference is a fixed cost, so every denial is worth working.
* **Swap and tune.** The model is one env var. Fine-tune on a clinic's own winning appeals without anyone's permission; the retrieval corpus and playbook are plain files.
* **Guardrails you control.** Because the draft is just text from a model you host, the fact-checker can reject it and fall back deterministically.

## Architecture

```
notice ─▶ ingest ─▶ extract ─▶ classify ─▶ predict ─▶ retrieve ─▶ triage ─▶ draft ─▶ verify ─▶ letter
                     regex +     CARC KB    TabPFN/     pgvector     EV of     open LLM   ground vs.
                     open LLM    + deadline sklearn     hybrid       3 moves   (Ollama)   notice
        └──────────── same steps run under: durable.py (default) or Temporal ────────────┘
```

| Piece | File | Notes |
|---|---|---|
| Open model client | `overturn/llm.py` | Ollama or any OpenAI-compatible local server; records host of every call |
| Odds model | `overturn/predict.py` | TabPFN client / local TabPFN / sklearn behind one interface; counterfactual "levers" |
| Retrieval | `overturn/vectorstore.py` | pgvector (Tiger Data) if `DATABASE_URL`, else in-memory with identical scoring |
| Durable runner | `overturn/durable.py` | checkpoint per step, retry with backoff, resume by `run_id` |
| Temporal | `overturn/temporal_workflow.py` | same steps as Activities; `TEMPORAL_ADDRESS` switches the server to it |
| Tracing | `overturn/tracing.py` | local waterfall + Sentry spans (metadata only, `send_default_pii=False`) |
| Guardrails | `overturn/letter.py` | `verify()` grounds codes/amounts against the notice |
| Playbook | `data/carc.json` | 12 common denial codes: meaning, argument, evidence, recode odds |

## Sponsor integrations: what is wired and what was tested

| Integration | Where | Tested in the build sandbox |
|---|---|---|
| Open model (Ollama / OpenAI-compatible) | `overturn/llm.py`, `letter.py` | logic tested against a fake Ollama server (bad, omitting and self-correcting models); real 3B/7B runs were done by the author on Windows |
| TabPFN | `predict.py` (`PRIORLABS_API_KEY`) | **sklearn fallback only**; TabPFN could not be installed there |
| **Tiger Data** | `vectorstore.py` (`DATABASE_URL`), `docs/TIGER.md` | hybrid search (keyword + vector, RRF) tested on the in-memory twin; **the Postgres/BM25/DiskANN SQL has not been executed** |
| **Temporal** | `temporal_workflow.py`, `docs/` compose | activity logic tested (JSON round-trip, retry attempts); **never run against a real Temporal server**; the local durable runner is tested |
| **Render** | `render.yaml`, `Dockerfile`, `deploy/ollama/`, `docs/RENDER.md` | blueprint parses; **not deployed** |
| **Backboard** | `backboard.py`, `arena.py`, "Model arena" button | client and scoring tested against a fake Backboard server; **real API shapes unverified**, errors surface raw |
| **Tinker** | `tinker_ft/` (dataset, LoRA training), `tinker_model.py`, arena | dataset builder tested (300 pairs, all pass the fact-check) and arena wiring tested with a stub; **training and Tinker sampling never run** (SDK not installable there) |
| **Mastra** | `mastra-agent/` | **not run** (no npm packages in the sandbox); API usage follows Mastra's docs |
| **Entire** | `docs/ENTIRE.md` | guide only: Entire records sessions as they happen, so you must enable it and keep building |
| Sentry | `tracing.py` (`SENTRY_DSN`) | local trace tested; Sentry mirroring not run |

18 unit tests: `python -m unittest discover -s tests`. Run each integration once on your own machine before you write it up as a result; this table is the honest state.

### Model arena (Backboard)

`Model arena` (header button) sends the same synthetic notices and the same drafting prompt to a local baseline plus hosted open-weight models via Backboard, and ranks them by **first-draft pass rate** under the fact-checker. Set `BACKBOARD_API_KEY` and restart. It is the one feature that sends data off the machine, so it uses synthetic notices only, warns before running, and every call counts toward the external-call badge.

### Fine-tuning (Tinker)

`docs/TINKER.md`: build an SFT dataset from the playbook and past wins, LoRA-tune a small open model on Tinker, then compare base vs tuned in the Model arena.

### Denial Desk agent (Mastra)

`mastra-agent/` is a TypeScript Mastra agent (open model via Ollama, memory, two tools) plus a typed triage workflow, both calling Overturn's HTTP API. See its README.

## Configure

See `.env.example`. Cost assumptions for the triage panel: `OVERTURN_COST_APPEAL` ($85) and `OVERTURN_COST_RECODE` ($22).
