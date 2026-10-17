# Article outline: "Overturn: appeals that never leave the clinic"

**Hook.** Small clinics write off a large share of denied claims because working each one costs more staff time than it is worth, and sending patient data to a closed API is a compliance headache. Overturn makes every denial cheap to work and keeps PHI on the machine.

**Sections**
1. The problem: a denial notice is PHI plus money. Show screenshot `docs/2_decision.png`.
2. What it does: 8-step agent (read, extract, decode, predict, retrieve, triage, draft, fact-check).
3. Why open mattered (fill with your own measurements):
   - Privacy: header badge counts external model calls; show "0".
   - Cost: run 14 queue items; record tokens and latency from the Trace tab; compare to an API price you cite yourself.
   - Control: swap the model with one env var; the guardrail catches hallucinated codes/amounts (see `tests/test_overturn.py::test_hallucinated_llm_draft_is_rejected`).
   - Fine-tune angle: clinic's own winning appeals are the corpus (and a Tinker fine-tune is the natural next step).
4. Sponsor sections (one paragraph + screenshot each): TabPFN (odds + counterfactual levers), Tiger Data (pgvector hybrid retrieval), Temporal (crash/resume demo, `docs/5_crash.png`), Sentry (waterfall, `docs/4_trace.png`), Render (blueprint).
5. What did not work / honest limits: synthetic data, odds model is only as good as the clinic's history, drafts need human review, appeal windows vary by payer.
6. What next.

**Before you publish: run these on your machine and replace the "not tested" rows in README**
- `ollama pull qwen2.5:7b-instruct` and record real token/latency numbers.
- `pip install tabpfn-client` + `PRIORLABS_API_KEY`: compare AUC vs the sklearn fallback shown in the header.
- Start the compose stack (pgvector + Temporal) and kill the worker mid-run for the Temporal demo.
- Set `SENTRY_DSN` and capture a real trace screenshot.


## Sponsor sections: what to run, screenshot and say

**Temporal.** `temporal server start-dev`, `pip install temporalio`, set `TEMPORAL_ADDRESS=localhost:7233`, run `python -m overturn.temporal_workflow worker` in one terminal and the app in another. Run a claim, screenshot the Temporal UI (workflow history, 8 activities). Then pick "crash" mode / kill the worker mid-run and show it resume. The retry demo ("flaky") shows an activity attempt 2. Say: the same eight functions run under the toy runner or Temporal unchanged. Status: simulated in tests only until you run it.

**Tiger Data.** Follow `docs/TIGER.md`: create a Tiger Cloud service, `pip install "psycopg[binary]"`, set `DATABASE_URL`. The header chip should read BM25 + DiskANN (or the fallback it names). Run `python scripts/hybrid_compare.py` (or `/api/search?q=...`) to show keyword vs vector vs hybrid on a query where they disagree; mark a won appeal and show it retrieved next time. If an extension is unavailable the app logs which index it skipped; report that honestly.

**Render.** `docs/RENDER.md`: blueprint deploy (web + private Ollama service with a disk). Screenshot the live URL and the service graph. Be upfront that CPU inference on Render is slow; a tiny model is fine for a demo.

**Backboard.** Open "Model arena", pick 3 to 5 open-weight models plus the local baseline, run 6 to 12 notices. Report the first-draft pass rate, latency and the top failure per model. This is a real finding for the article: which models need the corrective retry and which do not. Caveat: small samples on synthetic data; and it is the only place data leaves the machine.

**Mastra.** `mastra-agent/README.md`. Screenshot Mastra Studio with the agent calling `analyze-claim`, and the triage workflow run. Use a 7B+ model for reliable tool calling.

**Entire.** `docs/ENTIRE.md`. Enable it and do a real follow-up session (e.g. add a denial code); screenshot `entire status` and a checkpoint explain. Do not claim it recorded the original build unless it did.

**Tinker.** `docs/TINKER.md`. Train the LoRA (under $1 of your $10 credit), then run the Model arena with base vs fine-tuned Qwen next to the local model. The headline number is first-draft pass rate before and after tuning, plus the template fallback as the honest baseline. Screenshot the Tinker console run, the loss printout and the arena ranking. Say plainly that the training data is synthetic and what that limits.
