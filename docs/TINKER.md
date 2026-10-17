# Tinker: fine-tune a small open model on the clinic's winning appeals

**Idea.** Overturn's model only writes the *argument section* of an appeal (the facts are filled in by code). That is a narrow, checkable task, ideal for a small fine-tuned model: cheaper than a big one, and the fact-checker gives an objective score. The Model arena compares the **base model** with **your fine-tuned LoRA** on notices it has never seen, using the same prompt and the same checker.

> **Status.** `build_dataset.py` is run and unit-tested (300 examples built, all pass the fact-check). `train.py` and the arena's Tinker sampling are written from the Tinker SDK docs and **have not been run** (the SDK could not be installed in the build sandbox). Treat the first run as the test; errors will name the line. Your results go in the article; none are claimed here.

## Cost
Your screenshot shows a $10 grant (Tinker x MLH, valid to Oct 16 2027). Qwen3.5-4B listed at $0.737 per million training tokens when this was written (check the Models & Pricing page). The dataset is about 140k tokens, so 3 epochs is roughly 0.4M tokens, **well under $1**. Sampling for the arena is $1.005 per million tokens.

## Steps (Windows, Anaconda Prompt in the project folder)

```bat
pip install tinker
set TINKER_API_KEY=your-key          :: console -> API Keys
python -m tinker_ft.build_dataset 300
python -m tinker_ft.train
```

`train.py` prints `SAMPLER WEIGHTS: tinker://.../sampler_weights/overturn-appeal-writer` and downloads the LoRA adapter to `./adapter`.

Then restart Overturn with both variables set:

```bat
set TINKER_API_KEY=your-key
set TINKER_MODEL_PATH=tinker://...the path printed above...
python run.py
```

Open **Model arena**: you now get "Qwen3.5-4B base (Tinker)" and "... fine-tuned on winning appeals (Tinker)" next to the local model and any Backboard models. Run 12 notices each. Screenshot the ranking.

Knobs (env): `TINKER_BASE_MODEL` (default `Qwen/Qwen3.5-4B`), `TUNE_EPOCHS` 3, `TUNE_BATCH` 8, `TUNE_LR` 1e-4, `TUNE_RANK` 16. Model names change; if Qwen3.5-4B is retired, pick one from the Models page.

## What to be honest about in the article
* The synthetic completions are the playbook argument plus lessons from 18 past wins, so the tune teaches **format and house style** (two short paragraphs, no letterhead, placeholders instead of invented facts). It will not discover new legal arguments. With a real clinic you replace them with the arguments of appeals that actually won (the "Mark as won" button saves exactly that).
* The arena is a small smoke test, not a benchmark. Report base vs tuned first-draft pass rate, and what changed in the failure list.
* The prompt includes the core argument, so a tuned model may largely copy it. Say whether the tuned model beats simply using the template; the fallback to the template is the real baseline.
* Using the tuned model **locally** means merging or converting the adapter (the Tinker cookbook has "Merge into a HuggingFace model" and "Build a PEFT adapter" guides) and serving it with vLLM or converting for Ollama. That step is not done here.
* Arena and Tinker sampling send only synthetic notices to hosted services and are counted as external calls.
