# Denial Desk — a Mastra agent on top of Overturn

A conversational front-end for the Overturn pipeline, built with [Mastra](https://mastra.ai):

* **Agent** (`src/mastra/agents/denial-desk.ts`): an open-weight model (Ollama, OpenAI-compatible endpoint) with two tools and persistent memory (LibSQL). Ask it "what should I work on first?" or "draft the appeal for CLM-1003".
* **Tools** (`tools/overturn-tools.ts`): `list-denial-queue` and `analyze-claim`, thin wrappers over Overturn's HTTP API. The agent never sees raw notices, only the structured result.
* **Workflow** (`workflows/triage-queue.ts`): deterministic `pick top claims -> analyze each -> report`, with typed steps. Use it for the morning batch; use the agent for questions.

> **Status: written against Mastra's documented API, not run in the build sandbox.** Mastra's packages move fast; if an import path or option name has changed, `npx mastra dev` will tell you which line. Fix it there; the shape (agent + tools + workflow + memory) stays the same.

## Run it

```bash
# 1) in the main project: python run.py        (Overturn on http://127.0.0.1:8000)
# 2) Ollama running with a tool-capable model: ollama pull qwen2.5:7b-instruct
cd mastra-agent
cp .env.example .env       # (Windows: copy .env.example .env)
npm install
npx mastra dev             # Studio at http://localhost:4111
```

Use a tool-calling-capable model. The 3B model may ignore tools; 7B and up is more reliable. Overturn itself runs fine on 3B.

The queue claim ids come from the synthetic queue (`/api/queue`).
