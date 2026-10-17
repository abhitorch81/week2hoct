import { Agent } from "@mastra/core/agent";
import { Memory } from "@mastra/memory";
import { LibSQLStore } from "@mastra/libsql";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import { analyzeClaimTool, listQueue } from "../tools/overturn-tools";

// Open-weight model served locally by Ollama through its OpenAI-compatible endpoint.
const ollama = createOpenAICompatible({
  name: "ollama",
  baseURL: process.env.OLLAMA_OPENAI_URL ?? "http://127.0.0.1:11434/v1",
});

// Memory persists the working thread (which claims were reviewed, what the biller decided).
const memory = new Memory({
  storage: new LibSQLStore({ id: "denial-desk-memory", url: "file:./denial-desk.db" }),
  options: { lastMessages: 20 },
});

export const denialDesk = new Agent({
  name: "Denial Desk",
  instructions: [
    "You are the denial-management assistant for a small medical clinic's biller.",
    "Use list-denial-queue to see what is open and analyze-claim to work a specific claim.",
    "Always report: the denial reason in plain English, days left to appeal, the recommended move and why, and whether the letter passed the fact-check.",
    "Never invent claim numbers, amounts or codes: quote only what the tools return. If a tool fails, say so.",
    "A letter is a DRAFT for a human to review and sign. Never say an appeal will succeed; give the probability the tool returned.",
  ].join("\n"),
  model: ollama(process.env.DENIAL_DESK_MODEL ?? "qwen2.5:7b-instruct"),
  tools: { listQueue, analyzeClaim: analyzeClaimTool },
  memory,
});
