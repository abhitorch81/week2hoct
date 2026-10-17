import { createStep, createWorkflow } from "@mastra/core/workflows";
import { z } from "zod";
import { analyzeClaim, getQueue } from "../overturn-client";

const claimSummary = z.object({
  id: z.string(), payer: z.string(), denied: z.number(), best_move: z.string(), expected_value: z.number(),
  days_left: z.number(),
});

const pickTop = createStep({
  id: "pick-top-claims",
  inputSchema: z.object({ top: z.number().int().min(1).max(10) }),
  outputSchema: z.object({ claims: z.array(claimSummary) }),
  execute: async ({ inputData }) => {
    const q = await getQueue();
    return {
      claims: q.items
        .filter((c) => c.best !== "writeoff")
        .slice(0, inputData.top)
        .map((c) => ({ id: c.id, payer: c.payer, denied: c.denied, best_move: c.best, expected_value: c.ev, days_left: c.days_left })),
    };
  },
});

const draftEach = createStep({
  id: "draft-each",
  inputSchema: z.object({ claims: z.array(claimSummary) }),
  outputSchema: z.object({
    drafts: z.array(z.object({ id: z.string(), move: z.string(), fact_check_passed: z.boolean(), engine: z.string() })),
  }),
  execute: async ({ inputData }) => {
    const drafts = [];
    for (const c of inputData.claims) {
      const r = await analyzeClaim(c.id);
      drafts.push({ id: c.id, move: r.triage.recommended, fact_check_passed: !!r.verify.passed, engine: r.letter.engine });
    }
    return { drafts };
  },
});

const report = createStep({
  id: "report",
  inputSchema: z.object({
    drafts: z.array(z.object({ id: z.string(), move: z.string(), fact_check_passed: z.boolean(), engine: z.string() })),
  }),
  outputSchema: z.object({ summary: z.string() }),
  execute: async ({ inputData }) => ({
    summary: inputData.drafts
      .map((d) => `${d.id}: ${d.move} (${d.fact_check_passed ? "fact-check passed" : "NEEDS REVIEW"}, ${d.engine})`)
      .join("\n"),
  }),
});

export const triageQueue = createWorkflow({
  id: "triage-queue",
  inputSchema: z.object({ top: z.number().int().min(1).max(10) }),
  outputSchema: z.object({ summary: z.string() }),
})
  .then(pickTop)
  .then(draftEach)
  .then(report)
  .commit();
