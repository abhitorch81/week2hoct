import { createTool } from "@mastra/core/tools";
import { z } from "zod";
import { analyzeClaim, getQueue } from "../overturn-client";

// Turn any thrown value into readable text (Mastra otherwise shows "[object Object]").
function why(e: unknown): string {
  if (e instanceof Error) {
    const cause = (e as any).cause;
    return e.message + (cause ? ` (cause: ${cause?.code ?? cause?.message ?? String(cause)})` : "");
  }
  try { return JSON.stringify(e); } catch { return String(e); }
}

export const listQueue = createTool({
  id: "list-denial-queue",
  description:
    "List the clinic's open claim denials, ranked by expected recovery (overturn probability x amount minus cost to work it).",
  inputSchema: z.object({ top: z.number().int().min(1).max(20).default(5) }),
  // Mastra <1.0 passes { context }, newer versions pass the validated input directly.
  execute: async (arg: any) => {
    try {
      const context = arg?.context ?? arg;
      const top = Number(context?.top ?? 5);
      const q = await getQueue();
      return {
        total_expected_recovery: q.total_ev,
        total_denied: q.total_denied,
        claims: q.items.slice(0, top).map((c: any) => ({
          id: c.id, payer: c.payer, denial_code: `CO-${c.carc}`, reason: c.title, denied: c.denied,
          days_left_to_appeal: c.days_left, overturn_probability: c.p, best_move: c.best, expected_value: c.ev,
        })),
      };
    } catch (e) {
      console.error("[overturn-tools] list-denial-queue failed:", e);
      return { error: `list-denial-queue failed: ${why(e)}` };
    }
  },
});

export const analyzeClaimTool = createTool({
  id: "analyze-claim",
  description:
    "Run the full Overturn analysis for one claim: extract facts, decode the denial, predict overturn odds, " +
    "compare appeal vs correct-and-resubmit vs write-off, and draft a fact-checked appeal letter.",
  inputSchema: z.object({ claim_id: z.string() }),
  execute: async (arg: any) => {
    try {
      const context = arg?.context ?? arg;
      const r = await analyzeClaim(String(context?.claim_id));
      return {
        claim_id: r.facts.claim_id, payer: r.facts.payer, denied: r.facts.denied, denial: r.entry.title,
        days_left: r.facts.days_left,
        overturn_probability: r.prediction.p,
        recommended: r.triage.recommended, why: r.triage.why,
        letter_engine: r.letter.engine,
        fact_check_passed: r.verify.passed,
        open_items: r.verify.todo,
        letter: r.letter.text,
      };
    } catch (e) {
      console.error("[overturn-tools] analyze-claim failed:", e);
      return { error: `analyze-claim failed: ${why(e)}` };
    }
  },
});
