import { createTool } from '@mastra/core/tools';
import { z } from 'zod';

"use strict";
const BASE = process.env.OVERTURN_URL ?? "http://127.0.0.1:8000";
async function getQueue() {
  const r = await fetch(`${BASE}/api/queue`);
  if (!r.ok) throw new Error(`Overturn queue failed: HTTP ${r.status}`);
  return await r.json();
}
async function analyzeClaim(claimId) {
  const s = await fetch(`${BASE}/api/sample/${encodeURIComponent(claimId)}`);
  if (!s.ok) throw new Error(`Unknown claim ${claimId} (HTTP ${s.status})`);
  const { text, inputs } = await s.json();
  const r = await fetch(`${BASE}/api/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, inputs })
  });
  if (!r.ok || !r.body) throw new Error(`Overturn analyze failed: HTTP ${r.status}`);
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  let final = null;
  let error = null;
  for (; ; ) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      const ev = JSON.parse(line);
      if (ev.type === "final") final = ev.result;
      if (ev.type === "error") error = ev.error ?? "pipeline error";
    }
  }
  if (!final) throw new Error(error ?? "Overturn finished without a result");
  return final;
}

"use strict";
function why(e) {
  if (e instanceof Error) {
    const cause = e.cause;
    return e.message + (cause ? ` (cause: ${cause?.code ?? cause?.message ?? String(cause)})` : "");
  }
  try {
    return JSON.stringify(e);
  } catch {
    return String(e);
  }
}
const listQueue = createTool({
  id: "list-denial-queue",
  description: "List the clinic's open claim denials, ranked by expected recovery (overturn probability x amount minus cost to work it).",
  inputSchema: z.object({ top: z.number().int().min(1).max(20).default(5) }),
  // Mastra <1.0 passes { context }, newer versions pass the validated input directly.
  execute: async (arg) => {
    try {
      const context = arg?.context ?? arg;
      const top = Number(context?.top ?? 5);
      const q = await getQueue();
      return {
        total_expected_recovery: q.total_ev,
        total_denied: q.total_denied,
        claims: q.items.slice(0, top).map((c) => ({
          id: c.id,
          payer: c.payer,
          denial_code: `CO-${c.carc}`,
          reason: c.title,
          denied: c.denied,
          days_left_to_appeal: c.days_left,
          overturn_probability: c.p,
          best_move: c.best,
          expected_value: c.ev
        }))
      };
    } catch (e) {
      console.error("[overturn-tools] list-denial-queue failed:", e);
      return { error: `list-denial-queue failed: ${why(e)}` };
    }
  }
});
const analyzeClaimTool = createTool({
  id: "analyze-claim",
  description: "Run the full Overturn analysis for one claim: extract facts, decode the denial, predict overturn odds, compare appeal vs correct-and-resubmit vs write-off, and draft a fact-checked appeal letter.",
  inputSchema: z.object({ claim_id: z.string() }),
  execute: async (arg) => {
    try {
      const context = arg?.context ?? arg;
      const r = await analyzeClaim(String(context?.claim_id));
      return {
        claim_id: r.facts.claim_id,
        payer: r.facts.payer,
        denied: r.facts.denied,
        denial: r.entry.title,
        days_left: r.facts.days_left,
        overturn_probability: r.prediction.p,
        recommended: r.triage.recommended,
        why: r.triage.why,
        letter_engine: r.letter.engine,
        fact_check_passed: r.verify.passed,
        open_items: r.verify.todo,
        letter: r.letter.text
      };
    } catch (e) {
      console.error("[overturn-tools] analyze-claim failed:", e);
      return { error: `analyze-claim failed: ${why(e)}` };
    }
  }
});

export { analyzeClaimTool, listQueue };
