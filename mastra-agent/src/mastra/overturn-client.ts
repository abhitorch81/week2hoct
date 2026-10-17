// Thin client for the Overturn HTTP API. All patient data stays on the Overturn server; the agent only
// ever sees the structured result (facts, odds, recommendation, letter) it asks for.
const BASE = process.env.OVERTURN_URL ?? "http://127.0.0.1:8000";

export async function getQueue() {
  const r = await fetch(`${BASE}/api/queue`);
  if (!r.ok) throw new Error(`Overturn queue failed: HTTP ${r.status}`);
  return (await r.json()) as { items: any[]; total_ev: number; total_denied: number };
}

/** Runs the full Overturn pipeline for a queue claim and returns the final result object. */
export async function analyzeClaim(claimId: string) {
  const s = await fetch(`${BASE}/api/sample/${encodeURIComponent(claimId)}`);
  if (!s.ok) throw new Error(`Unknown claim ${claimId} (HTTP ${s.status})`);
  const { text, inputs } = (await s.json()) as { text: string; inputs: Record<string, boolean> };
  const r = await fetch(`${BASE}/api/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, inputs }),
  });
  if (!r.ok || !r.body) throw new Error(`Overturn analyze failed: HTTP ${r.status}`);
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  let final: any = null;
  let error: string | null = null;
  for (;;) {
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
