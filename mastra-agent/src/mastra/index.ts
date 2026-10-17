import { Mastra } from "@mastra/core/mastra";
import { LibSQLStore } from "@mastra/libsql";
import { denialDesk } from "./agents/denial-desk";
import { triageQueue } from "./workflows/triage-queue";

export const mastra = new Mastra({
  agents: { denialDesk },
  workflows: { triageQueue },
  storage: new LibSQLStore({ id: "overturn-mastra-store", url: "file:./mastra.db" }),
});
