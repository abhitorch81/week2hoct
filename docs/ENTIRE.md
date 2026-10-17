# Entire: recording how Overturn was built

[Entire](https://github.com/entireio/cli) captures AI-agent sessions (prompts, transcript, files touched) as **checkpoints stored in your git repo**, linked to your commits by an `Entire-Checkpoint:` trailer. For this challenge the point is a reviewable record of *how* the project was built with an agent, not just the finished code.

> **Honest status.** Entire records sessions *as they happen*. The Overturn code in this folder was generated in a Claude chat session that Entire did not observe, so no checkpoints ship with it. To qualify for "Best Use of Entire", do the steps below with your own coding agent while you keep building (extend Overturn, run the Mastra agent, fix the issues you hit). The commands are from Entire's README (checked October 2026); run `entire --help` if something has changed.

## 1. Install (Windows)

PowerShell 5.1+:

```powershell
irm https://entire.io/install.ps1 | iex
```

or with Scoop: `scoop bucket add entire https://github.com/entireio/scoop-bucket.git` then `scoop install entire/entire`. It installs `entire.exe` in `%USERPROFILE%\.local\bin`; open a new terminal so PATH updates. Requires Git.

Supported agents: Claude Code, Codex, Cursor, Copilot CLI, Factory AI Droid, OpenCode, Antigravity, Pi (preview). You need one installed and logged in.

## 2. Put Overturn in a git repo and enable Entire

```powershell
cd overturn
git init
git add . ; git commit -m "Overturn: initial import"
entire enable            # interactive: choose your agent
entire status            # verify
```

Non-interactive: `entire enable --agent claude-code` (check `entire agent list` for exact names). Use `--local` to keep settings out of the repo.

## 3. Build something real with the agent

Give your agent tasks that leave a story in the transcript, for example:

1. "Add a CARC code CO-45 to `data/carc.json` with an argument, evidence list and tests."
2. "The model arena shows qwen scoring low on 'No unsupported dollar amounts'. Find out why and fix the prompt."
3. "Make the Mastra Denial Desk agent run against my Overturn server and fix any import errors."

Commit as you go. Entire never commits to your branch; each of your commits gets a checkpoint trailer.

## 4. Use the history (this is your demo material)

```powershell
entire session list                 # sessions recorded in this repo
entire checkpoint list              # checkpoints
entire checkpoint explain <id>      # what happened in one checkpoint
entire search "hallucinated"        # search checkpoints, commits and sessions
entire session resume <branch>      # restore a checkpointed session on a branch
git for-each-ref refs/entire/checkpoints    # the raw refs live in your repo
```

Screenshots worth taking for the article: `entire status`, a `checkpoint explain` of the letter-guardrail fix, and a `search` result.

## 5. Privacy: read before you push

* Transcripts live **in your repository**. If you push to a public GitHub repo, the prompts and agent output are public.
* Entire redacts detected secrets (API keys, tokens) on a best-effort basis. Do not rely on that: never paste keys, real PHI or real claim data into agent prompts. Overturn's data is synthetic; keep it that way.
* Push the checkpoint refs only if you intend to publish them (`git push origin 'refs/entire/*'` or Entire's own sync; check `entire --help`).

## What to write in the article

Say what you actually recorded: which agent, how many sessions, one concrete moment where the history helped (a regression traced back to a prompt change, for instance). If you only enabled Entire for the last part of the build, say that.
