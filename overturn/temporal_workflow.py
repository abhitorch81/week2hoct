"""Temporal version of the same pipeline (optional).

    pip install temporalio
    temporal server start-dev                                  # local dev server + UI at http://localhost:8233
    python -m overturn.temporal_workflow worker                # terminal 2: the worker
    set TEMPORAL_ADDRESS=localhost:7233 && python run.py       # terminal 3: the app now dispatches to Temporal

Each pipeline step is an Activity: Temporal retries it automatically and stores its result in the
workflow's event history. The Workflow is just the ordered loop plus a `progress` query that the
web server polls to stream live step updates to the browser.

The crash demo: start a run, press Ctrl+C in the worker terminal while a step is running, restart the
worker. The workflow resumes at the step it was on; finished steps are not recomputed.

Temporal Cloud: set TEMPORAL_ADDRESS=<ns>.<acct>.tmprl.cloud:7233, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY.
"""
from __future__ import annotations
import asyncio, sys, time, uuid
from datetime import timedelta
from . import config, pipeline
from .tracing import Trace

try:
    from temporalio import activity, workflow
    from temporalio.client import Client, WorkflowExecutionStatus
    from temporalio.common import RetryPolicy
    from temporalio.worker import Worker, UnsandboxedWorkflowRunner
    AVAILABLE = True
except Exception:  # pragma: no cover
    activity = workflow = Client = RetryPolicy = Worker = WorkflowExecutionStatus = UnsandboxedWorkflowRunner = None
    AVAILABLE = False

STEP_FNS = dict(pipeline.STEPS)
STEP_NAMES = [n for n, _ in pipeline.STEPS]


def execute_step(name: str, state: dict, attempt: int = 1) -> dict:
    """The body of one Activity, kept free of Temporal imports so it can be unit-tested."""
    state["_attempt"] = attempt
    t0 = time.perf_counter()
    data = STEP_FNS[name](state, Trace(name))
    return {"state": state, "data": data, "ms": round((time.perf_counter() - t0) * 1000, 1), "attempts": attempt}


if AVAILABLE:
    @activity.defn(name="run_step")
    async def run_step(name: str, state: dict) -> dict:
        # The step runs in a thread (a model call can take minutes). Meanwhile we heartbeat every 5 s, so if the
        # worker process dies Temporal notices within HEARTBEAT_S and reschedules the step on a live worker.
        task = asyncio.create_task(asyncio.to_thread(execute_step, name, state, activity.info().attempt))
        while not task.done():
            activity.heartbeat(name)
            await asyncio.wait({task}, timeout=5)
        return task.result()

    @workflow.defn(name="AppealWorkflow")
    class AppealWorkflow:
        def __init__(self) -> None:
            self.progress: list[dict] = []

        @workflow.query(name="progress")
        def get_progress(self) -> list[dict]:
            return self.progress

        @workflow.run
        async def run(self, state: dict) -> dict:
            for name in STEP_NAMES:
                r = await workflow.execute_activity(
                    "run_step", args=[name, state], start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(seconds=20),
                    retry_policy=RetryPolicy(maximum_attempts=4, initial_interval=timedelta(seconds=1),
                                             backoff_coefficient=2.0, non_retryable_error_types=["ValueError"]))
                state = r["state"]
                self.progress.append({"step": name, "data": r["data"], "ms": r["ms"], "attempts": r["attempts"]})
            return {"state": state, "progress": self.progress}


async def connect():
    kw = {"namespace": config.TEMPORAL_NAMESPACE}
    if config.TEMPORAL_API_KEY:
        kw["api_key"] = config.TEMPORAL_API_KEY
    if config.TEMPORAL_TLS:
        kw["tls"] = True
    return await Client.connect(config.TEMPORAL_ADDRESS or "localhost:7233", **kw)


def _final_from(out: dict) -> dict:
    tr, cursor = Trace("appeal"), 0.0
    for p in out["progress"]:
        tr.spans.append({"id": uuid.uuid4().hex[:8], "name": p["step"], "parent": None, "start_ms": cursor,
                         "dur_ms": p["ms"], "attrs": {"attempt": p["attempts"], "runner": "temporal"}, "status": "ok"})
        cursor += p["ms"]
    res = pipeline.build_result(out["state"], tr)
    res["trace"]["total_ms"] = cursor
    return res


async def stream(text: str, inputs: dict, chaos: str = "off"):
    """Async generator of the same events the local runner emits."""
    if not AVAILABLE:
        yield {"type": "error", "step": "temporal", "message": "temporalio is not installed (pip install temporalio)."}
        return
    try:
        client = await connect()
    except Exception as e:
        yield {"type": "error", "step": "temporal", "message": f"Cannot reach Temporal at {config.TEMPORAL_ADDRESS}: {e}"}
        return
    wid = f"appeal-{uuid.uuid4().hex[:8]}"
    handle = await client.start_workflow(
        "AppealWorkflow", {"text": text, "inputs": inputs, "chaos": "flaky" if chaos == "flaky" else "off"},
        id=wid, task_queue=config.TEMPORAL_TASK_QUEUE)
    yield {"type": "start", "run_id": wid, "resumed": False, "runner": "temporal",
           "ui_url": f"{config.TEMPORAL_UI}/namespaces/{config.TEMPORAL_NAMESPACE}/workflows/{wid}",
           "steps": [{"id": n, "label": pipeline.STEP_LABELS[n]} for n in STEP_NAMES]}
    if chaos == "crash":
        yield {"type": "note", "message": "Temporal mode: press Ctrl+C in the worker terminal while a step is running, then start the worker again. The workflow resumes where it stopped."}
    seen, started, last = 0, time.monotonic(), time.monotonic()
    while seen < len(STEP_NAMES):
        try:
            prog = await handle.query("progress")
        except Exception:
            prog = []
        for p in prog[seen:]:
            if p["attempts"] > 1:
                yield {"type": "retry", "step": p["step"], "attempt": p["attempts"] - 1, "wait_s": 0,
                       "error": f"activity needed {p['attempts']} attempts (Temporal retried it automatically)"}
            yield {"type": "step_done", "step": p["step"], "ms": p["ms"], "data": p["data"], "restored": False}
            last = time.monotonic()
        seen = max(seen, len(prog))
        if seen >= len(STEP_NAMES):
            break
        if seen == 0 and time.monotonic() - started > 25:
            yield {"type": "error", "step": "temporal",
                   "message": f"No worker picked up the workflow within 25s. Start one: python -m overturn.temporal_workflow worker (task queue '{config.TEMPORAL_TASK_QUEUE}')."}
            return
        desc = await handle.describe()
        if desc.status != WorkflowExecutionStatus.RUNNING:
            break
        await asyncio.sleep(0.4)
    try:
        out = await handle.result()
    except Exception as e:
        yield {"type": "error", "step": "temporal", "message": f"Workflow failed: {e}"}
        return
    yield {"type": "final", "result": _final_from(out)}


async def worker():
    """Run the worker. By default the workflow runs WITHOUT Temporal's import sandbox: the sandbox re-imports the
    workflow module in an isolated interpreter state, which fails when that module pulls in numpy/scikit-learn
    (via `pipeline`). The workflow body is only a loop over activities, so it is deterministic by construction.
    Set TEMPORAL_SANDBOX=1 to turn the sandbox back on."""
    import os
    client = await connect()
    kw = {} if os.environ.get("TEMPORAL_SANDBOX") == "1" else {"workflow_runner": UnsandboxedWorkflowRunner()}
    try:
        w = Worker(client, task_queue=config.TEMPORAL_TASK_QUEUE, workflows=[AppealWorkflow], activities=[run_step], **kw)
    except Exception as e:
        cause, chain = e, []
        while cause is not None:
            chain.append(f"{type(cause).__name__}: {cause}")
            cause = cause.__cause__ or cause.__context__
        print("[overturn] worker failed to start. Cause chain:\n  " + "\n  ".join(chain), flush=True)
        raise
    async with w:
        print(f"[overturn] Temporal worker running on task queue '{config.TEMPORAL_TASK_QUEUE}' "
              f"({config.TEMPORAL_ADDRESS or 'localhost:7233'}), sandbox "
              f"{'on' if os.environ.get('TEMPORAL_SANDBOX') == '1' else 'off'}. Ctrl+C to stop.", flush=True)
        await asyncio.Event().wait()


if __name__ == "__main__" and len(sys.argv) > 1 and sys.argv[1] == "worker":
    asyncio.run(worker())
