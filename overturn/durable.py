"""A tiny durable-execution runner: per-step checkpoints, retries with backoff, crash/resume.

Same guarantees you'd want from Temporal for this workflow, in ~60 lines and zero infrastructure.
(When TEMPORAL_ADDRESS is set the server uses temporal_workflow.py instead.)
"""
from __future__ import annotations
import json, time, pathlib, shutil
from . import config, pipeline
from .tracing import Trace

MAX_ATTEMPTS = 3


def _ckpt_dir(run_id):
    d = config.CKPT / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def run(text: str, inputs: dict, *, run_id: str, chaos: str = "off", crash_after: int | None = None):
    """Generator of events. Yields dicts; the final one has type 'final' (or 'crash')."""
    d = _ckpt_dir(run_id)
    state_path = d / "state.json"
    done = set(json.loads((d / "done.json").read_text())) if (d / "done.json").exists() else set()
    state = json.loads(state_path.read_text()) if state_path.exists() and done else {"text": text, "inputs": inputs, "chaos": chaos}
    if state_path.exists() and done:
        state["chaos"] = chaos
    trace = Trace("appeal")
    yield {"type": "start", "run_id": run_id, "resumed": bool(done), "steps": [{"id": n, "label": pipeline.STEP_LABELS[n]} for n, _ in pipeline.STEPS]}
    with trace.span("appeal_workflow", run_id=run_id):
        for i, (name, fn) in enumerate(pipeline.STEPS):
            if name in done:
                with trace.span(name, restored=True):
                    pass
                yield {"type": "step_done", "step": name, "restored": True, "ms": 0, "data": state.get("_data", {}).get(name, {})}
                continue
            for attempt in range(1, MAX_ATTEMPTS + 1):
                yield {"type": "step_start", "step": name, "attempt": attempt}
                state["_attempt"] = attempt
                t0 = time.perf_counter()
                try:
                    with trace.span(name, attempt=attempt):
                        data = fn(state, trace)
                    break
                except Exception as e:
                    if attempt == MAX_ATTEMPTS or isinstance(e, ValueError):
                        yield {"type": "error", "step": name, "message": str(e)}
                        return
                    wait = 0.5 * 2 ** (attempt - 1)
                    yield {"type": "retry", "step": name, "attempt": attempt, "error": f"{type(e).__name__}: {e}", "wait_s": wait}
                    time.sleep(wait)
            ms = round((time.perf_counter() - t0) * 1000, 1)
            done.add(name)
            state.setdefault("_data", {})[name] = data
            state_path.write_text(json.dumps(state, default=str))
            (d / "done.json").write_text(json.dumps(sorted(done)))
            yield {"type": "step_done", "step": name, "ms": ms, "data": data, "restored": False}
            if crash_after is not None and i + 1 == crash_after:
                yield {"type": "crash", "after": name, "run_id": run_id}
                return
    yield {"type": "final", "result": pipeline.build_result(state, trace)}
    shutil.rmtree(d, ignore_errors=True)
