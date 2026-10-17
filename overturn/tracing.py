"""Tracing: a local span recorder that also forwards to Sentry when configured.

The local recorder is what the UI's waterfall renders. If SENTRY_DSN is set and
sentry-sdk is installed, every span is mirrored to Sentry performance tracing.
Only metadata (step names, durations, token counts, model name) is attached;
never denial-letter text or patient data (send_default_pii=False).
"""
from __future__ import annotations
import contextlib, contextvars, time, uuid
from . import config

try:  # optional
    import sentry_sdk
except Exception:  # pragma: no cover
    sentry_sdk = None

_enabled = False


def init_sentry() -> bool:
    global _enabled
    if sentry_sdk and config.SENTRY_DSN and not _enabled:
        sentry_sdk.init(dsn=config.SENTRY_DSN, traces_sample_rate=1.0, send_default_pii=False,
                        release="overturn@0.1.0", environment="demo")
        _enabled = True
    return _enabled


def sentry_active() -> bool:
    return _enabled


class Trace:
    def __init__(self, name: str):
        self.id = uuid.uuid4().hex[:16]
        self.name = name
        self.t0 = time.perf_counter()
        self.spans: list[dict] = []
        self._stack: list[dict] = []

    @contextlib.contextmanager
    def span(self, name: str, **attrs):
        sp = {"id": uuid.uuid4().hex[:8], "name": name, "parent": self._stack[-1]["id"] if self._stack else None,
              "start_ms": (time.perf_counter() - self.t0) * 1000, "attrs": dict(attrs), "status": "ok"}
        self.spans.append(sp)
        self._stack.append(sp)
        cm = sentry_sdk.start_span(op="overturn.step", description=name) if _enabled else contextlib.nullcontext()
        try:
            with cm as s:
                if s is not None:
                    for k, v in attrs.items():
                        s.set_data(k, v)
                yield sp
        except Exception as e:
            sp["status"] = "error"
            sp["attrs"]["error"] = f"{type(e).__name__}: {e}"[:200]
            if _enabled:
                sentry_sdk.capture_exception(e)
            raise
        finally:
            sp["dur_ms"] = (time.perf_counter() - self.t0) * 1000 - sp["start_ms"]
            self._stack.pop()

    def to_dict(self):
        return {"id": self.id, "name": self.name, "total_ms": (time.perf_counter() - self.t0) * 1000,
                "spans": self.spans, "sentry": _enabled}
