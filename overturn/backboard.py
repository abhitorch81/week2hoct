"""Backboard client (https://backboard.io): one API in front of many hosted models.

Used ONLY by the optional "Model arena", to compare open-weight models on the same synthetic
denial notices. Never used for real PHI: every call is recorded as an EXTERNAL call in the egress
counter, and the UI warns before sending anything.

The API key is read from the BACKBOARD_API_KEY environment variable and is never logged or returned.

NOTE: written from Backboard's public docs, which are not fully consistent about endpoint shapes, and not
verified against the live service from the build sandbox. Raw HTTP errors are surfaced as-is so a
mismatch is easy to spot and fix in this one file.
"""
from __future__ import annotations
import json, re, urllib.request, urllib.error, uuid
from urllib.parse import urlparse
from . import config, llm

# substrings that identify open-weight model families in Backboard's catalogue
OPEN_HINTS = ("llama", "qwen", "deepseek", "mistral", "mixtral", "gemma", "gpt-oss", "phi", "glm", "kimi", "olmo", "nemotron")


class BackboardError(RuntimeError):
    pass


def configured() -> bool:
    return bool(config.BACKBOARD_API_KEY)


def _req(method: str, path: str, *, form: dict | None = None, js: dict | None = None, timeout: float = 120) -> dict | list:
    if not configured():
        raise BackboardError("BACKBOARD_API_KEY is not set")
    url = config.BACKBOARD_BASE_URL.rstrip("/") + path
    headers = {"X-API-Key": config.BACKBOARD_API_KEY, "Accept": "application/json"}
    data = None
    if js is not None:
        data, headers["Content-Type"] = json.dumps(js).encode(), "application/json"
    elif form is not None:
        boundary = uuid.uuid4().hex
        parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n' for k, v in form.items()]
        data = ("".join(parts) + f"--{boundary}--\r\n").encode()
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:300]
        raise BackboardError(f"HTTP {e.code} from {method} {path}: {body}") from None
    except urllib.error.URLError as e:
        raise BackboardError(f"cannot reach Backboard: {e.reason}") from None


def is_open_weight(name: str) -> bool:
    n = name.lower()
    return any(h in n for h in OPEN_HINTS)


def list_models() -> list[dict]:
    """[{provider, name}] for every model Backboard reports (best-effort parse of several shapes)."""
    raw = _req("GET", "/models")
    items = raw.get("models") or raw.get("data") or raw.get("items") or [] if isinstance(raw, dict) else raw
    out = []
    for m in items:
        if isinstance(m, str):
            out.append({"provider": "", "name": m})
        elif isinstance(m, dict):
            name = m.get("name") or m.get("model_name") or m.get("id") or ""
            if name:
                out.append({"provider": m.get("provider") or m.get("llm_provider") or "", "name": name,
                            "context": m.get("context_limit") or m.get("context_length")})
    return out


def open_models() -> list[dict]:
    """Open-weight models worth comparing. Backboard also lists thousands of community fine-tunes served by
    'featherless'; those are hidden unless BACKBOARD_ALL_MODELS=1. Curated providers come first."""
    import os
    allm = os.environ.get("BACKBOARD_ALL_MODELS") == "1"
    ms = [m for m in list_models() if is_open_weight(m["name"]) or is_open_weight(m["provider"])]
    if not allm:
        ms = [m for m in ms if str(m.get("provider", "")).lower() != "featherless"]
    ms.sort(key=lambda m: (str(m.get("provider", "")).lower() == "featherless", m["provider"], m["name"]))
    return ms[:200]


_assistant: str | None = None


def _assistant_id() -> str:
    global _assistant
    if not _assistant:
        a = _req("POST", "/assistants", js={"name": "Overturn arena", "system_prompt": "You follow instructions exactly."})
        _assistant = a.get("assistant_id") or a.get("id")
        if not _assistant:
            raise BackboardError(f"unexpected assistant response: {str(a)[:200]}")
    return _assistant


def chat(system: str, user: str, provider: str, model: str) -> dict:
    """One stateless completion through Backboard. Returns {text, tokens_in, tokens_out, model}."""
    t = _req("POST", f"/assistants/{_assistant_id()}/threads", js={})
    tid = t.get("thread_id") or t.get("id")
    if not tid:
        raise BackboardError(f"unexpected thread response: {str(t)[:200]}")
    r = _req("POST", f"/threads/{tid}/messages", timeout=config.LLM_TIMEOUT,
             form={"content": f"{system}\n\n{user}", "llm_provider": provider, "model_name": model,
                   "stream": "false", "memory": "off"})
    text = r.get("content") or r.get("text") or ""
    if not text:
        raise BackboardError(f"empty reply: {str(r)[:200]}")
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    tin, tout = int(r.get("input_tokens") or 0), int(r.get("output_tokens") or 0)
    llm.CALLS.append({"host": urlparse(config.BACKBOARD_BASE_URL).netloc, "local": False, "tokens_in": tin, "tokens_out": tout})
    return {"text": text, "tokens_in": tin, "tokens_out": tout, "model": model}
