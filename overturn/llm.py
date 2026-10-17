"""Open-weight LLM client. Talks to a *local* server: Ollama, or any OpenAI-compatible
endpoint (llama.cpp `llama-server`, vLLM, LM Studio). No closed API is ever called.

Every call records the host it contacted so the UI can truthfully show whether any
inference left the machine.
"""
from __future__ import annotations
import json, re, urllib.request, urllib.error, ipaddress
from urllib.parse import urlparse
from . import config

CALLS: list[dict] = []   # {"host", "local", "tokens_in", "tokens_out"}


def _is_local(url: str) -> bool:
    host = urlparse(url).hostname or ""
    if host in ("localhost", "ollama", "host.docker.internal"):
        return True
    try:
        ip = ipaddress.ip_address(host)
        return ip.is_loopback or ip.is_private
    except ValueError:
        return False


def _post(url: str, payload: dict, timeout: float) -> dict:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _get(url: str, timeout: float = 1.5) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


_status_cache: dict | None = None


def status(refresh: bool = False) -> dict:
    """Which open model is reachable right now?"""
    global _status_cache
    if _status_cache and not refresh:
        return _status_cache
    st = {"mode": "offline", "model": None, "endpoint": None, "local": True}
    if config.LLM_MODE in ("auto", "ollama"):
        try:
            tags = _get(config.OLLAMA_URL + "/api/tags")
            names = [m["name"] for m in tags.get("models", [])]
            if names:
                model = config.MODEL if config.MODEL in names else names[0]
                st = {"mode": "ollama", "model": model, "endpoint": config.OLLAMA_URL, "local": _is_local(config.OLLAMA_URL)}
        except Exception:
            pass
    if st["mode"] == "offline" and config.OPENAI_BASE_URL and config.LLM_MODE in ("auto", "openai"):
        try:
            _get(config.OPENAI_BASE_URL.rstrip("/") + "/models")
            st = {"mode": "openai", "model": config.MODEL, "endpoint": config.OPENAI_BASE_URL, "local": _is_local(config.OPENAI_BASE_URL)}
        except Exception:
            pass
    _status_cache = st
    return st


def available() -> bool:
    return status()["mode"] != "offline"


def chat(system: str, user: str, *, json_mode: bool = False, temperature: float = 0.2, max_tokens: int = 1400) -> dict:
    """Returns {"text", "tokens_in", "tokens_out", "model"}. Raises if no model is reachable."""
    st = status()
    if st["mode"] == "offline":
        raise RuntimeError("no open-weight model reachable")
    msgs = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    if st["mode"] == "ollama":
        payload = {"model": st["model"], "messages": msgs, "stream": False,
                   "options": {"temperature": temperature, "num_predict": max_tokens}}
        if json_mode:
            payload["format"] = "json"
        out = _post(config.OLLAMA_URL + "/api/chat", payload, config.LLM_TIMEOUT)
        text = out["message"]["content"]
        tin, tout = out.get("prompt_eval_count", 0), out.get("eval_count", 0)
    else:
        payload = {"model": st["model"], "messages": msgs, "temperature": temperature, "max_tokens": max_tokens}
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        out = _post(config.OPENAI_BASE_URL.rstrip("/") + "/chat/completions", payload, config.LLM_TIMEOUT)
        text = out["choices"][0]["message"]["content"]
        u = out.get("usage", {})
        tin, tout = u.get("prompt_tokens", 0), u.get("completion_tokens", 0)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    CALLS.append({"host": urlparse(st["endpoint"]).netloc, "local": st["local"], "tokens_in": tin, "tokens_out": tout})
    return {"text": text, "tokens_in": tin, "tokens_out": tout, "model": st["model"]}


def egress_summary() -> dict:
    ext = [c for c in CALLS if not c["local"]]
    return {"calls": len(CALLS), "external_calls": len(ext),
            "tokens": sum(c["tokens_in"] + c["tokens_out"] for c in CALLS)}
