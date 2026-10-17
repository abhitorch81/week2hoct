"""Tinker (Thinking Machines) sampling client: lets the arena score a base model and the model you fine-tuned.

Tinker is a hosted training/sampling API, so these calls are EXTERNAL and counted as such. The fine-tuned
LoRA can be downloaded (tinker_ft/train.py does) for use outside Tinker.

Written from the Tinker SDK docs (ServiceClient, create_sampling_client, SamplingParams); the build sandbox could not
install the SDK, so this file has NOT been run. Env: TINKER_API_KEY, TINKER_BASE_MODEL, TINKER_MODEL_PATH.
"""
from __future__ import annotations
import re
from . import config, llm


def configured() -> bool:
    return bool(config.TINKER_API_KEY)


def prompt_text(tokenizer, system: str, user: str) -> str:
    """Chat-formatted prompt ending at the assistant turn (thinking disabled when the template supports it)."""
    msgs = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    for kw in ({"enable_thinking": False}, {}):
        try:
            return tokenizer.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True, **kw)
        except Exception:
            continue
    return f"System: {system}\n\nUser: {user}\n\nAssistant:"


_clients: dict = {}


def _client(model_path: str | None):
    import tinker
    key = model_path or config.TINKER_BASE_MODEL
    if key not in _clients:
        svc = tinker.ServiceClient()
        sc = (svc.create_sampling_client(model_path=model_path) if model_path
              else svc.create_sampling_client(base_model=config.TINKER_BASE_MODEL))
        tok = sc.get_tokenizer() if hasattr(sc, "get_tokenizer") else svc.create_lora_training_client(
            base_model=config.TINKER_BASE_MODEL, rank=8).get_tokenizer()
        _clients[key] = (sc, tok)
    return _clients[key]


def chat(system: str, user: str, model_path: str | None = None, max_tokens: int = 500) -> dict:
    from tinker import types
    sc, tok = _client(model_path)
    ids = tok.encode(prompt_text(tok, system, user), add_special_tokens=False)
    res = sc.sample(prompt=types.ModelInput.from_ints(ids), num_samples=1,
                    sampling_params=types.SamplingParams(max_tokens=max_tokens, temperature=0.25)).result()
    text = tok.decode(res.sequences[0].tokens)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S)
    text = re.sub(r"<\|im_end\|>.*", "", text, flags=re.S).strip()
    llm.CALLS.append({"host": "tinker.thinkingmachines.ai", "local": False, "tokens_in": len(ids), "tokens_out": len(res.sequences[0].tokens)})
    return {"text": text, "tokens_in": len(ids), "tokens_out": len(res.sequences[0].tokens), "model": model_path or config.TINKER_BASE_MODEL}
