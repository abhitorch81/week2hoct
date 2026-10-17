"""LoRA supervised fine-tuning on Tinker: teach a small open model to write the argument section.

    pip install tinker                       # plus: set TINKER_API_KEY (Tinker console -> API Keys)
    python -m tinker_ft.build_dataset 300
    python -m tinker_ft.train                # ~3 epochs; prints the tinker:// path of the sampler weights
    set TINKER_MODEL_PATH=tinker://...       # then restart Overturn and run the Model arena

Written from the Tinker SDK cheatsheet (create_lora_training_client, forward_backward, optim_step,
save_weights_for_sampler, checkpoint download). NOT run in the build sandbox (SDK not installable there): if a
call signature differs in your SDK version, the traceback points at the exact line.
"""
from __future__ import annotations
import json, os, random, sys, tarfile, urllib.request, pathlib
from overturn import config
from overturn.tinker_model import prompt_text

BASE = os.environ.get("TINKER_BASE_MODEL", "Qwen/Qwen3.5-4B")
EPOCHS = int(os.environ.get("TUNE_EPOCHS", "3"))
BATCH = int(os.environ.get("TUNE_BATCH", "8"))
LR = float(os.environ.get("TUNE_LR", "1e-4"))
RANK = int(os.environ.get("TUNE_RANK", "16"))


def to_datum(tok, ex, types):
    prompt = tok.encode(prompt_text(tok, ex["system"], ex["prompt"]), add_special_tokens=False)
    comp = tok.encode(ex["completion"] + "<|im_end|>", add_special_tokens=False)
    full = prompt + comp
    return types.Datum(
        model_input=types.ModelInput.from_ints(tokens=full[:-1]),
        loss_fn_inputs=dict(weights=[0.0] * (len(prompt) - 1) + [1.0] * len(comp), target_tokens=full[1:]))


def main():
    import tinker
    from tinker import types
    data = [json.loads(l) for l in (config.DATA / "tinker_sft.jsonl").read_text().splitlines() if l.strip()]
    random.Random(0).shuffle(data)
    svc = tinker.ServiceClient()                       # reads TINKER_API_KEY
    tc = svc.create_lora_training_client(base_model=BASE, rank=RANK)
    tok = tc.get_tokenizer()
    datums = [to_datum(tok, ex, types) for ex in data]
    steps = EPOCHS * ((len(datums) + BATCH - 1) // BATCH)
    print(f"{len(datums)} examples, {steps} steps, base={BASE}, rank={RANK}, lr={LR}")
    step = 0
    for ep in range(EPOCHS):
        random.Random(ep).shuffle(datums)
        for i in range(0, len(datums), BATCH):
            fb = tc.forward_backward(datums[i:i + BATCH], loss_fn="cross_entropy")
            op = tc.optim_step(types.AdamParams(learning_rate=LR))     # queued right behind fwd/bwd
            res = fb.result(); op.result()
            step += 1
            if step % 5 == 0 or step == steps:
                loss = getattr(res, "loss", None)
                if loss is None:
                    out = getattr(res, "metrics", {}) or {}
                    loss = out.get("loss:sum") or out.get("loss")
                print(f"step {step}/{steps} epoch {ep + 1} loss {loss}")
    ck = tc.save_weights_for_sampler("overturn-appeal-writer").result()
    print("\nSAMPLER WEIGHTS:", ck.path)
    print("Next: set TINKER_MODEL_PATH=" + ck.path + " and run the Model arena.")
    try:
        rest = svc.create_rest_client()
        url = rest.get_checkpoint_archive_url_from_tinker_path(ck.path).result().url
        out = pathlib.Path("adapter"); out.mkdir(exist_ok=True)
        urllib.request.urlretrieve(url, out / "checkpoint.tar")
        with tarfile.open(out / "checkpoint.tar") as t:
            t.extractall(out, filter="data")
        print("LoRA adapter downloaded to ./adapter (adapter_model.safetensors + adapter_config.json)")
    except Exception as e:
        print("adapter download skipped:", e)


if __name__ == "__main__":
    sys.exit(main())
