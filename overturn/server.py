"""Overturn web server (Starlette). `uvicorn overturn.server:app`"""
from __future__ import annotations
import asyncio, json, uuid, pathlib
from starlette.applications import Starlette
from starlette.responses import JSONResponse, StreamingResponse, FileResponse, Response
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from . import arena, backboard, config, durable, extract, kb, llm, pdfgen, pipeline, predict, synth, tracing, vectorstore

STATIC = config.ROOT / "static"
QUEUE_SIZE = 14


def _queue_rows():
    return [synth.make_open_denial(i) for i in range(QUEUE_SIZE)]


async def health(request):
    st = llm.status(refresh=True)
    p = predict.get()
    return JSONResponse({
        "llm": st, "predictor": {"provider": p.provider, "auc": p.auc, "rows": len(p.df), "note": p.note},
        "store": vectorstore.get().kind, "store_detail": vectorstore.get().detail, "store_docs": vectorstore.get().count(), "sentry": tracing.sentry_active(),
        "backboard": backboard.configured(), "tinker": bool(config.TINKER_API_KEY), "tinker_tuned": bool(config.TINKER_MODEL_PATH), "runner": "temporal" if config.TEMPORAL_ADDRESS else "durable-local",
        "egress": llm.egress_summary(), "today": config.today().isoformat(),
        "assumptions": {"cost_appeal": config.COST_APPEAL, "cost_recode": config.COST_RECODE}})


async def queue(request):
    p = predict.get()
    out = []
    for r in _queue_rows():
        days_since = (config.today() - synth.date.fromisoformat(r["denial_date"])).days
        f = {**r, "days_since_denial": days_since}
        prob = p.quick(f)
        e = kb.get(r["carc"])
        left = kb.window_days(r["payer"]) - days_since
        opts, bestopt, _ = pipeline.compute_options({**r}, e, prob, left)
        best, ev = bestopt["id"], bestopt["ev"]
        out.append({"id": r["id"], "payer": r["payer"], "carc": r["carc"], "title": e["title"], "cpt": r["cpt"],
                    "denied": r["denied"], "days_left": left, "p": round(prob, 3), "best": best, "ev": round(max(ev, 0), 2)})
    out.sort(key=lambda x: -x["ev"])
    return JSONResponse({"items": out, "total_ev": round(sum(o["ev"] for o in out), 2),
                         "total_denied": round(sum(o["denied"] for o in out), 2)})


async def sample(request):
    cid = request.path_params["cid"]
    for i, r in enumerate(_queue_rows()):
        if r["id"] == cid:
            return JSONResponse({"text": synth.render_denial_letter(r),
                                 "inputs": {"has_clinical_notes": r["has_clinical_notes"], "prior_auth_on_file": r["prior_auth_on_file"], "in_network": r["in_network"]}})
    return JSONResponse({"error": "not found"}, 404)


async def upload(request):
    form = await request.form()
    f = form["file"]
    data = await f.read()
    try:
        return JSONResponse({"text": extract.read_bytes(f.filename, data)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, 400)


def _events(body: dict):
    text, inputs = body["text"], body.get("inputs", {})
    chaos, run_id = body.get("chaos", "off"), body.get("run_id") or uuid.uuid4().hex[:10]
    crash_after = 4 if chaos == "crash" else None
    if config.TEMPORAL_ADDRESS:
        from . import temporal_workflow as tw
        loop = asyncio.new_event_loop()
        agen = tw.stream(text, inputs, chaos)
        try:
            while True:
                try:
                    yield loop.run_until_complete(agen.__anext__())
                except StopAsyncIteration:
                    break
        finally:
            loop.close()
        return
    yield from durable.run(text, inputs, run_id=run_id, chaos="flaky" if chaos == "flaky" else "off", crash_after=crash_after)


async def analyze(request):
    body = await request.json()
    if not body.get("text", "").strip():
        return JSONResponse({"error": "empty notice"}, 400)

    def gen():
        for ev in _events(body):
            yield json.dumps(ev, default=str) + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson", headers={"Cache-Control": "no-store"})


async def wins(request):
    b = await request.json()
    text = (b.get("text") or "").strip()
    if len(text) < 60 or not b.get("carc"):
        return JSONResponse({"error": "need an argument paragraph and a denial code"}, 400)
    st = vectorstore.get()
    new_id = st.add(str(b["carc"]), b.get("payer") or "", text[:1200])
    return JSONResponse({"id": new_id, "store": st.kind, "docs": st.count()})


async def search_demo(request):
    q = request.query_params
    st = vectorstore.get()
    res = {m: st.search(q.get("q", "modifier 59 separate site"), q.get("carc", "97"), q.get("payer", ""), k=3, mode=m)
           for m in ("keyword", "vector", "hybrid")}
    return JSONResponse({"store": st.kind, "detail": st.detail, "results": res})


async def bb_models(request):
    if not backboard.configured():
        return JSONResponse({"configured": False, "models": [], "error": "Set BACKBOARD_API_KEY and restart to enable the arena."})
    try:
        return JSONResponse({"configured": True, "models": await asyncio.to_thread(backboard.open_models)})
    except Exception as e:
        return JSONResponse({"configured": True, "models": [], "error": str(e)}, 502)


async def arena_run(request):
    b = await request.json()
    models = [m for m in b.get("models", []) if m.get("name")][:6]
    if not models and not llm.available() and not config.TINKER_API_KEY:
        return JSONResponse({"error": "pick at least one model"}, 400)
    if models and not backboard.configured():
        return JSONResponse({"error": "BACKBOARD_API_KEY is not set"}, 400)

    def gen():
        try:
            for ev in arena.stream(models, b.get("cases", 6), bool(b.get("local", True))):
                yield json.dumps(ev, default=str) + "\n"
        except Exception as e:
            yield json.dumps({"type": "error", "error": str(e)}) + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson", headers={"Cache-Control": "no-store"})


async def pdf(request):
    body = await request.json()
    return Response(pdfgen.letter_pdf(body["text"]), media_type="application/pdf",
                    headers={"Content-Disposition": 'attachment; filename="appeal-letter.pdf"'})


async def index(request):
    return FileResponse(STATIC / "index.html")


import contextlib


@contextlib.asynccontextmanager
async def lifespan(app):
    tracing.init_sentry()
    await asyncio.to_thread(predict.get)
    await asyncio.to_thread(vectorstore.get)
    await asyncio.to_thread(llm.status, True)
    yield

app = Starlette(
    routes=[Route("/", index), Route("/api/health", health), Route("/api/queue", queue),
            Route("/api/sample/{cid}", sample), Route("/api/upload", upload, methods=["POST"]),
            Route("/api/analyze", analyze, methods=["POST"]), Route("/api/pdf", pdf, methods=["POST"]), Route("/api/wins", wins, methods=["POST"]), Route("/api/search", search_demo), Route("/api/backboard/models", bb_models), Route("/api/arena", arena_run, methods=["POST"]),
            Mount("/static", StaticFiles(directory=STATIC), name="static")],
    lifespan=lifespan)
