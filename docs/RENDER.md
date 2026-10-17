# Deploying Overturn on Render

Render hosts two things here: the **web service** (UI + API + the agent pipeline) and a **private service** that
runs the open-weight model with Ollama. The model service is private, so only Overturn can talk to it.

## Steps

1. Push the project folder to a GitHub repo (the repo root must contain `render.yaml`).
2. Render dashboard -> **New -> Blueprint** -> choose the repo -> **Apply**. Render builds both Dockerfiles.
3. On the first start the `ollama` service pulls `qwen2.5:7b-instruct` into its persistent disk (a few minutes). The web service reaches it through `OLLAMA_HOSTPORT`, which the blueprint fills in from the private service.
4. Open the web service URL. The header badge should read `qwen2.5:7b-instruct · ollama · local` and **External model calls 0** (Render private networking counts as local: it is a private address).
5. Optional environment variables (Dashboard -> your web service -> Environment): `DATABASE_URL` (Tiger Cloud), `SENTRY_DSN`, `PRIORLABS_API_KEY`, `BACKBOARD_API_KEY`. Each change redeploys.

## Honest limits

* **CPU inference is slow.** The 7B model on this laptop's CPU took about two and a half minutes per draft; expect similar or slower on a Render CPU plan. Render has no GPU instances in the standard plans, so for demos you can keep the model on your PC and use Render only for the front end by setting `OVERTURN_LLM=offline` (letters come from the template engine) or pointing `OLLAMA_URL` at a model server you run elsewhere.
* **Plan sizes and prices change.** The blueprint asks for a `pro` plan on the model service. The 7B model needs roughly 8 GB of RAM. Check Render's current plans before applying; edit `plan:` in `render.yaml` if you choose differently.
* **Free web services sleep** after idle time and cannot attach disks. Use `starter` or higher for the web service so the demo is awake when judges open it.
* **Not deployed or tested from the build sandbox.** The blueprint follows Render's documented syntax (`fromService`, `dockerfilePath`, `dockerContext`, `disk`, `healthCheckPath`), but run it once and read the build logs.

## What to show in the write-up

* Screenshot of the Render dashboard with both services running.
* The header badge on the deployed URL.
* The Trace tab from a run on Render (latency and token count on the deployed model).
