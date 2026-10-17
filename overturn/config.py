import os, pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
CKPT = pathlib.Path(os.environ.get("OVERTURN_CKPT", ROOT / ".checkpoints"))

# --- open-weight model serving (any of these works; all are local) -----------
OLLAMA_URL = os.environ.get("OLLAMA_URL") or (f"http://{os.environ['OLLAMA_HOSTPORT']}" if os.environ.get("OLLAMA_HOSTPORT") else "http://127.0.0.1:11434")   # OLLAMA_HOSTPORT is set by Render private services
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "")   # llama.cpp server / vLLM / LM Studio, e.g. http://127.0.0.1:8080/v1
MODEL = os.environ.get("OVERTURN_MODEL", "qwen2.5:7b-instruct")
LLM_TIMEOUT = float(os.environ.get("OVERTURN_LLM_TIMEOUT", "180"))
LLM_MODE = os.environ.get("OVERTURN_LLM", "auto")          # auto | ollama | openai | offline

# --- predictor ----------------------------------------------------------------
PREDICTOR = os.environ.get("OVERTURN_PREDICTOR", "auto")   # auto | tabpfn-client | tabpfn-local | sklearn
PRIORLABS_API_KEY = os.environ.get("PRIORLABS_API_KEY", "")

# --- vector store --------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")          # Postgres + pgvector (Tiger Data / Timescale)
EMBED_DIM = 512

# --- observability / durability -----------------------------------------------
SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "")
TEMPORAL_TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "overturn")

# --- economics (assumptions, shown in the UI; edit for your practice) ----------
COST_APPEAL = float(os.environ.get("OVERTURN_COST_APPEAL", "85"))    # staff time to prepare + track an appeal
COST_RECODE = float(os.environ.get("OVERTURN_COST_RECODE", "22"))    # staff time for a corrected claim
API_REF_PRICE_PER_MTOK = float(os.environ.get("OVERTURN_REF_PRICE", "5.0"))  # illustrative closed-API $/1M tokens


def today():
    import datetime
    v = os.environ.get("OVERTURN_TODAY")
    return datetime.date.fromisoformat(v) if v else datetime.date.today()

# --- Temporal (Cloud or self-hosted) ---------------------------------------------
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
TEMPORAL_API_KEY = os.environ.get("TEMPORAL_API_KEY", "")          # Temporal Cloud API key (optional)
TEMPORAL_TLS = os.environ.get("TEMPORAL_TLS", "1" if TEMPORAL_API_KEY else "0") == "1"
TEMPORAL_UI = os.environ.get("TEMPORAL_UI", "http://localhost:8233")

# --- Backboard (optional model arena) ---------------------------------------------
BACKBOARD_API_KEY = os.environ.get("BACKBOARD_API_KEY", "")
BACKBOARD_BASE_URL = os.environ.get("BACKBOARD_BASE_URL", "https://app.backboard.io/api")

# --- Tinker (optional: fine-tune a small open model on winning appeals; scored in the arena) ---
TINKER_API_KEY = os.environ.get("TINKER_API_KEY", "")
TINKER_BASE_MODEL = os.environ.get("TINKER_BASE_MODEL", "Qwen/Qwen3.5-4B")
TINKER_MODEL_PATH = os.environ.get("TINKER_MODEL_PATH", "")     # tinker://<run>/sampler_weights/<name> printed by train.py
