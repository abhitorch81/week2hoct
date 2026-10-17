# Tiger Data: hybrid search for the appeal agent

Overturn retrieves past winning appeals with **hybrid search**: a keyword side and a vector side, fused with
Reciprocal Rank Fusion (RRF), plus a small boost when the denial code and payer match. The same fusion runs in
memory (no setup) or inside Postgres on Tiger Cloud.

| Side | In memory | On Tiger Cloud |
|---|---|---|
| keyword | TF-IDF cosine | **BM25** via `pg_textsearch` (falls back to Postgres full-text if the extension is missing) |
| vector | 512-d hashed n-gram vectors | **pgvector** cosine, **StreamingDiskANN** index from `vectorscale` (falls back to HNSW) |
| fusion | RRF (k = 60) | RRF in one SQL statement |

## 1. Create a free Tiger Cloud service

1. Sign in at tigerdata.com and create a **Free** service (Postgres 17 or newer, which includes `pg_textsearch` and `pgvectorscale`).
2. Copy the connection string from the service page.

## 2. Point Overturn at it (Windows, Anaconda Prompt)

```
conda activate overturn
pip install "psycopg[binary]"
set DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DBNAME?sslmode=require
python run.py
```

On start-up Overturn creates the extensions (if allowed), the `winning_appeals` table and the indexes, and loads
the 18 seed appeals. The header badge changes to **Retrieval pgvector** and its tooltip names the BM25/DiskANN setup.
In the app, the **Mark as won - save argument** button writes a new row (identifiers are scrubbed first), so retrieval learns from outcomes.

## 3. Compare keyword vs vector vs hybrid (article material)

```
python scripts/hybrid_compare.py
```

Or open `http://127.0.0.1:8000/api/search?q=modifier+59+separate+site&carc=97&payer=Aetna` for the three result lists as JSON.

## 4. Let an agent manage the database with Tiger MCP (optional)

The Tiger CLI install lines in Tiger's docs cover macOS (`brew install timescale/tap/tiger`) and Linux/WSL
(`curl -sL https://assets.tigerdata.com/releases/install-tiger-cli.sh | sh`). On Windows, use WSL for this step. Then:

```
tiger auth login
tiger service list
tiger mcp install        # pick your coding agent when prompted
```

Prompts to try in that agent (screenshot the session for the write-up):

* "Using Tiger MCP, describe the `winning_appeals` table and its indexes."
* "Run EXPLAIN ANALYZE on a hybrid query against `winning_appeals` and tell me whether the DiskANN and BM25 indexes are used."
* "Add a `outcome_amount numeric` column to `winning_appeals` and show me the migration you ran."

## What was and was not tested

* The in-memory fusion logic is unit-tested (`tests/test_overturn.py`).
* The Postgres SQL path (`PgStore`) was written from Tiger's hybrid-search tutorial but **has not been run**: the build sandbox had no pgvector. Run `scripts/hybrid_compare.py` against your service first; if a statement fails, the error names the line.
