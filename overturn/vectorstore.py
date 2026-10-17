"""Retrieval over past winning appeals: true hybrid search (keyword + vector, fused with RRF).

* DATABASE_URL set -> Postgres on Tiger Data (Tiger Cloud) or any Postgres with pgvector.
    keyword side : BM25 via `pg_textsearch` (Tiger Cloud) -- falls back to Postgres full-text search
    vector side  : cosine distance via pgvector, StreamingDiskANN index from `vectorscale` when available
    fusion       : Reciprocal Rank Fusion, plus a small boost when denial code / payer match
* Otherwise -> in-memory store with the *same* fusion logic (TF-IDF for the keyword side), zero setup.

Embeddings are a 512-d hashed n-gram vector computed locally (no embedding API). Swap `embed()` for an
open embedding model (e.g. nomic-embed-text via Ollama) and change EMBED_DIM.

search(mode=...) accepts "hybrid" (default), "keyword" or "vector" so the three can be compared.
"""
from __future__ import annotations
import json, re, uuid
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer, TfidfVectorizer
from . import config

RRF_K = 60
CARC_BOOST, PAYER_BOOST = 0.03, 0.01      # same order of magnitude as one RRF term (1/61 ~ 0.016)

_hv = HashingVectorizer(n_features=config.EMBED_DIM, ngram_range=(1, 2), alternate_sign=False, norm="l2", stop_words="english")


def embed(texts: list[str]) -> np.ndarray:
    return _hv.transform(texts).toarray().astype("float32")


def _vec_literal(v) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v.tolist()) + "]"


def _load_corpus():
    return [json.loads(l) for l in (config.DATA / "winning_appeals.jsonl").read_text().splitlines() if l.strip()]


def _or_query(q: str, limit: int = 25) -> str:
    words = list(dict.fromkeys(w.lower() for w in re.findall(r"[A-Za-z0-9]{3,}", q)))[:limit]
    return " | ".join(words) or "appeal"


def _rank(scores: np.ndarray) -> dict[int, int]:
    order = np.argsort(-scores)
    return {int(i): r + 1 for r, i in enumerate(order) if scores[i] > 0}


class MemoryStore:
    kind = "memory"
    detail = "in-memory hybrid (TF-IDF + hashed vectors, RRF)"

    def __init__(self):
        self.docs = _load_corpus()
        self._build()

    def _build(self):
        texts = [d["text"] for d in self.docs]
        self.vecs = embed(texts)
        self.tfidf = TfidfVectorizer(stop_words="english", ngram_range=(1, 1)).fit(texts)
        self.tf = self.tfidf.transform(texts)

    def add(self, carc: str, payer: str, text: str) -> str:
        doc = {"id": "u" + uuid.uuid4().hex[:6], "carc": str(carc), "payer": payer, "text": text}
        self.docs.append(doc)
        self._build()
        return doc["id"]

    def count(self) -> int:
        return len(self.docs)

    def search(self, query: str, carc: str, payer: str, k=3, mode: str = "hybrid"):
        sims = self.vecs @ embed([query])[0]
        kw = (self.tf @ self.tfidf.transform([query]).T).toarray().ravel()
        rk, rv = _rank(kw), _rank(sims)
        out = []
        for i, d in enumerate(self.docs):
            score = 0.0
            if mode in ("hybrid", "keyword") and i in rk:
                score += 1 / (RRF_K + rk[i])
            if mode in ("hybrid", "vector") and i in rv:
                score += 1 / (RRF_K + rv[i])
            if mode == "hybrid":
                score += (CARC_BOOST if d["carc"] == str(carc) else 0) + (PAYER_BOOST if d["payer"] == payer else 0)
            if score > 0:
                out.append((score, i, d))
        out.sort(key=lambda x: -x[0])
        return [{"id": d["id"], "carc": d["carc"], "payer": d["payer"], "text": d["text"], "score": round(sc, 4),
                 "vector_sim": round(float(sims[i]), 3), "keyword_rank": rk.get(i), "vector_rank": rv.get(i)}
                for sc, i, d in out[:k]]


class PgStore:
    kind = "pgvector"

    def __init__(self, url):
        import psycopg
        self.conn = psycopg.connect(url, autocommit=True)
        c = self.conn
        self.bm25 = self.diskann = False
        for ext, flag in (("pg_textsearch", "bm25"),):
            try:
                c.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
                setattr(self, flag, True)
            except Exception:
                pass
        try:
            c.execute("CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE")      # also installs pgvector
            self.diskann = True
        except Exception:
            c.execute("CREATE EXTENSION IF NOT EXISTS vector")
        c.execute(f"""CREATE TABLE IF NOT EXISTS winning_appeals(
            id text PRIMARY KEY, carc text, payer text, body text, embedding vector({config.EMBED_DIM}),
            created_at timestamptz DEFAULT now())""")
        c.execute("CREATE INDEX IF NOT EXISTS winning_appeals_carc ON winning_appeals(carc)")
        self._index("winning_appeals_vec", "diskann" if self.diskann else "hnsw", "embedding vector_cosine_ops")
        if self.bm25:
            if not self._index("winning_appeals_bm25", "bm25", "body", "WITH (text_config = 'english')"):
                self.bm25 = False
        else:
            self._index("winning_appeals_fts", "gin", "to_tsvector('english', body)")
        if c.execute("SELECT count(*) FROM winning_appeals").fetchone()[0] == 0:
            docs = _load_corpus(); V = embed([d["text"] for d in docs])
            for d, v in zip(docs, V):
                self._insert(d["id"], d["carc"], d["payer"], d["text"], v)
        self.detail = ("BM25 (pg_textsearch)" if self.bm25 else "Postgres full-text") + " + " + \
                      ("DiskANN (vectorscale)" if self.diskann else "pgvector") + ", RRF"

    def _index(self, name, method, cols, opts="") -> bool:
        try:
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON winning_appeals USING {method} ({cols}) {opts}")
            return True
        except Exception as e:
            print(f"[overturn] index {name} ({method}) not created: {e}")
            return False

    def _insert(self, id_, carc, payer, text, v):
        self.conn.execute("INSERT INTO winning_appeals(id, carc, payer, body, embedding) VALUES (%s,%s,%s,%s,%s::vector)",
                          (id_, str(carc), payer, text, _vec_literal(v)))

    def add(self, carc: str, payer: str, text: str) -> str:
        id_ = "u" + uuid.uuid4().hex[:6]
        self._insert(id_, carc, payer, text, embed([text])[0])
        return id_

    def count(self) -> int:
        return self.conn.execute("SELECT count(*) FROM winning_appeals").fetchone()[0]

    def search(self, query, carc, payer, k=3, mode: str = "hybrid"):
        v = _vec_literal(embed([query])[0])
        if self.bm25:
            bq = "to_bm25query(%(q)s, 'winning_appeals_bm25')"
            kw_cte = (f"SELECT id, ROW_NUMBER() OVER (ORDER BY s) AS r FROM (SELECT id, body <@> {bq} AS s FROM winning_appeals "
                      f"ORDER BY body <@> {bq} LIMIT 20) t WHERE s < 0")
            args = {"q": query}
        else:
            kw_cte = ("SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank_cd(to_tsvector('english', body), to_tsquery('english', %(qor)s)) DESC) AS r "
                      "FROM winning_appeals WHERE to_tsvector('english', body) @@ to_tsquery('english', %(qor)s) LIMIT 20")
            args = {"qor": _or_query(query)}
        args.update({"v": v, "carc": str(carc), "payer": payer, "k": k, "rrf": RRF_K, "cb": CARC_BOOST, "pb": PAYER_BOOST})
        use_kw, use_vec, boost = mode in ("hybrid", "keyword"), mode in ("hybrid", "vector"), mode == "hybrid"
        sql = f"""
        WITH kw AS ({kw_cte}),
        sem AS (SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> %(v)s::vector) AS r FROM winning_appeals
                ORDER BY embedding <=> %(v)s::vector LIMIT 20)
        SELECT w.id, w.carc, w.payer, w.body,
               {"COALESCE(1.0/(%(rrf)s + kw.r), 0)" if use_kw else "0"} + {"COALESCE(1.0/(%(rrf)s + sem.r), 0)" if use_vec else "0"}
               + {"(CASE WHEN w.carc = %(carc)s THEN %(cb)s ELSE 0 END) + (CASE WHEN w.payer = %(payer)s THEN %(pb)s ELSE 0 END)" if boost else "0"} AS score,
               1 - (w.embedding <=> %(v)s::vector) AS sim, kw.r, sem.r
        FROM winning_appeals w LEFT JOIN kw ON kw.id = w.id LEFT JOIN sem ON sem.id = w.id
        WHERE {"kw.id IS NOT NULL" if use_kw and not use_vec else "sem.id IS NOT NULL" if use_vec and not use_kw else "(kw.id IS NOT NULL OR sem.id IS NOT NULL)"}
        ORDER BY score DESC LIMIT %(k)s"""
        try:
            rows = self.conn.execute(sql, args).fetchall()
        except Exception as e:
            if not self.bm25:
                raise
            print(f"[overturn] BM25 query failed ({str(e).splitlines()[0]}); falling back to Postgres full-text search", flush=True)
            self.bm25 = False
            self._index("winning_appeals_fts", "gin", "to_tsvector('english', body)")
            self.detail = "Postgres full-text (BM25 query failed) + " + ("DiskANN (vectorscale)" if self.diskann else "pgvector") + ", RRF"
            return self.search(query, carc, payer, k=k, mode=mode)
        return [{"id": r[0], "carc": r[1], "payer": r[2], "text": r[3], "score": round(float(r[4]), 4),
                 "vector_sim": round(float(r[5]), 3), "keyword_rank": r[6], "vector_rank": r[7]} for r in rows]


_store = None


def get():
    global _store
    if _store is None:
        if config.DATABASE_URL:
            try:
                _store = PgStore(config.DATABASE_URL)
            except Exception as e:
                print("[overturn] Postgres unavailable, using the in-memory store:", e)
        if _store is None:
            _store = MemoryStore()
    return _store
