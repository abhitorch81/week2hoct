"""Model arena + Backboard client against a fake Backboard server (the real API is not called)."""
import http.server, json, os, re, sys, threading, unittest, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

GOOD = ("The record shows the service was medically necessary after conservative treatment failed, and the clinical "
        "rationale is documented by the treating clinician for the date of service. We ask the payer to review its decision.\n\n"
        "Less intensive alternatives were tried first, which supports the service as billed.")
BAD = "We were denied $9,999.99 and also billed CPT 11111 for this case."


class FakeBB(http.server.BaseHTTPRequestHandler):
    seen_keys = []
    def log_message(self, *a): pass
    def _send(self, obj, code=200):
        b = json.dumps(obj).encode(); self.send_response(code); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        FakeBB.seen_keys.append(self.headers.get("X-API-Key"))
        self._send({"models": [{"name": "llama-3.3-70b", "provider": "together"}, {"name": "gpt-4o", "provider": "openai"},
                               {"name": "qwen-good", "provider": "x"}, {"name": "bad-qwen", "provider": "x"}, {"name": "broken-llama", "provider": "x"}]})
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        if self.path.endswith("/assistants"): return self._send({"assistant_id": "a1"})
        if self.path.endswith("/threads"): return self._send({"thread_id": "t1"})
        model = re.search(r'name="model_name"\r\n\r\n(.*?)\r\n', body).group(1)
        if "broken" in model: return self._send({"detail": "model unavailable"}, 500)
        self._send({"content": BAD if "bad" in model else GOOD, "input_tokens": 100, "output_tokens": 50})


class T(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeBB)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        os.environ.update({"BACKBOARD_API_KEY": "test-key", 
                           "BACKBOARD_BASE_URL": f"http://127.0.0.1:{cls.srv.server_address[1]}"})

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_only_open_weight_models_are_offered(self):
        from overturn import backboard
        names = [m["name"] for m in backboard.open_models()]
        self.assertIn("llama-3.3-70b", names)
        self.assertNotIn("gpt-4o", names)
        self.assertIn("test-key", FakeBB.seen_keys)

    def test_arena_scores_and_counts_external_calls(self):
        from overturn import arena, llm
        before = llm.egress_summary()["external_calls"]
        evs = list(arena.stream([{"name": "qwen-good", "provider": "x"}, {"name": "bad-qwen", "provider": "x"},
                                 {"name": "broken-llama", "provider": "x"}], n_cases=3, include_local=False))
        self.assertTrue(evs[0]["external"])
        rank = {r["label"].split(" ")[0]: r for r in evs[-1]["ranking"]}
        self.assertEqual(rank["qwen-good"]["pass_rate"], 1.0)
        self.assertEqual(rank["bad-qwen"]["pass_rate"], 0.0)
        self.assertTrue(rank["bad-qwen"]["top_failures"])
        self.assertEqual(rank["broken-llama"]["errors"], 3)
        self.assertIn("HTTP 500", rank["broken-llama"]["error_sample"])
        self.assertEqual(evs[-1]["ranking"][0]["label"].split(" ")[0], "qwen-good")
        self.assertGreater(llm.egress_summary()["external_calls"], before)   # hosted calls are never hidden

    def test_missing_key_is_a_clear_error(self):
        from overturn import backboard, config
        key, config.BACKBOARD_API_KEY = config.BACKBOARD_API_KEY, ""
        try:
            with self.assertRaises(backboard.BackboardError):
                backboard.list_models()
        finally:
            config.BACKBOARD_API_KEY = key


if __name__ == "__main__":
    unittest.main()
