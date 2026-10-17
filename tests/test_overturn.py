"""Run: python -m unittest discover -s tests
Includes a fake Ollama server so the LLM code path and the hallucination guardrail are tested
without needing a GPU or model download."""
import http.server, json, os, threading, unittest, importlib, shutil, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
os.environ["OVERTURN_CKPT"] = "/tmp/overturn-test-ckpt"

BAD_LETTER = ("The claim for CPT 29881 under CO-50 was wrongly denied. We were denied $9,999.99 and also billed CPT 11111 "
              "correctly, and the surgeon notes establish necessity for this level of care in the record.")
GOOD_BODY = ("The documentation shows the knee procedure was medically necessary after conservative treatment failed, and "
             "the clinical rationale for the level of care is recorded in the treating surgeon's notes for the date of service. "
             "We ask that the payer review the determination against its own published criteria.\n\n"
             "The patient's condition did not respond to less intensive alternatives, which supports the service as billed.")


class FakeOllama(http.server.BaseHTTPRequestHandler):
    mode = "bad"
    def log_message(self, *a): pass
    def _send(self, obj):
        b = json.dumps(obj).encode(); self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        self._send({"models": [{"name": "fake:1b"}]})
    def do_POST(self):
        FakeOllama.last = self.rfile.read(int(self.headers["Content-Length"])).decode()
        n = int(self.rfile.read(0) or 0)
        body = getattr(FakeOllama, "last", "")
        mode = FakeOllama.mode
        if mode == "fix":   # bad first, good once the prompt contains corrective feedback
            text = GOOD_BODY if "PREVIOUS ATTEMPT" in body else BAD_LETTER
        elif mode == "omit":  # a sloppy body that simply omits all facts: header supplies them
            text = GOOD_BODY
        else:
            text = BAD_LETTER
        self._send({"message": {"content": text}, "prompt_eval_count": 120, "eval_count": 60})


class T(unittest.TestCase):
    def test_extract(self):
        from overturn import synth, extract
        d = synth.make_open_denial(9)
        f = extract.regex_extract(synth.render_denial_letter(d))
        self.assertEqual(f["claim_id"], d["id"]); self.assertEqual(f["cpt"], d["cpt"]); self.assertEqual(f["carc"], d["carc"])
        self.assertAlmostEqual(f["denied"], d["denied"], 2)

    def test_predictor_signal(self):
        from overturn import predict
        p = predict.get()
        self.assertGreater(p.auc, 0.7)
        base = {"payer": "Aetna", "carc": "16", "specialty": "Radiology", "billed": 500, "days_since_denial": 10,
                "has_clinical_notes": 1, "prior_auth_on_file": 0, "in_network": 1, "appeal_level": 1}
        hard = dict(base, carc="18", payer="UnitedHealthcare", days_since_denial=170)
        self.assertGreater(p.predict(base)["p"], p.predict(hard)["p"])

    def test_triage_never_recodes_a_medical_necessity_denial(self):
        from overturn import pipeline, kb
        opts, best, _ = pipeline.compute_options({"denied": 5000}, kb.get("50"), 0.5, 100)
        self.assertNotEqual(best["id"], "recode")
        opts, best, _ = pipeline.compute_options({"denied": 5000}, kb.get("4"), 0.5, 100)
        self.assertEqual(best["id"], "recode")
        opts, best, _ = pipeline.compute_options({"denied": 40}, kb.get("50"), 0.5, 100)
        self.assertEqual(best["id"], "writeoff")

    def test_amount_check_tolerates_formatting(self):
        from overturn import letter
        f = {"claim_id": "CLM-1", "cpt": "29881", "carc": "50", "denied": 6286.64, "billed": 7122.79, "payer": "BCBS"}
        ok = letter.verify("Claim CLM-1 BCBS CPT 29881 CO-50. We dispute the denial of $6286.64 (billed $7,123).", f)
        self.assertTrue(ok["passed"])
        bad = letter.verify("Claim CLM-1 BCBS CPT 29881 CO-50. We dispute $9,999.", f)
        self.assertFalse(bad["passed"])

    def test_durable_resume(self):
        from overturn import durable, synth
        shutil.rmtree("/tmp/overturn-test-ckpt", ignore_errors=True)
        txt = synth.render_denial_letter(synth.make_open_denial(4))
        ev1 = list(durable.run(txt, {}, run_id="r1", crash_after=4))
        self.assertEqual(ev1[-1]["type"], "crash")
        ev2 = list(durable.run(txt, {}, run_id="r1"))
        restored = [e["step"] for e in ev2 if e["type"] == "step_done" and e["restored"]]
        self.assertEqual(restored, ["ingest", "extract", "classify", "predict"])
        self.assertEqual(ev2[-1]["type"], "final")

    def test_flaky_tool_retries(self):
        from overturn import durable, synth
        txt = synth.render_denial_letter(synth.make_open_denial(6))
        ev = list(durable.run(txt, {}, run_id="r2", chaos="flaky"))
        self.assertTrue(any(e["type"] == "retry" for e in ev)); self.assertEqual(ev[-1]["type"], "final")

    def _run_with_fake(self, mode, run_id):
        FakeOllama.mode = mode
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeOllama)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        os.environ["OLLAMA_URL"] = f"http://127.0.0.1:{srv.server_address[1]}"
        from overturn import config, llm
        importlib.reload(config); importlib.reload(llm)
        from overturn import letter, pipeline, durable, synth
        importlib.reload(letter); importlib.reload(pipeline); importlib.reload(durable)
        txt = synth.render_denial_letter(synth.make_open_denial(9))
        final = [e for e in durable.run(txt, {}, run_id=run_id) if e["type"] == "final"][0]["result"]
        srv.shutdown()
        return final

    def test_model_that_omits_facts_still_gets_a_correct_letter(self):
        final = self._run_with_fake("omit", "r4")
        self.assertTrue(final["letter"]["engine"].startswith("llm:"))
        self.assertEqual(final["letter"]["attempts"], 1)
        self.assertTrue(final["verify"]["passed"])
        self.assertIn("$6,286.64", final["letter"]["text"])   # from the deterministic header
        self.assertIn("CO-50", final["letter"]["text"])

    def test_corrective_retry_recovers_a_bad_first_draft(self):
        final = self._run_with_fake("fix", "r5")
        self.assertTrue(final["letter"]["engine"].startswith("llm:"))
        self.assertEqual(final["letter"]["attempts"], 2)
        self.assertTrue(final["verify"]["passed"])

    def test_temporal_activity_logic_survives_json_roundtrip_and_retry(self):
        """Simulates what Temporal does: JSON-serialise state between activities and retry a failed one."""
        from overturn import temporal_workflow as tw, synth, pipeline
        state = {"text": synth.render_denial_letter(synth.make_open_denial(9)), "inputs": {}, "chaos": "flaky"}
        progress = []
        for name in tw.STEP_NAMES:
            for attempt in range(1, 5):
                try:
                    r = tw.execute_step(name, json.loads(json.dumps(state)), attempt)   # fresh deserialised copy per attempt
                    break
                except pipeline.FlakyTool:
                    continue
            state = json.loads(json.dumps(r["state"]))
            progress.append({"step": name, "data": r["data"], "ms": r["ms"], "attempts": r["attempts"]})
        self.assertEqual([p["attempts"] for p in progress if p["step"] == "retrieve"], [2])
        final = tw._final_from({"state": state, "progress": progress})
        self.assertEqual(len(final["trace"]["spans"]), 8)
        self.assertTrue(final["verify"]["passed"])

    def test_hybrid_search_modes_and_learning_from_wins(self):
        from overturn import vectorstore
        s = vectorstore.MemoryStore()
        n = s.count()
        q = "modifier 59 separate incision different diagnosis bundled"
        hyb = s.search(q, "97", "Aetna", k=3, mode="hybrid")
        self.assertTrue(all(h["carc"] == "97" for h in hyb[:2]))                      # code boost works
        self.assertTrue(s.search(q, "97", "Aetna", mode="keyword")[0]["keyword_rank"] == 1)
        self.assertTrue(s.search(q, "97", "Aetna", mode="vector")[0]["vector_rank"] == 1)
        new_id = s.add("97", "Aetna", "Separate incision and different diagnosis documented; modifier 59 supported by the operative report.")
        self.assertEqual(s.count(), n + 1)
        self.assertEqual(s.search(q, "97", "Aetna", k=1)[0]["id"], new_id)             # a saved win is retrievable

    def test_scrub_removes_identifiers(self):
        from overturn import letter
        f = {"patient": "Sam Moreau", "member_id": "X329844221", "claim_id": "CLM-26009", "npi": "1378207167", "provider": "Northgate Imaging Center"}
        out = letter.scrub("Sam Moreau (X329844221) at Northgate Imaging Center, claim CLM-26009", f)
        for bad in f.values():
            self.assertNotIn(bad, out)

    def test_clean_body_strips_letterhead_and_closing(self):
        from overturn import letter
        raw = "RE: Appeal\nDear Reviewer,\n\n**Para one.**\n\nPara two.\n\nSincerely,\nClinic"
        self.assertEqual(letter.clean_body(raw), "Para one.\n\nPara two.")

    def test_hallucinated_llm_draft_is_rejected(self):
        final = self._run_with_fake("bad", "r3")
        self.assertEqual(final["letter"]["attempts"], 2)
        self.assertIn("rejected by fact-check", final["letter"]["engine"])
        self.assertTrue(final["verify"]["passed"])
        self.assertTrue(final["letter"]["rejected_failed"])
        self.assertIn("Unsupported", json.dumps(final["letter"]["rejected_failed"]))
        self.assertGreater(final["engine"]["tokens"], 0)
        self.assertEqual(final["engine"]["external_calls"], 0)


if __name__ == "__main__":
    unittest.main()
