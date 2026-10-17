import json, os, sys, unittest, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
GOOD = ("The record shows the service was medically necessary after conservative treatment failed, and the clinical "
        "rationale is documented by the treating clinician for the date of service. We ask the payer to review its decision.")


class T(unittest.TestCase):
    def test_dataset_pairs_match_inference_prompt_and_pass_factcheck(self):
        from tinker_ft import build_dataset
        from overturn import arena, letter
        rows, dropped = build_dataset.build(25)
        self.assertGreaterEqual(len(rows), 20)
        ex = rows[0]
        self.assertEqual(ex["system"], letter.BODY_SYSTEM)
        self.assertIn("FACTS", ex["prompt"])
        self.assertNotIn("$", ex["completion"])               # facts live in the deterministic header, not the argument
        self.assertLessEqual(len(ex["completion"].split()), 170)
        case = arena.prepare_case(1000)                       # same prompt builder as the arena
        self.assertEqual(rows[0]["prompt"], case["prompt"])

    def test_arena_adds_tinker_base_and_tuned_contestants(self):
        from overturn import arena, config, tinker_model, llm
        config.TINKER_API_KEY, config.TINKER_MODEL_PATH = "k", "tinker://run/sampler_weights/x"
        seen = []
        orig = tinker_model.chat
        tinker_model.chat = lambda s, u, model_path=None, max_tokens=500: (seen.append(model_path) or
            {"text": GOOD, "tokens_in": 10, "tokens_out": 5, "model": "m"})
        try:
            cons = arena.contestants([], include_local=False)
            self.assertEqual(len(cons), 2)
            self.assertTrue(all(not c["local"] for c in cons))
            for c in cons:
                r = arena.run_one(c["call"], arena.prepare_case(0))
                self.assertTrue(r["ok"], r)
            self.assertEqual(seen, [None, "tinker://run/sampler_weights/x"])
        finally:
            tinker_model.chat = orig
            config.TINKER_API_KEY, config.TINKER_MODEL_PATH = "", ""


if __name__ == "__main__":
    unittest.main()
