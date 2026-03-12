import json
import tempfile
from pathlib import Path
import unittest

from automation.pipeline_state_store import PipelineStateStore


class TestPipelineStateStoreTechDebt(unittest.TestCase):
    def test_append_creates_and_appends(self):
        with tempfile.TemporaryDirectory() as td:
            store = PipelineStateStore(td)
            run_id = "run-1"
            stage = "provision"
            artifact = "tech-debt.json"

            # first append with dict
            path1 = store.append_json_array_artifact(run_id, stage, artifact, {"id": 1, "note": "first"})
            self.assertTrue(Path(path1).exists())
            data = json.loads(Path(path1).read_text(encoding="utf-8"))
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0].get("id"), 1)

            # append a plain string -> should be wrapped as {'note': <string>}
            path2 = store.append_json_array_artifact(run_id, stage, artifact, "followup note")
            self.assertTrue(Path(path2).exists())
            data2 = json.loads(Path(path2).read_text(encoding="utf-8"))
            self.assertEqual(len(data2), 2)
            self.assertEqual(data2[1].get("note"), "followup note")

    def test_append_with_existing_nonlist_resets_and_appends(self):
        with tempfile.TemporaryDirectory() as td:
            store = PipelineStateStore(td)
            run_id = "run-2"
            stage = "provision"
            artifact = "tech-debt.json"

            # create an existing file with non-list content
            target = store.stage_artifacts_dir(run_id, stage) / artifact
            target.write_text(json.dumps({"broken": True}), encoding="utf-8")

            # append should recover and create a list with the new entry
            path = store.append_json_array_artifact(run_id, stage, artifact, {"id": 42})
            self.assertTrue(Path(path).exists())
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            self.assertIsInstance(data, list)
            self.assertEqual(len(data), 1)
            self.assertEqual(data[0].get("id"), 42)


if __name__ == "__main__":
    unittest.main()
