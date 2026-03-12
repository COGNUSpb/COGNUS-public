import tempfile
import json
import unittest
from types import SimpleNamespace
from pathlib import Path

from automation.pipeline_state_store import PipelineStateStore
from automation.pipeline_contract import PipelineRun
from automation import pipeline_provision as pp


class FakeSshExecutor:
    def __init__(self, stdout="identities_reloaded", stderr=""):
        self._stdout = stdout
        self._stderr = stderr

    def execute_unit_with_output(self, *, run, host_id, component_id, operation, idempotency_key, command, timeout_seconds=30.0):
        return SimpleNamespace(stdout=self._stdout, stderr=self._stderr)


class TestControlledRestartHelper(unittest.TestCase):
    def test_controlled_restart_records_tech_debt(self):
        with tempfile.TemporaryDirectory() as td:
            store = PipelineStateStore(td)
            # create a minimal PipelineRun
            run = PipelineRun.new(
                blueprint_fingerprint="a" * 64,
                resolved_schema_version="1.0.0",
                change_id="change-xyz",
            )
            ssh = FakeSshExecutor(stdout="identities_reloaded")

            out = pp._controlled_restart_container(
                run=run,
                state_store=store,
                ssh_executor=ssh,
                host_id="host1",
                component_id="api1",
                container_name="api1",
                reason="test-reason",
            )

            self.assertIsNotNone(out)
            # artifact should exist
            artifacts = store.list_stage_artifacts(run.run_id, "provision")
            self.assertTrue(any("workarounds-a2-tech-debt.json" in name for name in artifacts))
            path = store.stage_artifacts_dir(run.run_id, "provision") / "workarounds-a2-tech-debt.json"
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            self.assertIsInstance(data, list)
            self.assertTrue(any(item.get("action") == "controlled_restart" or item.get("host_id") == "host1" for item in data))


if __name__ == "__main__":
    unittest.main()
