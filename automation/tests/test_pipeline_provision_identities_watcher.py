import tempfile
import json
import unittest
from types import SimpleNamespace
from pathlib import Path

from automation.pipeline_state_store import PipelineStateStore
from automation.pipeline_contract import PipelineRun
from automation import pipeline_provision as pp


class FakeSshExecutorMulti:
    def __init__(self, responses):
        # responses is an iterable of (stdout, stderr)
        self._responses = list(responses)
        self._idx = 0

    def execute_unit_with_output(self, *, run, host_id, component_id, operation, idempotency_key, command, timeout_seconds=30.0):
        if self._idx >= len(self._responses):
            out = ""
            err = ""
        else:
            out, err = self._responses[self._idx]
        self._idx += 1
        return SimpleNamespace(stdout=out, stderr=err)


class TestIdentitiesWatcher(unittest.TestCase):
    def test_probe_detects_change_and_restarts(self):
        with tempfile.TemporaryDirectory() as td:
            store = PipelineStateStore(td)
            run = PipelineRun.new(
                blueprint_fingerprint="a" * 64,
                resolved_schema_version="1.0.0",
                change_id="change-1",
            )
            # First call returns new sha, second call will be restart output
            responses = [("deadbeefcafef00ddeadbeefcafef00ddeadbeefcafef00ddeadbeefcafef00d", ""), ("identities_reloaded", "")]
            ssh = FakeSshExecutorMulti(responses)

            res = pp._probe_identities_and_restart_if_changed(
                run=run,
                state_store=store,
                ssh_executor=ssh,
                host_id="hostX",
                component_id="apiX",
                container_name="apiX",
            )
            self.assertTrue(res)
            # check sha artifact persisted
            art_name = f"gateway-hostX-apiX-identities-sha256.txt"
            path = store.stage_artifacts_dir(run.run_id, "provision") / art_name
            self.assertTrue(path.exists())
            self.assertEqual(path.read_text(encoding="utf-8").strip(), "deadbeefcafef00ddeadbeefcafef00ddeadbeefcafef00ddeadbeefcafef00d")
            # check tech-debt recorded for controlled_restart
            td_path = store.stage_artifacts_dir(run.run_id, "provision") / "workarounds-a2-tech-debt.json"
            self.assertTrue(td_path.exists())
            data = json.loads(td_path.read_text(encoding="utf-8"))
            self.assertTrue(any(
                item.get("action") in {"controlled_restart", "identities_change_detected"} or item.get("reason") == "identities_json_changed"
                for item in data
            ))


if __name__ == "__main__":
    unittest.main()
