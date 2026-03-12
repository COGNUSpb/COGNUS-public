import json
import shutil
import tempfile
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _validate_and_populate_gateway_inventory
from automation.pipeline_state_store import PipelineStateStore


class DummyCaptured:
    def __init__(self, stdout: str, stderr: str = "") -> None:
        self.stdout = stdout
        self.stderr = stderr


class DummyExecutor:
    def __init__(self, stdout: str) -> None:
        self._stdout = stdout
        self._logs = ""

    def with_logs(self, logs: str) -> "DummyExecutor":
        self._logs = logs
        return self

    def execute_unit_with_output(
        self,
        *,
        run,
        host_id: str,
        component_id: str,
        operation: str,
        idempotency_key: str,
        command: str,
        timeout_seconds: int = None,
        metadata: dict = None,
    ):
        # simulate docker ps vs docker logs
        if operation == "fetch_gateway_logs" or (isinstance(command, str) and command.strip().startswith("docker logs")):
            return DummyCaptured(self._logs, "")
        # simulate port probe for gateway
        if operation.startswith("gateway_port_probe"):
            # Always succeed for test_gateway_found_creates_artifact_and_no_issues
            return DummyCaptured("open succeeded", "")
        return DummyCaptured(self._stdout, "")


class TestGatewayValidation(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tempdir)
        # valid sha256 fingerprint
        fingerprint = "a" * 64
        self.run = PipelineRun.new(
            blueprint_fingerprint=fingerprint, resolved_schema_version="1.0", change_id="test-change"
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def test_gateway_found_creates_artifact_and_no_issues(self):
        nodes_by_host = {
            "host1": [
                {"component_type": "api_gateway", "component_id": "gateway1"}
            ]
        }

        stdout = "gateway1||cognus/ccapi-go:latest||0.0.0.0:443->443/tcp\n"
        executor = DummyExecutor(stdout).with_logs("startup OK\nlistening on 443\n")
        checkpoints = []

        issues = _validate_and_populate_gateway_inventory(
            run=self.run,
            nodes_by_host=nodes_by_host,
            state_store=self.state_store,
            ssh_executor=executor,
            checkpoints=checkpoints,
        )

        self.assertEqual(len(issues), 0)

        artifact_path = self.state_store.stage_artifacts_dir(self.run.run_id, "prepare") / "gateway-inventory.json"
        self.assertTrue(artifact_path.exists())
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        self.assertIn("host1/gateway1", payload.get("gateway_inventory", {}))
        self.assertTrue(payload["gateway_inventory"]["host1/gateway1"]["found"])

        # logs artifact should exist
        logs_artifact = self.state_store.stage_artifacts_dir(self.run.run_id, "prepare") / "gateway-logs-host1-gateway1-gateway1.txt"
        self.assertTrue(logs_artifact.exists())
        logs_content = logs_artifact.read_text(encoding="utf-8")
        self.assertIn("startup OK", logs_content)

        # stage-reports entry should be emitted
        events_file = (
            Path(self.state_store.artifacts_dir) / self.run.run_id / "stage-reports" / "prepare-events.jsonl"
        )
        self.assertTrue(events_file.exists())
        content = events_file.read_text(encoding="utf-8").strip()
        self.assertTrue(content)

    def test_gateway_missing_returns_issue_and_checkpoint(self):
        nodes_by_host = {
            "host2": [
                {"component_type": "api_gateway", "component_id": "gatewayX"}
            ]
        }

        # no matching container in stdout
        stdout = "other_container||some/image:latest||"
        executor = DummyExecutor(stdout)
        checkpoints = []

        issues = _validate_and_populate_gateway_inventory(
            run=self.run,
            nodes_by_host=nodes_by_host,
            state_store=self.state_store,
            ssh_executor=executor,
            checkpoints=checkpoints,
        )

        self.assertTrue(any(getattr(i, "code", "") == "prepare_gateway_container_missing" for i in issues))
        self.assertEqual(len(checkpoints), 1)
        cp = checkpoints[0]
        self.assertEqual(str(cp.get("status", "")).lower(), "failed")

        artifact_path = self.state_store.stage_artifacts_dir(self.run.run_id, "prepare") / "gateway-inventory.json"
        self.assertTrue(artifact_path.exists())
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        self.assertIn("host2/gatewayX", payload.get("gateway_inventory", {}))
        self.assertFalse(payload["gateway_inventory"]["host2/gatewayX"]["found"])


if __name__ == "__main__":
    unittest.main()
