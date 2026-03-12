import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _tls_san_inspect_and_apply_hostnameoverride
from automation.pipeline_state_store import PipelineStateStore


class FakeSshExecutor:
    def __init__(self, tls_stdout: str = "", apply_stdout: str = ""):
        self._tls = tls_stdout
        self._apply = apply_stdout

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get("operation", ""))
        if "tls_san" in op:
            c.stdout = self._tls
        elif "apply_hostnameoverride" in op or "apply-hostnameoverride" in op:
            c.stdout = self._apply
        else:
            c.stdout = ""
        c.stderr = ""
        return c


class TlsSanTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_san_present_no_action(self):
        # simulate openssl output with SAN
        tls_out = "Subject Alternative Name:\n  DNS: example.com, IP Address: 127.0.0.1"
        ssh = FakeSshExecutor(tls_stdout=tls_out, apply_stdout="NO_CHANGE")
        issues = []
        checkpoints = []
        _tls_san_inspect_and_apply_hostnameoverride(
            run=self.run,
            run_id=self.run.run_id,
            host_id="host1",
            component_id="gw1",
            connection_json_path="/var/cognus/api-runtime/connection.json",
            host_ref="example.com",
            peer_port=7051,
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        # should not add missing SAN issue
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertNotIn('verify_gateway_tls_san_missing', codes)

    def test_san_missing_apply_patch(self):
        tls_out = ""  # no SAN info
        ssh = FakeSshExecutor(tls_stdout=tls_out, apply_stdout="PATCHED")
        issues = []
        checkpoints = []
        _tls_san_inspect_and_apply_hostnameoverride(
            run=self.run,
            run_id=self.run.run_id,
            host_id="host2",
            component_id="gw2",
            connection_json_path="/var/cognus/api-runtime/connection.json",
            host_ref="example.com",
            peer_port=7051,
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_tls_san_missing', codes)
        self.assertIn('tls_san_missing', codes)
        self.assertIn('verify_gateway_tls_hostnameoverride_applied', codes)
        # artifact should exist
        artifact_path = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host2-gw2-hostnameoverride-apply.txt'
        self.assertTrue(artifact_path.exists())


if __name__ == '__main__':
    unittest.main()
