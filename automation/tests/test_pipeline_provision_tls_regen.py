import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _tls_regenerate_if_ca_available
from automation.pipeline_state_store import PipelineStateStore


class FakeSshExecutor:
    def __init__(self, stdout: str = "", behavior: str = "no_ca"):
        # behavior: 'no_ca', 'ok', 'fail'
        self._stdout = stdout
        self._behavior = behavior

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get('operation', ''))
        if 'tls_regenerate' in op:
            if self._behavior == 'no_ca':
                c.stdout = 'NO_CA'
            elif self._behavior == 'ok':
                c.stdout = 'CERT_GENERATED'
            else:
                c.stdout = 'FAILED'
        else:
            c.stdout = ''
        c.stderr = ''
        return c


class TlsRegenTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_regen_skipped_when_no_ca(self):
        ssh = FakeSshExecutor(behavior='no_ca')
        issues = []
        checkpoints = []
        _tls_regenerate_if_ca_available(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host1',
            component_id='gw1',
            cert_path='/var/cognus/api-runtime/identities/org1/cert.pem',
            key_path='/var/cognus/api-runtime/identities/org1/key.pem',
            ca_cert_path='/var/cognus/crypto/ca/tlsca.crt',
            ca_key_path='/var/cognus/crypto/ca/tlsca.key',
            host_ref='example.com',
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_tls_regen_no_ca', codes)

    def test_regen_applied_when_ca_present(self):
        ssh = FakeSshExecutor(behavior='ok')
        issues = []
        checkpoints = []
        _tls_regenerate_if_ca_available(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host2',
            component_id='gw2',
            cert_path='/var/cognus/api-runtime/identities/org1/cert.pem',
            key_path='/var/cognus/api-runtime/identities/org1/key.pem',
            ca_cert_path='/var/cognus/crypto/ca/tlsca.crt',
            ca_key_path='/var/cognus/crypto/ca/tlsca.key',
            host_ref='example.com',
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_tls_regenerated_applied', codes)
        artifact = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host2-gw2-tls-regen.txt'
        self.assertTrue(artifact.exists())


if __name__ == '__main__':
    unittest.main()
