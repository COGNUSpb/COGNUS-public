import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _discover_peer_orderer_cert_and_regenerate
from automation.pipeline_state_store import PipelineStateStore


class FakeSshExecutor:
    def __init__(self, discovery_stdout: str = "", behavior: str = "no_ca"):
        # behavior: 'no_ca', 'ok', 'fail'
        self._discovery_stdout = discovery_stdout
        self._behavior = behavior

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get('operation', ''))
        if 'discover_peer_tls' in op:
            c.stdout = self._discovery_stdout
        elif 'tls_regenerate' in op:
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


class PeerOrdererTlsRegenTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_discovery_no_pairs_emits_info(self):
        ssh = FakeSshExecutor(discovery_stdout='')
        issues = []
        checkpoints = []
        _discover_peer_orderer_cert_and_regenerate(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host1',
            component_id='peer-group',
            host_ref='10.0.0.5',
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_peer_orderer_tls_not_found', codes)

    def test_discovery_and_regen_applied(self):
        # simulate detection of one pair and regen success
        discovery = '/var/hyperledger/production/tls/server.crt|/var/hyperledger/production/tls/server.key\n'
        ssh = FakeSshExecutor(discovery_stdout=discovery, behavior='ok')
        issues = []
        checkpoints = []
        _discover_peer_orderer_cert_and_regenerate(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host2',
            component_id='orderer-main',
            host_ref='orderer.example.local',
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_tls_regenerated_applied', codes)
        artifact = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host2-orderer-main-peer1-tls-regen.txt'
        self.assertTrue(artifact.exists())


if __name__ == '__main__':
    unittest.main()
