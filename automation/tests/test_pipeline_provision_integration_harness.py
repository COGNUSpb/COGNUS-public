import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import (
    _tls_san_inspect_and_apply_hostnameoverride,
    _discover_peer_orderer_cert_and_regenerate,
    _ensure_peer_join_channel,
)
from automation.pipeline_state_store import PipelineStateStore


class FakeIntegrationSshExecutor:
    def __init__(self):
        self.calls = []

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        op = str(kwargs.get('operation', ''))
        self.calls.append(op)
        c = C()
        # TLS SAN check
        if 'tls_san' in op:
            # simulate cert without SAN
            c.stdout = 'Subject: CN=example\nX509v3 Subject Alternative Name:\n'
        elif 'apply_hostnameoverride' in op:
            c.stdout = 'PATCHED\n'
        elif 'discover-peer-tls' in op:
            # simulate one cert|key pair discovered
            c.stdout = '/var/hyperledger/production/tls/server.crt|/var/hyperledger/production/tls/server.key\n'
        elif 'tls_regen' in op:
            c.stdout = 'CERT_GENERATED\n'
        elif 'peer_channel_fetch' in op:
            c.stdout = 'Saved to /tmp/mychan.block\n'
        elif 'peer_channel_blockcheck' in op:
            c.stdout = 'EXISTS\n'
        elif 'peer_channel_join' in op:
            c.stdout = 'Join successful\n'
        elif 'peer-channel-list-after' in op:
            c.stdout = 'mychannel\n'
        else:
            c.stdout = ''
        c.stderr = ''
        return c


class IntegrationHarnessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")
        self.ssh = FakeIntegrationSshExecutor()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_end_to_end_helpers_flow(self):
        issues = []
        checkpoints = []
        # 1) TLS SAN inspect + apply hostnameOverride
        _tls_san_inspect_and_apply_hostnameoverride(
            run=self.run,
            run_id=self.run.run_id,
            host_id='h1',
            component_id='gw1',
            connection_json_path='/var/cognus/api-runtime/connection.json',
            host_ref='example.com',
            peer_port=7051,
            ssh_executor=self.ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_gateway_tls_hostnameoverride_applied', codes)
        # artifact persisted
        art = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-h1-gw1-hostnameoverride-apply.txt'
        self.assertTrue(art.exists())

        # 2) discover peer/orderer certs and attempt regen
        _discover_peer_orderer_cert_and_regenerate(
            run=self.run,
            run_id=self.run.run_id,
            host_id='h1',
            component_id='gw1',
            host_ref='example.com',
            ssh_executor=self.ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        # regen artifact
        regen_art = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-h1-gw1-peer-tls-discovery.txt'
        self.assertTrue(regen_art.exists())

        # 3) ensure peer joined channel
        _ensure_peer_join_channel(
            run=self.run,
            run_id=self.run.run_id,
            host_id='h1',
            component_id='peer1',
            channel_name='mychannel',
            orderer_host='orderer.example',
            orderer_port=7050,
            orderer_tls_cafile=None,
            ssh_executor=self.ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        join_codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_channel_join_applied', join_codes)
        join_art = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-h1-peer1-channel-join-mychannel.txt'
        self.assertTrue(join_art.exists())


if __name__ == '__main__':
    unittest.main()
