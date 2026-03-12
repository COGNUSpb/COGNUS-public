import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _ensure_peer_join_channel
from automation.pipeline_state_store import PipelineStateStore


class FakeMspRegenSshExecutor:
    def __init__(self, list_output: str = "", fetch_behavior: str = "missing", ca_present: bool = True, join_behavior: str = 'ok'):
        self._list_output = list_output
        self._fetch_behavior = fetch_behavior
        self._ca_present = ca_present
        self._join_behavior = join_behavior

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get('operation', ''))
        if 'peer-channel-list' in op and 'after' not in op:
            c.stdout = self._list_output
        elif 'peer_channel_fetch' in op:
            if self._fetch_behavior == 'exists':
                c.stdout = 'Saved to /tmp/channel.block\n'
            else:
                c.stdout = 'ERROR: fetch failed\n'
        elif 'peer_channel_blockcheck' in op:
            if self._fetch_behavior == 'exists':
                c.stdout = 'EXISTS\n'
            else:
                c.stdout = 'MISSING\n'
        elif 'peer_channel_msp_fetch' in op:
            # return a sha256sum-like output for /tmp/_orderer_cert.pem
            c.stdout = f"abcd  /tmp/_orderer_cert.pem\n"
        elif 'peer_channel_msp_localpeer' in op:
            c.stdout = f"efgh  /var/hyperledger/peer/msp/tlscacerts/orderer-tlsca.pem\n"
        elif 'peer_channel_msp_align' in op:
            c.stdout = 'copied\n'
        elif 'peer_channel_msp_ca_check' in op:
            c.stdout = 'CA_FOUND\n' if self._ca_present else 'NO_CA\n'
        elif 'peer_channel_msp_regen' in op:
            c.stdout = 'REGEN_DONE\n'
        elif 'peer_channel_join' in op:
            if self._join_behavior == 'ok':
                c.stdout = 'Join successful\n'
            else:
                c.stdout = 'ERROR: join failed\n'
        elif 'peer-channel-list-after' in op:
            if self._join_behavior == 'ok':
                c.stdout = self._list_output + '\nmychannel'
            else:
                c.stdout = self._list_output
        else:
            c.stdout = ''
        c.stderr = ''
        return c


class MspRegenTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_regen_and_rejoin(self):
        ssh = FakeMspRegenSshExecutor(list_output='', fetch_behavior='missing', ca_present=True, join_behavior='ok')
        issues = []
        checkpoints = []
        _ensure_peer_join_channel(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host1',
            component_id='peer1',
            channel_name='mychannel',
            orderer_host='orderer.example',
            orderer_port=7050,
            orderer_tls_cafile=None,
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('verify_msp_regenerated', codes)
        self.assertIn('verify_channel_join_applied', codes)
        artifact = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host1-peer1-msp-regen-mychannel.txt'
        self.assertTrue(artifact.exists())

    def test_missing_ca_emits_msp_ca_invalid(self):
        ssh = FakeMspRegenSshExecutor(list_output='', fetch_behavior='missing', ca_present=False, join_behavior='fail')
        issues = []
        checkpoints = []
        _ensure_peer_join_channel(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host2',
            component_id='peer2',
            channel_name='mychannel',
            orderer_host='orderer.example',
            orderer_port=7050,
            orderer_tls_cafile=None,
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('msp_ca_invalid', codes)


if __name__ == '__main__':
    unittest.main()
