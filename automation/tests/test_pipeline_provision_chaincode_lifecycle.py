import tempfile
import shutil
import unittest
from pathlib import Path

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _ensure_chaincode_lifecycle
from automation.pipeline_state_store import PipelineStateStore


class FakeLifecycleSshExecutor:
    def __init__(self):
        pass

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get('operation', ''))
        if 'chaincode_install' in op:
            c.stdout = 'Installed'
        elif 'chaincode_queryinstalled' in op:
            c.stdout = 'Package ID: pkg12345:abcd, Label: mycc_1.0'
        elif 'chaincode_approve' in op:
            c.stdout = 'Approved'
        elif 'chaincode_commit' in op:
            c.stdout = 'Committed'
        elif 'chaincode_querycommitted' in op:
            c.stdout = 'Name: mycc, Version: 1.0, Sequence: 1'
        else:
            c.stdout = ''
        c.stderr = ''
        return c


class ChaincodeLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(blueprint_fingerprint=("a" * 64), resolved_schema_version="1.0", change_id="test-change")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_lifecycle_happy_path(self):
        ssh = FakeLifecycleSshExecutor()
        issues = []
        checkpoints = []
        _ensure_chaincode_lifecycle(
            run=self.run,
            run_id=self.run.run_id,
            host_id='host1',
            component_id='peer1',
            chaincode_name='mycc',
            chaincode_version='1.0',
            ssh_executor=ssh,
            state_store=self.state_store,
            checkpoints=checkpoints,
            issues=issues,
        )
        codes = [getattr(i, 'code', '') for i in issues]
        self.assertIn('lifecycle_chaincode_installed', codes)
        self.assertIn('lifecycle_chaincode_committed', codes)
        art1 = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host1-peer1-chaincode-packageid-mycc.txt'
        art2 = Path(self.state_store.stage_artifacts_dir(self.run.run_id, 'provision')) / 'gateway-host1-peer1-chaincode-querycommitted-mycc.txt'
        self.assertTrue(art1.exists())
        self.assertTrue(art2.exists())


if __name__ == '__main__':
    unittest.main()
