import tempfile
import shutil
import unittest

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import _ensure_orderer_channel_participation
from automation.pipeline_state_store import PipelineStateStore


class FakeOrdererParticipationSshExecutor:
    def __init__(self, list_before: str = "", list_after: str = "mychannel\n", status_after: str = "status=active\n"):
        self._list_before = list_before
        self._list_after = list_after
        self._status_after = status_after

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        op = str(kwargs.get("operation", ""))
        if "orderer_channel_list_after" in op:
            c.stdout = self._list_after
        elif "orderer_channel_list" in op:
            c.stdout = self._list_before
        elif "orderer_participation_status_after" in op:
            c.stdout = self._status_after
        elif "orderer_participation_status" in op:
            c.stdout = "status=inactive\n"
        elif "orderer_channel_ensure" in op:
            c.stdout = "ensured\n"
        else:
            c.stdout = ""
        c.stderr = ""
        return c


class OrdererParticipationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.state_store = PipelineStateStore(self.tmpdir)
        self.run = PipelineRun.new(
            blueprint_fingerprint=("a" * 64),
            resolved_schema_version="1.0",
            change_id="test-change",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir)

    def test_orderer_participation_converges(self):
        ssh = FakeOrdererParticipationSshExecutor()
        issues = []
        result = _ensure_orderer_channel_participation(
            run=self.run,
            run_id=self.run.run_id,
            host_id="host1",
            orderer_component_id="orderer1",
            channel_name="mychannel",
            ssh_executor=ssh,
            state_store=self.state_store,
            issues=issues,
        )
        self.assertTrue(result.get("converged", False))
        codes = [getattr(i, "code", "") for i in issues]
        self.assertIn("verify_orderer_channel_converged", codes)


if __name__ == "__main__":
    unittest.main()
