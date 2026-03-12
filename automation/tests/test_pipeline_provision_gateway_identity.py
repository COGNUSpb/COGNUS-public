import unittest
import os
import sys
import importlib
import automation.pipeline_provision
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from unittest.mock import MagicMock
from automation.pipeline_contract import PipelineRun
from automation.pipeline_state_store import PipelineStateStore
from automation.pipeline_provision import validate_gateway_identity

class DummyResponse:
    def __init__(self, status_code=200, text="OK", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {"Content-Type": "application/json"}

class TestGatewayIdentityValidation(unittest.TestCase):
    def setUp(self):
        self.tempdir = "/tmp/test_gateway_identity"
        self.state_store = PipelineStateStore(self.tempdir)
        self.run = PipelineRun.new(
            blueprint_fingerprint="a" * 64,
            resolved_schema_version="1.0.0",
            change_id="change-1",
        )

    def test_identity_validation_success(self):
        # Patch requests.get
        import automation.pipeline_provision as pp
        pp.requests = MagicMock()
        pp.requests.get.return_value = DummyResponse(200, '{"org":"ORG1","result":"ok"}')
        content = validate_gateway_identity(
            # Standard test setup
            run=self.run,
            state_store=self.state_store,
            host="localhost",
            port=8080,
            org_id="ORG1",
            component_id="gateway1",
        )
        self.assertIn("HTTP 200", content)
        self.assertIn("ORG1", content)

    def test_identity_validation_error(self):
        import automation.pipeline_provision as pp
        pp.requests = MagicMock()
        pp.requests.get.side_effect = Exception("Connection refused")
        content = validate_gateway_identity(
            run=self.run,
            state_store=self.state_store,
            host="localhost",
            port=8080,
            org_id="ORG2",
            component_id="gateway2",
        )
        self.assertIsNone(content)
        # Check artifact written
        art = self.state_store.stage_artifacts_dir(self.run.run_id, "provision") / "gateway-identity-test-response.txt"
        self.assertTrue(art.exists())
        self.assertIn("ERROR", art.read_text())

if __name__ == "__main__":
    unittest.main()
