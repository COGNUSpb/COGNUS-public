import json
import tempfile
import os
import unittest

from automation.pipeline_contract import PipelineRun
from automation.pipeline_provision import (
    _materialize_api_runtime_bootstrap_for_entry,
    _validate_and_populate_gateway_inventory,
)
from automation.pipeline_provision import _api_runtime_bootstrap_host_prep_command
from automation.pipeline_state_store import PipelineStateStore


class FakeSshExecutor:
    def __init__(self, stdout: str = ""):
        self._stdout = stdout

    def execute_unit_with_output(self, **kwargs):
        class C:
            pass

        c = C()
        c.stdout = self._stdout
        c.stderr = ""
        return c


class PipelineProvisionIdentitiesHealthTest(unittest.TestCase):
    def test_materialize_api_runtime_bootstrap_for_entry_creates_identities_and_connection(self):
        entry = {
            "runtime_name": "api-gw",
            "service_context": {"org_id": "org1", "channel_ids": ["mychannel"], "chaincode_ids": ["cc1"]},
        }
        manifest_runtime_contract = {
            "runtime_contract_fingerprint": "f" * 64,
            "correlation": {"manifest_fingerprint": "a" * 64},
            "api_runtime_registry": {
                "manifest_org_id": "org1",
                "orderer_names": ["orderer-main"],
                "organizations": {
                    "org1": {
                        "msp_id": "Org1MSP",
                        "peer_names": ["peer-main"],
                        "scoped_channels": ["mychannel"],
                        "declared_chaincodes": ["cc1"],
                    }
                },
            },
        }

        bootstrap = _materialize_api_runtime_bootstrap_for_entry(
            entry=entry,
            manifest_org_id="org1",
            scoped_channels=set(["mychannel"]),
            declared_chaincodes=set(["cc1"]),
            peer_names_by_org={"org1": ["peer0"]},
            default_peer_name="peer0",
            default_orderer_name="orderer0",
            manifest_runtime_contract=manifest_runtime_contract,
        )

        self.assertIn("identities", bootstrap)
        self.assertIn("connection_profile", bootstrap)
        self.assertIn("paths", bootstrap)
        self.assertEqual(bootstrap["org_id"], "org1")
        self.assertEqual(bootstrap.get("source_contract"), "org-runtime-manifest")
        self.assertEqual(bootstrap.get("manifest_fingerprint"), "a" * 64)
        self.assertEqual(bootstrap.get("peer_name"), "peer-main")
        self.assertEqual(bootstrap.get("orderer_name"), "orderer-main")
        # identities should include org1 and default
        self.assertIn("org1", bootstrap["identities"])
        self.assertIn("default", bootstrap["identities"])

    def test_validate_and_populate_gateway_inventory_writes_artifact_and_no_missing_issue(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            state_store = PipelineStateStore(tmp.name)

            run = PipelineRun.new(
                blueprint_fingerprint=("a" * 64),
                resolved_schema_version="1.0",
                change_id="test-change",
            )

            # create a node with api_gateway component
            nodes_by_host = {
                "host1": [
                    {"component_type": "api_gateway", "component_id": "api-gateway-1", "name": "api-gateway-1"}
                ]
            }

            # fake ssh executor returns a matching docker ps line with published port
            stdout = "api-gateway-1||cognus/ccapi:latest||0.0.0.0:443->443/tcp\n"
            ssh_exec = FakeSshExecutor(stdout=stdout)

            checkpoints = []
            issues = _validate_and_populate_gateway_inventory(
                run=run,
                nodes_by_host=nodes_by_host,
                state_store=state_store,
                ssh_executor=ssh_exec,
                checkpoints=checkpoints,
            )

            # expect no missing-container issue
            codes = [i.code for i in issues]
            self.assertNotIn("prepare_gateway_container_missing", codes)

            # artifact should exist
            self.assertTrue(state_store.artifact_exists(run.run_id, "prepare", "gateway-inventory.json"))

        finally:
            tmp.cleanup()

    def test_api_runtime_bootstrap_host_prep_command_includes_wallet_creation(self):
        template = {
            "storage_mount": {"host_path": "/opt/cognus/api-runtime", "container_path": "/app/data"},
        }
        bootstrap = {
            "org_id": "org1",
            "connection_profile": {},
            "identities": {"org1": {}},
        }
        cmd = _api_runtime_bootstrap_host_prep_command(template=template, bootstrap=bootstrap)
        # expect the command to create the wallet directory on host and set permissions
        self.assertIn("mkdir -p /opt/cognus/api-runtime/wallet", cmd)
        self.assertIn("chmod 700 /opt/cognus/api-runtime/wallet", cmd)
        self.assertIn("test -w /opt/cognus/api-runtime/wallet", cmd)
        self.assertIn('"mount_expected":"/app/data"', cmd)
        # expect the command to attempt to validate connection.json on host (python/jq)
        self.assertTrue(
            any(token in cmd for token in ("python3 - <<'PY'", "python - <<'PY'", "jq -e .")),
            msg="host-prep must include a connection.json validation step",
        )
        self.assertNotIn("wallet-runtime.txt || true", cmd)
        self.assertIn("grep -q", cmd)
        self.assertIn("wallet_probe_test", cmd)
        self.assertIn("gateway-index.js", cmd)

    def test_materialize_api_runtime_bootstrap_uses_app_data_paths(self):
        entry = {
            "runtime_name": "api-gw",
            "service_context": {"org_id": "org1", "channel_ids": ["mychannel"], "chaincode_ids": ["cc1"]},
        }
        manifest_runtime_contract = {
            "runtime_contract_fingerprint": "f" * 64,
            "correlation": {"manifest_fingerprint": "a" * 64},
            "api_runtime_registry": {
                "manifest_org_id": "org1",
                "orderer_names": ["orderer-main"],
                "organizations": {
                    "org1": {
                        "msp_id": "Org1MSP",
                        "peer_names": ["peer-main"],
                        "scoped_channels": ["mychannel"],
                        "declared_chaincodes": ["cc1"],
                    }
                },
            },
        }

        bootstrap = _materialize_api_runtime_bootstrap_for_entry(
            entry=entry,
            manifest_org_id="org1",
            scoped_channels=set(["mychannel"]),
            declared_chaincodes=set(["cc1"]),
            peer_names_by_org={"org1": ["peer0"]},
            default_peer_name="peer0",
            default_orderer_name="orderer0",
            manifest_runtime_contract=manifest_runtime_contract,
        )

        self.assertEqual(bootstrap.get("runtime_dir"), "/app/data")
        self.assertEqual(bootstrap.get("paths", {}).get("connection_json"), "/app/data/connection.json")
        self.assertEqual(bootstrap.get("paths", {}).get("identities_json"), "/app/data/identities.json")


if __name__ == "__main__":
    unittest.main()
