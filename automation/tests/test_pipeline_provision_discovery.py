import unittest

from automation.pipeline_provision import (
    _materialize_api_runtime_bootstrap_for_entry,
    _discovery_runtime_check_command,
    _validate_api_runtime_ccp_contract,
)


class PipelineProvisionDiscoveryTest(unittest.TestCase):
    def test_materialize_respects_service_context_discovery_flag(self):
        entry = {
            "runtime_name": "api-gw",
            "service_context": {"org_id": "org1", "discoveryAsLocalhost": True},
        }
        manifest_runtime_contract = {
            "api_runtime_registry": {
                "manifest_org_id": "org1",
                "orderer_names": ["orderer-main"],
                "organizations": {
                    "org1": {
                        "msp_id": "Org1MSP",
                        "peer_names": ["peer-main"],
                        "scoped_channels": ["channel-main"],
                        "declared_chaincodes": ["cc-main"],
                    }
                },
            }
        }
        bootstrap = _materialize_api_runtime_bootstrap_for_entry(
            entry=entry,
            manifest_org_id="org1",
            scoped_channels=set(),
            declared_chaincodes=set(),
            peer_names_by_org={"org1": ["peer0"]},
            default_peer_name="peer0",
            default_orderer_name="orderer0",
            manifest_runtime_contract=manifest_runtime_contract,
        )
        self.assertIn("identities", bootstrap)
        self.assertTrue(bootstrap["identities"]["default"].get("discoveryAsLocalhost", False))
        self.assertEqual(bootstrap.get("peer_name"), "peer-main")
        self.assertEqual(bootstrap.get("orderer_name"), "orderer-main")

    def test_materialize_identities_multiorg_requires_explicit_org_selection(self):
        entry = {
            "runtime_name": "api-gw",
            "service_context": {"org_id": "org1", "discoveryAsLocalhost": False},
        }
        manifest_runtime_contract = {
            "api_runtime_registry": {
                "manifest_org_id": "org1",
                "orderer_names": ["orderer-main"],
                "organizations": {
                    "org1": {
                        "msp_id": "Org1MSP",
                        "peer_names": ["peer-org1"],
                        "scoped_channels": ["channel-a"],
                        "declared_chaincodes": ["cc-a"],
                    },
                    "org2": {
                        "msp_id": "Org2MSP",
                        "peer_names": ["peer-org2"],
                        "scoped_channels": ["channel-b"],
                        "declared_chaincodes": ["cc-b"],
                    },
                },
            }
        }
        bootstrap = _materialize_api_runtime_bootstrap_for_entry(
            entry=entry,
            manifest_org_id="org1",
            scoped_channels=set(),
            declared_chaincodes=set(),
            peer_names_by_org={"org1": ["peer-org1"], "org2": ["peer-org2"]},
            default_peer_name="peer0",
            default_orderer_name="orderer0",
            manifest_runtime_contract=manifest_runtime_contract,
        )

        identities = bootstrap.get("identities", {})
        organizations = identities.get("organizations", {})
        self.assertIn("org1", organizations)
        self.assertIn("org2", organizations)
        self.assertNotIn("defaultOrg", identities)
        self.assertNotIn("default", identities)
        self.assertEqual(
            identities.get("selectionContract", {}).get("queryParam"), "org"
        )
        self.assertEqual(
            identities.get("selectionContract", {}).get("header"), "x-fabric-org"
        )
        required_fields = {
            "mspId",
            "certPath",
            "keyPath",
            "ccpPath",
            "discoveryAsLocalhost",
        }
        for org_entry in organizations.values():
            self.assertTrue(required_fields.issubset(set(org_entry.keys())))

    def test_materialize_identities_single_org_sets_safe_default_org(self):
        entry = {
            "runtime_name": "api-gw",
            "service_context": {"org_id": "org1", "discoveryAsLocalhost": False},
        }
        manifest_runtime_contract = {
            "api_runtime_registry": {
                "manifest_org_id": "org1",
                "orderer_names": ["orderer-main"],
                "organizations": {
                    "org1": {
                        "msp_id": "Org1MSP",
                        "peer_names": ["peer-org1"],
                        "scoped_channels": ["channel-a"],
                        "declared_chaincodes": ["cc-a"],
                    }
                },
            }
        }
        bootstrap = _materialize_api_runtime_bootstrap_for_entry(
            entry=entry,
            manifest_org_id="org1",
            scoped_channels=set(),
            declared_chaincodes=set(),
            peer_names_by_org={"org1": ["peer-org1"]},
            default_peer_name="peer0",
            default_orderer_name="orderer0",
            manifest_runtime_contract=manifest_runtime_contract,
        )

        identities = bootstrap.get("identities", {})
        self.assertEqual(identities.get("defaultOrg"), "org1")
        self.assertIn("default", identities)
        self.assertIn("org1", identities.get("organizations", {}))

    def test_discovery_command_contains_python_and_path(self):
        cmd = _discovery_runtime_check_command("/var/cognus/api-runtime/connection.json")
        self.assertIn("python", cmd)
        self.assertIn("/var/cognus/api-runtime/connection.json", cmd)

    def test_ccp_contract_invalid_channel_and_tls_emit_codes(self):
        bootstrap = {
            "connection_profile": {
                "channels": {},
                "peers": {
                    "peer0.org1.example.com": {
                        "url": "grpc://peer0.org1.example.com:7051",
                        "tlsCACerts": {},
                    }
                },
                "orderers": {},
            },
            "identities": {
                "organizations": {
                    "org1": {
                        "discoveryAsLocalhost": True,
                    }
                }
            },
        }
        entry = {
            "service_context": {
                "channel_ids": ["mychannel"],
            }
        }
        issues = []

        _validate_api_runtime_ccp_contract(
            bootstrap_payload=bootstrap,
            entry=entry,
            environment_profile_ref="external-linux",
            path_prefix="provision_execution_plan.entries[0].api_runtime_bootstrap",
            issues=issues,
        )

        codes = {getattr(issue, "code", "") for issue in issues}
        self.assertIn("ccp_invalid", codes)
        self.assertIn("ccp_unreachable", codes)
        self.assertIn("ccp_tls_mismatch", codes)

    def test_ccp_discovery_access_denied_sets_fallback(self):
        bootstrap = {
            "connection_profile": {
                "channels": {"mychannel": {}},
                "peers": {
                    "peer0.org1.example.com": {
                        "url": "grpcs://peer0.org1.example.com:7051",
                        "tlsCACerts": {"path": "/tmp/tls.pem"},
                    }
                },
                "orderers": {
                    "orderer.example.com": {
                        "url": "grpcs://orderer.example.com:7050",
                        "tlsCACerts": {"path": "/tmp/orderer.pem"},
                    }
                },
            },
            "identities": {"organizations": {"org1": {"discoveryAsLocalhost": False}}},
        }
        entry = {"service_context": {"channel_ids": ["mychannel"], "discovery_error": "access denied by ACL"}}
        issues = []

        _validate_api_runtime_ccp_contract(
            bootstrap_payload=bootstrap,
            entry=entry,
            environment_profile_ref="external-linux",
            path_prefix="provision_execution_plan.entries[0].api_runtime_bootstrap",
            issues=issues,
        )

        codes = {getattr(issue, "code", "") for issue in issues}
        self.assertIn("ccp_discovery_access_denied_fallback", codes)
        resolution = bootstrap.get("discovery_resolution", {})
        self.assertEqual(resolution.get("mode"), "static_fallback")


if __name__ == "__main__":
    unittest.main()
