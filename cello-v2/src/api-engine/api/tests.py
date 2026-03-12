#
# SPDX-License-Identifier: Apache-2.0
#
import base64
import io
from pathlib import Path
import tarfile
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from api.routes.runbook import views as runbook_views


class RunbookApiContractTests(APITestCase):
	def setUp(self):
		user_model = get_user_model()
		self.user = user_model.objects.create_user(
			username="runbook-test-user",
			password="runbook-test-password",
		)
		self.user.role = "admin"
		self.user.save(update_fields=["role"])
		self.client.force_authenticate(user=self.user)

		self.tempdir = TemporaryDirectory()
		self.original_store_file = runbook_views.RUNBOOK_STORE_FILE
		self.original_legacy_store_file = runbook_views.RUNBOOK_LEGACY_STORE_FILE
		runbook_views.RUNBOOK_STORE_FILE = Path(self.tempdir.name) / "runbook_store.json"
		runbook_views.RUNBOOK_LEGACY_STORE_FILE = (
			Path(self.tempdir.name) / "legacy_runbook_store.json"
		)
		self.original_enabled_node_types = runbook_views.RUNBOOK_ENABLED_NODE_TYPES
		runbook_views.RUNBOOK_ENABLED_NODE_TYPES = tuple(
			runbook_views.RUNBOOK_RUNTIME_IMAGE_BY_NODE_TYPE.keys()
		)
		self.default_chaincode_artifact = Path(self.tempdir.name) / "stcs-cc_1.0.tar.gz"
		self._write_chaincode_package(self.default_chaincode_artifact, "stcs-cc_1.0")
		self.ssh_commands = []
		self.mock_ssh_returncode = 0
		self.mock_ssh_stdout = "Linux\n"
		self.mock_ssh_stderr = ""
		self.ssh_patcher = patch.object(
			runbook_views.subprocess,
			"run",
			side_effect=self._mock_ssh_run,
		)
		self.ssh_patcher.start()

	def tearDown(self):
		self.ssh_patcher.stop()
		runbook_views.RUNBOOK_STORE_FILE = self.original_store_file
		runbook_views.RUNBOOK_LEGACY_STORE_FILE = self.original_legacy_store_file
		runbook_views.RUNBOOK_ENABLED_NODE_TYPES = self.original_enabled_node_types
		self.tempdir.cleanup()
		super().tearDown()

	def _mock_ssh_run(self, command, **kwargs):
		self.ssh_commands.append({"command": command, "kwargs": kwargs})
		return SimpleNamespace(
			returncode=self.mock_ssh_returncode,
			stdout=self.mock_ssh_stdout,
			stderr=self.mock_ssh_stderr,
		)

	def _write_chaincode_package(self, archive_path, label):
		archive_path = Path(archive_path)
		archive_path.parent.mkdir(parents=True, exist_ok=True)
		metadata_payload = (
			'{"path":"github.com/hyperledger-labs/cc-tools-demo/chaincode",'
			f'"type":"golang","label":"{label}"}}'
		).encode("utf-8")
		code_buffer = io.BytesIO()
		with tarfile.open(fileobj=code_buffer, mode="w:gz"):
			pass
		code_payload = code_buffer.getvalue()

		with tarfile.open(archive_path, "w:gz") as package:
			metadata_info = tarfile.TarInfo("metadata.json")
			metadata_info.size = len(metadata_payload)
			package.addfile(metadata_info, io.BytesIO(metadata_payload))

			code_info = tarfile.TarInfo("code.tar.gz")
			code_info.size = len(code_payload)
			package.addfile(code_info, io.BytesIO(code_payload))

	def _start_payload(self, **overrides):
		payload = {
			"change_id": "cr-a1-7-2-001",
			"provider_key": "external-linux",
			"environment_profile": "dev-external-linux",
			"blueprint_version": "1.0.0",
			"blueprint_fingerprint": "ab" * 32,
			"manifest_fingerprint": "cd" * 32,
			"source_blueprint_fingerprint": "ef" * 32,
			"resolved_schema_version": "1.0.0",
			"pipeline_preconditions_ready": True,
			"blueprint_validated": True,
			"preflight_approved": True,
			"a2_2_minimum_artifacts": [
				"provision-plan",
				"reconcile-report",
				"inventory-final",
				"stage-reports",
				"verify-report",
				"ssh-execution-log",
			],
			"a2_2_available_artifacts": [
				"provision-plan",
				"reconcile-report",
				"inventory-final",
				"stage-reports/prepare-report",
				"verify-report",
				"ssh-execution-log",
			],
			"host_mapping": [
				{
					"node_id": "peer0.org1",
					"node_type": "peer",
					"org_id": "org1",
					"host_ref": "host-org1-peer0",
					"host_address": "10.0.0.10",
					"ssh_user": "ubuntu",
					"ssh_port": 22,
					"preflight_status": "apto",
				}
			],
			"machine_credentials": [
				{
					"machine_id": "host-org1-peer0",
					"credential_ref": "vault://infra/keys/host-org1-peer0",
					"credential_fingerprint": "fp-host-org1-peer0",
					"reuse_confirmed": False,
				}
			],
			"api_registry": [
				{
					"api_id": "api-org1",
					"org_name": "Org1",
					"channel_id": "stcs-channel",
					"chaincode_id": "stcs-cc",
					"route_path": "/org1/stcs",
				}
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"org_name": "Org1",
						"channels": ["stcs-channel"],
						"chaincodes": ["stcs-cc"],
					}
				],
				"channels": [
					{
						"channel_id": "stcs-channel",
						"member_orgs": ["Org1"],
						"chaincodes": ["stcs-cc"],
					}
				],
				"chaincodes": [
					{
						"channel_id": "stcs-channel",
						"chaincode_id": "stcs-cc",
						"artifact_ref": f"local-file:{self.default_chaincode_artifact}",
					}
				],
			},
			"handoff_contract_version": "a2.frontend-handoff.v1",
			"handoff_fingerprint": "f1" * 32,
			"handoff_payload": {
				"correlation": {
					"change_id": "cr-a1-7-2-001",
					"run_id": "run-a1-7-2-001",
					"manifest_fingerprint": "cd" * 32,
					"source_blueprint_fingerprint": "ef" * 32,
				}
			},
			"handoff_trace": [
				{
					"event": "handoff_created",
					"timestamp_utc": "2026-02-20T10:00:00Z",
					"change_id": "cr-a1-7-2-001",
					"run_id": "run-a1-7-2-001",
				}
			],
			"runbook_resume_context": {
				"resume_key": "cr-a1-7-2-001|run-a1-7-2-001",
			},
		}
		payload.update(overrides)
		return payload

	def test_start_is_idempotent_for_same_execution_context(self):
		endpoint = reverse("runbook-start")

		first_response = self.client.post(endpoint, self._start_payload(), format="json")
		self.assertEqual(first_response.status_code, status.HTTP_201_CREATED)

		second_response = self.client.post(endpoint, self._start_payload(), format="json")
		self.assertEqual(second_response.status_code, status.HTTP_200_OK)

		first_run = first_response.data["data"]["run"]
		second_run = second_response.data["data"]["run"]
		self.assertEqual(first_run["run_id"], second_run["run_id"])
		self.assertEqual(first_run["manifest_fingerprint"], "cd" * 32)
		self.assertEqual(first_run["source_blueprint_fingerprint"], "ef" * 32)
		self.assertEqual(first_run["handoff_contract_version"], "a2.frontend-handoff.v1")
		self.assertEqual(first_run["handoff_fingerprint"], "f1" * 32)
		self.assertEqual(first_run["a2_2_minimum_artifacts"][0], "provision-plan")
		self.assertEqual(first_run["topology_catalog"]["organizations"][0]["org_id"], "org1")
		self.assertEqual(second_run["source_mode"], "official")
		self.assertEqual(
			second_response.data["data"]["snapshot"]["contract_version"],
			"a1.7-runbook-api.v1",
		)

	def test_start_rejects_conflicting_run_id_with_different_context(self):
		endpoint = reverse("runbook-start")
		run_id = "run-a1-7-2-conflict"

		first_response = self.client.post(
			endpoint,
			self._start_payload(run_id=run_id, blueprint_fingerprint="cd" * 32),
			format="json",
		)
		self.assertEqual(first_response.status_code, status.HTTP_201_CREATED)

		conflict_response = self.client.post(
			endpoint,
			self._start_payload(run_id=run_id, blueprint_fingerprint="ef" * 32),
			format="json",
		)
		self.assertEqual(conflict_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(conflict_response.data["msg"], "runbook_run_id_conflict")
		self.assertEqual(conflict_response.data["data"]["source_mode"], "official")

	def test_start_persists_actor_scope_for_operator_without_organization(self):
		user_model = get_user_model()
		operator = user_model.objects.create_user(
			username="operador.runbook",
			email="operador.runbook@example.org",
			password="runbook-test-password",
		)
		operator.role = "operator"
		operator.organization = None
		operator.save(update_fields=["role", "organization"])
		self.client.force_authenticate(user=operator)

		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(run_id="run-operator-scope"),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_201_CREATED)
		run_state = response.data["data"]["run"]
		self.assertEqual(run_state["actor_scope"]["username"], "operador.runbook")
		self.assertEqual(
			run_state["actor_scope"]["email"], "operador.runbook@example.org"
		)
		self.assertEqual(run_state["actor_scope"]["role"], "operator")
		self.assertEqual(run_state["actor_scope"]["organization_name"], "")

	def test_operator_without_organization_can_advance_owned_legacy_run(self):
		user_model = get_user_model()
		operator = user_model.objects.create_user(
			username="operador.legacy",
			email="operador.legacy@example.org",
			password="runbook-test-password",
		)
		operator.role = "operator"
		operator.organization = None
		operator.save(update_fields=["role", "organization"])
		self.client.force_authenticate(user=operator)

		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(run_id="run-operator-legacy"),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		store_payload["runs"][run_id].pop("actor_scope", None)
		runbook_views._save_store(store_payload)

		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_200_OK)
		self.assertEqual(operate_response.data["status"], "successful")

	def test_operator_without_organization_cannot_advance_foreign_run(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(run_id="run-admin-owned"),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		user_model = get_user_model()
		operator = user_model.objects.create_user(
			username="operador.foreign",
			email="operador.foreign@example.org",
			password="runbook-test-password",
		)
		operator.role = "operator"
		operator.organization = None
		operator.save(update_fields=["role", "organization"])
		self.client.force_authenticate(user=operator)

		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_403_FORBIDDEN)
		self.assertEqual(operate_response.data["msg"], "runbook_scope_forbidden")

	def test_start_rejects_chaincode_without_explicit_binding(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={
					"organizations": [
						{
							"org_id": "org1",
							"org_name": "Org1",
							"channels": ["stcs-channel"],
							"chaincodes": ["stcs-cc"],
						}
					],
					"channels": [
						{
							"channel_id": "stcs-channel",
							"member_orgs": ["Org1"],
							"chaincodes": ["stcs-cc"],
						}
					],
					"chaincodes": [
						{
							"channel_id": "stcs-channel",
							"chaincode_id": "stcs-cc",
						}
					],
				},
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "runbook_chaincode_binding_invalid")
		self.assertEqual(
			response.data["data"]["details"]["issues"][0]["reason"],
			"binding_missing",
		)

	def test_start_rejects_chaincode_with_invalid_relative_binding(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={
					"organizations": [
						{
							"org_id": "org1",
							"org_name": "Org1",
							"channels": ["stcs-channel"],
							"chaincodes": ["stcs-cc"],
						}
					],
					"channels": [
						{
							"channel_id": "stcs-channel",
							"member_orgs": ["Org1"],
							"chaincodes": ["stcs-cc"],
						}
					],
					"chaincodes": [
						{
							"channel_id": "stcs-channel",
							"chaincode_id": "stcs-cc",
							"source_ref": "relative/path/to/chaincode",
						}
					],
				},
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "runbook_chaincode_binding_invalid")
		self.assertEqual(
			response.data["data"]["details"]["issues"][0]["reason"],
			"binding_invalid",
		)

	def test_operate_rejects_invalid_transition_with_structured_error(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		run_id = start_response.data["data"]["run"]["run_id"]

		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "resume"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(operate_response.data["msg"], "runbook_invalid_transition")
		self.assertEqual(operate_response.data["data"]["source_mode"], "official")
		self.assertEqual(operate_response.data["data"]["details"]["action"], "resume")
		self.assertEqual(operate_response.data["data"]["details"]["run_id"], run_id)

	def test_preflight_runs_dynamic_ssh_probe_and_returns_apto(self):
		response = self.client.post(
			reverse("runbook-preflight"),
			{
				"change_id": "cr-a1-7-2-001",
				"provider_key": "external-linux",
				"host_mapping": [
					{
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
						"ssh_user": "web3",
						"ssh_port": 21525,
					}
				],
				"machine_credentials": [
					{
						"machine_id": "machine1",
						"credential_ref": "vault://infra/keys/machine1",
						"credential_fingerprint": "fp-machine1",
						"reuse_confirmed": False,
					}
				],
			},
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_200_OK)
		preflight = response.data["data"]["preflight"]
		self.assertEqual(preflight["overall_status"], "apto")
		self.assertEqual(preflight["summary"]["apto"], 1)
		self.assertEqual(preflight["summary"]["bloqueado"], 0)
		self.assertEqual(preflight["hosts"][0]["status"], "apto")
		self.assertEqual(preflight["hosts"][0]["diagnostics"]["exit_code"], 0)
		self.assertGreaterEqual(len(self.ssh_commands), 1)
		self.assertIn("web3@10.0.0.10", self.ssh_commands[0]["command"])

	def test_preflight_marks_host_bloqueado_when_ssh_fails(self):
		self.mock_ssh_returncode = 255
		self.mock_ssh_stdout = ""
		self.mock_ssh_stderr = "Permission denied (publickey)."

		response = self.client.post(
			reverse("runbook-preflight"),
			{
				"change_id": "cr-a1-7-2-001",
				"provider_key": "external-linux",
				"host_mapping": [
					{
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
						"ssh_user": "web3",
						"ssh_port": 21525,
					}
				],
				"machine_credentials": [
					{
						"machine_id": "machine1",
						"credential_ref": "vault://infra/keys/machine1",
						"credential_fingerprint": "fp-machine1",
						"reuse_confirmed": False,
					}
				],
			},
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_200_OK)
		preflight = response.data["data"]["preflight"]
		self.assertEqual(preflight["overall_status"], "bloqueado")
		self.assertEqual(preflight["summary"]["bloqueado"], 1)
		self.assertEqual(preflight["hosts"][0]["status"], "bloqueado")
		self.assertEqual(preflight["hosts"][0]["diagnostics"]["exit_code"], 255)
		self.assertIn("publickey", preflight["hosts"][0]["diagnostics"]["stderr"])

	def test_preflight_forces_uploaded_identity_file_when_local_file_reference_is_used(self):
		fake_pem = (
			"-----BEGIN OPENSSH PRIVATE KEY-----\n"
			"c3NoLWtleS1wYXlsb2FkLXRlc3Q=\n"
			"-----END OPENSSH PRIVATE KEY-----\n"
		)
		fake_pem_base64 = base64.b64encode(fake_pem.encode("utf-8")).decode("ascii")

		response = self.client.post(
			reverse("runbook-preflight"),
			{
				"change_id": "cr-a1-7-2-001",
				"provider_key": "external-linux",
				"host_mapping": [
					{
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
						"ssh_user": "web3",
						"ssh_port": 21525,
					}
				],
				"machine_credentials": [
					{
						"machine_id": "machine1",
						"credential_ref": "local-file:web3-livre.pem",
						"credential_payload": fake_pem_base64,
						"credential_fingerprint": "fp-machine1",
						"reuse_confirmed": False,
					}
				],
			},
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_200_OK)
		self.assertGreaterEqual(len(self.ssh_commands), 1)
		ssh_command = self.ssh_commands[0]["command"]
		self.assertIn("-i", ssh_command)
		identity_index = ssh_command.index("-i") + 1
		identity_file = Path(ssh_command[identity_index])
		self.assertTrue(identity_file.exists())
		self.assertEqual(identity_file.read_text(encoding="utf-8"), fake_pem)
		self.assertIn("IdentitiesOnly=yes", ssh_command)
		self.assertIn("PreferredAuthentications=publickey", ssh_command)
		self.assertIn("PasswordAuthentication=no", ssh_command)
		self.assertIn("KbdInteractiveAuthentication=no", ssh_command)

	def test_start_rejects_reused_credential_without_explicit_confirmation(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine-a",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
					{
						"node_id": "peer1.org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine-b",
						"host_address": "10.0.0.11",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
				],
				machine_credentials=[
					{
						"machine_id": "machine-a",
						"credential_ref": "vault://infra/keys/shared",
						"reuse_confirmed": False,
					},
					{
						"machine_id": "machine-b",
						"credential_ref": "vault://infra/keys/shared",
						"reuse_confirmed": True,
					},
				],
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(
			response.data["msg"],
			"runbook_machine_credentials_reuse_not_confirmed",
		)

	def test_advance_uses_real_docker_provisioning_command_in_runtime_checkpoint(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(4):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		provision_runtime_commands = [
			row["command"][-1]
			for row in self.ssh_commands
			if isinstance(row, dict)
			and isinstance(row.get("command"), list)
			and row["command"]
			and "COGNUS_PROVISION_RUNTIME_OK" in str(row["command"][-1])
		]

		self.assertGreaterEqual(len(provision_runtime_commands), 1)
		self.assertIn("docker run -d --name", provision_runtime_commands[0])
		self.assertIn("docker image inspect", provision_runtime_commands[0])
		self.assertIn("docker pull", provision_runtime_commands[0])
		self.assertIn("hyperledger/fabric-peer:2.5", provision_runtime_commands[0])
		self.assertNotIn("alpine:3.20", provision_runtime_commands[0])
		self.assertNotIn("while true; do sleep 3600; done", provision_runtime_commands[0])

	def test_start_rejects_forbidden_runtime_image_registry_prefix(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
							"runtime_image": "goledger/chaincode-gateway:latest",
					}
				],
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "runbook_runtime_image_catalog_violation")
		self.assertEqual(
			response.data["data"]["details"]["violations"][0]["reason"],
			"forbidden_runtime_registry_prefix",
		)

	def test_start_filters_disabled_runtime_components_from_host_mapping(self):
		with patch.object(
			runbook_views,
			"RUNBOOK_ENABLED_NODE_TYPES",
			("peer", "orderer", "ca"),
		):
			response = self.client.post(
				reverse("runbook-start"),
				self._start_payload(
					host_mapping=[
						{
							"node_id": "peer0.org1",
							"node_type": "peer",
							"org_id": "org1",
							"host_ref": "host-org1-peer0",
							"host_address": "10.0.0.10",
							"ssh_user": "ubuntu",
							"ssh_port": 22,
							"preflight_status": "apto",
						},
						{
							"node_id": "apigateway-org1",
							"node_type": "apigateway",
							"org_id": "org1",
							"host_ref": "host-org1-peer0",
							"host_address": "10.0.0.10",
							"ssh_user": "ubuntu",
							"ssh_port": 22,
							"preflight_status": "apto",
								"runtime_image": "cognus/chaincode-gateway:latest",
						},
					],
					topology_catalog={
						"organizations": [
							{
								"org_id": "org1",
								"org_name": "Org1",
								"peers": [
									{
										"node_id": "peer0.org1",
										"host_ref": "host-org1-peer0",
									}
								],
								"service_host_mapping": {
									"apiGateway": "host-org1-peer0",
								},
							}
						]
					},
				),
				format="json",
			)

		self.assertEqual(response.status_code, status.HTTP_201_CREATED)
		run_host_mapping = response.data["data"]["run"]["host_mapping"]
		self.assertEqual(len(run_host_mapping), 1)
		self.assertEqual(run_host_mapping[0]["node_type"], "peer")

	def test_start_fails_when_no_enabled_runtime_components_remain(self):
		with patch.object(runbook_views, "RUNBOOK_ENABLED_NODE_TYPES", ("peer",)):
			response = self.client.post(
				reverse("runbook-start"),
				self._start_payload(
					host_mapping=[
						{
							"node_id": "apigateway-org1",
							"node_type": "apigateway",
							"org_id": "org1",
							"host_ref": "host-org1-peer0",
							"host_address": "10.0.0.10",
							"ssh_user": "ubuntu",
							"ssh_port": 22,
							"preflight_status": "apto",
								"runtime_image": "cognus/chaincode-gateway:latest",
						}
					],
					topology_catalog={},
				),
				format="json",
			)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(
			response.data["msg"],
			"runbook_host_mapping_no_enabled_components",
		)

	def test_advance_bootstraps_apigateway_image_with_local_cognus_alias_and_pull_fallback(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
							"runtime_image": "cognus/chaincode-gateway:latest",
					}
				],
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(4):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		provision_runtime_commands = [
			row["command"][-1]
			for row in self.ssh_commands
			if isinstance(row, dict)
			and isinstance(row.get("command"), list)
			and row["command"]
			and "COGNUS_PROVISION_RUNTIME_OK" in str(row["command"][-1])
		]
		self.assertGreaterEqual(len(provision_runtime_commands), 1)
		self.assertIn("docker tag", provision_runtime_commands[0])
		self.assertIn("chaincode-gateway:latest", provision_runtime_commands[0])
		self.assertIn("cognus/chaincode-gateway:latest", provision_runtime_commands[0])
		self.assertNotIn("goledger/chaincode-gateway:latest", provision_runtime_commands[0])
		self.assertIn("docker pull", provision_runtime_commands[0])

	def test_runtime_image_bootstrap_command_only_fails_when_alias_and_pull_fail(self):
		command = runbook_views._build_runtime_image_bootstrap_command(
			"ca",
			"hyperledger/fabric-ca:1.5",
		)

		self.assertIn("if ! docker image inspect", command)
		self.assertIn("|| { printf '%s\\n'", command)
		self.assertIn("exit 125; };", command)
		self.assertNotIn("|| printf '%s\\n'", command)

	def test_provision_runtime_command_applies_resource_guards_for_chaincode(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-resource-guard",
				"change_id": "cr-test-resource-guard",
			},
			{
				"node_id": "chaincode-org1",
				"node_type": "chaincode",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "hyperledger/fabric-ccenv:2.5",
			},
		)

		self.assertIn("docker run -d --name", command)
		self.assertIn("--cpus 0.25", command)
		self.assertIn("--memory 256m", command)
		self.assertIn("--pids-limit 128", command)
		self.assertIn("--log-opt max-size=25m", command)
		self.assertIn("--log-opt max-file=3", command)
		self.assertIn("sleep 2;", command)
		self.assertIn("COGNUS_RUNTIME_MEMORY_LOW:chaincode:512:", command)
		self.assertIn("hyperledger/fabric-ccenv:2.5 tail -f /dev/null", command)

	def test_provision_runtime_command_publishes_apigateway_port_from_topology(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-apigw-port",
				"change_id": "cr-test-apigw-port",
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"service_parameters": {
								"apiGateway": {
									"port": 31522,
								}
							},
						}
					]
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("--publish 31522:8085", command)

	def test_provision_runtime_command_apigateway_enforces_app_data_mount_and_identity_bootstrap(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-apigw-bootstrap",
				"change_id": "cr-test-apigw-bootstrap",
				"host_mapping": [
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"msp_id": "Org1MSP",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
							"apis": [
								{
									"channel_id": "library-channel",
									"chaincode_id": "reserve-book-cc",
								}
							],
						},
					]
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("--volume /tmp/cognus/runtime/apigateway/apigateway-org1:/app/data", command)
		self.assertIn("--env IDENTITY_CONFIG=/app/data/identities.json", command)
		self.assertIn("--env CCP_PATH=/app/data/connection.json", command)
		self.assertIn("--env WALLET_PATH=/app/data/wallet", command)
		self.assertIn("/app/data/identities.json", command)
		self.assertIn("ImRpc2NvdmVyeUVuYWJsZWQiOmZhbHNl", command)
		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_cert", command)
		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_key", command)
		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:missing_tlsca", command)
		self.assertIn("orderer-tlsca.pem", command)
		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:orderer_tlsca_missing", command)
		self.assertIn("APIGW_ORG_CRYPTO_DIR=/var/cognus/crypto/org1", command)
		self.assertIn("APIGW_DECLARED_MSP_ID=Org1MSP", command)
		self.assertIn("APIGW_EFFECTIVE_MSP_ID=\"$APIGW_DECLARED_MSP_ID\"", command)
		self.assertIn(
			"if [ -f \"$APIGW_MSP_DIR/.cognus-sample-msp\" ] || [ -s \"$APIGW_MSP_DIR/signcerts/peer.pem\" ]; then APIGW_EFFECTIVE_MSP_ID=SampleOrg; fi;",
			command,
		)
		self.assertIn(
			"sed -i \"s#\\\"mspId\\\":\\\"$APIGW_DECLARED_MSP_ID\\\"#\\\"mspId\\\":\\\"$APIGW_EFFECTIVE_MSP_ID\\\"#g\" /tmp/cognus/runtime/apigateway/apigateway-org1/identities.json",
			command,
		)
		self.assertIn(
			"sed -i \"s#\\\"mspid\\\":\\\"$APIGW_DECLARED_MSP_ID\\\"#\\\"mspid\\\":\\\"$APIGW_EFFECTIVE_MSP_ID\\\"#g\" /tmp/cognus/runtime/apigateway/apigateway-org1/connection.json",
			command,
		)
		self.assertIn(
			"if [ ! -s \"$APIGW_MSP_DIR/signcerts/cert.pem\" ] || [ ! -s \"$APIGW_MSP_DIR/keystore/key.pem\" ] || [ ! -s \"$APIGW_MSP_DIR/tlscacerts/tlsca-cert.pem\" ]; then",
			command,
		)
		self.assertIn("find \"$APIGW_ORG_CRYPTO_DIR\" -type f", command)
		self.assertIn("find \"$APIGW_CRYPTO_ROOT\" -type f -readable", command)
		self.assertIn("sudo -n find \"$APIGW_CRYPTO_ROOT\"", command)
		self.assertIn("docker run --rm --pull never -v \"$APIGW_CRYPTO_ROOT\":/hostcrypto:ro alpine:3.20", command)
		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID", command)
		self.assertIn("gateway-index.js", command)
		self.assertIn("docker cp", command)
		self.assertIn(":/app/index.js", command)

	def test_runtime_host_warmup_scope_limits_to_current_image_and_shared_gateway_image(self):
		host_rows = [
			{
				"node_id": "ca-org1",
				"node_type": "ca",
				"runtime_image": "hyperledger/fabric-ca:1.5",
			},
			{
				"node_id": "peer0-org1",
				"node_type": "peer",
				"runtime_image": "hyperledger/fabric-peer:2.5",
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
			{
				"node_id": "netapi-org1",
				"node_type": "netapi",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		]

		selected_for_ca = runbook_views._select_runtime_host_rows_for_warmup(
			host_rows,
			host_rows[0],
		)
		selected_for_apigw = runbook_views._select_runtime_host_rows_for_warmup(
			host_rows,
			host_rows[2],
		)

		self.assertEqual([row["node_type"] for row in selected_for_ca], ["ca"])
		self.assertEqual(
			sorted(row["node_type"] for row in selected_for_apigw),
			["apigateway", "netapi"],
		)

	@patch.object(runbook_views, "_pull_runtime_image_via_ssh")
	@patch.object(runbook_views, "_probe_remote_runtime_image")
	@patch.object(runbook_views, "_seed_runtime_image_via_ssh")
	@patch.object(runbook_views, "_ensure_local_runtime_image_available")
	def test_runtime_host_warmup_prefers_local_seed_for_gateway_images(
		self,
		mock_local_image,
		mock_seed_image,
		mock_probe_image,
		mock_pull_image,
	):
		mock_local_image.return_value = {"status": "ready"}
		mock_seed_image.return_value = {"status": "seeded", "reason": "runtime_image_seeded"}
		mock_probe_image.return_value = {"available": True}
		mock_pull_image.return_value = {"status": "pulled"}

		result = runbook_views._warm_runtime_image_on_host(
			runtime_image="cognus/chaincode-gateway:latest",
			node_type="apigateway",
			host_address="10.0.0.10",
			ssh_user="ubuntu",
			ssh_port=22,
			identity_file="/tmp/test.pem",
		)

		self.assertEqual(result["status"], "seeded")
		mock_seed_image.assert_called_once()
		mock_pull_image.assert_not_called()

	def test_orderer_dev_bootstrap_uses_runtime_msp_and_prefers_mounted_identity(self):
		command = runbook_views.RUNBOOK_ORDERER_DEV_TLS_BOOTSTRAP

		self.assertIn("${ORDERER_GENERAL_LISTENPORT:-7050}", command)
		self.assertIn("${ORDERER_GENERAL_CLUSTER_LISTENPORT:-7051}", command)
		self.assertIn("if [ \"$ORDERER_GENERAL_CLUSTER_LISTENPORT\" = \"$ORDERER_GENERAL_LISTENPORT\" ]; then export ORDERER_GENERAL_CLUSTER_LISTENPORT=$((ORDERER_GENERAL_LISTENPORT + 1)); fi;", command)
		self.assertIn("COGNUS_ORDERER_RESET_LEDGER=${COGNUS_ORDERER_RESET_LEDGER:-0}", command)
		self.assertIn("COGNUS_ORDERER_LEDGER_PRESENT=0", command)
		self.assertIn("COGNUS_HOST_MSP_READY=0", command)
		self.assertIn("bootstrap: preserving existing orderer ledger", command)
		self.assertIn("if [ \"$COGNUS_ORDERER_RESET_LEDGER\" = \"1\" ]; then rm -rf \"$PROD_DIR/chains\" \"$PROD_DIR/pendingops\" \"$PROD_DIR/etcdraft\"", command)
		self.assertIn("COGNUS_SAMPLE_MSP=0", command)
		self.assertIn("if [ -s \"$SRC_MSP/signcerts/cert.pem\" ] && [ -s \"$SRC_MSP/keystore/key.pem\" ]; then COGNUS_HOST_MSP_READY=1; fi;", command)
		self.assertIn("if [ \"$COGNUS_HOST_MSP_READY\" != \"1\" ]; then rm -rf \"$MSP_DIR/signcerts\" \"$MSP_DIR/keystore\" \"$MSP_DIR/admincerts\" \"$MSP_DIR/cacerts\" \"$MSP_DIR/tlscacerts\" \"$MSP_DIR/tlsintermediatecerts\"", command)
		self.assertIn("touch \"$MSP_DIR/.cognus-sample-msp\"", command)
		self.assertIn("export ORDERER_GENERAL_LOCALMSPID=SampleOrg", command)
		self.assertIn("${ORDERER_GENERAL_LOCALMSPID:-Org1MSP}", command)
		self.assertIn("${ORDERER_GENERAL_LOCALMSPDIR:-/etc/hyperledger/fabric/msp}", command)
		self.assertIn("SRC_MSP=/var/lib/cognus/msp", command)
		self.assertIn("if [ -d \"$SRC_MSP\" ]; then cp -a \"$SRC_MSP/.\" \"$MSP_DIR/\"", command)
		self.assertNotIn("rm -rf \"$PROD_DIR/chains\" \"$PROD_DIR/pendingops\" \"$PROD_DIR/etcdraft\" >/dev/null 2>&1 || true; mkdir -p", command)
		self.assertNotIn("export ORDERER_GENERAL_LOCALMSPID=Org1MSP", command)

	def test_orderer_runtime_command_override_is_not_dropped_when_builtin_bootstrap_exceeds_limit(self):
		self.assertGreater(
			len(runbook_views.RUNBOOK_ORDERER_DEV_TLS_BOOTSTRAP.encode("utf-8")),
			runbook_views.RUNBOOK_RUNTIME_COMMAND_OVERRIDE_MAX_BYTES - 4096,
		)

		override_args = runbook_views._resolve_runtime_docker_run_command_override_args(
			"orderer"
		)

		self.assertIn("sh", override_args)
		self.assertIn("bootstrap: preserving existing orderer ledger", override_args)
		self.assertIn("exec orderer", override_args)

	def test_peer_dev_bootstrap_switches_to_sampleorg_when_sample_msp_detected(self):
		command = runbook_views.RUNBOOK_PEER_DEV_MSP_BOOTSTRAP

		self.assertIn("${CORE_PEER_MSPCONFIGPATH:-/etc/hyperledger/fabric/msp}", command)
		self.assertIn(".cognus-sample-msp", command)
		self.assertIn("export CORE_PEER_LOCALMSPID=SampleOrg", command)
		self.assertIn("exec peer node start", command)

	def test_configtx_profile_autodetect_step_uses_portable_awk_pattern(self):
		command = runbook_views._build_configtx_profile_autodetect_step("library-channel")

		self.assertIn("^[[:space:]][[:space:]][A-Za-z0-9_.-]+:[[:space:]]*$", command)
		self.assertNotIn("^[[:space:]]{2}[A-Za-z0-9_.-]+:[[:space:]]*$", command)

	def test_provision_runtime_command_apigateway_blocks_when_org_scope_missing(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-apigw-empty-org",
				"change_id": "cr-test-apigw-empty-org",
				"topology_catalog": {"organizations": []},
			},
			{
				"node_id": "apigateway-empty",
				"node_type": "apigateway",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("COGNUS_APIGW_IDENTITIES_INVALID_DEFERRED:empty_organizations", command)
		self.assertIn("exit 126", command)

	def test_provision_runtime_command_apigateway_uses_service_context_scope_fallback(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-apigw-scope-fallback",
				"change_id": "cr-test-apigw-scope-fallback",
				"host_mapping": [
					{
						"node_id": "peer0-org-inf16",
						"node_type": "peer",
						"org_id": "org-inf16",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org-inf16",
							"msp_id": "OrgInf16MSP",
							"channels": [],
							"chaincodes": [],
							"apis": [],
						},
					]
				},
			},
			{
				"node_id": "apigateway-org-inf16",
				"node_type": "apigateway",
				"org_id": "org-inf16",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
				"service_context": {
					"org_id": "org-inf16",
					"channel_ids": ["library-channel"],
					"chaincode_ids": ["reserve-book-cc"],
				},
			},
		)

		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED:library-channel:reserve-book-cc", command)
		self.assertNotIn("COGNUS_CHANNEL_NOT_JOINED:library-channel", command)
		self.assertIn("/app/data/identities.json", command)
		self.assertIn("/app/data/connection.json", command)
		self.assertIn("DEFAULT_ORG=org-inf16", command)
		self.assertIn("label cognus.run_id=run-test-apigw-scope-fallback", command)

	def test_provision_runtime_host_order_prioritizes_peer_before_apigateway(self):
		host_mapping = [
			{"node_id": "apigateway-org1", "node_type": "apigateway", "host_ref": "machine1", "org_id": "org1"},
			{"node_id": "peer0-org1", "node_type": "peer", "host_ref": "machine1", "org_id": "org1"},
			{"node_id": "orderer0-org1", "node_type": "orderer", "host_ref": "machine1", "org_id": "org1"},
			{"node_id": "couch-org1", "node_type": "couch", "host_ref": "machine1", "org_id": "org1"},
			{"node_id": "netapi-org1", "node_type": "netapi", "host_ref": "machine1", "org_id": "org1"},
		]

		ordered = runbook_views._ordered_runtime_host_mapping_for_provision(host_mapping)
		ordered_ids = [row.get("node_id") for row in ordered]

		self.assertEqual(
			ordered_ids,
			["couch-org1", "orderer0-org1", "peer0-org1", "apigateway-org1", "netapi-org1"],
		)

	def test_build_ssh_command_enables_controlmaster_reuse(self):
		command = runbook_views._build_ssh_command(
			"10.0.0.10",
			"ubuntu",
			22,
			"echo COGNUS_SSH_OK",
			"/tmp/test-runbook-key.pem",
		)

		self.assertIn("ControlMaster=auto", command)
		self.assertIn(
			f"ControlPersist={runbook_views.RUNBOOK_SSH_CONTROL_PERSIST_SECONDS}s",
			command,
		)
		self.assertTrue(any(item.startswith("ControlPath=") for item in command))

	def test_run_remote_command_streams_large_scripts_via_stdin_wrapper(self):
		large_remote_command = "x" * (
			runbook_views.RUNBOOK_SSH_INLINE_COMMAND_MAX_BYTES + 128
		)

		result = runbook_views._run_remote_command(
			"10.0.0.10",
			"ubuntu",
			22,
			large_remote_command,
			"/tmp/test-runbook-key.pem",
		)

		self.assertEqual(result["returncode"], self.mock_ssh_returncode)
		self.assertGreaterEqual(len(self.ssh_commands), 1)
		ssh_call = self.ssh_commands[-1]
		self.assertIn(
			"COGNUS_SSH_STDIN_SCRIPT=$(mktemp /tmp/cognus-ssh-script.XXXXXX)",
			ssh_call["command"][-1],
		)
		self.assertEqual(
			ssh_call["kwargs"].get("input"),
			large_remote_command,
		)

	def test_run_remote_command_retries_via_stdin_wrapper_on_e2big(self):
		completed = SimpleNamespace(
			returncode=0,
			stdout="COGNUS_SSH_OK\n",
			stderr="",
		)
		with patch.object(
			runbook_views.subprocess,
			"run",
			side_effect=[
				OSError(7, "Argument list too long"),
				completed,
			],
		) as subprocess_run_mock:
			result = runbook_views._run_remote_command(
				"10.0.0.10",
				"ubuntu",
				22,
				"echo COGNUS_SSH_OK",
				"/tmp/test-runbook-key.pem",
			)

		self.assertEqual(result["returncode"], 0)
		self.assertEqual(result["stdout"], "COGNUS_SSH_OK")
		self.assertEqual(subprocess_run_mock.call_count, 2)
		self.assertIsNone(subprocess_run_mock.call_args_list[0].kwargs.get("input"))
		self.assertEqual(
			subprocess_run_mock.call_args_list[1].kwargs.get("input"),
			"echo COGNUS_SSH_OK",
		)
		self.assertIn(
			"COGNUS_SSH_STDIN_SCRIPT=$(mktemp /tmp/cognus-ssh-script.XXXXXX)",
			subprocess_run_mock.call_args_list[1].args[0][-1],
		)

	def test_remote_docker_ready_cache_reuses_first_probe_for_same_host(self):
		run_state = {}
		with patch.object(
			runbook_views,
			"_ensure_remote_docker_ready",
			return_value={
				"attempted": False,
				"status": "ready",
				"stdout": "Docker version 26.1.0",
				"stderr": "",
				"returncode": 0,
			},
		) as docker_ready_mock:
			first = runbook_views._ensure_remote_docker_ready_cached(
				run_state,
				"10.0.0.10",
				"ubuntu",
				22,
			)
			second = runbook_views._ensure_remote_docker_ready_cached(
				run_state,
				"10.0.0.10",
				"ubuntu",
				22,
			)

		self.assertFalse(first["cache_hit"])
		self.assertTrue(second["cache_hit"])
		self.assertEqual(docker_ready_mock.call_count, 1)
		self.assertEqual(second["status"], "ready")

	def test_runtime_host_image_warmup_deduplicates_shared_images_and_reuses_cache(self):
		run_state = {}
		host_rows = [
			{
				"node_id": "peer0-org1",
				"node_type": "peer",
				"runtime_image": "hyperledger/fabric-peer:2.5",
			},
			{
				"node_id": "orderer0-org1",
				"node_type": "orderer",
				"runtime_image": "hyperledger/fabric-orderer:2.5.12",
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
			{
				"node_id": "netapi-org1",
				"node_type": "netapi",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		]
		available_images = set()

		def probe_side_effect(*args, **kwargs):
			runtime_image = kwargs.get("runtime_image", "")
			is_available = runtime_image in available_images
			return {
				"available": is_available,
				"returncode": 0 if is_available else 1,
				"stdout": "",
				"stderr": "",
				"timed_out": False,
				"runtime_image": runtime_image,
			}

		def pull_side_effect(*args, **kwargs):
			runtime_image = kwargs.get("runtime_image", "")
			available_images.add(runtime_image)
			return {
				"attempted": True,
				"status": "pulled",
				"reason": "runtime_image_pulled_remotely",
				"runtime_image": runtime_image,
			}

		with patch.object(
			runbook_views,
			"_probe_remote_runtime_image",
			side_effect=probe_side_effect,
		), patch.object(
			runbook_views,
			"_pull_runtime_image_via_ssh",
			side_effect=pull_side_effect,
		) as pull_mock:
			first = runbook_views._ensure_runtime_host_image_warmup(
				run_state,
				host_rows,
				"10.0.0.10",
				"ubuntu",
				22,
			)
			second = runbook_views._ensure_runtime_host_image_warmup(
				run_state,
				host_rows,
				"10.0.0.10",
				"ubuntu",
				22,
			)

		self.assertFalse(first["cache_hit"])
		self.assertEqual(first["status"], "ready")
		self.assertEqual(first["worker_count"], 3)
		self.assertEqual(len(first["requested_images"]), 3)
		self.assertEqual(pull_mock.call_count, 3)
		self.assertTrue(second["cache_hit"])
		self.assertEqual(second["status"], "ready")
		self.assertEqual(pull_mock.call_count, 3)

	def test_gateway_index_loader_recovers_from_empty_cache(self):
		runbook_views._RUNBOOK_CHAINCODE_GATEWAY_INDEX_SOURCE_CACHE = ""
		source = runbook_views._load_runbook_chaincode_gateway_index_source()

		self.assertTrue(isinstance(source, str) and len(source) > 0)
		self.assertIn("resolveChannelNameFromCcp", source)

	def test_verify_consistency_command_blocks_when_chaincode_not_committed(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-verify-cc",
				"change_id": "cr-test-verify-cc",
				"host_mapping": [
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
							"service_host_mapping": {
								"apiGateway": "machine1",
							},
						},
					],
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("peer lifecycle chaincode querycommitted -C library-channel", command)
		self.assertIn("COGNUS_RUN_ID=run-test-verify-cc", command)
		self.assertIn("peer channel list 2>/dev/null | grep -F library-channel >/dev/null", command)
		self.assertIn("COGNUS_PROFILE_LIST=$(awk", command)
		self.assertNotIn("LibraryChannel", command)
		self.assertNotIn("cctools-chaincodes/proj-blockchain-stcs-web3.0", command)
		self.assertNotIn("cctools-chaincodes/bookReserveChain", command)
		self.assertNotIn("cctools-chaincodes/fakenews-cc", command)
		self.assertIn("peer channel join -b '$COGNUS_PEER_BLOCK'", command)
		self.assertIn("COGNUS_CHANNEL_NOT_JOINED:library-channel", command)
		self.assertIn("COGNUS_CHAINCODE_NOT_COMMITTED:library-channel:reserve-book-cc", command)
		self.assertIn("COGNUS_ORDERER_PARTICIPATION_JOIN_FAILED:library-channel", command)
		self.assertIn("certificate signed by unknown authority|creator org unknown", command)
		self.assertIn("s#127\\.0\\.0\\.1:7050#$COGNUS_ORDERER_CONTAINER:7050#g", command)
		self.assertNotIn("|| peer channel fetch 0 '$channel_block' -c library-channel -o $COGNUS_ORDERER_CONTAINER:7050 >/dev/null 2>&1", command)
		self.assertIn("/etc/hyperledger/fabric/tls/ca.crt", command)

	def test_verify_consistency_command_uses_chaincode_artifact_metadata_from_handoff(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-verify-artifact-metadata",
				"change_id": "cr-test-verify-artifact-metadata",
				"host_mapping": [
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["stcs-channel"],
							"chaincodes": ["stcs-cc"],
							"service_host_mapping": {
								"peer": "machine1",
								"orderer": "machine1",
								"apiGateway": "machine1",
							},
						},
					],
				},
				"handoff_payload": {
					"network": {
						"chaincodes": [
							{
								"name": "stcs-cc",
								"channel": "stcs-channel",
								"package_pattern": "cc-tools",
								"package_file_name": "stcs-cc_2.3.tar.gz",
							}
						]
					}
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("for COGNUS_CC_FILE in stcs-cc_2.3.tar.gz; do", command)
		self.assertIn("--version 2.3", command)
		self.assertIn("grep -i -- \"$COGNUS_CC_TERM\"", command)
		self.assertNotIn("LibraryChannel", command)

	def test_verify_consistency_command_scopes_chaincodes_by_declared_pairs(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-verify-scoped-pairs",
				"change_id": "cr-test-verify-scoped-pairs",
				"host_mapping": [
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"api_registry": [
					{
						"api_id": "api-fakenews",
						"org_id": "org1",
						"channel_id": "fakenews-channel",
						"chaincode_id": "fakenews-cc",
						"route_path": "/api/fakenews-channel/fakenews-cc/*/*",
					},
					{
						"api_id": "api-stcs",
						"org_id": "org1",
						"channel_id": "stcs-channel",
						"chaincode_id": "stcs-cc",
						"route_path": "/api/stcs-channel/stcs-cc/*/*",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["fakenews-channel", "stcs-channel"],
							"chaincodes": ["fakenews-cc", "stcs-cc"],
							"service_host_mapping": {
								"peer": "machine1",
								"orderer": "machine1",
								"apiGateway": "machine1",
							},
						},
					],
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("COGNUS_CHAINCODE_NOT_COMMITTED:fakenews-channel:fakenews-cc", command)
		self.assertIn("COGNUS_CHAINCODE_NOT_COMMITTED:stcs-channel:stcs-cc", command)
		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED:fakenews-channel:stcs-cc", command)
		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED:stcs-channel:fakenews-cc", command)

	def test_seed_remote_chaincode_packages_prefers_scoped_pairs(self):
		local_pkg = Path(self.tempdir.name) / "fakenews-cc_1.0.tar.gz"
		local_pkg.write_bytes(b"fake-package")
		run_state = {
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"channels": ["fakenews-channel", "stcs-channel"],
						"chaincodes": ["fakenews-cc", "stcs-cc"],
						"service_host_mapping": {
							"peer": "machine1",
							"orderer": "machine1",
							"apiGateway": "machine1",
						},
						"apis": [
							{
								"channel_id": "fakenews-channel",
								"chaincode_id": "fakenews-cc",
								"route_path": "/api/fakenews-channel/fakenews-cc/*/*",
							}
						],
					}
				]
			}
		}
		host_row = {
			"node_id": "apigateway-org1",
			"node_type": "apigateway",
			"org_id": "org1",
			"host_ref": "machine1",
			"host_address": "10.0.0.10",
		}

		with patch.object(
			runbook_views,
			"_resolve_or_build_local_chaincode_artifact",
			return_value={
				"available": True,
				"mode": "autobuild",
				"local_path": str(local_pkg),
			},
		), patch.object(
			runbook_views,
			"_upload_local_file_to_remote",
			return_value={"returncode": 0, "stdout": "", "stderr": ""},
		) as upload_mock:
			seed_result = runbook_views._seed_remote_chaincode_packages_for_host(
				run_state,
				host_row,
				"200.137.197.215",
				"web3",
				21525,
				identity_file="/tmp/test.pem",
			)

		self.assertTrue(seed_result["available"])
		self.assertEqual(len(seed_result["rows"]), 1)
		self.assertEqual(seed_result["rows"][0]["channel_id"], "fakenews-channel")
		self.assertEqual(seed_result["rows"][0]["chaincode_id"], "fakenews-cc")
		self.assertIn("fakenews-cc", seed_result["rows"][0]["remote_path"])
		self.assertEqual(upload_mock.call_count, 1)

	def test_seed_remote_chaincode_packages_merges_exact_topology_pairs_when_api_registry_is_partial(self):
		local_pkg = Path(self.tempdir.name) / "chaincode_1.0.tar.gz"
		local_pkg.write_bytes(b"fake-package")
		run_state = {
			"api_registry": [
				{
					"api_id": "api-1",
					"org_name": "INF-UFG",
					"channel_id": "stcs-channel-dev",
					"chaincode_id": "stcs-cc-dev",
					"route_path": "/api/stcs-channel-dev/stcs-cc-dev/*/*",
				}
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org-inf-ufg",
						"org_name": "INF-UFG",
						"channels": ["fakenews-channel-dev", "stcs-channel-dev"],
						"chaincodes": ["fakenews-cc-dev", "stcs-cc-dev"],
						"apis": [
							{
								"channel_id": "stcs-channel-dev",
								"chaincode_id": "stcs-cc-dev",
								"route_path": "/api/stcs-channel-dev/stcs-cc-dev/*/*",
							}
						],
					}
				],
				"chaincodes": [
					{
						"channel_id": "fakenews-channel-dev",
						"chaincode_id": "fakenews-cc-dev",
					},
					{
						"channel_id": "stcs-channel-dev",
						"chaincode_id": "stcs-cc-dev",
					},
				],
			},
		}
		host_row = {
			"node_id": "apigateway-org-inf-ufg",
			"node_type": "apigateway",
			"org_id": "org-inf-ufg",
			"host_ref": "machine1",
			"host_address": "10.0.0.10",
		}

		with patch.object(
			runbook_views,
			"_resolve_or_build_local_chaincode_artifact",
			return_value={
				"available": True,
				"mode": "autobuild",
				"local_path": str(local_pkg),
			},
		), patch.object(
			runbook_views,
			"_upload_local_file_to_remote",
			return_value={"returncode": 0, "stdout": "", "stderr": ""},
		) as upload_mock:
			scope = runbook_views._collect_gateway_channel_chaincode_pairs(run_state, host_row)
			seed_result = runbook_views._seed_remote_chaincode_packages_for_host(
				run_state,
				host_row,
				"200.137.197.215",
				"web3",
				21525,
				identity_file="/tmp/test.pem",
			)

		self.assertEqual(
			{
				(row["channel_id"], row["chaincode_id"])
				for row in scope["pairs"]
			},
			{
				("fakenews-channel-dev", "fakenews-cc-dev"),
				("stcs-channel-dev", "stcs-cc-dev"),
			},
		)
		self.assertTrue(all(row["upload_returncode"] == 0 for row in seed_result["rows"]))
		self.assertEqual(upload_mock.call_count, 2)
		self.assertEqual(
			{
				(row["channel_id"], row["chaincode_id"])
				for row in seed_result["rows"]
			},
			{
				("fakenews-channel-dev", "fakenews-cc-dev"),
				("stcs-channel-dev", "stcs-cc-dev"),
			},
		)

	def test_build_remote_chaincode_package_seed_failure_payload_reports_missing_bindings(self):
		error_payload = runbook_views._build_remote_chaincode_package_seed_failure_payload(
			{"run_id": "run-test"},
			"provision",
			"provision.runtime",
			"machine1",
			"200.137.197.215",
			"org-inf-ufg",
			"apigateway-org-inf-ufg",
			"apigateway",
			{
				"rows": [],
				"missing_pairs": [
					{
						"channel_id": "stcs-channel-dev",
						"chaincode_id": "stcs-cc-dev",
						"mode": "binding_missing",
					}
				],
			},
		)

		self.assertEqual(
			error_payload["code"],
			"runbook_chaincode_artifact_unavailable",
		)
		self.assertEqual(
			error_payload["details"]["missing_pairs"][0]["mode"],
			"binding_missing",
		)

	def test_build_remote_chaincode_package_seed_failure_payload_reports_upload_failure(self):
		error_payload = runbook_views._build_remote_chaincode_package_seed_failure_payload(
			{"run_id": "run-test"},
			"provision",
			"provision.runtime",
			"machine1",
			"200.137.197.215",
			"org-inf-ufg",
			"apigateway-org-inf-ufg",
			"apigateway",
			{
				"rows": [
					{
						"channel_id": "stcs-channel-dev",
						"chaincode_id": "stcs-cc-dev",
						"upload_returncode": 1,
					}
				],
				"missing_pairs": [],
			},
		)

		self.assertEqual(
			error_payload["code"],
			"runbook_chaincode_package_seed_failed",
		)
		self.assertEqual(
			error_payload["details"]["failed_rows"][0]["chaincode_id"],
			"stcs-cc-dev",
		)

	def test_build_remote_chaincode_package_seed_failure_payload_ignores_success_rows(self):
		error_payload = runbook_views._build_remote_chaincode_package_seed_failure_payload(
			{"run_id": "run-test"},
			"provision",
			"provision.runtime",
			"machine1",
			"200.137.197.215",
			"org-inf-ufg",
			"apigateway-org-inf-ufg",
			"apigateway",
			{
				"rows": [
					{
						"channel_id": "stcs-channel-dev",
						"chaincode_id": "stcs-cc-dev",
						"upload_returncode": 0,
					}
				],
				"missing_pairs": [],
			},
		)

		self.assertEqual(error_payload, {})

	def test_coerce_exit_code_preserves_zero(self):
		self.assertEqual(runbook_views._coerce_exit_code(0, default=1), 0)
		self.assertEqual(runbook_views._coerce_exit_code("0", default=1), 0)
		self.assertEqual(runbook_views._coerce_exit_code(None, default=1), 1)

	def test_build_local_chaincode_artifact_falls_back_to_host_source_when_container_source_missing(self):
		artifact_path = Path(self.tempdir.name) / "fakenews-cc_1.0.tar.gz"
		metadata = {
			"chaincode_id": "fakenews-cc",
			"channel_id": "fakenews-channel",
			"candidate_files": [],
			"search_terms": ["fakenews-cc"],
			"version": "1.0",
		}

		with patch.object(
			runbook_views,
			"_resolve_local_chaincode_source_dir",
			return_value="",
		), patch.object(
			runbook_views,
			"_build_local_chaincode_artifact_from_host_source",
			return_value=str(artifact_path),
		) as host_fallback_mock:
			result = runbook_views._build_local_chaincode_artifact_from_source(metadata)

		self.assertEqual(result, str(artifact_path))
		host_fallback_mock.assert_called_once()

	def test_resolve_chaincode_artifact_metadata_derives_envless_package_candidates(self):
		metadata = runbook_views._resolve_chaincode_artifact_metadata(
			{
				"topology_catalog": {
					"chaincodes": [
						{
							"channel_id": "stcs-channel-dev",
							"chaincode_id": "stcs-cc-dev",
						}
					]
				}
			},
			"stcs-channel-dev",
			"stcs-cc-dev",
		)

		self.assertIn("stcs-cc-dev", metadata["search_terms"])
		self.assertIn("stcs-cc", metadata["search_terms"])
		self.assertIn("stcs", metadata["search_terms"])
		self.assertIn("stcs-cc_1.0.tar.gz", metadata["candidate_files"])

	def test_resolve_chaincode_artifact_metadata_ignores_generic_package_pattern_aliases(self):
		metadata = runbook_views._resolve_chaincode_artifact_metadata(
			{
				"topology_catalog": {
					"chaincodes": [
						{
							"channel_id": "fakenews-channel-dev",
							"chaincode_id": "fakenews-cc-dev",
							"package_pattern": "cc-tools",
							"package_file_name": "fakenews-cc_1.1.tar.gz",
						}
					]
				}
			},
			"fakenews-channel-dev",
			"fakenews-cc-dev",
		)

		self.assertIn("fakenews-cc-dev", metadata["search_terms"])
		self.assertIn("fakenews-cc", metadata["search_terms"])
		self.assertIn("fakenews", metadata["search_terms"])
		self.assertNotIn("cc-tools", metadata["search_terms"])
		self.assertNotIn("tools", metadata["search_terms"])
		self.assertEqual(["fakenews-cc_1.1.tar.gz"], metadata["candidate_files"])

	def test_resolve_local_chaincode_source_dir_uses_explicit_source_ref(self):
		project_root = Path(self.tempdir.name) / "proj-blockchain-stcs-web3.0"
		project_chaincode_dir = project_root / "chaincode"
		project_chaincode_dir.mkdir(parents=True, exist_ok=True)
		(project_chaincode_dir / "go.mod").write_text("module stcs\n", encoding="utf-8")
		(project_chaincode_dir / "main.go").write_text("package main\n", encoding="utf-8")
		(project_chaincode_dir / "txList.go").write_text("package main\n", encoding="utf-8")

		resolved_source_dir = runbook_views._resolve_local_chaincode_source_dir(
			{
				"chaincode_id": "stcs-cc-dev",
				"source_refs": [f"local-file:{project_root}"],
			}
		)

		self.assertEqual(str(project_chaincode_dir), resolved_source_dir)

	def test_resolve_local_chaincode_artifact_path_accepts_explicit_archive_binding(self):
		archive_path = Path(self.tempdir.name) / "fakenews-cc_1.1.tar.gz"
		self._write_chaincode_package(archive_path, "reserve-book-cc_1.0")

		resolved_path = runbook_views._resolve_local_chaincode_artifact_path(
			{
				"chaincode_id": "fakenews-cc-dev",
				"artifact_refs": [f"local-file:{archive_path}"],
				"search_terms": ["fakenews-cc-dev", "fakenews-cc", "fakenews"],
				"candidate_files": ["fakenews-cc_1.1.tar.gz"],
			}
		)

		self.assertEqual(str(archive_path), resolved_path)

	def test_resolve_chaincode_runtime_mode_uses_ccaas_for_explicit_binding_in_auto_mode(self):
		with patch.object(runbook_views, "RUNBOOK_CHAINCODE_RUNTIME_MODE", "auto"):
			self.assertEqual(
				runbook_views._resolve_chaincode_runtime_mode(
					{
						"chaincode_id": "stcs-cc-dev",
						"artifact_refs": [f"local-file:{self.default_chaincode_artifact}"],
					}
				),
				"ccaas",
			)
			self.assertEqual(
				runbook_views._resolve_chaincode_runtime_mode(
					{
						"chaincode_id": "stcs-cc-dev",
					}
				),
				"legacy",
			)

	def test_build_chaincode_ccaas_adapter_source_uses_chaincode_server(self):
		adapter_source = runbook_views._build_chaincode_ccaas_adapter_source()

		self.assertIn("shim.ChaincodeServer", adapter_source)
		self.assertIn("CHAINCODE_SERVER_ADDRESS", adapter_source)
		self.assertIn("CHAINCODE_ID", adapter_source)
		self.assertIn("TLSProps", adapter_source)

	def test_verify_consistency_command_uses_ccaas_runtime_for_explicit_binding(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-verify-ccaas",
				"change_id": "cr-test-verify-ccaas",
				"host_mapping": [
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
						"runtime_image": "cognus/chaincode-gateway:latest",
					},
				],
				"api_registry": [
					{
						"api_id": "api-org1",
						"org_name": "Org1",
						"channel_id": "library-channel",
						"chaincode_id": "reserve-book-cc",
						"route_path": "/api/library-channel/reserve-book-cc/*/*",
					}
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"msp_id": "Org1MSP",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
							"apis": [
								{
									"channel_id": "library-channel",
									"chaincode_id": "reserve-book-cc",
									"route_path": "/api/library-channel/reserve-book-cc/*/*",
								},
							],
						}
					],
					"channels": [
						{
							"channel_id": "library-channel",
							"member_orgs": ["Org1"],
							"chaincodes": ["reserve-book-cc"],
						}
					],
					"chaincodes": [
						{
							"channel_id": "library-channel",
							"chaincode_id": "reserve-book-cc",
							"artifact_ref": f"local-file:{self.default_chaincode_artifact}",
							"version": "1.0",
						}
					],
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("COGNUS_CCAAS_GO_MODULE_MISSING:library-channel:reserve-book-cc", command)
		self.assertIn("\"type\":\"ccaas\"", command)
		self.assertIn("docker build --build-arg COGNUS_GO_IMAGE=", command)
		self.assertIn("docker run -d --name \"$COGNUS_CCAAS_CONTAINER\"", command)
		self.assertIn("--label cognus.runtime_mode=ccaas", command)
		self.assertIn("peer lifecycle chaincode calculatepackageid", command)
		self.assertIn("CHAINCODE_ID=\"$COGNUS_CC_PACKAGE_ID\"", command)
		self.assertIn("metadata.json code.tar.gz", command)
		self.assertIn("COGNUS_CCAAS_INSTALL_DIAG:", command)
		self.assertIn("COGNUS_CCAAS_PACKAGE_NOT_INSTALLED:library-channel:reserve-book-cc", command)
		self.assertIn("COGNUS_CCAAS_RUNTIME_PRECHECK_PENDING:library-channel:reserve-book-cc", command)
		self.assertIn("COGNUS_CCAAS_RUNTIME_POSTCHECK_PENDING:library-channel:reserve-book-cc", command)
		self.assertIn("COGNUS_CCAAS_RUNTIME_RUNNING_STREAK=0", command)
		self.assertIn("COGNUS_CCAAS_RUNTIME_STATUS:", command)

	def test_resolve_or_build_local_chaincode_artifact_reports_binding_modes(self):
		missing_binding = runbook_views._resolve_or_build_local_chaincode_artifact(
			{
				"chaincode_id": "stcs-cc-dev",
			}
		)
		invalid_binding = runbook_views._resolve_or_build_local_chaincode_artifact(
			{
				"chaincode_id": "stcs-cc-dev",
				"source_refs": ["relative/path/to/chaincode"],
			}
		)

		self.assertEqual("binding_missing", missing_binding["mode"])
		self.assertEqual("binding_invalid", invalid_binding["mode"])

	def test_build_local_chaincode_artifact_from_source_ignores_cached_archive_with_label_mismatch(self):
		cached_root = Path(self.tempdir.name) / "cognus_chaincode_autobuild" / "fakenews-cc-dev"
		cached_artifact = cached_root / "fakenews-cc_1.1.tar.gz"
		self._write_chaincode_package(cached_artifact, "reserve-book-cc_1.0")
		fallback_artifact = Path(self.tempdir.name) / "fallback.tar.gz"

		with patch.object(runbook_views, "gettempdir", return_value=self.tempdir.name), patch.object(
			runbook_views,
			"_resolve_local_chaincode_source_dir",
			return_value="",
		), patch.object(
			runbook_views,
			"_build_local_chaincode_artifact_from_host_source",
			return_value=str(fallback_artifact),
		) as host_fallback_mock:
			result = runbook_views._build_local_chaincode_artifact_from_source(
				{
					"chaincode_id": "fakenews-cc-dev",
					"version": "1.1",
					"search_terms": ["fakenews-cc-dev", "fakenews-cc", "fakenews"],
					"candidate_files": ["fakenews-cc_1.1.tar.gz"],
				}
			)

		self.assertEqual(str(fallback_artifact), result)
		host_fallback_mock.assert_called_once()
		self.assertFalse(cached_artifact.exists())

	def test_chaincode_source_sanitize_step_removes_windows_metadata_files(self):
		command = runbook_views._build_chaincode_source_sanitize_step("/workspace/src")

		self.assertIn(":Zone.Identifier", command)
		self.assertIn(".DS_Store", command)
		self.assertIn("Thumbs.db", command)
		self.assertIn("__MACOSX", command)

	def test_verify_consistency_command_skips_duplicate_chaincode_guard_for_secondary_gateway(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-verify-secondary-gateway",
				"change_id": "cr-test-verify-secondary-gateway",
				"host_mapping": [
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "netapi-org1",
						"node_type": "netapi",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
						}
					],
				},
			},
			{
				"node_id": "netapi-org1",
				"node_type": "netapi",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("docker inspect", command)
		self.assertIn("netapi-org1", command)
		self.assertIn("COGNUS_RUNTIME_TOPOLOGY_MISSING:peer:peer0-org1", command)
		self.assertNotIn("peer lifecycle chaincode querycommitted -C library-channel", command)
		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED:library-channel:reserve-book-cc", command)

	def test_collect_verify_consistency_diagnostics_escapes_run_id_placeholder(self):
		with patch.object(
			runbook_views,
			"_run_remote_command",
			return_value={
				"returncode": 0,
				"stdout": "\n".join(
					[
						"__COGNUS_CHANNEL__=library-channel",
						"block_sha=abc123",
						"block_size=256",
						"__COGNUS_CHANNEL_END__=library-channel",
					]
				),
				"stderr": "",
			},
		) as remote_command_mock:
			diagnostics = runbook_views._collect_verify_consistency_diagnostics(
				{
					"run_id": "run-test-verify-diag",
					"topology_catalog": {
						"organizations": [
							{
								"org_id": "org1",
								"channels": ["library-channel"],
								"chaincodes": ["reserve-book-cc"],
							}
						],
					},
				},
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
				},
				"10.0.0.10",
				"ubuntu",
				22,
			)

		self.assertTrue(diagnostics["available"])
		self.assertEqual(diagnostics["channels"][0]["channel_id"], "library-channel")
		self.assertEqual(diagnostics["channels"][0]["block_size"], 256)
		remote_command = remote_command_mock.call_args.kwargs["remote_command"]
		self.assertIn(
			"COGNUS_DIAG_ROOT=/tmp/cognus-run-${COGNUS_RUN_ID}-library-channel;",
			remote_command,
		)

	def test_collect_remote_chaincode_commit_matrix_uses_detected_peer_msp(self):
		querycommitted_text = (
			"Committed chaincode definitions on channel 'library-channel':\n"
			"Name: reserve-book-cc, Version: 1.0, Sequence: 1"
		)
		with patch.object(
			runbook_views,
			"_run_remote_command",
			return_value={
				"returncode": 0,
				"stdout": "\n".join(
					[
						"__COGNUS_PAIR__=library-channel|reserve-book-cc",
						"committed=1",
						"package_id=reserve-book-cc_1.0:abc123",
						"querycommitted_b64={}".format(
							base64.b64encode(
								querycommitted_text.encode("utf-8")
							).decode("ascii")
						),
						"__COGNUS_PAIR_END__=library-channel|reserve-book-cc",
					]
				),
				"stderr": "",
			},
		) as remote_command_mock:
			commit_matrix = runbook_views._collect_remote_chaincode_commit_matrix(
				{
					"run_id": "run-test-commit-matrix",
					"host_mapping": [
						{
							"node_id": "peer0-org1",
							"node_type": "peer",
							"org_id": "org1",
							"host_ref": "machine1",
							"host_address": "10.0.0.10",
						},
						{
							"node_id": "apigateway-org1",
							"node_type": "apigateway",
							"org_id": "org1",
							"host_ref": "machine1",
							"host_address": "10.0.0.10",
						},
					],
					"topology_catalog": {
						"organizations": [
							{
								"org_id": "org1",
								"msp_id": "Org1MSP",
								"apis": [
									{
										"channel_id": "library-channel",
										"chaincode_id": "reserve-book-cc",
										"route_path": "/api/library-channel/reserve-book-cc/*/*",
									},
								],
							},
						],
					},
				},
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				"10.0.0.10",
				"ubuntu",
				22,
			)

		self.assertTrue(commit_matrix["available"])
		self.assertEqual(len(commit_matrix["rows"]), 1)
		self.assertTrue(commit_matrix["rows"][0]["committed"])
		self.assertEqual(
			commit_matrix["rows"][0]["package_id"],
			"reserve-book-cc_1.0:abc123",
		)
		self.assertIn("Name: reserve-book-cc", commit_matrix["rows"][0]["querycommitted"])
		remote_command = remote_command_mock.call_args.kwargs["remote_command"]
		self.assertIn("COGNUS_PEER_CORE_LOCAL_MSPID", remote_command)
		self.assertIn("COGNUS_PEER_SAMPLE_MSP=0", remote_command)
		self.assertIn("COGNUS_PRIMARY_CHANNEL_VALIDATE", remote_command)
		self.assertIn("COGNUS_ALT_CHANNEL_VALIDATE", remote_command)
		self.assertIn(
			"env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode queryinstalled",
			remote_command,
		)
		self.assertIn(
			"env CORE_PEER_MSPCONFIGPATH=$COGNUS_PEER_ADMIN_MSPCONFIGPATH CORE_PEER_LOCALMSPID=$COGNUS_PEER_LOCAL_MSPID peer lifecycle chaincode querycommitted -C library-channel",
			remote_command,
		)
		self.assertIn(
			"printf %s \\\"${CORE_PEER_LOCALMSPID:-}\\\"",
			remote_command,
		)
		self.assertIn(
			"docker ps --format '{{.Names}}' | grep -Fx \"$COGNUS_PEER_CONTAINER\"",
			remote_command,
		)
		self.assertNotIn("${{CORE_PEER_LOCALMSPID:-}}", remote_command)
		self.assertIn("/tmp/cognus/admin-msp", remote_command)

	def test_assess_gateway_final_gate_reuses_cached_scope_between_apigateway_and_netapi(self):
		run_state = {
			"run_id": "run-test-verify-cache",
			"host_mapping": [
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				{
					"node_id": "netapi-org1",
					"node_type": "netapi",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"channels": ["library-channel"],
						"chaincodes": ["reserve-book-cc"],
						"apis": [
							{
								"channel_id": "library-channel",
								"chaincode_id": "reserve-book-cc",
								"route_path": "/api/library-channel/reserve-book-cc/*/*",
							},
						],
					},
				],
			},
		}
		commit_matrix = {
			"available": True,
			"org_id": "org1",
			"rows": [
				{
					"channel_id": "library-channel",
					"chaincode_id": "reserve-book-cc",
					"committed": True,
				},
			],
		}
		diagnostics = {
			"available": True,
			"channels": [
				{
					"channel_id": "library-channel",
					"block_sha": "abc123",
					"block_size": 256,
				},
			],
		}
		with patch.object(
			runbook_views,
			"_collect_verify_consistency_diagnostics",
			return_value=diagnostics,
		) as diagnostics_mock, patch.object(
			runbook_views,
			"_collect_remote_chaincode_commit_matrix",
			return_value=commit_matrix,
		) as commit_matrix_mock, patch.object(
			runbook_views,
			"_probe_chaincode_gateway_api",
			return_value={"available": True, "details": {}},
		) as probe_mock:
			first_gate = runbook_views._assess_gateway_final_gate(
				run_state,
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				"10.0.0.10",
				8443,
				"ubuntu",
				22,
				timeout_seconds=180,
			)
			second_gate = runbook_views._assess_gateway_final_gate(
				run_state,
				{
					"node_id": "netapi-org1",
					"node_type": "netapi",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				"10.0.0.10",
				31509,
				"ubuntu",
				22,
				timeout_seconds=180,
			)

		self.assertTrue(first_gate["ok"])
		self.assertTrue(second_gate["ok"])
		self.assertEqual(diagnostics_mock.call_count, 1)
		self.assertEqual(commit_matrix_mock.call_count, 1)
		self.assertEqual(probe_mock.call_count, 2)

	def test_assess_gateway_final_gate_accepts_remote_loopback_probe(self):
		run_state = {
			"run_id": "run-test-gateway-loopback",
			"host_mapping": [
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"channels": ["library-channel"],
						"chaincodes": ["reserve-book-cc"],
						"apis": [
							{
								"channel_id": "library-channel",
								"chaincode_id": "reserve-book-cc",
								"route_path": "/api/library-channel/reserve-book-cc/*/*",
							},
						],
					},
				],
			},
		}
		commit_matrix = {
			"available": True,
			"org_id": "org1",
			"rows": [
				{
					"channel_id": "library-channel",
					"chaincode_id": "reserve-book-cc",
					"committed": True,
				},
			],
		}
		diagnostics = {
			"available": True,
			"channels": [
				{
					"channel_id": "library-channel",
					"block_sha": "abc123",
					"block_size": 256,
				},
			],
		}
		external_probe = {
			"available": False,
			"host_address": "10.0.0.10",
			"host_port": 8443,
			"details": {
				"checks": [
					{
						"path": "/health",
						"status_code": 0,
						"ok": False,
						"error": "<urlopen error timed out>",
					},
				],
				"first_failed_path": "/health",
				"first_failed_status_code": 0,
				"first_failed_body": "",
			},
		}
		loopback_probe = {
			"available": True,
			"host_address": "10.0.0.10",
			"host_port": 8443,
			"details": {
				"checks": [
					{
						"path": "/health",
						"status_code": 200,
						"ok": True,
					},
				],
				"loopback_host": "127.0.0.1",
				"probe_transport": "ssh_loopback",
			},
		}
		with patch.object(
			runbook_views,
			"_collect_verify_consistency_diagnostics",
			return_value=diagnostics,
		), patch.object(
			runbook_views,
			"_collect_remote_chaincode_commit_matrix",
			return_value=commit_matrix,
		), patch.object(
			runbook_views,
			"_probe_chaincode_gateway_api",
			return_value=external_probe,
		) as external_probe_mock, patch.object(
			runbook_views,
			"_probe_chaincode_gateway_api_via_ssh",
			return_value=loopback_probe,
		) as loopback_probe_mock:
			gate = runbook_views._assess_gateway_final_gate(
				run_state,
				{
					"node_id": "apigateway-org1",
					"node_type": "apigateway",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				"10.0.0.10",
				8443,
				"ubuntu",
				22,
			)

		self.assertTrue(gate["ok"])
		self.assertTrue(gate["probe"]["available"])
		self.assertEqual(external_probe_mock.call_count, 1)
		self.assertEqual(loopback_probe_mock.call_count, 1)
		self.assertEqual(
			gate["probe"]["details"]["external_probe"]["details"]["first_failed_path"],
			"/health",
		)

	def test_probe_chaincode_gateway_api_via_ssh_accepts_valid_payload_even_with_nonzero_returncode(self):
		with patch.object(
			runbook_views,
			"_run_remote_command",
			return_value={
				"returncode": 1,
				"stdout": json.dumps(
					{
						"available": True,
						"host_address": "127.0.0.1",
						"host_port": 8443,
						"details": {
							"checks": [
								{
									"path": "/health",
									"method": "GET",
									"url": "http://127.0.0.1:8443/health",
									"status_code": 200,
									"ok": True,
								},
							],
						},
					}
				),
				"stderr": "",
			},
		) as remote_command_mock:
			probe = runbook_views._probe_chaincode_gateway_api_via_ssh(
				"10.0.0.10",
				8443,
				"ubuntu",
				22,
				timeout_seconds=20,
				required_checks=[{"path": "/health", "method": "GET"}],
			)

		self.assertTrue(probe["available"])
		self.assertEqual(probe["details"]["probe_transport"], "ssh_loopback")
		self.assertEqual(probe["details"]["loopback_host"], "127.0.0.1")
		self.assertEqual(probe["details"]["ssh_probe_returncode"], 1)
		remote_command = remote_command_mock.call_args.kwargs["remote_command"]
		self.assertIn("hostname -I", remote_command)
		self.assertEqual(
			probe_mock.call_args.kwargs["timeout_seconds"],
			runbook_views.RUNBOOK_GATEWAY_HTTP_PROBE_TIMEOUT_SECONDS,
		)

	def test_verify_consistency_command_enforces_runtime_topology_guard(self):
		command = runbook_views._build_ssh_remote_command(
			"verify",
			"verify.consistency",
			{
				"run_id": "run-test-topology-guard",
				"change_id": "cr-test-topology-guard",
				"enabled_runtime_node_types": [
					"peer",
					"orderer",
					"ca",
					"couch",
					"apigateway",
					"netapi",
				],
				"host_mapping": [
					{
						"node_id": "orderer0-org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
						}
					],
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertIn("COGNUS_RUNTIME_TOPOLOGY_MISSING:orderer:orderer0-org1", command)
		self.assertIn("COGNUS_RUNTIME_TOPOLOGY_MISSING:peer:peer0-org1", command)
		self.assertIn("COGNUS_RUNTIME_TOPOLOGY_NOT_RUNNING:apigateway:apigateway-org1", command)
		self.assertIn("label=cognus.run_id=$COGNUS_TOPOLOGY_GUARD_RUN_ID", command)
		self.assertIn("participation-detail.json", command)
		self.assertIn('"consensusRelation":"(consenter|follower)"', command)
		self.assertIn("COGNUS_CHANNEL_BLOCK_INVALID:library-channel", command)
		self.assertIn("COGNUS_CHANNEL_BLOCK_RECOVERED_VIA_TX:library-channel", command)
		self.assertIn("COGNUS_CHANNEL_BLOCK_RECOVERED_DIRECT:library-channel", command)
		self.assertIn("COGNUS_CHANNEL_BLOCK_CHANNEL_MISMATCH:library-channel", command)
		self.assertIn("COGNUS_CHANNEL_BLOCK_DIRECT_DIAG:", command)
		self.assertIn("COGNUS_CONFIGTX_SOURCE_MISSING:library-channel", command)
		self.assertIn("COGNUS_CONFIGTX_FALLBACK_APPLIED:library-channel:multi_consenter", command)
		self.assertIn("COGNUS_CONFIGTX_FALLBACK_APPLIED:library-channel:host_source_retry", command)
		self.assertIn("COGNUS_EFFECTIVE_ORDERER_MSPID", command)
		self.assertIn("COGNUS_PEER_CORE_LOCAL_MSPID", command)
		self.assertIn("COGNUS_PEER_SAMPLE_MSP=0", command)
		self.assertIn("COGNUS_ORDERER_SAMPLE_MSP=0", command)
		self.assertIn("if [ \"$COGNUS_PEER_SAMPLE_MSP\" = \"1\" ]; then COGNUS_ALT_LOCAL_MSPID=$COGNUS_PEER_LOCAL_MSPID;", command)
		self.assertIn("if [ \"$COGNUS_PEER_SAMPLE_MSP\" != \"1\" ] && ! docker exec \"$COGNUS_PEER_CONTAINER\" sh -lc \"$COGNUS_PEER_CMD_PREFIX peer lifecycle chaincode querycommitted -C library-channel", command)
		self.assertIn("core_yaml_local_mspid", command)
		self.assertIn("sample_msp_detected", command)
		self.assertIn("alt_local_msp_promoted", command)
		self.assertIn("s#SampleOrg#$COGNUS_EFFECTIVE_ORDERER_MSPID#g", command)
		self.assertIn("sudo -n cat \"$COGNUS_CC_CANDIDATE\" > \"$COGNUS_CC_STAGE\"", command)
		self.assertIn("COGNUS_PRIMARY_CHANNEL_VALIDATE", command)
		self.assertIn("COGNUS_ALT_CHANNEL_VALIDATE", command)
		self.assertIn("channel.block.direct.err", command)
		self.assertIn("for COGNUS_DIRECT_BLOCK_CANDIDATE in SampleSingleMSPChannel SampleSingleMSPSolo SampleDevModeSolo", command)
		self.assertIn("-e COGNUS_DIRECT_BLOCK_CANDIDATE=\"$COGNUS_DIRECT_BLOCK_CANDIDATE\"", command)
		self.assertIn("peer channel create -c library-channel -f '$COGNUS_PEER_TX' --outputBlock '$COGNUS_PEER_BLOCK'", command)
		self.assertIn("COGNUS_CHANNEL_FETCH_DIAG:", command)
		self.assertIn("COGNUS_CHANNEL_CREATE_DIAG:", command)
		self.assertIn("participation-post-retry.log", command)
		self.assertIn("/var/hyperledger/production/orderer/chains/library-channel", command)
		self.assertNotIn("/var/hyperledger/production/orderer/*", command)

	def test_fabric_failure_classifier_prioritizes_orderer_channel_state_before_chaincode(self):
		stderr = (
			"COGNUS_CHAINCODE_NOT_COMMITTED:library-channel:reserve-book-cc\n"
			"COGNUS_ORDERER_PARTICIPATION_JOIN_FAILED:library-channel\n"
			"COGNUS_LIFECYCLE_ERROR_CLASS:orderer_channel_state\n"
		)

		failure = runbook_views._classify_fabric_runtime_failure(stderr)
		self.assertIsNotNone(failure)
		self.assertEqual(failure[0], "runbook_fabric_orderer_channel_state_invalid")
		self.assertEqual(failure[2].get("channel_id"), "library-channel")

	def test_fabric_failure_classifier_flags_bad_request_as_orderer_state(self):
		failure = runbook_views._classify_fabric_runtime_failure(
			"Error: failed to send transaction: got unexpected status: BAD_REQUEST -- channel creation request not allowed"
		)

		self.assertIsNotNone(failure)
		self.assertEqual(failure[0], "runbook_fabric_orderer_channel_state_invalid")
		self.assertEqual(failure[2].get("lifecycle_error_class"), "orderer_channel_state")

	def test_configure_artifacts_command_apigateway_runs_chaincode_guard(self):
		command = runbook_views._build_ssh_remote_command(
			"configure",
			"configure.artifacts",
			{
				"run_id": "run-test-configure-cc",
				"change_id": "cr-test-configure-cc",
				"host_mapping": [
					{
						"node_id": "peer0-org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"channels": ["library-channel"],
							"chaincodes": ["reserve-book-cc"],
							"service_host_mapping": {
								"apiGateway": "machine1",
							},
						},
					],
				},
			},
			{
				"node_id": "apigateway-org1",
				"node_type": "apigateway",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "cognus/chaincode-gateway:latest",
			},
		)

		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED_DEFERRED:library-channel:reserve-book-cc", command)
		self.assertNotIn("COGNUS_CHANNEL_NOT_JOINED_DEFERRED:library-channel", command)
		self.assertNotIn("COGNUS_CHAINCODE_NOT_COMMITTED:library-channel:reserve-book-cc", command)
		self.assertIn("COGNUS_CONFIGURE_ARTIFACTS_OK", command)

	def test_apigateway_bootstrap_payload_uses_grpc_for_peer_without_tls(self):
		run_state = {
			"host_mapping": [
				{
					"node_id": "peer0-org1",
					"node_type": "peer",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
				},
				{
					"node_id": "orderer0-org1",
					"node_type": "orderer",
					"org_id": "org1",
					"host_ref": "machine1",
					"host_address": "10.0.0.10",
					"environment": [
						("ORDERER_GENERAL_TLS_ENABLED", "true"),
					],
				},
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"channels": ["library-channel"],
						"chaincodes": ["reserve-book-cc"],
					}
				],
			},
		}

		host_row = {
			"node_id": "apigateway-org1",
			"node_type": "apigateway",
			"org_id": "org1",
			"host_ref": "machine1",
			"host_address": "10.0.0.10",
		}

		payload = runbook_views._build_apigateway_bootstrap_payload(run_state, host_row)
		peers = payload.get("connection_profile", {}).get("peers", {})
		orderers = payload.get("connection_profile", {}).get("orderers", {})
		self.assertEqual(len(peers), 1)
		self.assertEqual(len(orderers), 1)
		peer_url = next(iter(peers.values())).get("url", "")
		orderer_entry = next(iter(orderers.values()))
		orderer_url = orderer_entry.get("url", "")
		self.assertTrue(peer_url.startswith("grpc://"))
		self.assertFalse(peer_url.startswith("grpcs://"))
		self.assertTrue(orderer_url.startswith("grpcs://"))
		self.assertEqual(
			orderer_entry.get("tlsCACerts", {}).get("path"),
			"/app/data/orderer-tlsca.pem",
		)
		self.assertEqual(
			payload.get("identities", {}).get("default", {}).get("mspId"),
			"Org1MSP",
		)
		self.assertEqual(
			orderer_entry.get("grpcOptions", {}).get("ssl-target-name-override"),
			"SampleOrg-orderer",
		)

	def test_gateway_verify_probe_checks_use_ccapi_search_for_committed_pairs(self):
		run_state = {
			"api_registry": [
				{
					"api_id": "api-1",
					"org_name": "org1",
					"channel_id": "library-channel",
					"chaincode_id": "reserve-book-cc",
					"route_path": "/api/library-channel/reserve-book-cc/*/*",
				}
			],
			"topology_catalog": {
				"organizations": [
					{
						"org_id": "org1",
						"channels": ["library-channel"],
						"chaincodes": ["reserve-book-cc"],
					}
				],
			},
		}
		host_row = {
			"node_id": "apigateway-org1",
			"node_type": "apigateway",
			"org_id": "org1",
			"host_ref": "machine1",
			"host_address": "10.0.0.10",
		}

		probe_spec = runbook_views._build_gateway_verify_probe_checks(
			run_state,
			host_row,
			committed_rows=[
				{
					"channel_id": "library-channel",
					"chaincode_id": "reserve-book-cc",
					"committed": True,
				},
				{
					"channel_id": "library-channel",
					"chaincode_id": "ignored-cc",
					"committed": False,
				},
			],
		)

		self.assertEqual(probe_spec["org_id"], "org1")
		self.assertEqual(probe_spec["checks"][0]["path"], "/health")
		self.assertEqual(probe_spec["checks"][0]["method"], "GET")
		self.assertEqual(len(probe_spec["checks"]), 2)
		self.assertEqual(
			probe_spec["checks"][1]["path"],
			"/api/library-channel/reserve-book-cc/query/search",
		)
		self.assertEqual(probe_spec["checks"][1]["method"], "POST")
		self.assertEqual(
			probe_spec["checks"][1]["headers"].get("X-Fabric-Org"),
			"org1",
		)
		self.assertEqual(
			probe_spec["checks"][1]["body"]["query"]["selector"]["@assetType"],
			"person",
		)
		self.assertEqual(
			probe_spec["checks"][1]["body"]["query"]["bookmark"],
			"",
		)

	def test_provision_runtime_command_sets_orderer_channel_participation_env(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-orderer-env",
				"change_id": "cr-test-orderer-env",
			},
			{
				"node_id": "orderer0-org1",
				"node_type": "orderer",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "hyperledger/fabric-orderer:2.5",
			},
		)

		self.assertIn("--env ORDERER_GENERAL_LISTENADDRESS=0.0.0.0", command)
		self.assertIn("--env ORDERER_GENERAL_BOOTSTRAPMETHOD=none", command)
		self.assertIn("--env ORDERER_CHANNELPARTICIPATION_ENABLED=true", command)
		self.assertIn("--env ORDERER_GENERAL_LOCALMSPID=Org1MSP", command)
		self.assertIn("--env ORDERER_GENERAL_LOCALMSPDIR=/etc/hyperledger/fabric/msp", command)
		self.assertIn("--env ORDERER_GENERAL_CLUSTER_LISTENPORT=7051", command)

	def test_resolve_topology_organization_msp_id_infers_from_org_alias_fields(self):
		resolved = runbook_views._resolve_topology_organization_msp_id(
			{
				"org_id": "inf-ufg",
				"org_name": "inf-ufg",
				"org_label": "org-inf-ufg",
			}
		)

		self.assertEqual(resolved, "InfUfgMSP")

	def test_resolve_canonical_org_id_accepts_org_label_alias(self):
		registry = runbook_views._build_topology_org_alias_registry(
			{
				"organizations": [
					{
						"org_id": "inf-ufg",
						"org_name": "inf-ufg",
						"org_label": "org-inf-ufg",
					}
				]
			}
		)

		self.assertEqual(
			runbook_views._resolve_canonical_org_id("org-inf-ufg", registry),
			"inf-ufg",
		)

	def test_provision_runtime_command_sets_peer_local_msp_env(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-peer-env",
				"change_id": "cr-test-peer-env",
			},
			{
				"node_id": "peer0-inf16",
				"node_type": "peer",
				"org_id": "inf16",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "hyperledger/fabric-peer:2.5",
			},
		)

		self.assertIn("--env CORE_PEER_LOCALMSPID=Org1MSP", command)
		self.assertIn("--env CORE_PEER_MSPCONFIGPATH=/etc/hyperledger/fabric/msp", command)
		self.assertIn(
			"--volume /tmp/cognus/crypto/inf16/peer0-inf16/msp:/etc/hyperledger/fabric/msp",
			command,
		)
		self.assertIn(
			"--volume /tmp/cognus/crypto/inf16/peer0-inf16/msp:/var/lib/cognus/msp",
			command,
		)
		self.assertIn(
			"/tmp/cognus/crypto/inf16/peer0-inf16/msp/signcerts/cert.pem",
			command,
		)
		self.assertIn(
			"/tmp/cognus/crypto/inf16/peer0-inf16/msp/admincerts/admincert.pem",
			command,
		)
		self.assertIn(
			"/tmp/cognus/crypto/inf16/peer0-inf16/msp/cacerts/cacert.pem",
			command,
		)

	def test_provision_runtime_command_sets_peer_couchdb_env(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-peer-couch",
				"change_id": "cr-test-peer-couch",
				"host_mapping": [
					{
						"node_id": "couch-inf16",
						"node_type": "couch",
						"org_id": "inf16",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
					{
						"node_id": "peer0-inf16",
						"node_type": "peer",
						"org_id": "inf16",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
					},
				],
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "inf16",
							"service_parameters": {
								"couch": {
									"admin_user": "couchdb",
									"admin_password_ref": "vault://couch/inf16/admin",
								}
							},
						}
					]
				},
			},
			{
				"node_id": "peer0-inf16",
				"node_type": "peer",
				"org_id": "inf16",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "hyperledger/fabric-peer:2.5",
			},
		)

		self.assertIn("--env CORE_LEDGER_STATE_STATEDATABASE=CouchDB", command)
		self.assertIn(
			"--env CORE_LEDGER_STATE_COUCHDBCONFIG_COUCHDBADDRESS=cognusrb-cr-test-peer-couch-run-test-peer-couch-couch-inf16:5984",
			command,
		)
		self.assertIn("--env CORE_LEDGER_STATE_COUCHDBCONFIG_USERNAME=couchdb", command)
		self.assertIn(
			"--env CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD={}".format(
				runbook_views.RUNBOOK_COUCHDB_ADMIN_PASSWORD
			),
			command,
		)

	def test_resolve_topology_organization_for_host_infers_msp_id_when_missing(self):
		organization = runbook_views._resolve_topology_organization_for_host(
			{
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "inf16",
							"org_name": "inf16",
						}
					]
				}
			},
			{
				"node_id": "peer0-inf16",
				"node_type": "peer",
				"org_id": "inf16",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
			},
		)

		self.assertEqual(organization.get("msp_id"), "Org1MSP")

	def test_provision_runtime_command_sets_couchdb_admin_credentials(self):
		command = runbook_views._build_ssh_remote_command(
			"provision",
			"provision.runtime",
			{
				"run_id": "run-test-couch-env",
				"change_id": "cr-test-couch-env",
				"topology_catalog": {
					"organizations": [
						{
							"org_id": "org1",
							"service_parameters": {
								"couch": {
									"admin_user": "couch-admin",
									"admin_password_ref": "vault://couch/org1/admin",
								}
							},
						}
					]
				},
			},
			{
				"node_id": "couch-org1",
				"node_type": "couch",
				"org_id": "org1",
				"host_ref": "machine1",
				"host_address": "10.0.0.10",
				"runtime_image": "couchdb:3.2.2",
			},
		)

		self.assertIn("--env COUCHDB_USER=couch-admin", command)
		self.assertIn(
			"--env COUCHDB_PASSWORD={}".format(
				runbook_views.RUNBOOK_COUCHDB_ADMIN_PASSWORD
			),
			command,
		)

	def test_advance_returns_runtime_image_missing_when_cognus_image_is_unavailable(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
							"runtime_image": "cognus/chaincode-gateway:latest",
					}
				],
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(3):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		self.mock_ssh_returncode = 125
		self.mock_ssh_stdout = ""
		self.mock_ssh_stderr = "COGNUS_RUNTIME_IMAGE_MISSING:cognus/chaincode-gateway:latest"

		with patch.object(
			runbook_views,
			"_ensure_remote_docker_ready",
			return_value={
				"attempted": False,
				"status": "ready",
				"stdout": "Docker version 25.0.0",
				"stderr": "",
				"returncode": 0,
			},
		):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
		self.assertEqual(operate_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(
			operate_response.data["msg"],
			"runbook_runtime_image_local_unavailable",
		)
		self.assertEqual(
			operate_response.data["data"]["details"]["runtime_image"],
			"cognus/chaincode-gateway:latest",
		)

	def test_advance_retries_provision_runtime_after_seeding_cognus_runtime_image(self):
		runtime_missing_once = {"triggered": False}

		def _mock_run_with_runtime_seed(command, **kwargs):
			self.ssh_commands.append({"command": command, "kwargs": kwargs})
			if isinstance(command, list) and command[:3] == ["docker", "image", "inspect"]:
				return SimpleNamespace(returncode=0, stdout="", stderr="")
			if (
				isinstance(command, list)
				and len(command) >= 3
				and command[0] == "/bin/bash"
				and command[1] == "-lc"
				and "docker image save cognus/chaincode-gateway:latest | ssh" in command[2]
			):
				return SimpleNamespace(returncode=0, stdout="", stderr="")
			if isinstance(command, list) and command and command[0] == "ssh":
				remote_command = str(command[-1] or "")
				if (
					"COGNUS_PROVISION_RUNTIME_OK" in remote_command
					and not runtime_missing_once["triggered"]
				):
					runtime_missing_once["triggered"] = True
					return SimpleNamespace(
						returncode=125,
						stdout="",
						stderr="COGNUS_RUNTIME_IMAGE_MISSING:cognus/chaincode-gateway:latest",
					)
				return SimpleNamespace(
					returncode=0,
					stdout="COGNUS_PROVISION_RUNTIME_OK\n",
					stderr="",
				)
			return SimpleNamespace(returncode=0, stdout="", stderr="")

		runbook_views.subprocess.run.side_effect = _mock_run_with_runtime_seed

		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
							"runtime_image": "cognus/chaincode-gateway:latest",
					}
				],
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		with patch.object(runbook_views, "RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED", True), patch.object(
			runbook_views,
			"RUNBOOK_RUNTIME_IMAGE_SEED_ENABLED_NODE_TYPES",
			("apigateway",),
		):
			for _ in range(4):
				operate_response = self.client.post(
					reverse("runbook-operate"),
					{"run_id": run_id, "action": "advance"},
					format="json",
				)

		self.assertEqual(operate_response.status_code, status.HTTP_200_OK)
		self.assertTrue(runtime_missing_once["triggered"])
		seed_commands = [
			row["command"]
			for row in self.ssh_commands
			if isinstance(row, dict)
			and isinstance(row.get("command"), list)
			and len(row.get("command") or []) >= 3
			and row["command"][0] == "/bin/bash"
			and row["command"][1] == "-lc"
			and "docker image save cognus/chaincode-gateway:latest | ssh" in row["command"][2]
		]
		self.assertGreaterEqual(len(seed_commands), 1)

	def test_start_normalizes_legacy_ccapi_runtime_image_to_chaincode_gateway(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "apigateway-org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
						"runtime_image": "cognus/ccapi-go:latest",
					}
				],
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_201_CREATED)
		host_mapping = response.data["data"]["run"]["host_mapping"]
		self.assertEqual(host_mapping[0]["runtime_image"], "cognus/chaincode-gateway:latest")

	def test_advance_returns_memory_pressure_error_when_runtime_guard_is_triggered(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				topology_catalog={},
				host_mapping=[
					{
						"node_id": "chaincode-org1",
						"node_type": "chaincode",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
						"runtime_image": "hyperledger/fabric-ccenv:2.5",
					}
				],
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(3):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		self.mock_ssh_returncode = 137
		self.mock_ssh_stdout = ""
		self.mock_ssh_stderr = "COGNUS_RUNTIME_MEMORY_LOW:chaincode:512:131072"
		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(operate_response.data["msg"], "runbook_runtime_memory_pressure")
		self.assertEqual(
			operate_response.data["data"]["details"]["required_memory_mb"],
			512,
		)

	def test_status_sanitizes_local_timeline_events_in_official_mode(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		store_payload["runs"][run_id]["events"].append(
			{
				"id": "evt-local-1",
				"timestamp_utc": "2026-02-17T12:00:00Z",
				"level": "info",
				"code": "runbook_started_local",
				"message": "Evento local simulado.",
				"run_id": run_id,
				"change_id": "cr-a1-7-2-001",
				"stage": "prepare",
				"checkpoint": "prepare.preflight",
			}
		)
		runbook_views._save_store(store_payload)

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		event_codes = [
			event["code"] for event in status_response.data["data"]["run"]["events"]
		]
		self.assertFalse(any("_local" in code for code in event_codes))
		self.assertIn("runbook_official_timeline_sanitized", event_codes)

	def test_status_exposes_a2a_entry_gate_with_org_readiness(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		a2a_entry_gate = status_response.data["data"]["run"]["a2a_entry_gate"]
		self.assertEqual(a2a_entry_gate["contract_version"], "a2a-entry-gate.v1")
		self.assertIn(a2a_entry_gate["status"], {"pending", "partial"})
		self.assertEqual(a2a_entry_gate["mode"], "read_only_blocked")
		self.assertFalse(a2a_entry_gate["action_availability"]["add_peer"])
		self.assertTrue(a2a_entry_gate["checks"]["handoff_correlation_valid"])
		self.assertGreaterEqual(len(a2a_entry_gate["organization_readiness"]), 1)
		self.assertEqual(
			a2a_entry_gate["organization_readiness"][0]["organization_id"],
			"org1",
		)

	def test_status_blocks_a2a_entry_gate_when_handoff_correlation_diverges(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		store_payload["runs"][run_id]["handoff_payload"]["correlation"][
			"manifest_fingerprint"
		] = "ff" * 32
		runbook_views._save_store(store_payload)

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		a2a_entry_gate = status_response.data["data"]["run"]["a2a_entry_gate"]
		self.assertEqual(a2a_entry_gate["status"], "blocked")
		self.assertEqual(a2a_entry_gate["mode"], "read_only_blocked")
		self.assertFalse(a2a_entry_gate["checks"]["handoff_correlation_valid"])
		issue_codes = [issue["code"] for issue in a2a_entry_gate["issues"]]
		self.assertIn("runbook_a2a_handoff_correlation_mismatch", issue_codes)
		self.assertFalse(a2a_entry_gate["action_availability"]["add_channel"])

	def test_status_blocks_a2a_entry_gate_when_a2_3_readiness_is_unsatisfied(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				a2_3_handoff={
					"handoff_decision": "allow",
					"change_id": "cr-a1-7-2-001",
					"run_id": "run-a1-7-2-001",
					"manifest_fingerprint": "cd" * 32,
					"source_blueprint_fingerprint": "ef" * 32,
				},
				a2_3_readiness_checklist={
					"change_id": "cr-a1-7-2-001",
					"run_id": "run-a1-7-2-001",
					"manifest_fingerprint": "cd" * 32,
					"source_blueprint_fingerprint": "ef" * 32,
					"checklist": {
						"runtime_converged": True,
						"ready_for_a2_4_a2_5": False,
					},
				},
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		a2a_entry_gate = status_response.data["data"]["run"]["a2a_entry_gate"]
		self.assertEqual(a2a_entry_gate["status"], "blocked")
		self.assertFalse(a2a_entry_gate["checks"]["a2_3_readiness_valid"])
		self.assertFalse(a2a_entry_gate["action_availability"]["open_operational_dashboard"])
		issue_codes = [issue["code"] for issue in a2a_entry_gate["issues"]]
		self.assertIn("runbook_a2a_a2_3_readiness_not_satisfied", issue_codes)

	def test_status_exposes_runtime_telemetry_contract_with_redacted_env(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		runtime_telemetry = status_response.data["data"]["run"]["runtime_telemetry"]
		self.assertEqual(
			runtime_telemetry["contract_version"],
			"a2a-runtime-telemetry.v1",
		)
		self.assertEqual(runtime_telemetry["source_of_truth"], "official_backend_artifacts")
		self.assertEqual(runtime_telemetry["correlation"]["run_id"], run_id)
		self.assertGreaterEqual(len(runtime_telemetry["organizations"]), 1)

		organization_payload = runtime_telemetry["organizations"][0]
		self.assertEqual(organization_payload["organization"]["org_id"], "org1")
		self.assertIn("components", organization_payload)
		self.assertIn("artifacts", organization_payload)
		self.assertIn("data_freshness", organization_payload)

		peer_component = next(
			component
			for component in organization_payload["components"]
			if component["component_type"] == "peer"
		)
		self.assertIn(peer_component["status"], {"running", "unknown", "degraded"})
		self.assertEqual(peer_component["required_state"], "running")
		self.assertIn("observed_state", peer_component)
		self.assertIn("ports", peer_component)
		self.assertIn("mounts", peer_component)
		self.assertIn("env", peer_component)

		redacted_env = next(
			env_row
			for env_row in peer_component["env"]
			if env_row["key"] == "CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD"
		)
		self.assertTrue(redacted_env["value_redacted"])
		self.assertEqual(redacted_env["value"], "")
		self.assertTrue(redacted_env["value_digest"])

	def test_status_exposes_official_organization_read_model_with_workspace_blocks(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				a2_2_available_artifacts=[
					"provision-plan",
					"reconcile-report",
					"inventory-final",
					"stage-reports/prepare-report",
					"stage-reports/verify-report",
					"runtime-reconcile-report",
					"api-smoke-report",
					"ssh-execution-log",
				],
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
					{
						"node_id": "orderer0.org1",
						"node_type": "orderer",
						"org_id": "org1",
						"host_ref": "host-org1-orderer0",
						"host_address": "10.0.0.11",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
					{
						"node_id": "ca.org1",
						"node_type": "ca",
						"org_id": "org1",
						"host_ref": "host-org1-ca",
						"host_address": "10.0.0.12",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
					{
						"node_id": "api.org1",
						"node_type": "apigateway",
						"org_id": "org1",
						"host_ref": "host-org1-api",
						"host_address": "10.0.0.13",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
					{
						"node_id": "netapi.org1",
						"node_type": "netapi",
						"org_id": "org1",
						"host_ref": "host-org1-netapi",
						"host_address": "10.0.0.14",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
				],
				machine_credentials=[
					{
						"machine_id": "host-org1-peer0",
						"credential_ref": "vault://infra/keys/host-org1-peer0",
						"credential_fingerprint": "fp-host-org1-peer0",
						"reuse_confirmed": False,
					},
					{
						"machine_id": "host-org1-orderer0",
						"credential_ref": "vault://infra/keys/host-org1-orderer0",
						"credential_fingerprint": "fp-host-org1-orderer0",
						"reuse_confirmed": False,
					},
					{
						"machine_id": "host-org1-ca",
						"credential_ref": "vault://infra/keys/host-org1-ca",
						"credential_fingerprint": "fp-host-org1-ca",
						"reuse_confirmed": False,
					},
					{
						"machine_id": "host-org1-api",
						"credential_ref": "vault://infra/keys/host-org1-api",
						"credential_fingerprint": "fp-host-org1-api",
						"reuse_confirmed": False,
					},
					{
						"machine_id": "host-org1-netapi",
						"credential_ref": "vault://infra/keys/host-org1-netapi",
						"credential_fingerprint": "fp-host-org1-netapi",
						"reuse_confirmed": False,
					},
				],
				topology_catalog={
					"organizations": [
						{
							"org_id": "org1",
							"org_name": "Org1",
							"channels": [
								{
									"channel_id": "stcs-channel",
									"member_orgs": ["Org1", "Org2"],
									"chaincodes": ["stcs-cc"],
								}
							],
							"chaincodes": ["stcs-cc"],
							"peers": [
								{
									"node_id": "peer0.org1",
									"host_ref": "host-org1-peer0",
									"desired_state": "required",
								}
							],
							"orderers": [
								{
									"node_id": "orderer0.org1",
									"host_ref": "host-org1-orderer0",
									"desired_state": "required",
								}
							],
							"cas": [
								{
									"node_id": "ca.org1",
									"host_ref": "host-org1-ca",
									"desired_state": "required",
								}
							],
						}
					],
					"channels": [
						{
							"channel_id": "stcs-channel",
							"member_orgs": ["Org1", "Org2"],
							"chaincodes": ["stcs-cc"],
						}
					],
					"chaincodes": [
						{
							"channel_id": "stcs-channel",
							"chaincode_id": "stcs-cc",
							"artifact_ref": f"local-file:{self.default_chaincode_artifact}",
						}
					],
					"business_groups": [
						{
							"group_id": "bg-org1",
							"name": "BG Teste",
							"channels": [
								{
									"channel_id": "stcs-channel",
									"member_orgs": ["Org1", "Org2"],
								}
							],
						}
					],
				},
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)

		organization_read_model = status_response.data["data"]["run"]["organization_read_model"]
		self.assertEqual(
			organization_read_model["contract_version"],
			"a2a-organization-read-model.v1",
		)
		self.assertEqual(organization_read_model["source_of_truth"], "official_backend_artifacts")
		self.assertEqual(organization_read_model["correlation"]["run_id"], run_id)
		self.assertTrue(organization_read_model["read_model_fingerprint"])
		self.assertGreaterEqual(len(organization_read_model["organizations"]), 1)

		organization_workspace = organization_read_model["organizations"][0]
		self.assertEqual(organization_workspace["organization"]["org_id"], "org1")
		self.assertTrue(organization_workspace["read_model_fingerprint"])
		self.assertIn("workspace", organization_workspace)

		workspace_blocks = organization_workspace["workspace"]["blocks"]
		self.assertIn("ca", workspace_blocks)
		self.assertIn("api", workspace_blocks)
		self.assertIn("peers", workspace_blocks)
		self.assertIn("orderers", workspace_blocks)
		self.assertIn("business_group", workspace_blocks)
		self.assertIn("channels", workspace_blocks)
		self.assertEqual(workspace_blocks["business_group"]["items"][0]["name"], "BG Teste")
		self.assertEqual(workspace_blocks["channels"]["health"], "degraded")
		self.assertGreaterEqual(workspace_blocks["api"]["item_total"], 1)

		channel_projection = organization_workspace["workspace"]["projections"]["channels"][0]
		self.assertEqual(channel_projection["channel_id"], "stcs-channel")
		self.assertEqual(channel_projection["chaincode_total"], 1)
		self.assertEqual(channel_projection["member_orgs"], ["Org1", "Org2"])

		member_projection = organization_workspace["workspace"]["projections"]["organization_members"][0]
		self.assertEqual(member_projection["membership_role"], "local_org")
		self.assertEqual(member_projection["channel_total"], 1)

		chaincode_projection = organization_workspace["workspace"]["projections"]["chaincodes"][0]
		self.assertEqual(chaincode_projection["chaincode_id"], "stcs-cc")
		self.assertEqual(chaincode_projection["api_total"], 2)

		artifact_names = [row["artifact"] for row in organization_workspace["artifact_origins"]]
		self.assertIn("inventory-final", artifact_names)
		self.assertIn("verify-report", artifact_names)
		self.assertIn("runtime-reconcile-report", artifact_names)
		self.assertIn("api-smoke-report", artifact_names)
		self.assertTrue(
			next(
				row
				for row in organization_workspace["artifact_origins"]
				if row["artifact"] == "api-smoke-report"
			)["available"]
		)

	def _resolve_runtime_inspection_target(self, run_id):
		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)
		runtime_telemetry = status_response.data["data"]["run"]["runtime_telemetry"]
		organization_payload = runtime_telemetry["organizations"][0]
		peer_component = next(
			component
			for component in organization_payload["components"]
			if component["component_type"] == "peer"
		)
		return {
			"org_id": organization_payload["organization"]["org_id"],
			"host_id": peer_component["host_id"],
			"component_id": peer_component["component_id"],
		}

	def test_runtime_inspection_uses_official_cache_without_redundant_remote_polling(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]
		target = self._resolve_runtime_inspection_target(run_id)

		remote_calls = []

		def remote_side_effect(host, user, port, command, identity_file):
			remote_calls.append(command)
			if "docker ps -a" in command:
				return {"returncode": 0, "stdout": "peer0.org1\n", "stderr": ""}
			if "docker inspect --type container" in command:
				return {
					"returncode": 0,
					"stdout": '[{"Id":"abcdef1234567890","Created":"2025-01-01T00:00:00Z","Config":{"Image":"hyperledger/fabric-peer:2.5","Env":["CORE_VM_ENDPOINT=unix:///host/var/run/docker.sock","CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD=super-secret"],"Entrypoint":["peer"],"Cmd":["node","start"],"Labels":{"cognus.node_id":"peer0.org1"}},"State":{"Status":"running","Running":true,"Paused":false,"Restarting":false,"OOMKilled":false,"ExitCode":0,"StartedAt":"2025-01-01T00:00:05Z","FinishedAt":"0001-01-01T00:00:00Z"},"RestartCount":1,"Mounts":[{"Source":"/var/hyperledger/production","Destination":"/var/hyperledger/production","Mode":"rw","RW":true,"Type":"bind"}],"NetworkSettings":{"Ports":{"7051/tcp":[{"HostIp":"0.0.0.0","HostPort":"17051"}]},"Networks":{"fabric_test":{"IPAddress":"172.16.0.10"}}}}]',
					"stderr": "",
				}
			if "docker logs --tail" in command:
				return {
					"returncode": 0,
					"stdout": "peer node started\nlistening on 7051\n",
					"stderr": "",
				}
			return {"returncode": 1, "stdout": "", "stderr": "unexpected command"}

		with patch.object(runbook_views, "_run_remote_command", side_effect=remote_side_effect):
			first_response = self.client.get(
				reverse("runbook-runtime-inspection", kwargs={"pk": run_id}),
				target,
			)
			self.assertEqual(first_response.status_code, status.HTTP_200_OK)
			first_inspection = first_response.data["data"]["inspection"]
			self.assertFalse(first_inspection["stale"])
			self.assertTrue(first_inspection["scopes"]["docker_inspect"]["cache"]["cache_miss"])
			self.assertFalse(first_inspection["scopes"]["docker_inspect"]["cache"]["cache_hit"])
			self.assertEqual(
				first_inspection["scopes"]["docker_inspect"]["cache"]["inspection_source"],
				"remote_docker_inspect",
			)
			self.assertEqual(
				first_inspection["scopes"]["environment"]["payload"]["env"][1]["key"],
				"CORE_VM_ENDPOINT",
			)
			self.assertTrue(
				next(
					row
					for row in first_inspection["scopes"]["environment"]["payload"]["env"]
					if row["key"] == "CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD"
				)["value_redacted"]
			)

			second_response = self.client.get(
				reverse("runbook-runtime-inspection", kwargs={"pk": run_id}),
				target,
			)
			self.assertEqual(second_response.status_code, status.HTTP_200_OK)
			second_inspection = second_response.data["data"]["inspection"]
			self.assertTrue(second_inspection["scopes"]["docker_inspect"]["cache"]["cache_hit"])
			self.assertFalse(second_inspection["scopes"]["docker_inspect"]["cache"]["cache_miss"])

		self.assertEqual(len(remote_calls), 4)

	def test_runtime_inspection_marks_expired_cache_as_stale(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]
		target = self._resolve_runtime_inspection_target(run_id)

		store_payload = runbook_views._load_store()
		run_state = store_payload["runs"][run_id]
		cache_key = runbook_views._runtime_inspection_cache_key(
			target["org_id"],
			target["host_id"],
			target["component_id"],
			"docker_inspect",
		)
		runbook_views._upsert_runtime_inspection_cache(
			run_state,
			cache_key,
			{
				"org_id": target["org_id"],
				"host_id": target["host_id"],
				"component_id": target["component_id"],
				"inspection_scope": "docker_inspect",
				"refreshed_at": "2000-01-01T00:00:00Z",
				"last_successful_refreshed_at": "2000-01-01T00:00:00Z",
				"inspection_source": "remote_docker_inspect",
				"collection_status": "ready",
				"payload": {"container_name": "peer0.org1", "container_id": "abcdef123456"},
				"payload_hash": "stale-hash",
				"last_error": {},
			},
		)
		store_payload["runs"][run_id] = run_state
		runbook_views._save_store(store_payload)

		response = self.client.get(
			reverse("runbook-runtime-inspection", kwargs={"pk": run_id}),
			{
				**target,
				"inspection_scope": "docker_inspect",
			},
		)
		self.assertEqual(response.status_code, status.HTTP_200_OK)
		inspection = response.data["data"]["inspection"]
		self.assertTrue(inspection["stale"])
		self.assertTrue(inspection["scopes"]["docker_inspect"]["cache"]["stale"])
		self.assertTrue(inspection["scopes"]["docker_inspect"]["cache"]["cache_hit"])
		self.assertEqual(
			inspection["scopes"]["docker_inspect"]["payload"]["container_name"],
			"peer0.org1",
		)

	def test_runtime_inspection_refresh_failure_preserves_last_successful_snapshot(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]
		target = self._resolve_runtime_inspection_target(run_id)

		def remote_success(host, user, port, command, identity_file):
			if "docker ps -a" in command:
				return {"returncode": 0, "stdout": "peer0.org1\n", "stderr": ""}
			if "docker inspect --type container" in command:
				return {
					"returncode": 0,
					"stdout": '[{"Id":"abcdef1234567890","Created":"2025-01-01T00:00:00Z","Config":{"Image":"hyperledger/fabric-peer:2.5","Env":[],"Entrypoint":["peer"],"Cmd":["node","start"],"Labels":{"cognus.node_id":"peer0.org1"}},"State":{"Status":"running","Running":true,"Paused":false,"Restarting":false,"OOMKilled":false,"ExitCode":0,"StartedAt":"2025-01-01T00:00:05Z","FinishedAt":"0001-01-01T00:00:00Z"},"RestartCount":0,"Mounts":[],"NetworkSettings":{"Ports":{},"Networks":{}}}]',
					"stderr": "",
				}
			return {"returncode": 1, "stdout": "", "stderr": "unexpected command"}

		with patch.object(runbook_views, "_run_remote_command", side_effect=remote_success):
			seed_response = self.client.get(
				reverse("runbook-runtime-inspection", kwargs={"pk": run_id}),
				{
					**target,
					"inspection_scope": "docker_inspect",
				},
			)
			self.assertEqual(seed_response.status_code, status.HTTP_200_OK)

		def remote_failure(host, user, port, command, identity_file):
			if "docker ps -a" in command:
				return {"returncode": 0, "stdout": "peer0.org1\n", "stderr": ""}
			if "docker inspect --type container" in command:
				return {"returncode": 1, "stdout": "", "stderr": "permission denied"}
			return {"returncode": 1, "stdout": "", "stderr": "unexpected command"}

		with patch.object(runbook_views, "_run_remote_command", side_effect=remote_failure):
			refresh_response = self.client.get(
				reverse("runbook-runtime-inspection", kwargs={"pk": run_id}),
				{
					**target,
					"inspection_scope": "docker_inspect",
					"refresh": "true",
				},
			)

		self.assertEqual(refresh_response.status_code, status.HTTP_200_OK)
		inspection = refresh_response.data["data"]["inspection"]
		docker_inspect_scope = inspection["scopes"]["docker_inspect"]
		self.assertTrue(inspection["stale"])
		self.assertTrue(docker_inspect_scope["cache"]["stale"])
		self.assertEqual(docker_inspect_scope["cache"]["collection_status"], "failed")
		self.assertEqual(
			docker_inspect_scope["payload"]["container_id"],
			"abcdef123456",
		)
		self.assertEqual(
			docker_inspect_scope["cache"]["last_error"]["code"],
			"runbook_runtime_inspection_remote_failed",
		)

		store_payload = runbook_views._load_store()
		run_state = store_payload["runs"][run_id]
		trail_rows = run_state.get("runtime_inspection_trail", [])
		self.assertGreaterEqual(len(trail_rows), 2)
		self.assertIn("cache_hit", trail_rows[-1])
		self.assertIn("cache_miss", trail_rows[-1])
		self.assertIn("stale", trail_rows[-1])
		self.assertIn("inspection_source", trail_rows[-1])

	def test_store_persists_pipeline_run_checkpoints_and_versioned_artifacts(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		advance_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)
		self.assertEqual(advance_response.status_code, status.HTTP_200_OK)

		store_payload = runbook_views._load_store()
		run_state = store_payload["runs"][run_id]
		self.assertEqual(run_state["run_id"], run_id)
		self.assertEqual(run_state["change_id"], "cr-a1-7-2-001")
		self.assertEqual(run_state["blueprint_fingerprint"], "ab" * 32)
		self.assertEqual(run_state["resolved_schema_version"], "1.0.0")
		self.assertIn("status", run_state)
		self.assertIn("updated_at_utc", run_state)

		persisted_checkpoints = store_payload["checkpoints"][run_id]
		self.assertGreaterEqual(len(persisted_checkpoints), 2)
		for checkpoint in persisted_checkpoints:
			self.assertIn("idempotency_key", checkpoint)
			self.assertIn("attempt", checkpoint)
			self.assertIn("input_hash", checkpoint)
			self.assertIn("output_hash", checkpoint)
			self.assertIn("executor", checkpoint)
			self.assertIn("timestamp_utc", checkpoint)

		artifacts = store_payload["artifacts"][run_id]
		artifact_groups = {artifact["artifact_group"] for artifact in artifacts}
		self.assertIn("stage-reports", artifact_groups)
		self.assertIn("inventory-final", artifact_groups)
		self.assertIn("pipeline-report", artifact_groups)
		self.assertIn("decision-trace", artifact_groups)
		self.assertIn("history", artifact_groups)
		artifact_key_set = {artifact["artifact_key"] for artifact in artifacts}
		self.assertIn("decision-trace:decision-trace.jsonl", artifact_key_set)
		self.assertIn("history:history.jsonl", artifact_key_set)
		self.assertTrue(all(artifact["version"] >= 1 for artifact in artifacts))

		decision_rows = [
			artifact
			for artifact in artifacts
			if artifact.get("artifact_key") == "decision-trace:decision-trace.jsonl"
		]
		self.assertGreaterEqual(len(decision_rows), 1)
		latest_decision = sorted(
			decision_rows,
			key=lambda row: row.get("version", 0),
		)[-1]
		self.assertIn("decision_code", latest_decision["payload"])
		self.assertIn("required_evidence_keys", latest_decision["payload"])
		self.assertIn("missing_evidence_keys", latest_decision["payload"])
		self.assertIn("evidence_minimum_valid", latest_decision["payload"])

	def test_start_idempotency_does_not_duplicate_persisted_side_effects(self):
		endpoint = reverse("runbook-start")

		first_response = self.client.post(endpoint, self._start_payload(), format="json")
		self.assertEqual(first_response.status_code, status.HTTP_201_CREATED)
		run_id = first_response.data["data"]["run"]["run_id"]

		store_after_first = runbook_views._load_store()
		first_checkpoint_count = len(store_after_first["checkpoints"][run_id])
		first_artifact_count = len(store_after_first["artifacts"][run_id])

		second_response = self.client.post(endpoint, self._start_payload(), format="json")
		self.assertEqual(second_response.status_code, status.HTTP_200_OK)

		store_after_second = runbook_views._load_store()
		self.assertEqual(
			len(store_after_second["checkpoints"][run_id]),
			first_checkpoint_count,
		)
		self.assertEqual(
			len(store_after_second["artifacts"][run_id]),
			first_artifact_count,
		)

	def test_load_store_migrates_legacy_tmp_store_to_durable_path(self):
		legacy_payload = {
			"store_version": runbook_views.RUNBOOK_STORE_SCHEMA_VERSION,
			"runs": {"run-legacy": {"run_id": "run-legacy", "status": "completed"}},
			"checkpoints": {},
			"artifacts": {},
			"resource_locks": {},
			"access_audit": [],
		}
		runbook_views.RUNBOOK_LEGACY_STORE_FILE.write_text(
			runbook_views.json.dumps(legacy_payload, ensure_ascii=True, indent=2),
			encoding="utf-8",
		)

		loaded_payload = runbook_views._load_store()

		self.assertIn("run-legacy", loaded_payload["runs"])
		self.assertTrue(runbook_views.RUNBOOK_STORE_FILE.exists())
		self.assertEqual(
			runbook_views.RUNBOOK_STORE_FILE.read_text(encoding="utf-8"),
			runbook_views.RUNBOOK_LEGACY_STORE_FILE.read_text(encoding="utf-8"),
		)

	def test_verify_fails_when_persisted_checkpoint_evidence_is_inconsistent(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(7):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		store_payload = runbook_views._load_store()
		store_payload["checkpoints"][run_id] = [
			checkpoint
			for checkpoint in store_payload["checkpoints"][run_id]
			if not (
				checkpoint.get("stage") == "prepare"
				and checkpoint.get("checkpoint") == "prepare.preflight"
				and checkpoint.get("status") == "completed"
			)
		]
		runbook_views._save_store(store_payload)

		final_advance_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)
		self.assertEqual(final_advance_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(
			final_advance_response.data["msg"],
			"runbook_verify_artifact_inconsistent",
		)

	def test_start_rejects_sensitive_credentials_in_host_mapping(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_private_key": "-----BEGIN PRIVATE KEY-----",
					}
				]
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "runbook_sensitive_credential_forbidden")

	def test_start_rejects_incomplete_runtime_topology_mapping(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"node_type": "peer",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					}
				],
				topology_catalog={
					"organizations": [
						{
							"org_id": "org1",
							"org_name": "Org1",
							"service_host_mapping": {
								"peer": "host-org1-peer0",
								"couch": "host-org1-peer0",
								"apiGateway": "host-org1-peer0",
								"netapi": "host-org1-peer0",
							},
							"chaincodes": ["stcs-cc"],
						}
					]
				},
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "runbook_topology_runtime_mapping_incomplete")
		missing_components = response.data["data"]["details"]["missing_components"]
		self.assertTrue(any(row.get("node_type") == "couch" for row in missing_components))
		self.assertTrue(any(row.get("node_type") == "chaincode" for row in missing_components))

	def test_start_accepts_host_mapping_org_alias_for_topology_coverage(self):
		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0-inf2",
						"node_type": "peer",
						"org_id": "inf2",
						"host_ref": "machine1",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"ssh_port": 22,
						"preflight_status": "apto",
					},
				],
				machine_credentials=[
					{
						"machine_id": "machine1",
						"credential_ref": "vault://infra/keys/machine1",
						"credential_fingerprint": "fp-machine1",
						"reuse_confirmed": False,
					},
				],
				topology_catalog={
					"organizations": [
						{
							"org_id": "org-inf2",
							"org_name": "inf2",
							"peers": [
								{
									"node_id": "peer0-inf2",
									"host_ref": "machine1",
								}
							],
						}
					]
				},
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_201_CREATED)

	def test_advance_blocks_when_runtime_preflight_fails(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ubuntu",
						"preflight_status": "bloqueado",
					}
				]
			),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(operate_response.data["msg"], "runbook_runtime_check_failed")
		self.assertEqual(len(self.ssh_commands), 0)

		store_payload = runbook_views._load_store()
		self.assertEqual(store_payload["runs"][run_id]["status"], "failed")

	def test_start_normalizes_command_like_ssh_user_payload(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				host_mapping=[
					{
						"node_id": "peer0.org1",
						"org_id": "org1",
						"host_ref": "host-org1-peer0",
						"host_address": "10.0.0.10",
						"ssh_user": "ssh -p 21525 web3",
						"ssh_port": 22,
						"preflight_status": "apto",
					}
				]
			),
			format="json",
		)

		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]
		store_payload = runbook_views._load_store()
		host_row = store_payload["runs"][run_id]["host_mapping"][0]
		self.assertEqual(host_row["ssh_user"], "web3")
		self.assertEqual(host_row["ssh_port"], 21525)

	def test_advance_normalizes_legacy_command_like_ssh_user(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		store_payload["runs"][run_id]["host_mapping"][0]["ssh_user"] = "ssh -p 21525 web3"
		store_payload["runs"][run_id]["host_mapping"][0]["ssh_port"] = 22
		runbook_views._save_store(store_payload)

		advance_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)

		self.assertEqual(advance_response.status_code, status.HTTP_200_OK)
		self.assertGreaterEqual(len(self.ssh_commands), 1)
		ssh_command = self.ssh_commands[0]["command"]
		self.assertIn("web3@10.0.0.10", ssh_command)
		self.assertIn("-p", ssh_command)
		self.assertIn("21525", ssh_command)

	def test_advance_rejects_resource_lock_conflict(self):
		start_run_1 = self.client.post(
			reverse("runbook-start"),
			self._start_payload(run_id="run-lock-1"),
			format="json",
		)
		self.assertEqual(start_run_1.status_code, status.HTTP_201_CREATED)

		start_run_2 = self.client.post(
			reverse("runbook-start"),
			self._start_payload(
				run_id="run-lock-2",
				blueprint_fingerprint="cd" * 32,
			),
			format="json",
		)
		self.assertEqual(start_run_2.status_code, status.HTTP_201_CREATED)

		store_payload = runbook_views._load_store()
		store_payload.setdefault("resource_locks", {})["host:10.0.0.10"] = {
			"run_id": "run-lock-1",
			"change_id": "cr-a1-7-2-001",
			"stage": "prepare",
			"acquired_at_utc": "2026-02-17T00:00:00Z",
		}
		runbook_views._save_store(store_payload)

		operate_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": "run-lock-2", "action": "advance"},
			format="json",
		)

		self.assertEqual(operate_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(operate_response.data["msg"], "runbook_resource_lock_conflict")

	def test_retry_rejects_when_no_pending_checkpoint_exists(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		run_state = store_payload["runs"][run_id]
		run_state["status"] = "failed"
		run_state["last_failure"] = {
			"code": "runbook_stage_failed",
			"message": "Checkpoint failed after max attempts.",
		}
		for stage in run_state.get("stages", []):
			stage["status"] = "completed"
			for checkpoint in stage.get("checkpoints", []):
				checkpoint["status"] = "completed"
				checkpoint["started_at_utc"] = "2026-02-17T00:00:00Z"
				checkpoint["completed_at_utc"] = "2026-02-17T00:00:05Z"
		runbook_views._save_store(store_payload)

		retry_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "retry"},
			format="json",
		)

		self.assertEqual(retry_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(retry_response.data["msg"], "runbook_retry_no_pending_checkpoint")

	def test_advance_persists_host_evidence_by_stage(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		advance_response = self.client.post(
			reverse("runbook-operate"),
			{"run_id": run_id, "action": "advance"},
			format="json",
		)
		self.assertEqual(advance_response.status_code, status.HTTP_200_OK)

		store_payload = runbook_views._load_store()
		run_state = store_payload["runs"][run_id]
		self.assertIn("prepare", run_state["host_stage_evidences"])
		self.assertGreaterEqual(len(run_state["host_stage_evidences"]["prepare"]), 1)
		evidence_row = run_state["host_stage_evidences"]["prepare"][0]
		self.assertEqual(evidence_row["org_id"], "org1")
		self.assertEqual(evidence_row["stage"], "prepare")
		self.assertIn("fingerprint_sha256", evidence_row)
		self.assertIn("host_mapping", evidence_row)
		self.assertIn("host_inventory", run_state)
		self.assertGreaterEqual(len(run_state["host_inventory"]), 1)
		self.assertIn("fingerprint_sha256", run_state["host_inventory"][0])

		host_telemetry_events = [
			event
			for event in run_state.get("events", [])
			if event.get("code") == "runbook_stage_host_telemetry"
		]
		self.assertGreaterEqual(len(host_telemetry_events), 1)
		first_telemetry_event = host_telemetry_events[0]
		self.assertEqual(first_telemetry_event.get("org_id"), "org1")
		self.assertEqual(first_telemetry_event.get("host_ref"), "host-org1-peer0")
		self.assertTrue(first_telemetry_event.get("fingerprint_sha256"))

	def test_verify_fails_when_minimum_history_evidence_is_missing(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		run_id = start_response.data["data"]["run"]["run_id"]

		for _ in range(8):
			operate_response = self.client.post(
				reverse("runbook-operate"),
				{"run_id": run_id, "action": "advance"},
				format="json",
			)
			self.assertEqual(operate_response.status_code, status.HTTP_200_OK)

		store_payload = runbook_views._load_store()
		store_payload["artifacts"][run_id] = [
			artifact
			for artifact in store_payload["artifacts"][run_id]
			if artifact.get("artifact_key") != "history:history.jsonl"
		]
		runbook_views._save_store(store_payload)

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_200_OK)
		audited_run = status_response.data["data"]["run"]
		self.assertEqual(audited_run["status"], "failed")
		self.assertEqual(
			audited_run["last_failure"]["code"],
			"runbook_verify_artifact_inconsistent",
		)

	def test_start_rejects_user_role_without_operational_authorization(self):
		self.user.role = "user"
		self.user.save(update_fields=["role"])

		response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
		self.assertEqual(response.data["msg"], "runbook_access_forbidden")

	def test_status_rejects_run_without_valid_change_context(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)
		run_id = start_response.data["data"]["run"]["run_id"]

		store_payload = runbook_views._load_store()
		store_payload["runs"][run_id]["change_id"] = ""
		runbook_views._save_store(store_payload)

		status_response = self.client.get(reverse("runbook-status", kwargs={"pk": run_id}))
		self.assertEqual(status_response.status_code, status.HTTP_409_CONFLICT)
		self.assertEqual(status_response.data["msg"], "runbook_change_context_required")

	def test_start_persists_access_audit_row(self):
		start_response = self.client.post(
			reverse("runbook-start"),
			self._start_payload(),
			format="json",
		)
		self.assertEqual(start_response.status_code, status.HTTP_201_CREATED)

		store_payload = runbook_views._load_store()
		audit_rows = store_payload.get("access_audit", [])
		self.assertGreaterEqual(len(audit_rows), 1)
		latest = audit_rows[-1]
		self.assertEqual(latest.get("action"), "start")
		self.assertEqual(latest.get("decision_code"), "runbook_access_allowed")
		self.assertTrue(latest.get("authorized"))


class BlueprintApiSecurityContractTests(APITestCase):
	def setUp(self):
		user_model = get_user_model()
		self.user = user_model.objects.create_user(
			username="blueprint-security-user",
			password="blueprint-security-password",
		)
		self.user.role = "admin"
		self.user.save(update_fields=["role"])
		self.client.force_authenticate(user=self.user)

	def _publish_payload(self, **overrides):
		payload = {
			"blueprint": {},
			"change_id": "cr-a1-7-8-001",
			"execution_context": "external-linux onboarding flow",
			"allow_migration": False,
		}
		payload.update(overrides)
		return payload

	def test_publish_rejects_non_operational_role(self):
		self.user.role = "user"
		self.user.save(update_fields=["role"])

		response = self.client.post(
			reverse("blueprint-publish"),
			self._publish_payload(),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
		self.assertEqual(response.data["msg"], "blueprint_publish_access_forbidden")

	def test_publish_requires_external_linux_scope_in_execution_context(self):
		response = self.client.post(
			reverse("blueprint-publish"),
			self._publish_payload(execution_context="marketplace quick publish"),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
		self.assertEqual(response.data["msg"], "blueprint_publish_scope_invalid")

	def test_lint_returns_official_contract_payload(self):
		response = self.client.post(
			reverse("blueprint-lint"),
			{
				"blueprint": {},
				"allow_migration": False,
				"change_id": "cr-a1-7-8-002",
				"execution_context": "external-linux onboarding flow",
			},
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_200_OK)
		self.assertEqual(response.data.get("status"), "successful")
		self.assertIn("data", response.data)
		self.assertIn("valid", response.data["data"])

	def test_publish_returns_a2_gate_context_fields(self):
		response = self.client.post(
			reverse("blueprint-publish"),
			self._publish_payload(
				blueprint={
					"network": {},
					"orgs": [],
					"channels": [],
					"nodes": [],
					"policies": [],
					"environment_profile": {},
				}
			),
			format="json",
		)

		self.assertEqual(response.status_code, status.HTTP_201_CREATED)
		self.assertEqual(response.data.get("status"), "successful")
		payload = response.data["data"]
		self.assertEqual(payload.get("backend_state"), "ready")
		self.assertEqual(payload.get("a2_2_artifacts_ready"), True)
		self.assertEqual(payload.get("a2_2_missing_artifacts"), [])
		self.assertGreaterEqual(len(payload.get("a2_2_minimum_artifacts", [])), 6)
		self.assertEqual(
			payload.get("manifest_fingerprint"),
			payload.get("fingerprint_sha256", "").lower(),
		)
		self.assertEqual(
			payload.get("source_blueprint_fingerprint"),
			payload.get("fingerprint_sha256", "").lower(),
		)
