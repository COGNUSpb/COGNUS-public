import hashlib
import tempfile
import shutil
import sys
import os
from pathlib import Path

# Ensure repo root is on sys.path so 'automation' package can be imported when tests
# are executed from arbitrary working directories.
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from automation.provisioning_ssh_executor import ProvisioningSshExecutor, SshCommandResponse
from automation.pipeline_state_store import PipelineStateStore
from automation.pipeline_contract import PipelineRun


def fake_command_runner(request):
    cmd = request.command or ""
    if "docker inspect" in cmd:
        # return a JSON-like mounts output
        mounts = '[{"Source":"/var/lib/docker/volumes/ids/_data","Destination":"/var/cognus/api-runtime/identities.json","Mode":"rw"}]'
        return SshCommandResponse(exit_code=0, stdout=mounts, stderr="", timed_out=False)
    if "docker logs" in cmd:
        logs = "INFO boot: identities loaded from /var/cognus/api-runtime/identities.json"
        return SshCommandResponse(exit_code=0, stdout=logs, stderr="", timed_out=False)
    return SshCommandResponse(exit_code=1, stdout="", stderr="unknown command", timed_out=False)


def test_collect_container_mount_evidence_creates_artifact(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    state_store = PipelineStateStore(root)

    executor = ProvisioningSshExecutor(state_store=state_store, command_runner=fake_command_runner)

    fp = hashlib.sha256(b"blueprint").hexdigest()
    run = PipelineRun.new(blueprint_fingerprint=fp, resolved_schema_version="v1", change_id="test-change")

    artifact_path = executor.collect_container_mount_evidence(
        run=run,
        host_id="host1",
        component_id="gateway",
        container_name="ccapi-go",
        artifact_name="gateway-volume-mount-report.txt",
        tail_lines=10,
    )

    assert artifact_path is not None
    p = Path(artifact_path)
    assert p.exists()
    content = p.read_text(encoding="utf-8")
    assert "Volume Mounts:" in content
    assert "Container Logs:" in content
    assert "identities loaded" in content or "identities" in content
