from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from .pipeline_contract import PIPELINE_STAGE_ORDER, PipelineRun, utc_now_iso
from .pipeline_state_store import PipelineStateStore, payload_sha256


STAGE_REPORT_FILENAMES = {
    "prepare": "prepare-report.json",
    "provision": "provision-report.json",
    "configure": "configure-report.json",
    "verify": "verify-report.json",
}


@dataclass(frozen=True)
class StageLogEvent:
    timestamp_utc: str
    change_id: str
    run_id: str
    stage: str
    host_ref: str
    org_id: str
    fingerprint_sha256: str
    level: str
    event_code: str
    message: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceConsolidationResult:
    decision: str
    decision_reasons: List[Dict[str, str]]
    evidence_valid: bool
    pipeline_report_path: str
    stage_reports_dir: str
    inventory_final_path: str
    history_path: str
    decision_trace_path: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line)
        handle.write("\n")


def _run_artifacts_dir(state_store: PipelineStateStore, run: PipelineRun) -> Path:
    path = state_store.artifacts_dir / run.run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json_if_exists(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def emit_structured_stage_log(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
    stage: str,
    host_ref: str,
    org_id: str,
    event_code: str,
    message: str,
    level: str = "info",
    details: Optional[Dict[str, Any]] = None,
) -> str:
    normalized_stage = str(stage).strip().lower()
    if normalized_stage not in PIPELINE_STAGE_ORDER:
        raise ValueError(f"Etapa inválida '{stage}' para log estruturado.")

    event = StageLogEvent(
        timestamp_utc=utc_now_iso(),
        change_id=run.change_id,
        run_id=run.run_id,
        stage=normalized_stage,
        host_ref=str(host_ref).strip() or "n/a",
        org_id=str(org_id).strip() or "n/a",
        fingerprint_sha256=run.blueprint_fingerprint,
        level=str(level).strip().lower() or "info",
        event_code=str(event_code).strip() or "unspecified_event",
        message=str(message).strip() or "",
        details=dict(details or {}),
    )

    run_dir = _run_artifacts_dir(state_store, run)
    stage_reports_dir = run_dir / "stage-reports"
    target = stage_reports_dir / f"{normalized_stage}-events.jsonl"
    _append_jsonl(target, event.to_dict())
    return str(target)


def _collect_stage_reports(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
) -> Dict[str, Dict[str, Any]]:
    reports: Dict[str, Dict[str, Any]] = {}
    for stage in PIPELINE_STAGE_ORDER:
        filename = STAGE_REPORT_FILENAMES[stage]
        source = state_store.stage_artifacts_dir(run.run_id, stage) / filename
        payload = _read_json_if_exists(source)
        if payload is not None:
            reports[stage] = payload
    return reports


def _decision_from_verify_report(verify_report: Optional[Dict[str, Any]]) -> Tuple[str, List[Dict[str, str]]]:
    if not verify_report:
        return "block", [{"code": "missing_verify_report", "message": "verify-report.json ausente ou inválido."}]

    issues = verify_report.get("issues", [])
    error_codes = sorted(
        {
            str(issue.get("code", "")).strip()
            for issue in (issues if isinstance(issues, list) else [])
            if isinstance(issue, dict) and str(issue.get("level", "")).strip().lower() == "error" and str(issue.get("code", "")).strip()
        }
    )

    verdict = str(verify_report.get("verdict", "")).strip().lower()
    if verdict in {"success", "partial"} and not error_codes:
        return "allow", [{"code": "no_error_issues", "message": "verify sem erros críticos."}]

    if not error_codes:
        error_codes = ["verify_verdict_not_allow"]
    return "block", [{"code": code, "message": f"Motivo técnico reproduzível: {code}"} for code in error_codes]


def _build_inventory_final(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
    stage_reports: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    configure_inventory_path = state_store.stage_artifacts_dir(run.run_id, "configure") / "technical-inventory.json"
    configure_inventory = _read_json_if_exists(configure_inventory_path)

    verify_inventory_path = state_store.stage_artifacts_dir(run.run_id, "verify") / "inventory-final.json"
    verify_inventory = _read_json_if_exists(verify_inventory_path)

    base_inventory = configure_inventory or verify_inventory
    if base_inventory is None:
        return None

    verify_report = stage_reports.get("verify", {})
    return {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "fingerprint_sha256": run.blueprint_fingerprint,
        "source": "configure-technical-inventory" if configure_inventory is not None else "verify-inventory-final",
        "verify_verdict": str(verify_report.get("verdict", "")).strip().lower(),
        "inventory": base_inventory,
        "generated_at": utc_now_iso(),
    }


def consolidate_run_evidence(
    *,
    state_store: PipelineStateStore,
    run: PipelineRun,
) -> EvidenceConsolidationResult:
    run_dir = _run_artifacts_dir(state_store, run)
    stage_reports_dir = run_dir / "stage-reports"
    stage_reports_dir.mkdir(parents=True, exist_ok=True)

    stage_reports = _collect_stage_reports(state_store=state_store, run=run)
    for stage, report in sorted(stage_reports.items()):
        filename = STAGE_REPORT_FILENAMES[stage]
        target = stage_reports_dir / filename
        _atomic_write_text(target, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))

    inventory_final = _build_inventory_final(
        state_store=state_store,
        run=run,
        stage_reports=stage_reports,
    )
    inventory_final_path = run_dir / "inventory-final.json"
    if inventory_final is not None:
        _atomic_write_text(
            inventory_final_path,
            json.dumps(inventory_final, ensure_ascii=False, indent=2, sort_keys=True),
        )

    verify_report = stage_reports.get("verify")
    decision, decision_reasons = _decision_from_verify_report(verify_report)
    verify_issues = verify_report.get("issues", []) if isinstance(verify_report, dict) else []
    error_codes = sorted(
        {
            str(item.get("code", "")).strip()
            for item in verify_issues
            if isinstance(item, dict)
            and str(item.get("level", "")).strip().lower() == "error"
            and str(item.get("code", "")).strip()
        }
    )
    warning_codes = sorted(
        {
            str(item.get("code", "")).strip()
            for item in verify_issues
            if isinstance(item, dict)
            and str(item.get("level", "")).strip().lower() == "warning"
            and str(item.get("code", "")).strip()
        }
    )
    hints = sorted(
        {
            str(item).strip()
            for item in ((verify_report or {}).get("recommendations") or [])
            if str(item).strip()
        }
    )

    required_artifacts = {
        "pipeline_report": str(run_dir / "pipeline-report.json"),
        "stage_reports": str(stage_reports_dir),
        "inventory_final": str(inventory_final_path),
        "history": str(run_dir / "history.jsonl"),
    }

    stage_reports_present = bool(list(stage_reports_dir.glob("*-report.json")))
    inventory_present = inventory_final is not None and inventory_final_path.exists()
    evidence_valid = bool(verify_report is not None and stage_reports_present and inventory_present)

    missing_codes: List[str] = []
    if verify_report is None:
        missing_codes.append("missing_verify_report")
    if not stage_reports_present:
        missing_codes.append("missing_stage_reports")
    if not inventory_present:
        missing_codes.append("missing_inventory_final")

    if missing_codes:
        decision = "block"
        existing_codes = {reason.get("code") for reason in decision_reasons}
        for code in missing_codes:
            if code in existing_codes:
                continue
            decision_reasons.append(
                {
                    "code": code,
                    "message": f"Motivo técnico reproduzível: {code}",
                }
            )

    timestamp_utc = utc_now_iso()
    pipeline_report_path = run_dir / "pipeline-report.json"
    pipeline_report = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "fingerprint_sha256": run.blueprint_fingerprint,
        "resolved_schema_version": run.resolved_schema_version,
        "valid": decision == "allow" and evidence_valid,
        "errors": error_codes,
        "warnings": warning_codes,
        "hints": hints,
        "decision": decision,
        "decision_reasons": sorted(decision_reasons, key=lambda item: (item.get("code", ""), item.get("message", ""))),
        "evidence_valid": evidence_valid,
        "required_artifacts": required_artifacts,
        "stage_statuses": {
            stage: run.stage_statuses.get(stage, "pending")
            for stage in PIPELINE_STAGE_ORDER
        },
        "stage_reports_hash": payload_sha256(stage_reports),
        "generated_at": timestamp_utc,
    }
    _atomic_write_text(
        pipeline_report_path,
        json.dumps(pipeline_report, ensure_ascii=False, indent=2, sort_keys=True),
    )

    history_path = run_dir / "history.jsonl"
    decision_trace_path = run_dir / "decision-trace.jsonl"

    history_entry = {
        "timestamp_utc": timestamp_utc,
        "run_id": run.run_id,
        "change_id": run.change_id,
        "fingerprint_sha256": run.blueprint_fingerprint,
        "decision": decision,
        "decision_reasons": pipeline_report["decision_reasons"],
        "evidence_valid": evidence_valid,
        "pipeline_report_file": str(pipeline_report_path),
        "inventory_final_file": str(inventory_final_path),
    }
    _append_jsonl(history_path, history_entry)

    decision_trace_entry = {
        "timestamp_utc": timestamp_utc,
        "run_id": run.run_id,
        "change_id": run.change_id,
        "stage": "verify",
        "decision": decision,
        "decision_reasons": pipeline_report["decision_reasons"],
        "fingerprint_sha256": run.blueprint_fingerprint,
        "evidence_valid": evidence_valid,
    }
    _append_jsonl(decision_trace_path, decision_trace_entry)

    return EvidenceConsolidationResult(
        decision=decision,
        decision_reasons=pipeline_report["decision_reasons"],
        evidence_valid=evidence_valid,
        pipeline_report_path=str(pipeline_report_path),
        stage_reports_dir=str(stage_reports_dir),
        inventory_final_path=str(inventory_final_path),
        history_path=str(history_path),
        decision_trace_path=str(decision_trace_path),
    )


def generate_a2_6_checklist(*, state_store: PipelineStateStore, run: PipelineRun) -> str:
    """Gerar `a2-6-checklist.json` a partir dos artefatos disponíveis no run.

    A intenção é produzir um checklist simples com evidências mínimas do WP A2.6.
    """
    run_dir = _run_artifacts_dir(state_store, run)
    prepare_dir = state_store.stage_artifacts_dir(run.run_id, "prepare")
    configure_dir = state_store.stage_artifacts_dir(run.run_id, "configure")
    verify_dir = state_store.stage_artifacts_dir(run.run_id, "verify")

    def _exists_in(*paths: Path) -> bool:
        for p in paths:
            try:
                if p.exists():
                    return True
            except Exception:
                continue
        return False

    gateway_inventory = prepare_dir / "gateway-inventory.json"
    identities_json = prepare_dir / "identities.json"
    connection_json = prepare_dir / "connection.json"
    wallet_artifact = next(prepare_dir.glob("*wallet*"), None)
    gateway_logs = any(prepare_dir.glob("gateway-logs-*.txt"))

    # rough chaincode evidence discovery: any artifact mentioning package_id or chaincode
    chaincode_evidence = False
    for p in run_dir.rglob("*"):
        name = p.name.lower()
        if "package_id" in name or "package-id" in name or "chaincode" in name:
            chaincode_evidence = True
            break

    # Expanded checklist with technical probes
    issues = []
    checks = {}
    checks["gateway_inventory_present"] = gateway_inventory.exists()
    checks["gateway_logs_present"] = bool(gateway_logs)
    checks["identities_json_present"] = _exists_in(identities_json, configure_dir / "identities.json")
    checks["connection_json_present"] = _exists_in(connection_json, configure_dir / "connection.json")
    checks["wallet_persisted_artifact"] = bool(wallet_artifact)
    checks["chaincode_lifecycle_evidence"] = bool(chaincode_evidence)
    checks["verify_report_present"] = _exists_in(run_dir / "stage-reports" / "verify-report.json", verify_dir / "verify-report.json")

    # Probe: Channel joined
    channel_joined = False
    channel_join_artifacts = list(run_dir.rglob("*channel-join-*.txt"))
    for art in channel_join_artifacts:
        try:
            content = art.read_text(encoding="utf-8")
            if "joined" in content.lower() or "Join successful" in content:
                channel_joined = True
                break
        except Exception:
            continue
    checks["channel_joined"] = channel_joined
    if not channel_joined:
        issues.append({"code": "channel_not_joined", "message": "Canal não foi joinado com sucesso."})

    # Probe: Chaincode committed per channel
    chaincode_committed = False
    chaincode_commit_artifacts = list(run_dir.rglob("*chaincode-commit-*.txt"))
    for art in chaincode_commit_artifacts:
        try:
            content = art.read_text(encoding="utf-8")
            if "committed" in content.lower() or "commit successful" in content:
                chaincode_committed = True
                break
        except Exception:
            continue
    checks["chaincode_committed"] = chaincode_committed
    if not chaincode_committed:
        issues.append({"code": "chaincode_not_committed", "message": "Chaincode não foi commitado no canal."})

    # Probe: Gateway endpoint HTTP 200
    http_200 = False
    endpoint_artifacts = list(run_dir.rglob("*gateway-endpoint-http.txt"))
    for art in endpoint_artifacts:
        try:
            content = art.read_text(encoding="utf-8")
            if "HTTP/1.1 200" in content or "200 OK" in content:
                http_200 = True
                break
        except Exception:
            continue
    checks["gateway_http_200"] = http_200
    if not http_200:
        issues.append({"code": "gateway_http_not_200", "message": "Gateway endpoint não respondeu HTTP 200."})

    # Probe: TLS effectiveness (SAN)
    tls_effective = False
    tls_artifacts = list(run_dir.rglob("*tls-san-check.txt"))
    for art in tls_artifacts:
        try:
            content = art.read_text(encoding="utf-8")
            if "DNS:" in content or "IP:" in content or "IP Address:" in content:
                tls_effective = True
                break
        except Exception:
            continue
    checks["tls_effective"] = tls_effective
    if not tls_effective:
        issues.append({"code": "tls_not_effective", "message": "TLS não está efetivo (SAN ausente)."})

    checklist = {
        "run_id": run.run_id,
        "change_id": run.change_id,
        "fingerprint_sha256": run.blueprint_fingerprint,
        "generated_at": utc_now_iso(),
        "checks": checks,
        "issues": issues,
    }

    # Persist the checklist as an official artifact for the provision stage when possible
    if isinstance(state_store, PipelineStateStore):
        try:
            path = state_store.write_json_artifact(run.run_id, "provision", "a2-6-checklist.json", checklist)
            return str(path)
        except Exception:
            # fallback: write directly to run_dir
            pass

    target = run_dir / "a2-6-checklist.json"
    _atomic_write_text(target, json.dumps(checklist, ensure_ascii=False, indent=2, sort_keys=True))
    return str(target)
    return str(target)
