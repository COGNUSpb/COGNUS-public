# Auto-Provisioning And Operational Automation

## Objective

Define the automation baseline for provisioning, lifecycle handling, and service integration in COGNUS, with emphasis on:

- idempotency;
- distributed execution per organization;
- change traceability;
- reduced manual intervention.

This guide is aligned with `docs/primary-work-article/low-level-architecture-v2-no-notes.puml` and describes the current public baseline rather than future work packages.

## Scope

This document covers:

1. environment and network bootstrap;
2. post-commit automation hooks;
3. synchronization of operational artifacts;
4. minimum execution evidence.

It does not attempt to describe the full future governance model or every planned operator workspace feature. The focus here is the implemented provisioning and lifecycle baseline.

## Standard Automation Flow

1. **Environment preparation**
   - `start_env.sh` validates prerequisites and starts the base stack.
   - The execution must be reentrant and must not duplicate resources.

2. **Component provisioning**
   - Network, organizations, nodes, and channels are created through the interface or API.
   - Agents perform local tasks per organization.
   - The execution should register a `change-id` and checkpoints per step.

3. **Chaincode lifecycle**
   - `package/install/approve/commit` flow.
   - A post-commit hook triggers gateway and catalog synchronization.
   - Critical operations may require a guardrail check when enabled.

4. **Runtime synchronization**
   - Refresh of `identities.json` and connection profiles.
   - Refresh of marketplace manifests and templates.
   - The update trail must be anchored in technical evidence.

5. **Operational validation**
   - Smoke tests run when enabled.
   - Evidence artifacts are generated for the execution.
   - Results are published by `change-id`.

## Mandatory Principles

### Idempotency

- Re-running the same flow must not create inconsistencies.
- Steps must expose checkpoints and explicit status transitions.

### Per-organization isolation

- Local operations must not require unnecessary cross-organization access.
- Credentials and sensitive artifacts must keep the minimum required scope.

### Evidence per change

- Structured logs and relevant artifacts must be associated with the execution.
- Failures must record step, cause, and suggested action.
- Minimum evidence: artifact hash, status, executor, and timestamp.

### Operational security

- Avoid insecure global permissions.
- Prefer minimum privileges and explicit ownership review on critical directories.

## Current Integration Points

- `chaincode-gateway/hooks/post_commit.sh`
- `scripts/marketplace_sync.py`
- `scripts/run_marketplace_pipeline.sh`
- `scripts/api_smoke_test.py`
- `docs/api-tests/README.md`

## Useful Commands

Start the environment:

```bash
./start_env.sh
```

Run the synchronization pipeline:

```bash
./scripts/run_marketplace_pipeline.sh
```

Run synchronization only:

```bash
RUN_SMOKE_TESTS=0 ./scripts/run_marketplace_pipeline.sh
```

## Recommended Minimum Evidence

For each relevant execution, record:

1. start and end timestamps;
2. affected components (`org/channel/chaincode`);
3. per-step status;
4. error logs when applicable;
5. generated or updated artifacts;
6. hashes or checksums of critical files;
7. the related `change-id`.
