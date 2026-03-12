# Marketplace And Data Mapping: Operational Workflow

This guide explains how COGNUS keeps its internal catalog synchronized with published chaincodes, preserving traceability and execution evidence.

Architectural alignment: Layer 3 (`Marketplace Pipeline`) + Layer 4 (operational catalog) + Layer 2 (evidence per change).

## Automated Flow Summary

1. **Dynamic discovery:** `scripts/marketplace_sync.py` (standalone or via `run_marketplace_pipeline.sh`) reads `chaincode-gateway/data/identities.json` to identify channels and inspects `cello-v2/cello-storage/chaincode/` to discover chaincodes. For each combination, it queries the gateway (`getSchema`, `getTx`, `getEvents`, `getHeader`) and generates:
   - versioned manifests in `marketplace/<channel>__<chaincode>.json`;
   - dynamic templates in `marketplace/<channel>__<chaincode>_templates.json`, grouping fields, relationships, transactions, and events ready for publication.
2. **Post-commit hook:** `chaincode-gateway/hooks/post_commit.sh` triggers synchronization after a chaincode commit.
3. **Optional validation:** `scripts/api_smoke_test.py` validates critical endpoints after synchronization. It is enabled by default in the pipeline.

## Governance And Traceability

For control-plane consistency, each relevant synchronization should be linked to a change identifier (`change-id`) and should record:

- execution metadata (start, end, status);
- updated artifacts;
- errors and fallbacks;
- generated manifest hash;
- published or deprecated artifact version.

This record can be persisted in the orchestrator's operational history.

## Manual Execution

Synchronization plus smoke tests:

```bash
./scripts/run_marketplace_pipeline.sh
```

Synchronization only:

```bash
RUN_SMOKE_TESTS=0 ./scripts/run_marketplace_pipeline.sh
```

The variables below constrain the scope or customize access:

- `FABRIC_CHANNEL`: restrict processed channels.
- `CHAINCODE_NAME`: restrict processed chaincodes.
- `MARKETPLACE_BASE_URL`: overrides the gateway URL (default `http://localhost:8085/api`).
- `MARKETPLACE_JWT`: JWT for protected environments.
- `MARKETPLACE_ORG_OVERRIDE`: forces the `?org=<org>` parameter.
- `MARKETPLACE_RELOAD_CMD`: command executed after changes, for example restarting a catalog service.

## Generated Artifacts

Each manifest includes, at minimum:

- `schemaVersion` and `schemaHash`;
- `assets`, `transactions`, and `events` exactly as returned by ccapi, which is useful for auditability;
- `header`, as returned by `getHeader` when available;
- `baseUrl`, the endpoint used for the queries.

The `<channel>__<chaincode>_templates.json` file aggregates:

- `assets[].fields`: per-property metadata (`required`, `isKey`, `transformation`, `writers`, and similar fields);
- `assets[].relationships`: inferred references (`->assetType`);
- `transactions[]`: HTTP method, parameters, `metaTx` and `readOnly` flags, and the complete argument list;
- `events[]`: event type, recipients, chained transaction, and additional attributes.

Because the file is recreated on each synchronization, avoid maintaining static copies outside `marketplace/`.

## Recommended Publication Rules

1. Every publication must record author and context (`change-id`).
2. Publication without schema validation should be blocked.
3. Synchronization failure should abort artifact promotion.
4. Rollback should record the cause and the restored version.

## Post-Commit Hook

For production-style environments, define the variables below on the container or host running the gateway:

```bash
export MARKETPLACE_BASE_URL=http://chaincode-gateway:8085/api
export MARKETPLACE_JWT="$(cat /var/run/ccapi/token)"          # optional
export MARKETPLACE_ORG_OVERRIDE=INF-UFGMSP                    # optional
export MARKETPLACE_RELOAD_CMD="docker compose -f /opt/cello/docker-compose.yaml restart marketplace"
```

The `chaincode-gateway/hooks/post_commit.sh` hook uses these values when invoking `scripts/marketplace_sync.py`. Without explicit variables, it falls back to `http://localhost:8085/api`.

## Smoke Tests (`scripts/api_smoke_test.py`)

The script can be executed manually, reusing the generated manifests:

```bash
python3 scripts/api_smoke_test.py \
  --manifest marketplace/stcs-channel__stcs-cc.json \
  --base-url http://localhost:8085/api
```

Parameters such as `--create-num-protocolo`, `--update-num-protocolo`, `--doctor-hash`, `--patient-hash`, and `--delete-asset-ref` may be adjusted to the available dataset. Use `--skip-mutations` when you want to validate reads only.

Failures report the endpoint and the returned error message, which simplifies regression diagnosis after deployment.
