# COGNUS

Consortium Orchestration & Governance Network Unified System for permissioned blockchains.

## Public Snapshot Scope

- Adapted `cello-v2` control plane with API Engine, dashboard, and agents.
- `chaincode-gateway` integration and provisioning automation.
- Current operator-facing dashboard and SSH-based provisioning/lifecycle flows.
- Public documentation for architecture, operations, artifact reproduction, and curated research sources.

## Implemented Baseline

- Public self-registration for `operator`.
- Dashboard and operational navigation for operator/auditor usage.
- Organization and provisioning flows already implemented in the current product baseline.
- SSH-backed provisioning and runtime inspection workflows.
- Runbook-oriented evidence, inventory, and operational state handling.

## Intentionally Excluded

- Hardcoded admin credentials and public bootstrap instructions.
- Generated ledgers, cryptographic material, VM-specific state, and `cello-storage` runtime contents.
- Upstream Cello documentation mirrored from the original project.
- Third-party PDFs, zipped tool bundles, Windows metadata files, and private working notes.

## Requirements

- Linux x86_64.
- Docker Engine 20.10+ with Docker Compose v2.
- `git`, `make`, Node.js LTS, and Python 3.
- Package repository access, or local image archives when running fully offline.

## Quick Start From A Fresh Clone

```bash
git checkout public-main
chmod +x start_env.sh clean_env_zero.sh
./clean_env_zero.sh
./start_env.sh
```

`start_env.sh` creates the local storage layout it needs under `cello-v2/cello-storage/`. These runtime artifacts are local-only and are intentionally excluded from the public branch.

## Minimal Smoke Verification

```bash
docker compose -f cello-v2/bootup/docker-compose-files/docker-compose.dev.yml ps
curl -I http://localhost:8081
curl -I http://localhost:8080/api/v1/docs/
```

Expected core services:

- `cello-postgres`
- `cello-api-engine`
- `cello-dashboard`

Manual functional check:

1. Open `http://localhost:8081`.
2. Register an `operator`.
3. Log in and confirm access to `/overview`.
4. Open `Automação de provisão e lifecycle`.
5. Supply your own Linux host credentials for real provisioning, or use the local SSH harness described in [ARTIFACT.md](ARTIFACT.md).

## Public Documentation

- [ARTIFACT.md](ARTIFACT.md)
- [docs/README.md](docs/README.md)
- [docs/new-orchestrator-overview.md](docs/new-orchestrator-overview.md)
- [docs/auto-provisioning.md](docs/auto-provisioning.md)
- [docs/marketplace-workflow.md](docs/marketplace-workflow.md)
- [docs/visual-identity.md](docs/visual-identity.md)
- [docs/primary-work-article/README.md](docs/primary-work-article/README.md)
- [docs/systematic-mapping/README.md](docs/systematic-mapping/README.md)

## License And Lineage

This snapshot is distributed under Apache-2.0. It contains original COGNUS code and documentation plus derivative work built on top of the Apache-licensed Hyperledger Cello codebase. See [LICENSE](LICENSE), [NOTICE](NOTICE), and [CITATION.cff](CITATION.cff).
