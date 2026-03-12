# Artifact Guide

This document defines how to reproduce the `public-main` snapshot of COGNUS from a fresh clone.

## Objective

The public artifact is intended to be:

- available as source code and documentation;
- functional as a local orchestrator baseline;
- sustainable as a curated, public-safe repository snapshot;
- reproducible without private credentials or pre-generated runtime state.

## Included In The Public Artifact

- `cello-v2/`: adapted control plane, dashboard, API Engine, and agents.
- `chaincode-gateway/`: gateway used by the current orchestration baseline.
- `automation/`: provisioning helpers, SSH executor, and local harness.
- `docs/`: curated public documentation, architecture, operations guides, and research sources.
- Root publication metadata: `LICENSE`, `NOTICE`, `CITATION.cff`, and this guide.

## Excluded From The Public Artifact

- `cello-v2/cello-storage/**`: generated ledgers, keys, runbook stores, chaincode packages, and local database state.
- Private admin bootstrap values, secrets, tokens, `.pem` files, and environment-specific credentials.
- Mirrored upstream docs and third-party PDFs bundled only for local study.
- Draft archives, zipped tool bundles, and Windows `Zone.Identifier` metadata.

## Environment

- Linux x86_64.
- Docker Engine 20.10+ and Docker Compose v2.
- `git`, `make`, Python 3, and Node.js LTS.

## Reproduction Protocol

### 1. Start From A Fresh Clone

```bash
git checkout public-main
chmod +x start_env.sh clean_env_zero.sh
./clean_env_zero.sh
./start_env.sh
```

The startup flow creates local runtime storage on demand. No pre-generated `cello-storage` content is required from the repository.

### 2. Verify The Core Services

```bash
docker compose -f cello-v2/bootup/docker-compose-files/docker-compose.dev.yml ps
curl -I http://localhost:8081
curl -I http://localhost:8080/api/v1/docs/
```

Expected result:

- `cello-postgres`, `cello-api-engine`, and `cello-dashboard` are up.
- `http://localhost:8081` responds for the dashboard.
- `http://localhost:8080/api/v1/docs/` responds for the API surface.

### 3. Verify The Public UI Flow

1. Open `http://localhost:8081`.
2. Register a new `operator`.
3. Log in.
4. Confirm access to the dashboard at `/overview`.
5. Open `Automação de provisão e lifecycle` and inspect the provisioning screens.

This public snapshot keeps the current implemented product flow, but it does not publish a public admin bootstrap path.

### 4. Optional SSH Reproduction Without A Real VM

For local SSH-oriented experiments, use the harness in `automation/harness/`:

```bash
cd automation/harness
docker compose up --build -d
```

Harness endpoints:

- SSH: `localhost:2222`
- User: `cognus`
- Password: `cognus`
- HTTP file server: `http://localhost:8080`

This harness exists to reproduce SSH executor behavior without depending on a private external Linux VM.

### 5. Real Provisioning Reproduction

To validate the full provisioning flow against a real target, provide your own:

- host/IP;
- SSH user;
- private key;
- Linux VM with Docker support or permission for Docker installation.

These credentials are intentionally external to the repository and are not part of the public artifact.

## Sustainability Notes

- `public-main` is a frozen publication branch and is not intended to track future `develop` changes.
- Public docs are source-first: architecture diagrams, LaTeX sources, operating guides, and structured evidence.
- Generated runtime data must remain local and disposable.

## Related Material

- [README.md](README.md)
- [docs/README.md](docs/README.md)
