Acceptance harness (SSH + OpenSSL)

Purpose
- Provide a small local harness to run an SSH server with OpenSSL installed and an HTTP endpoint to serve certs/artifacts.

Quick start (local dev machine with Docker)

1. Build and run the harness:

```bash
cd automation/harness
docker-compose up --build -d
```

2. The harness exposes:
- SSH on localhost:2222 (user: cognus / pass: cognus)
- HTTP file server on http://localhost:8080 serving /srv (contains generated certs)

3. Stop and cleanup:

```bash
docker-compose down --volumes
```

Notes
- The harness is intentionally minimal: it generates a CA and a server certificate on first start and exposes them via HTTP for easy retrieval by tests.
- Use this environment to run local E2E flows against `automation/pipeline_provision.py` helpers by pointing `ProvisioningSshExecutor` to localhost:2222 and downloading certs from http://localhost:8080.
