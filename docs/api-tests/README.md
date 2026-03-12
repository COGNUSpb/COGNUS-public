# API Tests: Smoke And Operational Regression

This directory documents the gateway and ccapi endpoint validation used in the COGNUS operational cycle.

## Objective

Keep critical read and mutation flows working after:

- chaincode deploy or upgrade;
- gateway synchronization;
- operational changes in the environment.

## Main Script

- `scripts/api_smoke_test.py`

The script covers reference calls such as:

- `getSchema`, `getTx`, `getHeader`;
- `search`, `readAsset`, `readAssetHistory`;
- `CreateAsset`, `UpdateAsset`, `DeleteAsset`.

## Typical Execution

```bash
python3 scripts/api_smoke_test.py \
  --manifest marketplace/<channel>__<chaincode>.json \
  --base-url http://localhost:8085/api
```

Read-only validation, without mutations:

```bash
python3 scripts/api_smoke_test.py \
  --manifest marketplace/<channel>__<chaincode>.json \
  --base-url http://localhost:8085/api \
  --skip-mutations
```

## Recommended Practice

1. Run the suite after each chaincode commit or upgrade.
2. Run it before and after relevant infrastructure changes.
3. Record the execution output with timestamp and change context (`change-id`).
4. Keep a regression history per channel and chaincode.

## Typical Artifacts

Use this directory, when needed, to store:

- response dumps for comparison;
- environment-specific payloads;
- regression execution reports.

## Minimum Checklist Per Run

- tested channel and chaincode;
- chaincode version and sequence;
- validation status per endpoint;
- detailed failure message, when applicable;
- link to the related `change-id`.
